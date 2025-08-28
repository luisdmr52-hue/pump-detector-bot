#!/usr/bin/env python3
# main.py — Pump Detector (Contabo-optimized) + simple hot-reload of config.yaml
# Reads thresholds/tiers from config.yaml; secrets & symbols from .env

import os, json, time, asyncio, traceback
from typing import Dict, Any, Tuple
from collections import defaultdict

# ---- Faster loop on Linux (optional) ----
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:
    pass

import requests
import websockets
import yaml
from dotenv import load_dotenv
load_dotenv()

# =========================
# ----- ENV & CONFIG ------
# =========================
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Symbols list in .env
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]

CONFIG_PATH = os.getenv("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.yaml"))
CONFIG: Dict[str, Any] = {}
TIMEFRAMES = []
CONFIG_MTIME = 0.0  # last modified time we saw

def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if "timeframes" not in cfg or not isinstance(cfg["timeframes"], dict):
        raise ValueError("config.yaml must define 'timeframes' (mapping)")
    if "tiers" not in cfg or not isinstance(cfg["tiers"], dict):
        raise ValueError("config.yaml must define 'tiers' (mapping)")
    if "symbols" not in cfg or not isinstance(cfg["symbols"], dict):
        cfg["symbols"] = {}
    q = cfg.get("quality_filters", {})
    cfg["quality_filters"] = {
        "min_body_ratio": float(q.get("min_body_ratio", 0.5)),
        "max_spread_pct": float(q.get("max_spread_pct", 0.25)),
        "debounce_sec": int(q.get("debounce_sec", 20)),
    }
    # Normalize numeric fields inside timeframes
    for tf, v in cfg["timeframes"].items():
        v["delta_pct"] = float(v.get("delta_pct", 2.0))
        v["vol_mult"] = float(v.get("vol_mult", 2.5))
        v["min_notional"] = float(v.get("min_notional", 100000))
        v["ema_len"] = int(v.get("ema_len", 20))
        v["cooldown_min"] = int(v.get("cooldown_min", 20))
    # Normalize tiers
    for name, t in cfg["tiers"].items():
        t["vol_mult_adj"]  = float(t.get("vol_mult_adj", 0.0))
        t["min_notional"]  = float(t.get("min_notional", 0.0))
        t["delta_pct_adj"] = float(t.get("delta_pct_adj", 0.0))
    return cfg

# Load initial config
CONFIG = load_config(CONFIG_PATH)
TIMEFRAMES = list(CONFIG["timeframes"].keys())  # e.g. ["15m","30m","1h"]

# =========================
# ---- SMALL UTILITIES ----
# =========================
def now_ts() -> float: return time.time()

def pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a - b) / b * 100.0

def ema_update(prev_ema: float, value: float, length: int) -> float:
    if prev_ema <= 0: return value
    k = 2 / (length + 1)
    return value * k + prev_ema * (1 - k)

def tier_for_symbol(symbol: str) -> str:
    return str(CONFIG["symbols"].get(symbol.upper(), {}).get("tier", "B")).upper()

def merged_thresholds(symbol: str, tf: str) -> Dict[str, float]:
    base = CONFIG["timeframes"][tf].copy()
    tier_cfg = CONFIG["tiers"].get(tier_for_symbol(symbol), {})
    base["vol_mult"] = max(0.1, float(base["vol_mult"]) + float(tier_cfg.get("vol_mult_adj", 0.0)))
    base["min_notional"] = max(float(base["min_notional"]), float(tier_cfg.get("min_notional", base["min_notional"])))
    base["delta_pct"] = float(base["delta_pct"]) + float(tier_cfg.get("delta_pct_adj", 0.0))
    base["ema_len"] = int(base.get("ema_len", 20))
    base["cooldown_min"] = int(base.get("cooldown_min", 20))
    return base

# =========================
# ---- STATE CONTAINERS ---
# =========================
class TFState:
    def __init__(self, ema_len: int):
        self.ema_len = ema_len
        self.last_close = 0.0
        self.last_open = 0.0
        self.last_high = 0.0
        self.last_low = 0.0
        self.last_quote_vol = 0.0
        self.vol_ema = 0.0
        self.cooldown_until = 0.0
        self.debounce_start = 0.0
        self.debounce_active = False

STATE: Dict[str, Dict[str, TFState]] = defaultdict(dict)

# =========================
# ---- TELEGRAM ALERT -----
# =========================
def tg_send(msg: str) -> None:
    if not TG_TOKEN or not TG_CHAT_ID:
        print("[WARN] Telegram not configured; skipping alert.")
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = {"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, data=data, timeout=10)
        if r.status_code != 200:
            print("[WARN] Telegram error:", r.text)
    except Exception as e:
        print("[WARN] Telegram exception:", e)

# =========================
# ---- ORDERBOOK SPREAD ---
# =========================
BINANCE_REST = "https://api.binance.com"
ALT_HOSTS = ["api1.binance.com", "api2.binance.com", "api3.binance.com", "api4.binance.com", "api-gcp.binance.com"]

def get_orderbook_spread_pct(symbol: str) -> float:
    hosts = [BINANCE_REST.replace("https://", "").split("/")[0]] + ALT_HOSTS
    for h in hosts:
        try:
            r = requests.get(f"https://{h}/api/v3/depth?symbol={symbol}&limit=5", timeout=3)
            if r.status_code != 200: 
                continue
            ob = r.json()
            best_bid = float(ob["bids"][0][0]) if ob.get("bids") else 0.0
            best_ask = float(ob["asks"][0][0]) if ob.get("asks") else 0.0
            if best_bid <= 0 or best_ask <= 0: 
                return 999.0
            mid = (best_bid + best_ask) / 2.0
            return (best_ask - best_bid) / mid * 100.0
        except Exception:
            continue
    return 999.0

# =========================
# ---- QUALITY FILTERS ----
# =========================
def quality_filters(symbol: str, tf: str, open_: float, close: float, high: float, low: float) -> Tuple[bool, str]:
    q = CONFIG["quality_filters"]
    min_body_ratio = float(q["min_body_ratio"])
    max_spread_pct = float(q["max_spread_pct"])

    rng = max(1e-12, high - low)
    body = max(0.0, close - open_)
    body_ratio = (body / rng) if rng > 0 else 0.0
    if body_ratio < min_body_ratio:
        return False, f"body {body_ratio:.2f}<{min_body_ratio:.2f}"

    spread = get_orderbook_spread_pct(symbol)
    if spread > max_spread_pct:
        return False, f"spread {spread:.2f}%>{max_spread_pct:.2f}%"

    return True, "ok"

# =========================
# ---- CANDLE HANDLER -----
# =========================
def handle_closed_candle(symbol: str, tf: str, k: Dict[str, Any]) -> None:
    cfg = merged_thresholds(symbol, tf)
    state = STATE[symbol].setdefault(tf, TFState(cfg["ema_len"]))

    open_ = float(k["o"]); close = float(k["c"])
    high = float(k["h"]);  low  = float(k["l"])
    quote_vol = float(k.get("q", 0.0))
    trades = int(k.get("n", 0))

    prev_close = state.last_close if state.last_close > 0 else open_
    delta = pct(close, prev_close)

    # EMA of quote volume
    state.vol_ema = ema_update(state.vol_ema, quote_vol, state.ema_len)
    vol_mult = (quote_vol / state.vol_ema) if state.vol_ema > 0 else 1.0
    notional = quote_vol

    ok, _ = quality_filters(symbol, tf, open_, close, high, low)
    if not ok:
        state.debounce_active = False
        state.debounce_start = 0.0
        state.last_close, state.last_open = close, open_
        state.last_high, state.last_low = high, low
        state.last_quote_vol = quote_vol
        return

    passed = (delta >= cfg["delta_pct"] and vol_mult >= cfg["vol_mult"] and notional >= cfg["min_notional"])
    now = now_ts()

    if now < state.cooldown_until:
        state.debounce_active = False
        state.debounce_start = 0.0
        state.last_close, state.last_open = close, open_
        state.last_high, state.last_low = high, low
        state.last_quote_vol = quote_vol
        return

    db_sec = int(CONFIG["quality_filters"]["debounce_sec"])
    if passed:
        if not state.debounce_active:
            state.debounce_active = True
            state.debounce_start = now
        else:
            if (now - state.debounce_start) >= db_sec:
                msg = (
                    f"🚀 <b>Pump detected</b>\n"
                    f"Symbol: <b>{symbol}</b>\n"
                    f"Timeframe: <b>{tf}</b>\n"
                    f"Price: <code>{close:.6g}</code> ({delta:.2f}%)\n"
                    f"Volume: <code>{quote_vol:,.0f}</code> USDT ({vol_mult:.2f}× EMA)\n"
                    f"Trades: <code>{trades}</code>\n"
                    f"Body/Spread filters passed\n"
                    f"Req → Notional/Δ%/×: {cfg['min_notional']:,.0f} / {cfg['delta_pct']:.2f}% / {cfg['vol_mult']:.2f}×"
                )
                tg_send(msg)
                state.cooldown_until = now + (cfg["cooldown_min"] * 60)
                state.debounce_active = False
                state.debounce_start = 0.0
    else:
        state.debounce_active = False
        state.debounce_start = 0.0

    state.last_close, state.last_open = close, open_
    state.last_high, state.last_low = high, low
    state.last_quote_vol = quote_vol

# =========================
# ---- WEBSOCKET LOOP -----
# =========================
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

def build_stream_url(symbols, tfs):
    parts = []
    for s in symbols:
        s_l = s.lower()
        for tf in tfs:
            parts.append(f"{s_l}@kline_{tf}")
    return f"{BINANCE_WS_BASE}?streams=" + "/".join(parts)

async def ws_consumer():
    url = build_stream_url(SYMBOLS, TIMEFRAMES)
    backoff = 1
    while True:
        try:
            print(f"[WS] Connecting: {url}")
            async with websockets.connect(url, ping_interval=15, ping_timeout=10, max_queue=2048) as ws:
                print("[WS] Connected.")
                backoff = 1
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data")
                        if not data or data.get("e") != "kline":
                            continue
                        k = data.get("k", {})
                        if not k or not bool(k.get("x", False)):  # only closed candle
                            continue
                        symbol = data.get("s"); tf = k.get("i")
                        if symbol not in SYMBOLS or tf not in TIMEFRAMES:
                            continue
                        candle = {"o": k["o"], "c": k["c"], "h": k["h"], "l": k["l"], "q": k.get("q","0"), "n": k.get("n",0)}
                        handle_closed_candle(symbol, tf, candle)
                    except Exception as e:
                        print("[WS] Msg error:", e)
                        traceback.print_exc()
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("[WS] Connection error:", e)
            traceback.print_exc()
            await asyncio.sleep(min(backoff, 60))
            backoff = min(backoff * 2, 60)

# =========================
# ---- SIMPLE HOT-RELOAD ---
# =========================
def safe_apply_config(new_cfg: Dict[str, Any]) -> None:
    global CONFIG
    old = CONFIG
    merged = dict(old)
    tf_updates = {}
    for tf in TIMEFRAMES:
        tf_updates[tf] = {**old["timeframes"][tf], **new_cfg["timeframes"].get(tf, {})}
    merged["timeframes"] = tf_updates
    merged["tiers"] = new_cfg.get("tiers", old["tiers"])
    merged["symbols"] = new_cfg.get("symbols", old["symbols"])
    merged["quality_filters"] = new_cfg.get("quality_filters", old["quality_filters"])
    CONFIG = merged
    print("[HOT] config.yaml reloaded (safe fields).")

async def config_watcher(path: str, interval: int = 5):
    global CONFIG_MTIME
    try:
        CONFIG_MTIME = os.path.getmtime(path)
    except FileNotFoundError:
        print("[HOT] config.yaml not found at start.")
        return
    while True:
        await asyncio.sleep(interval)
        try:
            m = os.path.getmtime(path)
            if m != CONFIG_MTIME:
                CONFIG_MTIME = m
                new_cfg = load_config(path)
                new_tfs = list(new_cfg.get("timeframes", {}).keys())
                if set(new_tfs) != set(TIMEFRAMES):
                    print("[HOT] Timeframes changed; restart required.")
                safe_apply_config(new_cfg)
        except Exception as e:
            print("[HOT] watcher error:", e)

# =========================
# --------- MAIN ----------
# =========================
async def main():
    if not SYMBOLS:
        print("[ERR] No SYMBOLS configured in .env"); return
    if not TG_TOKEN or not TG_CHAT_ID:
        print("[WARN] Telegram not set; alerts will be printed only.")

    for s in SYMBOLS:
        for tf in TIMEFRAMES:
            cfg = merged_thresholds(s, tf)
            STATE[s][tf] = TFState(int(cfg["ema_len"]))

    await asyncio.gather(
        ws_consumer(),
        config_watcher(CONFIG_PATH, interval=5)
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting…")
