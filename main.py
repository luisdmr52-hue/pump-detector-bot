#!/usr/bin/env python3
# main.py — Pump Detector (Contabo-optimized)
# - Hot-reloads config.yaml every 5s
# - AUTO_SYMBOLS=USDT support (fetch all active USDT spot pairs)
# - Batches many symbols across multiple websocket connections
# - Spread REST check only right before sending an alert (fewer calls)

import os, json, time, asyncio, traceback, random
from typing import Dict, Any, Tuple, List
from collections import defaultdict

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

# Manual list (used only if AUTO_SYMBOLS is empty)
SYMBOLS_MANUAL = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]

AUTO_SYMBOLS = os.getenv("AUTO_SYMBOLS", "").upper().strip()     # "USDT" to auto-load all USDT spot markets
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "300"))               # cap to be safe
EXCLUDE_SYMBOLS = {s.strip().upper() for s in os.getenv("EXCLUDE_SYMBOLS", "BTCUPUSDT,BTCDOWNUSDT").split(",") if s.strip()}

CONFIG_PATH = os.getenv("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.yaml"))
CONFIG: Dict[str, Any] = {}
TIMEFRAMES: List[str] = []
CONFIG_MTIME = 0.0

# Websocket batching (streams per connection safety margin)
# Each symbol * timeframes = stream count. Keep per-WS under ~900 streams.
WS_MAX_STREAMS = 900

BINANCE_REST = "https://api.binance.com"
ALT_HOSTS = ["api1.binance.com", "api2.binance.com", "api3.binance.com", "api4.binance.com", "api-gcp.binance.com"]
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"

# =========================
# ------ LOAD CONFIG ------
# =========================
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
    for tf, v in cfg["timeframes"].items():
        v["delta_pct"] = float(v.get("delta_pct", 2.0))
        v["vol_mult"] = float(v.get("vol_mult", 2.5))
        v["min_notional"] = float(v.get("min_notional", 100000))
        v["ema_len"] = int(v.get("ema_len", 20))
        v["cooldown_min"] = int(v.get("cooldown_min", 20))
    for _, t in cfg["tiers"].items():
        t["vol_mult_adj"]  = float(t.get("vol_mult_adj", 0.0))
        t["min_notional"]  = float(t.get("min_notional", 0.0))
        t["delta_pct_adj"] = float(t.get("delta_pct_adj", 0.0))
    return cfg

CONFIG = load_config(CONFIG_PATH)
TIMEFRAMES = list(CONFIG["timeframes"].keys())  # e.g. ['15m','30m','1h']

# =========================
# ---- UTILS & HELPERS ----
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

def chunked(seq: List[str], n: int) -> List[List[str]]:
    return [seq[i:i+n] for i in range(0, len(seq), n)]

# =========================
# ----- AUTO SYMBOLS ------
# =========================
def fetch_usdt_symbols() -> List[str]:
    """
    One-shot fetch of all active USDT spot pairs.
    Filters out UP/DOWN tokens and any in EXCLUDE_SYMBOLS.
    """
    try:
        r = requests.get(f"{BINANCE_REST}/api/v3/exchangeInfo", timeout=10)
        r.raise_for_status()
        info = r.json()
        out = []
        for s in info.get("symbols", []):
            sym = s.get("symbol", "")
            if not sym.endswith("USDT"): 
                continue
            if sym in EXCLUDE_SYMBOLS:
                continue
            # skip leveraged tokens etc.
            if sym.endswith("UPUSDT") or sym.endswith("DOWNUSDT") or "BULL" in sym or "BEAR" in sym:
                continue
            status_ok = s.get("status") == "TRADING"
            perm = s.get("permissions") or []
            spot_ok = ("SPOT" in perm) or bool(s.get("isSpotTradingAllowed", False))
            if status_ok and spot_ok:
                out.append(sym)
        # Dedup, sort, cap
        out = sorted(set(out))
        if MAX_SYMBOLS > 0:
            out = out[:MAX_SYMBOLS]
        return out
    except Exception as e:
        print("[AUTO] fetch_usdt_symbols error:", e)
        return []

def resolve_symbols() -> List[str]:
    if AUTO_SYMBOLS == "USDT":
        autos = fetch_usdt_symbols()
        if autos:
            print(f"[AUTO] Using {len(autos)} USDT symbols (capped at {MAX_SYMBOLS}).")
            return autos
        print("[AUTO] Falling back to manual SYMBOLS (auto failed).")
    return SYMBOLS_MANUAL

SYMBOLS = resolve_symbols()

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
# ---- FILTERS / LOGIC ----
# =========================
def body_filter(open_: float, close: float, high: float, low: float, min_body_ratio: float) -> bool:
    rng = max(1e-12, high - low)
    body = max(0.0, close - open_)
    body_ratio = (body / rng) if rng > 0 else 0.0
    return body_ratio >= min_body_ratio

def handle_closed_candle(symbol: str, tf: str, k: Dict[str, Any]) -> None:
    cfg = merged_thresholds(symbol, tf)
    state = STATE[symbol].setdefault(tf, TFState(cfg["ema_len"]))

    open_ = float(k["o"]); close = float(k["c"])
    high = float(k["h"]);  low  = float(k["l"])
    quote_vol = float(k.get("q", 0.0))
    trades = int(k.get("n", 0))

    prev_close = state.last_close if state.last_close > 0 else open_
    delta = pct(close, prev_close)

    # volume EMA
    state.vol_ema = ema_update(state.vol_ema, quote_vol, state.ema_len)
    vol_mult = (quote_vol / state.vol_ema) if state.vol_ema > 0 else 1.0
    notional = quote_vol

    # cheap body filter first (no REST here)
    if not body_filter(open_, close, high, low, float(CONFIG["quality_filters"]["min_body_ratio"])):
        state.debounce_active = False
        state.debounce_start = 0.0
        state.last_close, state.last_open = close, open_
        state.last_high, state.last_low = high, low
        state.last_quote_vol = quote_vol
        return

    basic_pass = (delta >= cfg["delta_pct"] and vol_mult >= cfg["vol_mult"] and notional >= cfg["min_notional"])
    now = now_ts()

    # cooldown window
    if now < state.cooldown_until:
        state.debounce_active = False
        state.debounce_start = 0.0
        state.last_close, state.last_open = close, open_
        state.last_high, state.last_low = high, low
        state.last_quote_vol = quote_vol
        return

    db_sec = int(CONFIG["quality_filters"]["debounce_sec"])
    if basic_pass:
        if not state.debounce_active:
            state.debounce_active = True
            state.debounce_start = now
        else:
            if (now - state.debounce_start) >= db_sec:
                # REST spread check only right before alert (saves calls)
                spread = get_orderbook_spread_pct(symbol)
                max_spread = float(CONFIG["quality_filters"]["max_spread_pct"])
                if spread <= max_spread:
                    msg = (
                        f"🚀 <b>Pump detected</b>\n"
                        f"Symbol: <b>{symbol}</b>\n"
                        f"Timeframe: <b>{tf}</b>\n"
                        f"Price: <code>{close:.6g}</code> ({delta:.2f}%)\n"
                        f"Volume: <code>{quote_vol:,.0f}</code> USDT ({vol_mult:.2f}× EMA)\n"
                        f"Trades: <code>{trades}</code>\n"
                        f"Spread: {spread:.2f}% (<= {max_spread:.2f}%)\n"
                        f"Req → Notional/Δ%/×: {cfg['min_notional']:,.0f} / {cfg['delta_pct']:.2f}% / {cfg['vol_mult']:.2f}×"
                    )
                    tg_send(msg)
                    state.cooldown_until = now + (cfg["cooldown_min"] * 60)
                else:
                    print(f"[FILTER] {symbol} {tf} spread {spread:.2f}% > {max_spread:.2f}%")
                state.debounce_active = False
                state.debounce_start = 0.0
    else:
        state.debounce_active = False
        state.debounce_start = 0.0

    state.last_close, state.last_open = close, open_
    state.last_high, state.last_low = high, low
    state.last_quote_vol = quote_vol

# =========================
# ---- WEBSOCKET LOOPS ----
# =========================
def build_stream_url(symbols: List[str], tfs: List[str]) -> str:
    parts = []
    for s in symbols:
        s_l = s.lower()
        for tf in tfs:
            parts.append(f"{s_l}@kline_{tf}")
    return f"{BINANCE_WS_BASE}?streams=" + "/".join(parts)

async def ws_consumer(symbols: List[str], tfs: List[str], tag: str):
    url = build_stream_url(symbols, tfs)
    backoff = 1
    while True:
        try:
            print(f"[WS-{tag}] Connecting {len(symbols)} syms × {len(tfs)} tfs → {len(symbols)*len(tfs)} streams")
            async with websockets.connect(url, ping_interval=15, ping_timeout=10, max_queue=4096) as ws:
                print(f"[WS-{tag}] Connected.")
                backoff = 1
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        data = msg.get("data")
                        if not data or data.get("e") != "kline":
                            continue
                        k = data.get("k", {})
                        if not k or not bool(k.get("x", False)):  # closed candle only
                            continue
                        symbol = data.get("s"); tf = k.get("i")
                        if symbol not in symbols or tf not in tfs:
                            continue
                        candle = {"o": k["o"], "c": k["c"], "h": k["h"], "l": k["l"], "q": k.get("q","0"), "n": k.get("n",0)}
                        handle_closed_candle(symbol, tf, candle)
                    except Exception as e:
                        print(f"[WS-{tag}] Msg error:", e)
                        traceback.print_exc()
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[WS-{tag}] Conn error:", e)
            traceback.print_exc()
            await asyncio.sleep(min(backoff, 60))
            backoff = min(backoff * 2, 60)

def plan_batches(symbols: List[str], tfs: List[str]) -> List[List[str]]:
    per_ws_max_symbols = max(1, WS_MAX_STREAMS // max(1, len(tfs)))
    batches = chunked(symbols, per_ws_max_symbols)
    return batches

# =========================
# ---- HOT-RELOAD WATCH ---
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
                    print("[HOT] Timeframes changed in file; restart required.")
                safe_apply_config(new_cfg)
        except Exception as e:
            print("[HOT] watcher error:", e)

# =========================
# --------- MAIN ----------
# =========================
async def main():
    if not SYMBOLS:
        print("[ERR] No symbols resolved from .env / AUTO_SYMBOLS."); return
    if not TG_TOKEN or not TG_CHAT_ID:
        print("[WARN] Telegram not set; alerts will be printed only.")

    # init TF state per symbol
    for s in SYMBOLS:
        for tf in TIMEFRAMES:
            cfg = merged_thresholds(s, tf)
            STATE[s][tf] = TFState(int(cfg["ema_len"]))

    # plan websocket batches
    batches = plan_batches(SYMBOLS, TIMEFRAMES)
    tasks = []
    for idx, group in enumerate(batches, start=1):
        tag = f"{idx}/{len(batches)}"
        tasks.append(asyncio.create_task(ws_consumer(group, TIMEFRAMES, tag)))
    tasks.append(asyncio.create_task(config_watcher(CONFIG_PATH, interval=5)))

    print(f"[BOOT] Symbols: {len(SYMBOLS)}  | Timeframes: {TIMEFRAMES}  | WS connections: {len(batches)}")
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting…")
