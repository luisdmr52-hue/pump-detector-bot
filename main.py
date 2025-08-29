#!/usr/bin/env python3
# main.py — Pump Detector (Contabo-optimized v3.4)
# Adds: Buy-imbalance, Orderbook imbalance+liquidity, Higher-TF alignment, Grouped alerts
# Keeps: /healthz, /readyz, /metrics, /debugz, symbol auto-refresh, file logging, watchdog, confidence, regime

import os, json, time, asyncio, traceback, random, signal, logging
from logging.handlers import TimedRotatingFileHandler
from typing import Dict, Any, Tuple, List, Optional
from collections import defaultdict, deque

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

SYMBOLS_MANUAL = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
AUTO_SYMBOLS = os.getenv("AUTO_SYMBOLS", "").upper().strip()
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "300"))
EXCLUDE_SYMBOLS = {s.strip().upper() for s in os.getenv("EXCLUDE_SYMBOLS", "BTCUPUSDT,BTCDOWNUSDT").split(",") if s.strip()}

CONFIG_PATH = os.getenv("CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.yaml"))

CROSS_TF_DEDUP_SEC = int(os.getenv("CROSS_TF_DEDUP_SEC", "20"))
WS_MAX_STREAMS = 900

HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
HEALTH_STALE_SEC = int(os.getenv("HEALTH_STALE_SEC", "120"))

SYMBOL_REFRESH_HOURS = int(os.getenv("SYMBOL_REFRESH_HOURS", "6"))

WATCHDOG_RESTART_SEC = int(os.getenv("WATCHDOG_RESTART_SEC", "600"))
WATCHDOG_CHECK_SEC = int(os.getenv("WATCHDOG_CHECK_SEC", "30"))

MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "0"))
REGIME_FILTER = {x.strip().upper() for x in os.getenv("REGIME_FILTER", "").split(",") if x.strip()}
REGIME_REFRESH_MIN = int(os.getenv("REGIME_REFRESH_MIN", "5"))
REGIME_VOL_QUIET = float(os.getenv("REGIME_VOL_QUIET", "0.6"))
REGIME_VOL_HOT   = float(os.getenv("REGIME_VOL_HOT", "1.8"))
REGIME_SYMBOL = os.getenv("REGIME_SYMBOL", "BTCUSDT")
REGIME_INTERVAL = os.getenv("REGIME_INTERVAL", "1h")
REGIME_LOOKBACK = int(os.getenv("REGIME_LOOKBACK", "200"))

# NEW: Buy-imbalance (taker-buy %)
MIN_TAKER_BUY_RATIO = float(os.getenv("MIN_TAKER_BUY_RATIO", "0.0"))

# NEW: Orderbook depth/imbalance
OB_TOP_LEVELS = int(os.getenv("OB_TOP_LEVELS", "10"))
OB_MIN_DEPTH_USDT = float(os.getenv("OB_MIN_DEPTH_USDT", "0"))
OB_MIN_IMBALANCE = float(os.getenv("OB_MIN_IMBALANCE", "0.0"))

# NEW: Higher-TF alignment
HTF_FILTER = os.getenv("HTF_FILTER", "").strip()   # e.g. "1h,EMA20,ABOVE"
HTF_EMA_LEN = int(os.getenv("HTF_EMA_LEN", "20"))

# NEW: Grouped alerts
GROUP_WINDOW_SEC = int(os.getenv("GROUP_WINDOW_SEC", "0"))  # 0 = off
GROUP_MAX = int(os.getenv("GROUP_MAX", "8"))

# Logging
LOG_DIR = os.getenv("LOG_DIR", os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("pumpbot")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh = TimedRotatingFileHandler(os.path.join(LOG_DIR, "pumpbot.log"), when="midnight", backupCount=7, encoding="utf-8")
fh.setFormatter(_fmt)
ch = logging.StreamHandler(); ch.setFormatter(_fmt)
if not logger.handlers:
    logger.addHandler(fh); logger.addHandler(ch)

# Binance endpoints
BINANCE_REST = "https://api.binance.com"
ALT_HOSTS = ["api1.binance.com", "api2.binance.com", "api3.binance.com", "api4.binance.com", "api-gcp.binance.com"]
BINANCE_WS_BASES = ["wss://stream.binance.com:9443/stream", "wss://stream.binance.com:443/stream"]

CONFIG: Dict[str, Any] = {}
TIMEFRAMES: List[str] = []
CONFIG_MTIME = 0.0

# =========================
# -------- Helpers --------
# =========================
def log(level: str, msg: str) -> None:
    level_up = level.upper()
    if level_up in ("ERR", "ERROR"): logger.error(msg)
    elif level_up in ("WARN", "WARNING"): logger.warning(msg)
    else: logger.info(f"[{level}] {msg}")

def now_ts() -> float: return time.time()

def pct(a: float, b: float) -> float:
    if b == 0: return 0.0
    return (a - b) / b * 100.0

def ema_update(prev_ema: float, value: float, length: int) -> float:
    if prev_ema <= 0: return value
    k = 2 / (length + 1)
    return value * k + prev_ema * (1 - k)

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
TIMEFRAMES = list(CONFIG["timeframes"].keys())

# =========================
# ---- SYMBOL TIERS -------
# =========================
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
def _fetch_usdt_symbols_blocking() -> List[str]:
    try:
        r = requests.get(f"{BINANCE_REST}/api/v3/exchangeInfo", timeout=10)
        r.raise_for_status()
        info = r.json()
        out = []
        for s in info.get("symbols", []):
            sym = s.get("symbol", "")
            if not sym.endswith("USDT"): continue
            if sym in EXCLUDE_SYMBOLS: continue
            if sym.endswith("UPUSDT") or sym.endswith("DOWNUSDT") or "BULL" in sym or "BEAR" in sym: continue
            status_ok = s.get("status") == "TRADING"
            perm = s.get("permissions") or []
            spot_ok = ("SPOT" in perm) or bool(s.get("isSpotTradingAllowed", False))
            if status_ok and spot_ok: out.append(sym)
        out = sorted(set(out))
        if MAX_SYMBOLS > 0: out = out[:MAX_SYMBOLS]
        return out
    except Exception as e:
        log("AUTO", f"fetch_usdt_symbols error: {e}")
        return []

async def fetch_usdt_symbols() -> List[str]:
    return await asyncio.to_thread(_fetch_usdt_symbols_blocking)

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

LAST_ALERT_TS: Dict[str, float] = defaultdict(float)

CANDLES_TOTAL = 0
ALERTS_TOTAL = 0
LAST_CANDLE_TS_GLOBAL = 0.0

CURRENT_REGIME = "UNKNOWN"
REGIME_CODE_MAP = {"UNKNOWN": -1, "QUIET": 0, "NORMAL": 1, "HOT": 2}

DEBUG_MAX = 20
RECENT_ERRORS = deque(maxlen=DEBUG_MAX)
RECENT_ALERTS = deque(maxlen=DEBUG_MAX)

# Grouped alerts
_group_lock = asyncio.Lock()
_group_task: Optional[asyncio.Task] = None
_group_buf: List[Dict[str, Any]] = []

# =========================
# ---- Metrics text -------
# =========================
def metrics_text() -> str:
    lines = []
    lines.append("# HELP pumpbot_candles_total Number of closed-candle messages processed")
    lines.append("# TYPE pumpbot_candles_total counter")
    lines.append(f"pumpbot_candles_total {CANDLES_TOTAL}")

    lines.append("# HELP pumpbot_alerts_total Number of alerts sent")
    lines.append("# TYPE pumpbot_alerts_total counter")
    lines.append(f"pumpbot_alerts_total {ALERTS_TOTAL}")

    lines.append("# HELP pumpbot_ws_connections Current active websocket connections")
    lines.append("# TYPE pumpbot_ws_connections gauge")
    lines.append(f"pumpbot_ws_connections {len(WS_TASKS)}")

    lines.append("# HELP pumpbot_last_candle_timestamp_seconds Unix timestamp for last processed candle")
    lines.append("# TYPE pumpbot_last_candle_timestamp_seconds gauge")
    lines.append(f"pumpbot_last_candle_timestamp_seconds {int(LAST_CANDLE_TS_GLOBAL) if LAST_CANDLE_TS_GLOBAL else 0}")

    lines.append("# HELP pumpbot_regime_code Current regime code (UNKNOWN=-1, QUIET=0, NORMAL=1, HOT=2)")
    lines.append("# TYPE pumpbot_regime_code gauge")
    lines.append(f"pumpbot_regime_code {REGIME_CODE_MAP.get(CURRENT_REGIME, -1)}")
    return "\n".join(lines) + "\n"

# =========================
# ---- Debug helpers ------
# =========================
def dbg_err(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"{ts} | {msg}"
    RECENT_ERRORS.append(line)
    logger.error(line)

def dbg_alert(symbol: str, tf: str, delta: float, confidence: int) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line = f"{ts} | {symbol} {tf} | Δ={delta:.2f}% | conf={confidence}"
    RECENT_ALERTS.append(line)
    logger.info(line)

# =========================
# ---- Telegram alert -----
# =========================
def _tg_send_blocking(token: str, chat_id: str, msg: str) -> Tuple[bool, str]:
    try:
        if not token or not chat_id:
            return False, "Telegram not configured"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(url, data=data, timeout=10)
        if r.status_code != 200:
            return False, r.text
        return True, "ok"
    except Exception as e:
        return False, str(e)

async def tg_send(msg: str) -> None:
    global ALERTS_TOTAL
    ok, info = await asyncio.to_thread(_tg_send_blocking, TG_TOKEN, TG_CHAT_ID, msg)
    if ok: ALERTS_TOTAL += 1
    else: dbg_err(f"Telegram send failed: {info}")

# =========================
# ---- OB depth/imbalance -
# =========================
def _get_orderbook_stats_blocking(symbol: str, levels: int) -> Tuple[float, float, float]:
    levels = max(5, min(levels, 1000))
    hosts = [BINANCE_REST.replace("https://", "").split("/")[0]] + ALT_HOSTS
    for h in hosts:
        try:
            r = requests.get(f"https://{h}/api/v3/depth", params={"symbol": symbol, "limit": levels}, timeout=4)
            if r.status_code != 200: continue
            ob = r.json()
            bids = ob.get("bids", [])[:levels]
            asks = ob.get("asks", [])[:levels]
            best_bid = float(bids[0][0]) if bids else 0.0
            best_ask = float(asks[0][0]) if asks else 0.0
            if best_bid <= 0 or best_ask <= 0:
                spread = 999.0
            else:
                mid = (best_bid + best_ask) / 2.0
                spread = (best_ask - best_bid) / mid * 100.0
            bid_sum = sum(float(p)*float(q) for p,q in bids)
            ask_sum = sum(float(p)*float(q) for p,q in asks)
            return spread, bid_sum, ask_sum
        except Exception:
            continue
    return 999.0, 0.0, 0.0

async def get_orderbook_stats(symbol: str, levels: int) -> Tuple[float, float, float]:
    return await asyncio.to_thread(_get_orderbook_stats_blocking, symbol, levels)

# =========================
# ---- Confidence score ----
# =========================
def _ratio_component(value: float, threshold: float, cap: float = 2.0) -> float:
    if threshold <= 0: return 0.0
    return max(0.0, min(value / threshold, cap) / cap)

def compute_confidence(delta_pct: float, delta_req: float,
                       vol_mult: float, vol_req: float,
                       notional: float, min_notional: float,
                       spread: float, max_spread: float) -> int:
    w_d, w_v, w_n, w_s = 0.40, 0.40, 0.15, 0.05
    d_comp = _ratio_component(delta_pct,  delta_req, cap=2.5)
    v_comp = _ratio_component(vol_mult,   vol_req,   cap=3.0)
    n_comp = _ratio_component(notional,   min_notional, cap=3.0)
    s_comp = max(0.0, 1.0 - min(spread / max_spread, 1.0)) if max_spread > 0 and spread >= 0 else 0.0
    score = (w_d*d_comp + w_v*v_comp + w_n*n_comp + w_s*s_comp) * 100.0
    return int(round(max(0.0, min(score, 100.0))))

# =========================
# ---- Higher-TF filter ----
# =========================
def _parse_htf_filter(s: str) -> Optional[Tuple[str, str, str]]:
    if not s: return None
    parts = [x.strip().upper() for x in s.split(',') if x.strip()]
    if not parts: return None
    tf = parts[0]
    kind = parts[1] if len(parts) > 1 else f"EMA{HTF_EMA_LEN}"
    side = parts[2] if len(parts) > 2 else "ABOVE"
    if not kind.startswith("EMA"): kind = f"EMA{HTF_EMA_LEN}"
    if side not in ("ABOVE","BELOW"): side = "ABOVE"
    return tf, kind, side

HTF_PARSED = _parse_htf_filter(HTF_FILTER)

def _htf_alignment_blocking(symbol: str, tf: str, ema_len: int, side: str) -> bool:
    url = f"{BINANCE_REST}/api/v3/klines"
    limit = max(ema_len + 5, 40)
    params = {"symbol": symbol, "interval": tf, "limit": min(limit, 1000)}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            for h in ALT_HOSTS:
                rr = requests.get(f"https://{h}/api/v3/klines", params=params, timeout=10)
                if rr.status_code == 200: r = rr; break
        r.raise_for_status()
        raw = r.json() or []
        closes = [float(row[4]) for row in raw]
        if len(closes) < ema_len: return True
        ema = closes[0]; k = 2 / (ema_len + 1)
        for c in closes[1:]: ema = c * k + ema * (1 - k)
        last_close = closes[-1]
        return last_close >= ema if side == "ABOVE" else last_close <= ema
    except Exception as e:
        dbg_err(f"htf fetch error {symbol} {tf}: {e}")
        return True

async def htf_alignment_ok(symbol: str) -> bool:
    if not HTF_PARSED: return True
    tf, kind, side = HTF_PARSED
    try: ema_len = int(kind.replace("EMA",""))
    except Exception: ema_len = HTF_EMA_LEN
    return await asyncio.to_thread(_htf_alignment_blocking, symbol, tf, ema_len, side)

# =========================
# ---- Filters / Logic -----
# =========================
def body_filter(open_: float, close: float, high: float, low: float, min_body_ratio: float) -> bool:
    rng = max(1e-12, high - low)
    body = max(0.0, close - open_)
    body_ratio = (body / rng) if rng > 0 else 0.0
    return body_ratio >= min_body_ratio

async def _emit_alert(symbol: str, tf: str, msg: str, conf: int, delta: float):
    global _group_task
    if GROUP_WINDOW_SEC <= 0:
        await tg_send(msg)
        dbg_alert(symbol, tf, delta, conf)
        return
    async with _group_lock:
        _group_buf.append({"symbol": symbol, "tf": tf, "msg": msg, "conf": conf, "delta": delta, "t": now_ts()})
        if len(_group_buf) >= max(1, GROUP_MAX):
            await _flush_group_locked(); return
        if _group_task is None or _group_task.done():
            _group_task = asyncio.create_task(_delayed_flush())

async def _delayed_flush():
    await asyncio.sleep(GROUP_WINDOW_SEC)
    async with _group_lock:
        await _flush_group_locked()

async def _flush_group_locked():
    if not _group_buf: return
    items = sorted(_group_buf, key=lambda x: x["conf"], reverse=True)
    take = items[:max(1, GROUP_MAX)]
    header = f"🚀 <b>Pump batch</b> (n={len(items)}) | Regime: <b>{CURRENT_REGIME}</b>\n"
    lines = [f"• <b>{it['symbol']}</b> {it['tf']} — conf <b>{it['conf']}</b> | Δ={it['delta']:.2f}%" for it in take]
    body = header + "\n".join(lines)
    await tg_send(body)
    for it in take: dbg_alert(it['symbol'], it['tf'], it['delta'], it['conf'])
    _group_buf.clear()

async def maybe_alert(symbol: str, tf: str, stats: Dict[str, Any]) -> None:
    if REGIME_FILTER and CURRENT_REGIME not in REGIME_FILTER:
        log("FILTER", f"{symbol} {tf} skipped by regime={CURRENT_REGIME}")
        return

    last_ts = LAST_ALERT_TS[symbol]; now = now_ts()
    if CROSS_TF_DEDUP_SEC > 0 and (now - last_ts) < CROSS_TF_DEDUP_SEC:
        return

    if MIN_TAKER_BUY_RATIO > 0:
        tbr = stats.get('taker_buy_ratio', 0.0)
        if tbr < MIN_TAKER_BUY_RATIO:
            log("FILTER", f"{symbol} {tf} taker-buy {tbr:.2f} < {MIN_TAKER_BUY_RATIO:.2f}")
            return

    spread, bid_sum, ask_sum = await get_orderbook_stats(symbol, OB_TOP_LEVELS)
    max_spread = float(CONFIG["quality_filters"]["max_spread_pct"])
    if spread > max_spread:
        log("FILTER", f"{symbol} {tf} spread {spread:.2f}% > {max_spread:.2f}%"); return
    if OB_MIN_DEPTH_USDT > 0 and bid_sum < OB_MIN_DEPTH_USDT:
        log("FILTER", f"{symbol} {tf} bid depth {bid_sum:,.0f} < {OB_MIN_DEPTH_USDT:,.0f}"); return
    if OB_MIN_IMBALANCE > 0:
        denom = max(1.0, bid_sum + ask_sum)
        imb = (bid_sum - ask_sum) / denom
        if imb < OB_MIN_IMBALANCE:
            log("FILTER", f"{symbol} {tf} OB imbalance {imb:.2f} < {OB_MIN_IMBALANCE:.2f}")
            return

    if HTF_PARSED:
        ok = await htf_alignment_ok(symbol)
        if not ok:
            log("FILTER", f"{symbol} {tf} failed HTF alignment {HTF_PARSED}")
            return

    conf = compute_confidence(
        stats['delta'], stats['delta_req'],
        stats['vol_mult'], stats['vol_req'],
        stats['notional'], stats['min_notional'],
        spread, max_spread
    )
    if MIN_CONFIDENCE > 0 and conf < MIN_CONFIDENCE:
        log("FILTER", f"{symbol} {tf} conf {conf} < {MIN_CONFIDENCE}")
        return

    msg = (
        f"🚀 <b>Pump detected</b>\n"
        f"Symbol: <b>{symbol}</b>\n"
        f"Timeframe: <b>{tf}</b>\n"
        f"Regime: <b>{CURRENT_REGIME}</b>\n"
        f"Confidence: <b>{conf}/100</b>\n"
        f"Price: <code>{stats['close']:.6g}</code> ({stats['delta']:.2f}%)\n"
        f"Volume: <code>{stats['quote_vol']:,.0f}</code> USDT ({stats['vol_mult']:.2f}× EMA)\n"
        f"Taker-buy ratio: {stats.get('taker_buy_ratio',0.0):.2f}\n"
        f"Trades: <code>{stats['trades']}</code>\n"
        f"Spread: {spread:.2f}% (<= {max_spread:.2f}%)\n"
        f"Orderbook: bid {bid_sum:,.0f} vs ask {ask_sum:,.0f} (top {OB_TOP_LEVELS})\n"
        f"Req → Notional/Δ%/×: {stats['min_notional']:,.0f} / {stats['delta_req']:.2f}% / {stats['vol_req']:.2f}×"
    )
    LAST_ALERT_TS[symbol] = now
    await _emit_alert(symbol, tf, msg, conf, stats['delta'])

def prepare_stats(state: "TFState", cfg: Dict[str, Any], k: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    open_ = float(k["o"]); close = float(k["c"])
    high = float(k["h"]);  low  = float(k["l"])
    quote_vol = float(k.get("q", 0.0))
    taker_buy_q = float(k.get("tbq", 0.0))
    trades = int(k.get("n", 0))

    prev_close = state.last_close if state.last_close > 0 else open_
    delta = pct(close, prev_close)

    state.vol_ema = ema_update(state.vol_ema, quote_vol, state.ema_len)
    vol_mult = (quote_vol / state.vol_ema) if state.vol_ema > 0 else 1.0
    notional = quote_vol

    if not body_filter(open_, close, high, low, float(CONFIG["quality_filters"]["min_body_ratio"])):
        return False, {}

    basic_pass = (delta >= cfg["delta_pct"] and vol_mult >= cfg["vol_mult"] and notional >= cfg["min_notional"])
    tbr = (taker_buy_q / quote_vol) if quote_vol > 0 else 0.0

    stats = {
        "open": open_, "close": close, "high": high, "low": low,
        "quote_vol": quote_vol, "trades": trades,
        "delta": delta, "vol_mult": vol_mult, "notional": notional,
        "min_notional": cfg["min_notional"], "delta_req": cfg["delta_pct"], "vol_req": cfg["vol_mult"],
        "taker_buy_ratio": tbr
    }
    return basic_pass, stats

# =========================
# ---- WEBSOCKET LOOPS ----
# =========================
def build_stream_url(base: str, symbols: List[str], tfs: List[str]) -> str:
    parts = []
    for s in symbols:
        s_l = s.lower()
        for tf in tfs:
            parts.append(f"{s_l}@kline_{tf}")
    return f"{base}?streams=" + "/".join(parts)

WS_READY: set = set()

async def ws_consumer(symbols: List[str], tfs: List[str], tag: str):
    global CANDLES_TOTAL, LAST_CANDLE_TS_GLOBAL
    ws_bases = list(BINANCE_WS_BASES)
    random.shuffle(ws_bases)
    backoff = 1; base_idx = 0

    while True:
        base = ws_bases[base_idx % len(ws_bases)]
        url = build_stream_url(base, symbols, tfs)
        try:
            log("WS", f"[{tag}] Connecting {len(symbols)} syms × {len(tfs)} tfs → {len(symbols)*len(tfs)} streams | {base}")
            async with websockets.connect(
                url, ping_interval=15, ping_timeout=10, max_queue=4096, close_timeout=5, open_timeout=15
            ) as ws:
                log("WS", f"[{tag}] Connected on {base}")
                WS_READY.add(tag); backoff = 1
                async for raw in ws:
                    try:
                        msg = json.loads(raw); data = msg.get("data")
                        if not data or data.get("e") != "kline": continue
                        k = data.get("k", {})
                        if not k or not bool(k.get("x", False)): continue
                        symbol = data.get("s"); tf = k.get("i")
                        if symbol not in symbols or tf not in tfs: continue

                        st = STATE[symbol][tf]
                        passed, stats = prepare_stats(
                            st, merged_thresholds(symbol, tf),
                            {"o": k["o"], "c": k["c"], "h": k["h"], "l": k["l"], "q": k.get("q","0"), "tbq": k.get("Q","0"), "n": k.get("n",0)}
                        )

                        CANDLES_TOTAL += 1
                        LAST_CANDLE_TS_GLOBAL = now_ts()
                        nowt = LAST_CANDLE_TS_GLOBAL

                        if nowt < st.cooldown_until:
                            pass
                        else:
                            if passed:
                                if not st.debounce_active:
                                    st.debounce_active = True; st.debounce_start = nowt
                                else:
                                    db_sec = int(CONFIG["quality_filters"]["debounce_sec"])
                                    if (nowt - st.debounce_start) >= db_sec:
                                        await maybe_alert(symbol, tf, stats)
                                        st.cooldown_until = nowt + (merged_thresholds(symbol, tf)["cooldown_min"] * 60)
                                        st.debounce_active = False; st.debounce_start = 0.0
                            else:
                                st.debounce_active = False; st.debounce_start = 0.0

                        st.last_close, st.last_open = float(k["c"]), float(k["o"])
                        st.last_high, st.last_low = float(k["h"]), float(k["l"])
                        st.last_quote_vol = float(k.get("q", 0.0))

                    except Exception as e:
                        txt = f"[{tag}] Msg error: {type(e).__name__}: {e}"
                        log("WS", txt); dbg_err(txt); traceback.print_exc(); continue
        except asyncio.CancelledError:
            raise
        except Exception as e:
            wait = min(backoff, 60) + random.uniform(0, 0.75)
            txt = f"[{tag}] Conn error on {base}: {type(e).__name__}: {e}"
            log("WS", f"{txt} | retry in {wait:.1f}s"); dbg_err(txt); traceback.print_exc()
            await asyncio.sleep(wait); backoff = min(backoff * 2, 60)
        finally:
            WS_READY.discard(tag); base_idx += 1

def plan_batches(symbols: List[str], tfs: List[str]) -> List[List[str]]:
    per_ws_max_symbols = max(1, WS_MAX_STREAMS // max(1, len(tfs)))
    return chunked(symbols, per_ws_max_symbols)

# =========================
# ---- HOT-RELOAD WATCH ---
# =========================
def safe_apply_config(new_cfg: Dict[str, Any]) -> None:
    global CONFIG
    old = CONFIG; merged = dict(old)
    tf_updates = {}
    for tf in TIMEFRAMES:
        tf_updates[tf] = {**old["timeframes"][tf], **new_cfg["timeframes"].get(tf, {})}
    merged["timeframes"] = tf_updates
    merged["tiers"] = new_cfg.get("tiers", old["tiers"])
    merged["symbols"] = new_cfg.get("symbols", old["symbols"])
    merged["quality_filters"] = new_cfg.get("quality_filters", old["quality_filters"])
    CONFIG = merged
    log("HOT", "config.yaml reloaded (safe fields).")

async def config_watcher(path: str, interval: int = 5):
    global CONFIG_MTIME
    try:
        CONFIG_MTIME = os.path.getmtime(path)
    except FileNotFoundError:
        log("HOT", "config.yaml not found at start."); return
    while True:
        await asyncio.sleep(interval)
        try:
            m = os.path.getmtime(path)
            if m != CONFIG_MTIME:
                CONFIG_MTIME = m
                new_cfg = load_config(path)
                new_tfs = list(new_cfg.get("timeframes", {}).keys())
                if set(new_tfs) != set(TIMEFRAMES):
                    log("HOT", "Timeframes changed in file; restart required.")
                safe_apply_config(new_cfg)
        except Exception as e:
            dbg_err(f"watcher error: {e}")
            log("HOT", f"watcher error: {e}")

# =========================
# ---- HEALTH / HTTP ------
# =========================
HTTP_SERVER_TASK = None
WS_TASKS: List[asyncio.Task] = []
SYMBOLS: List[str] = []
_REBUILD_LOCK = asyncio.Lock()

def _ready_status() -> Tuple[bool, str]:
    if not SYMBOLS: return False, "no symbols"
    if not TIMEFRAMES: return False, "no timeframes"
    if not WS_TASKS: return False, "no ws tasks"
    expected = {f"{i+1}/{len(WS_TASKS)}" for i in range(len(WS_TASKS))}
    ready = expected.issubset(WS_READY) and len(WS_READY) == len(WS_TASKS)
    return (ready, f"{len(WS_READY)}/{len(WS_TASKS)} ws ready")

async def _handle_http(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    try:
        data = await reader.read(1024)
        req = data.decode(errors="ignore"); path = "/"
        if req.startswith("GET "):
            parts = req.split(" ")
            if len(parts) >= 2: path = parts[1]

        if path.startswith("/healthz"):
            age = now_ts() - (LAST_CANDLE_TS_GLOBAL or 0)
            ok = (LAST_CANDLE_TS_GLOBAL > 0) and (age < HEALTH_STALE_SEC)
            body = f"OK - last candle {int(LAST_CANDLE_TS_GLOBAL)} age={int(age)}s\n" if ok else f"ERROR - no fresh candles (age={int(age)}s)\n"
            status = "200 OK" if ok else "500 Internal Server Error"
            resp = f"HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
            writer.write(resp.encode()); await writer.drain()

        elif path.startswith("/readyz"):
            ok, detail = _ready_status()
            if ok:
                body = f"READY - {len(SYMBOLS)} symbols, {len(TIMEFRAMES)} TFs, {detail}\n"; status = "200 OK"
            else:
                body = f"NOT READY - {detail}\n"; status = "503 Service Unavailable"
            resp = f"HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
            writer.write(resp.encode()); await writer.drain()

        elif path.startswith("/debugz"):
            lines = [f"REGIME: {CURRENT_REGIME}", "", "== RECENT ERRORS =="]
            lines.extend(list(RECENT_ERRORS)[-DEBUG_MAX:] if RECENT_ERRORS else ["(none)"])
            lines.extend(["", "== RECENT ALERTS =="])
            lines.extend(list(RECENT_ALERTS)[-DEBUG_MAX:] if RECENT_ALERTS else ["(none)"])
            body = "\n".join(lines) + "\n"
            resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: "+str(len(body))+"\r\nConnection: close\r\n\r\n"+body
            writer.write(resp.encode()); await writer.drain()

        elif path.startswith("/metrics"):
            body = metrics_text()
            resp = "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: "+str(len(body))+"\r\nConnection: close\r\n\r\n"+body
            writer.write(resp.encode()); await writer.drain()

        else:
            body = "pumpbot\n"
            resp = f"HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n{body}"
            writer.write(resp.encode()); await writer.drain()

    except Exception as e:
        dbg_err(f"HTTP handler error: {e}")
    finally:
        try: writer.close(); await writer.wait_closed()
        except Exception: pass

async def start_http_server(host: str = "127.0.0.1", port: int = None):
    port = port or HEALTH_PORT
    server = await asyncio.start_server(_handle_http, host, port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
    log("HTTP", f"listening on {addrs} (/healthz, /readyz, /metrics, /debugz)")
    async with server:
        await server.serve_forever()

# =========================
# --- SYMBOL AUTO-REFRESH -
# =========================
async def rebuild_ws_tasks(loop_symbols: List[str]):
    global WS_TASKS, WS_READY
    async with _REBUILD_LOCK:
        for t in WS_TASKS: t.cancel()
        if WS_TASKS: await asyncio.gather(*WS_TASKS, return_exceptions=True)
        WS_TASKS = []; WS_READY.clear()

        batches = plan_batches(loop_symbols, TIMEFRAMES)
        for idx, group in enumerate(batches, start=1):
            tag = f"{idx}/{len(batches)}"
            WS_TASKS.append(asyncio.create_task(ws_consumer(group, TIMEFRAMES, tag)))
        log("BOOT", f"(re)built WS: {len(loop_symbols)} symbols, {len(TIMEFRAMES)} TFs, {len(batches)} WS conns")

async def symbol_refresher():
    if AUTO_SYMBOLS != "USDT":
        log("AUTO", "Symbol refresher disabled (AUTO_SYMBOLS != USDT)."); return
    interval = max(1, SYMBOL_REFRESH_HOURS) * 3600
    while True:
        await asyncio.sleep(interval)
        try:
            new_syms = await fetch_usdt_symbols()
            if not new_syms:
                log("AUTO", "Refresh got empty list; keeping current symbols."); continue
            cur = set(SYMBOLS); new = set(new_syms)
            added = sorted(list(new - cur)); removed = sorted(list(cur - new))
            if added or removed:
                log("AUTO", f"Symbol change: +{len(added)} / -{len(removed)}")
                for s in added:
                    for tf in TIMEFRAMES:
                        cfg = merged_thresholds(s, tf); STATE[s][tf] = TFState(int(cfg["ema_len"]))
                for s in removed: STATE.pop(s, None)
                SYMBOLS[:] = sorted(new_syms)
                await rebuild_ws_tasks(SYMBOLS)
            else:
                log("AUTO", "No symbol changes.")
        except Exception as e:
            dbg_err(f"refresher error: {e}"); log("AUTO", f"refresher error: {e}")

# =========================
# -------- REGIME ----------
# =========================
def _fetch_last_closes_blocking(symbol: str, interval: str, limit: int) -> List[float]:
    url = f"{BINANCE_REST}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": max(5, min(limit, 1000))}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            for h in ALT_HOSTS:
                rr = requests.get(f"https://{h}/api/v3/klines", params=params, timeout=10)
                if rr.status_code == 200: r = rr; break
        r.raise_for_status()
        raw = r.json() or []
        closes = [float(row[4]) for row in raw]
        return closes
    except Exception as e:
        dbg_err(f"regime fetch error: {e}")
        return []

async def regime_watcher():
    global CURRENT_REGIME
    last_regime = None
    while True:
        try:
            closes = await asyncio.to_thread(_fetch_last_closes_blocking, REGIME_SYMBOL, REGIME_INTERVAL, REGIME_LOOKBACK)
            if len(closes) >= 30:
                rets = []
                for i in range(1, len(closes)):
                    prev = closes[i-1]; cur = closes[i]
                    if prev > 0: rets.append((cur - prev) / prev * 100.0)
                if rets:
                    import statistics
                    sigma = statistics.pstdev(rets)
                    if sigma < REGIME_VOL_QUIET: CURRENT_REGIME = "QUIET"
                    elif sigma > REGIME_VOL_HOT: CURRENT_REGIME = "HOT"
                    else: CURRENT_REGIME = "NORMAL"
                else:
                    CURRENT_REGIME = "UNKNOWN"
            else:
                CURRENT_REGIME = "UNKNOWN"
            if CURRENT_REGIME != last_regime:
                log("REGIME", f"Regime changed → {CURRENT_REGIME}"); last_regime = CURRENT_REGIME
        except Exception as e:
            dbg_err(f"regime watcher error: {e}")
        await asyncio.sleep(max(60, REGIME_REFRESH_MIN * 60))

# =========================
# -------- WATCHDOG -------
# =========================
async def watchdog():
    if WATCHDOG_RESTART_SEC <= 0:
        log("SYS", "Watchdog disabled."); return
    while True:
        await asyncio.sleep(max(5, WATCHDOG_CHECK_SEC))
        age = now_ts() - (LAST_CANDLE_TS_GLOBAL or 0)
        if LAST_CANDLE_TS_GLOBAL > 0 and age > WATCHDOG_RESTART_SEC:
            msg = f"watchdog: no candles for {int(age)}s > {WATCHDOG_RESTART_SEC}, forcing restart"
            dbg_err(msg); log("SYS", "Watchdog triggering process exit for systemd restart")
            try:
                for h in logger.handlers:
                    try: h.flush()
                    except Exception: pass
            finally:
                os._exit(1)

# =========================
# --------- MAIN ----------
# =========================
_shutdown_event = asyncio.Event()
def _handle_sigterm(*_):
    log("SYS", "SIGTERM received, shutting down…")
    try: _shutdown_event.set()
    except Exception: pass

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, _handle_sigterm)
        except NotImplementedError: pass

    global SYMBOLS, WS_TASKS
    if AUTO_SYMBOLS == "USDT":
        SYMBOLS = await fetch_usdt_symbols()
        if SYMBOLS: log("AUTO", f"Using {len(SYMBOLS)} USDT symbols (capped at {MAX_SYMBOLS}).")
        else: log("AUTO", "Falling back to manual SYMBOLS (auto failed)."); SYMBOLS = SYMBOLS_MANUAL
    else:
        SYMBOLS = SYMBOLS_MANUAL

    if not SYMBOLS:
        log("ERR", "No symbols resolved from .env / AUTO_SYMBOLS."); return
    if not TG_TOKEN or not TG_CHAT_ID:
        log("WARN", "Telegram not set; alerts will be printed only (skipped send).")

    for s in SYMBOLS:
        for tf in TIMEFRAMES:
            cfg = merged_thresholds(s, tf)
            STATE[s][tf] = TFState(int(cfg["ema_len"]))

    batches = chunked(SYMBOLS, max(1, WS_MAX_STREAMS // max(1, len(TIMEFRAMES))))
    WS_TASKS = []
    for idx, group in enumerate(batches, start=1):
        tag = f"{idx}/{len(batches)}"
        WS_TASKS.append(asyncio.create_task(ws_consumer(group, TIMEFRAMES, tag)))

    cfg_task = asyncio.create_task(config_watcher(CONFIG_PATH, interval=5))
    http_task = asyncio.create_task(start_http_server("127.0.0.1", HEALTH_PORT))
    refresher_task = asyncio.create_task(symbol_refresher())
    watchdog_task = asyncio.create_task(watchdog())
    regime_task = asyncio.create_task(regime_watcher())

    log("BOOT", f"Symbols: {len(SYMBOLS)}  | Timeframes: {TIMEFRAMES}  | WS connections: {len(WS_TASKS)}")
    await _shutdown_event.wait()

    log("SYS", "Cancelling tasks…")
    for t in WS_TASKS + [cfg_task, http_task, refresher_task, watchdog_task, regime_task]:
        t.cancel()
    await asyncio.gather(*(WS_TASKS + [cfg_task, http_task, refresher_task, watchdog_task, regime_task]), return_exceptions=True)
    log("SYS", "Exited cleanly.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("SYS", "Exiting…")
