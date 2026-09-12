import asyncio
import aiohttp
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TG_TOKEN = os.environ.get("ARB_BOT_TOKEN", "")
CHAT_ID = None

config = {
    "min_profit_pct":  float(os.environ.get("MIN_PROFIT_PCT", "0.3")),
    "lot_usdt":        float(os.environ.get("LOT_USDT", "100")),
    "start_capital":   float(os.environ.get("START_CAPITAL", "10000")),
    "stop_loss_usdt":  float(os.environ.get("STOP_LOSS_USDT", "50")),
    "scan_interval":   6,
    "max_trades_per_min": int(os.environ.get("MAX_TRADES_PER_MIN", "5")),
    "convert_threshold_usdt": float(os.environ.get("CONVERT_THRESHOLD_USDT", "20")),
}

SUSPICIOUS_SPREAD_PCT = 5.0
MIN_DEPTH_LEVELS = 15
QUOTE_CURRENCIES = ["USDT", "BTC", "ETH"]

FEES = {
    "Binance": 0.10,
    "KuCoin":  0.10,
    "HTX":     0.20,
    "Gate":    0.20,
    "Bitget":  0.10,
    "MEXC":    0.05,
}

SYMBOLS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "TRX", "DOT", "AVAX",
    "LINK", "NEAR", "ATOM", "LTC", "BCH", "ETC", "BNB",
    "MATIC", "ARB", "OP", "SUI", "APT", "ZK", "STRK", "MANTA", "SEI",
    "TIA", "INJ", "WLD", "IMX", "METIS", "BLAST",
    "UNI", "AAVE", "CRV", "COMP", "MKR", "SNX", "YFI", "SUSHI", "CAKE",
    "DYDX", "LDO", "GMX", "RUNE", "1INCH", "BAL", "ZRX",
    "FET", "AGIX", "OCEAN", "RENDER", "TAO", "ARKM", "RLC",
    "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "BOME", "MEME",
    "SAND", "MANA", "AXS", "GALA", "ENJ", "APE", "ILV", "MAGIC",
    "VET", "HBAR", "ALGO", "XLM", "EOS", "FTM", "ROSE", "ONE", "KAVA",
    "CELO", "QTUM", "WAVES", "KSM", "ICP", "KAS", "EGLD", "FLOW",
    "XTZ", "NEO", "IOTA", "IOST", "ONT", "CKB",
    "GRT", "ANKR", "SKL", "STORJ", "FIL", "AR",
    "CHZ", "GMT", "RVN", "THETA", "MASK", "GAL", "PYTH", "JUP", "JTO",
    "TON", "ORDI", "WOO", "PERP", "LRC", "BAT",
]
QUOTE = "USDT"

coin_stats: Dict[str, dict] = {
    s: {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0}
    for s in SYMBOLS
}
pair_stats: Dict[tuple, dict] = {}
route_stats: Dict[tuple, dict] = {}
route_coin_stats: Dict[Tuple[str, str, str], dict] = {}
currency_balances: Dict[str, float] = {q: 0.0 for q in QUOTE_CURRENCIES if q != "USDT"}
conversions_log: List[dict] = []

stats = {
    "scans": 0, "signals": 0,
    "trades_sim": 0, "profit_sim": 0.0,
    "profit_since_alert": 0.0,
    "errors": 0, "start_time": datetime.now(),
    "trades_this_minute": 0,
    "minute_start": datetime.now(),
}
trade_history: List[dict] = []
last_signal_time: Dict[str, float] = {}

hourly_signals: Dict[int, int] = defaultdict(int)
hourly_plausible_signals: Dict[int, int] = defaultdict(int)
hourly_route_signals: Dict[Tuple[int, str, str], int] = defaultdict(int)
hourly_route_plausible: Dict[Tuple[int, str, str], int] = defaultdict(int)

TARGET_ROUTES: List[Tuple[str, str]] = [("KuCoin", "MEXC")]

config["auto_signal_min_pct"] = 0.3
config["auto_check_interval_sec"] = 60
AUTO_SIGNAL_COOLDOWN_SEC = 300

config["current_real_coin"] = "ONE"

config["worker_honest_threshold_pct"] = 6.3078

qualified_signals_log: List[dict] = []
QUALIFIED_SIGNALS_LOG_MAXLEN = 50

auto_route_candidates: Dict[Tuple[str, str], set] = defaultdict(set)

route_symbol_spread_history: Dict[Tuple[str, str, str], List[Tuple[float, float]]] = defaultdict(list)
ROUTE_SPREAD_HISTORY_WINDOW_SEC = 20 * 60

_last_auto_signal_time: Dict[Tuple[str, str, str], float] = {}


async def check_narrow_route(session, buy_ex: str, sell_ex: str, symbol: str,
                              lot_usdt: float) -> Optional[dict]:
    if buy_ex not in ORDERBOOK_FN or sell_ex not in ORDERBOOK_FN:
        return None
    buy_book = await ORDERBOOK_FN[buy_ex](session, SYMBOL_FMT[buy_ex](symbol))
    sell_book = await ORDERBOOK_FN[sell_ex](session, SYMBOL_FMT[sell_ex](symbol))
    if not buy_book or not sell_book:
        return None
    if (len(buy_book.get("asks", [])) < MIN_DEPTH_LEVELS or
            len(buy_book.get("bids", [])) < MIN_DEPTH_LEVELS or
            len(sell_book.get("asks", [])) < MIN_DEPTH_LEVELS or
            len(sell_book.get("bids", [])) < MIN_DEPTH_LEVELS):
        return None

    coins, avg_buy, _, buy_full = _walk_by_notional(buy_book["asks"], lot_usdt)
    if not buy_full or coins <= 0 or avg_buy <= 0:
        return None
    buy_fee = FEES.get(buy_ex, 0.1) / 100
    coins_after_fee = coins * (1 - buy_fee)
    quote_out, avg_sell, sell_full = _walk_by_qty(sell_book["bids"], coins_after_fee)
    if not sell_full or quote_out <= 0:
        return None
    sell_fee = FEES.get(sell_ex, 0.1) / 100
    final_usdt = quote_out * (1 - sell_fee)
    profit = final_usdt - lot_usdt
    net_pct = profit / lot_usdt * 100
    gross_pct = (avg_sell - avg_buy) / avg_buy * 100 if avg_buy > 0 else 0.0

    is_suspicious = gross_pct >= SUSPICIOUS_SPREAD_PCT

    return {
        "buy_ex": buy_ex, "sell_ex": sell_ex, "symbol": symbol,
        "buy_price": round(avg_buy, 8), "sell_price": round(avg_sell, 8),
        "gross_pct": round(gross_pct, 4), "net_pct": round(net_pct, 4),
        "profit_usdt": round(profit, 4),
        "depth_ok": True,
        "suspicious": is_suspicious,
        "buy_levels": (len(buy_book["asks"]), len(buy_book["bids"])),
        "sell_levels": (len(sell_book["asks"]), len(sell_book["bids"])),
    }


def record_route_spread(buy_ex: str, sell_ex: str, symbol: str, net_pct: float) -> None:
    now_ts = time.time()
    key = (buy_ex, sell_ex, symbol)
    hist = route_symbol_spread_history[key]
    hist.append((now_ts, net_pct))
    cutoff = now_ts - ROUTE_SPREAD_HISTORY_WINDOW_SEC
    while hist and hist[0][0] < cutoff:
        hist.pop(0)


def get_route_spread_trend(buy_ex: str, sell_ex: str, symbol: str) -> str:
    hist = route_symbol_spread_history.get((buy_ex, sell_ex, symbol))
    if not hist or len(hist) < 2:
        return "➡️ данных пока мало для тренда"
    first_pct = hist[0][1]
    last_pct = hist[-1][1]
    delta = last_pct - first_pct
    span_min = round((hist[-1][0] - hist[0][0]) / 60, 1)
    if delta < -0.15:
        return f"📉 сужается ({first_pct:+.2f}% → {last_pct:+.2f}% за {span_min} мин) — окно может закрыться"
    elif delta > 0.15:
        return f"📈 расширяется ({first_pct:+.2f}% → {last_pct:+.2f}% за {span_min} мин)"
    else:
        return f"➡️ стабилен ({first_pct:+.2f}% → {last_pct:+.2f}% за {span_min} мин)"


def get_route_spread_verdict(buy_ex: str, sell_ex: str, symbol: str) -> dict:
    hist = route_symbol_spread_history.get((buy_ex, sell_ex, symbol), [])
    if len(hist) < 3:
        return {"level": "yellow",
                "text": "🤔 Ещё рано судить — нужно минимум 3 точки истории "
                        "(проверь `/routetrend` через пару минут)."}

    values = [v for _, v in hist]
    last = values[-1]

    diffs = [values[i + 1] - values[i] for i in range(len(values) - 1)]
    monotonic_up = all(d >= -0.05 for d in diffs) and sum(diffs) > 0.15
    monotonic_down = all(d <= 0.05 for d in diffs) and sum(diffs) < -0.15

    approaching_or_over_threshold = last >= SUSPICIOUS_SPREAD_PCT - 1.0

    if monotonic_up and approaching_or_over_threshold:
        return {"level": "red",
                "text": f"🔴 Похоже на ПРОГРЕССИРУЮЩУЮ аномалию (та же картина, что была у "
                        f"RVN/LRC сегодня) — монотонный рост без откатов, уже {last:+.2f}% "
                        f"и приближается к порогу {SUSPICIOUS_SPREAD_PCT}% или уже за ним. "
                        f"Не рекомендую рассматривать для реальной торговли."}

    amplitude = max(values) - min(values)
    stable_and_safe = (not monotonic_up and not monotonic_down
                        and amplitude < 1.0 and 0 < last < SUSPICIOUS_SPREAD_PCT - 1.5)
    if stable_and_safe:
        return {"level": "green",
                "text": f"🟢 Похоже на настоящее, ограниченное по амплитуде окно — колеблется "
                        f"в узком диапазоне ({amplitude:.2f} п.п.), не убегает монотонно вверх. "
                        f"Более надёжный кандидат, чем разовый широкий спред."}

    if monotonic_down:
        return {"level": "yellow",
                "text": "📉 Разрыв закрывается сам — окно, похоже, уже уходит. "
                        "Вряд ли стоит спешить, момент упущен или уходит."}

    return {"level": "yellow",
            "text": "🤔 Пока нет чёткого паттерна — ни устойчивого сужения, ни явной "
                    "аномалии. Продолжаем наблюдать."}


price_history: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
PRICE_HISTORY_WINDOW_SEC = 15 * 60


def record_symbol_price(symbol: str, mid_price: float) -> None:
    if mid_price <= 0:
        return
    now_ts = time.time()
    hist = price_history[symbol]
    hist.append((now_ts, mid_price))
    cutoff = now_ts - PRICE_HISTORY_WINDOW_SEC
    while hist and hist[0][0] < cutoff:
        hist.pop(0)


def get_volatility_pct(symbol: str) -> Optional[float]:
    hist = price_history.get(symbol)
    if not hist or len(hist) < 2:
        return None
    prices = [p for _, p in hist]
    lo, hi = min(prices), max(prices)
    if lo <= 0:
        return None
    return round((hi - lo) / lo * 100, 3)


async def send_tg(session, text):
    if not CHAT_ID:
        return None
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={
            "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"
        }, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                body = await r.text()
                logger.error(f"TG sendMessage HTTP {r.status}: {body}")
                async with session.post(url, json={
                    "chat_id": CHAT_ID, "text": text
                }, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                    if r2.status != 200:
                        logger.error(f"TG sendMessage повтор без Markdown тоже не прошёл: HTTP {r2.status}: {await r2.text()}")
                        return None
                    data2 = await r2.json()
                    return (data2.get("result") or {}).get("message_id")
            data = await r.json()
            return (data.get("result") or {}).get("message_id")
    except Exception as e:
        logger.error(f"TG: {e}")
        return None


_last_pinned_message_id: Optional[int] = None


async def pin_message(session, message_id: int) -> None:
    if not CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/pinChatMessage"
    try:
        async with session.post(url, json={
            "chat_id": CHAT_ID, "message_id": message_id, "disable_notification": False
        }, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.error(f"TG pinChatMessage HTTP {r.status}: {await r.text()}")
    except Exception as e:
        logger.error(f"TG pin error: {e}")


async def unpin_message(session, message_id: int) -> None:
    if not CHAT_ID or not message_id:
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/unpinChatMessage"
    try:
        async with session.post(url, json={
            "chat_id": CHAT_ID, "message_id": message_id
        }, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.error(f"TG unpinChatMessage HTTP {r.status}: {await r.text()}")
    except Exception as e:
        logger.error(f"TG unpin error: {e}")


async def send_tg_pinned(session, text: str) -> None:
    global _last_pinned_message_id
    message_id = await send_tg(session, text)
    if not message_id:
        return
    if _last_pinned_message_id:
        await unpin_message(session, _last_pinned_message_id)
    await pin_message(session, message_id)
    _last_pinned_message_id = message_id


async def get_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        async with session.get(url,
            params={"offset": offset, "timeout": 30},
            timeout=aiohttp.ClientTimeout(total=35)) as r:
            return (await r.json()).get("result", [])
    except Exception:
        return []


def check_trade_limit() -> bool:
    now = datetime.now()
    elapsed = (now - stats["minute_start"]).total_seconds()
    if elapsed >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


_QUOTES_SORTED = sorted(QUOTE_CURRENCIES, key=len, reverse=True)

BINANCE_MARKET_BASE = "https://data-api.binance.vision"


async def get_binance(session) -> Dict:
    try:
        async with session.get(
            f"{BINANCE_MARKET_BASE}/api/v3/ticker/bookTicker",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in await r.json():
                sym = item.get("symbol", "")
                for q in _QUOTES_SORTED:
                    if sym.endswith(q):
                        base = sym[:-len(q)]
                        if base in SYMBOLS and base != q:
                            bid = float(item.get("bidPrice", 0) or 0)
                            ask = float(item.get("askPrice", 0) or 0)
                            if bid > 0 and ask > 0:
                                out[(base, q)] = {"bid": bid, "ask": ask}
                        break
            return out
    except Exception as e:
        logger.error(f"Binance: {e}")
        return {}


async def get_kucoin(session) -> Dict:
    try:
        async with session.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in (await r.json()).get("data", {}).get("ticker", []):
                sym = item.get("symbol", "")
                if "-" not in sym:
                    continue
                base, _, quote = sym.partition("-")
                if base in SYMBOLS and quote in QUOTE_CURRENCIES and base != quote:
                    bid = float(item.get("buy", 0) or 0)
                    ask = float(item.get("sell", 0) or 0)
                    if bid > 0 and ask > 0:
                        out[(base, quote)] = {"bid": bid, "ask": ask}
            return out
    except Exception as e:
        logger.error(f"KuCoin: {e}")
        return {}


async def get_htx(session) -> Dict:
    try:
        async with session.get(
            "https://api.huobi.pro/market/tickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in (await r.json()).get("data", []):
                sym = item.get("symbol", "").upper()
                for q in _QUOTES_SORTED:
                    if sym.endswith(q):
                        base = sym[:-len(q)]
                        if base in SYMBOLS and base != q:
                            bid = float(item.get("bid", 0) or 0)
                            ask = float(item.get("ask", 0) or 0)
                            if bid > 0 and ask > 0:
                                out[(base, q)] = {"bid": bid, "ask": ask}
                        break
            return out
    except Exception as e:
        logger.error(f"HTX: {e}")
        return {}


async def get_gate(session) -> Dict:
    try:
        async with session.get(
            "https://api.gateio.ws/api/v4/spot/tickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in await r.json():
                sym = item.get("currency_pair", "")
                if "_" not in sym:
                    continue
                base, _, quote = sym.partition("_")
                if base in SYMBOLS and quote in QUOTE_CURRENCIES and base != quote:
                    bid = float(item.get("highest_bid", 0) or 0)
                    ask = float(item.get("lowest_ask", 0) or 0)
                    if bid > 0 and ask > 0:
                        out[(base, quote)] = {"bid": bid, "ask": ask}
            return out
    except Exception as e:
        logger.error(f"Gate: {e}")
        return {}


async def get_bitget(session) -> Dict:
    try:
        async with session.get(
            "https://api.bitget.com/api/v2/spot/market/tickers",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in (await r.json()).get("data", []):
                sym = item.get("symbol", "").upper()
                for q in _QUOTES_SORTED:
                    if sym.endswith(q):
                        base = sym[:-len(q)]
                        if base in SYMBOLS and base != q:
                            bid = float(item.get("bidPr", 0) or 0)
                            ask = float(item.get("askPr", 0) or 0)
                            if bid > 0 and ask > 0:
                                out[(base, q)] = {"bid": bid, "ask": ask}
                        break
            return out
    except Exception as e:
        logger.error(f"Bitget: {e}")
        return {}


async def get_mexc(session) -> Dict:
    try:
        async with session.get(
            "https://api.mexc.com/api/v3/ticker/bookTicker",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in await r.json():
                sym = item.get("symbol", "").upper()
                for q in _QUOTES_SORTED:
                    if sym.endswith(q):
                        base = sym[:-len(q)]
                        if base in SYMBOLS and base != q:
                            bid = float(item.get("bidPrice", 0) or 0)
                            ask = float(item.get("askPrice", 0) or 0)
                            if bid > 0 and ask > 0:
                                out[(base, q)] = {"bid": bid, "ask": ask}
                        break
            return out
    except Exception as e:
        logger.error(f"MEXC: {e}")
        return {}


def sym_binance(base: str) -> str: return f"{base}USDT"
def sym_kucoin(base: str) -> str: return f"{base}-USDT"
def sym_htx(base: str) -> str: return f"{base.lower()}usdt"
def sym_gate(base: str) -> str: return f"{base}_USDT"
def sym_bitget(base: str) -> str: return f"{base}USDT"
def sym_mexc(base: str) -> str: return f"{base}USDT"


SYMBOL_FMT = {
    "Binance": sym_binance, "KuCoin": sym_kucoin, "HTX": sym_htx,
    "Gate": sym_gate, "Bitget": sym_bitget, "MEXC": sym_mexc,
}


async def get_orderbook_binance(session, symbol: str):
    try:
        async with session.get(f"{BINANCE_MARKET_BASE}/api/v3/depth",
                                params={"symbol": symbol, "limit": 50},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            return {
                "bids": [[float(p), float(q)] for p, q in d.get("bids", [])],
                "asks": [[float(p), float(q)] for p, q in d.get("asks", [])],
            }
    except Exception as e:
        logger.error(f"Binance orderbook {symbol}: {e}")
        return None


async def get_orderbook_kucoin(session, symbol: str):
    try:
        async with session.get("https://api.kucoin.com/api/v1/market/orderbook/level2_20",
                                params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            data = d.get("data", {}) or {}
            return {
                "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
                "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
            }
    except Exception as e:
        logger.error(f"KuCoin orderbook {symbol}: {e}")
        return None


async def get_orderbook_htx(session, symbol: str):
    try:
        async with session.get("https://api.huobi.pro/market/depth",
                                params={"symbol": symbol, "type": "step0"},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            tick = d.get("tick", {}) or {}
            bids = [[float(p), float(q)] for p, q in tick.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in tick.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        logger.error(f"HTX orderbook {symbol}: {e}")
        return None


async def get_orderbook_gate(session, symbol: str):
    try:
        async with session.get("https://api.gateio.ws/api/v4/spot/order_book",
                                params={"currency_pair": symbol, "limit": 50},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            bids = [[float(p), float(q)] for p, q in d.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in d.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        logger.error(f"Gate orderbook {symbol}: {e}")
        return None


async def get_orderbook_bitget(session, symbol: str):
    try:
        async with session.get("https://api.bitget.com/api/v2/spot/market/orderbook",
                                params={"symbol": symbol, "limit": "50"},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            data = d.get("data", {}) or {}
            bids = [[float(p), float(q)] for p, q in data.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        logger.error(f"Bitget orderbook {symbol}: {e}")
        return None


async def get_orderbook_mexc(session, symbol: str):
    try:
        async with session.get("https://api.mexc.com/api/v3/depth",
                                params={"symbol": symbol, "limit": 100},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            bids = [[float(p), float(q)] for p, q in d.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in d.get("asks", [])]
            if not bids or not asks:
                return None
            return {"bids": bids, "asks": asks}
    except Exception as e:
        logger.error(f"MEXC orderbook {symbol}: {e}")
        return None


ORDERBOOK_FN = {
    "Binance": get_orderbook_binance, "KuCoin": get_orderbook_kucoin,
    "HTX": get_orderbook_htx, "Gate": get_orderbook_gate,
    "Bitget": get_orderbook_bitget, "MEXC": get_orderbook_mexc,
}

ALL_EXCHANGES = ["Binance", "KuCoin", "MEXC"]

# НОВОЕ (по прямому запросу пользователя — треугольник по ВСЕМ монетам
# только на KuCoin и MEXC, без Binance): раньше треугольник использовал
# отдельный, маленький список TRIANGLE_SYMBOLS (~12 монет) на ВСЕХ
# биржах включая Binance (ALL_EXCHANGES). Теперь — полный список SYMBOLS
# (~110 монет), только на этих двух биржах (Binance заблокирован по API
# на территории пользователя, к тому же не нужен для этой конкретной
# схемы — треугольник происходит внутри одной биржи, KuCoin/MEXC уже
# используются в основной реальной торговле).
TRIANGLE_EXCHANGES = ["KuCoin", "MEXC"]

BSC_TOKEN_ADDRESSES = {
    "USDT": "0x55d398326f99059fF775485246999027B3197955",
    "WBNB": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
    "BUSD": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
    "USDC": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
    "CAKE": "0x0E09FaBB73Bd3Ade0a17ECC321fD13a19e81cE82",
}
_dex_price_cache: Dict[str, Tuple[float, float]] = {}
DEX_PRICE_CACHE_TTL_SEC = 30


async def get_pancakeswap_price_usd(session, token_address: str) -> Optional[float]:
    now = time.time()
    cached = _dex_price_cache.get(token_address)
    if cached and now - cached[0] < DEX_PRICE_CACHE_TTL_SEC:
        return cached[1]
    url = f"https://api.geckoterminal.com/api/v2/simple/networks/bsc/token_price/{token_address}"
    try:
        async with session.get(url, headers={"accept": "application/json"},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.error(f"GeckoTerminal price fetch HTTP {r.status} для {token_address}")
                return None
            data = await r.json()
            token_prices = (((data.get("data") or {}).get("attributes") or {}).get("token_prices") or {})
            price_str = token_prices.get(token_address.lower()) or token_prices.get(token_address)
            if price_str is None:
                logger.error(f"GeckoTerminal: цена не найдена в ответе для {token_address}: {data}")
                return None
            price = float(price_str)
            _dex_price_cache[token_address] = (now, price)
            return price
    except Exception as e:
        logger.error(f"GeckoTerminal price exception для {token_address}: {e}")
        return None


async def search_dex_pools(session, query: str, network: str = "bsc") -> Optional[List[dict]]:
    url = "https://api.geckoterminal.com/api/v2/search/pools"
    params = {"query": query, "network": network, "include": "base_token,quote_token"}
    try:
        async with session.get(url, params=params, headers={"accept": "application/json"},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                logger.error(f"GeckoTerminal search HTTP {r.status} для '{query}'")
                return None
            data = await r.json()
            pools = data.get("data", [])
            if not pools:
                return []

            included = data.get("included", [])
            token_map = {}
            for item in included:
                if item.get("type") == "token":
                    attrs = item.get("attributes", {})
                    token_map[item.get("id")] = {
                        "address": attrs.get("address"),
                        "symbol": attrs.get("symbol"),
                    }

            results = []
            for pool in pools[:5]:
                attrs = pool.get("attributes", {})
                rel = pool.get("relationships", {})
                base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id")
                base_token = token_map.get(base_id, {})
                try:
                    liquidity = float(attrs.get("reserve_in_usd") or 0)
                except (TypeError, ValueError):
                    liquidity = 0.0
                results.append({
                    "pool_name": attrs.get("name", "?"),
                    "token_symbol": base_token.get("symbol", "?"),
                    "token_address": base_token.get("address", "?"),
                    "price_usd": attrs.get("base_token_price_usd"),
                    "liquidity_usd": liquidity,
                    "created_at": attrs.get("pool_created_at", "?"),
                })
            results.sort(key=lambda x: x["liquidity_usd"], reverse=True)
            return results
    except Exception as e:
        logger.error(f"GeckoTerminal search exception для '{query}': {e}")
        return None


def _walk_by_notional(levels: List[list], target_notional: float):
    remaining = target_notional
    base_qty = 0.0
    for price, qty in levels:
        if price <= 0 or qty <= 0:
            continue
        level_notional = price * qty
        if level_notional <= remaining:
            base_qty += qty
            remaining -= level_notional
        else:
            base_qty += remaining / price
            remaining = 0.0
            break
    filled_notional = target_notional - remaining
    avg_price = filled_notional / base_qty if base_qty > 0 else 0.0
    return base_qty, avg_price, filled_notional, remaining <= 1e-9


def _walk_by_qty(levels: List[list], target_qty: float):
    remaining = target_qty
    quote_out = 0.0
    for price, qty in levels:
        if price <= 0 or qty <= 0:
            continue
        take = min(qty, remaining)
        quote_out += take * price
        remaining -= take
        if remaining <= 1e-12:
            break
    avg_price = quote_out / (target_qty - remaining) if (target_qty - remaining) > 0 else 0.0
    return quote_out, avg_price, remaining <= 1e-9


async def reverify_with_depth(session, opp: dict) -> Optional[dict]:
    if opp["quote"] != "USDT":
        return opp

    sym = opp["symbol"]
    buy_ex, sell_ex = opp["buy_ex"], opp["sell_ex"]
    if buy_ex not in ORDERBOOK_FN or sell_ex not in ORDERBOOK_FN:
        return None

    buy_book = await ORDERBOOK_FN[buy_ex](session, SYMBOL_FMT[buy_ex](sym))
    sell_book = await ORDERBOOK_FN[sell_ex](session, SYMBOL_FMT[sell_ex](sym))
    if not buy_book or not sell_book:
        return None

    lot_usdt = opp["volume_usdt"]
    coins, avg_buy, filled_usdt, buy_full = _walk_by_notional(buy_book["asks"], lot_usdt)
    if not buy_full or coins <= 0:
        return None
    buy_fee = FEES.get(buy_ex, 0.1) / 100
    coins_after_fee = coins * (1 - buy_fee)
    quote_out, avg_sell, sell_full = _walk_by_qty(sell_book["bids"], coins_after_fee)
    if not sell_full or quote_out <= 0:
        return None
    sell_fee = FEES.get(sell_ex, 0.1) / 100
    final_usdt = quote_out * (1 - sell_fee)
    profit = final_usdt - lot_usdt
    net_pct = profit / lot_usdt * 100

    if net_pct < config["min_profit_pct"]:
        return None

    reverified = dict(opp)
    reverified["net_pct"] = round(net_pct, 4)
    reverified["profit_usdt"] = round(profit, 4)
    reverified["gross_pct"] = round((avg_sell - avg_buy) / avg_buy * 100, 4) if avg_buy > 0 else opp["gross_pct"]
    reverified["depth_verified"] = True
    return reverified


async def verify_candidate(session, sym: str, lot_usdt: float) -> dict:
    row = {"symbol": sym, "exchanges": {}, "ok": True, "reasons": [], "cross_spread": None}

    for ex in ALL_EXCHANGES:
        symbol_fmt = SYMBOL_FMT[ex](sym)
        try:
            book = await ORDERBOOK_FN[ex](session, symbol_fmt)
        except Exception as e:
            book = None
            logger.error(f"verify_candidate {ex} {sym}: {e}")

        if not book or not book.get("bids") or not book.get("asks"):
            row["ok"] = False
            row["reasons"].append(f"{ex}: нет данных стакана")
            row["exchanges"][ex] = None
            continue

        bid_levels, ask_levels = len(book["bids"]), len(book["asks"])
        qty, avg_price, filled, full = _walk_by_notional(book["asks"], lot_usdt)
        slip = (round((avg_price - book["asks"][0][0]) / book["asks"][0][0] * 100, 3)
                if avg_price > 0 and book["asks"][0][0] > 0 else None)
        row["exchanges"][ex] = {
            "bid": book["bids"][0][0], "ask": book["asks"][0][0],
            "bid_levels": bid_levels, "ask_levels": ask_levels,
            "slippage_pct": slip, "book_full": full,
        }
        if bid_levels < MIN_DEPTH_LEVELS or ask_levels < MIN_DEPTH_LEVELS:
            row["ok"] = False
            row["reasons"].append(f"{ex}: тонкий стакан ({ask_levels} ask / {bid_levels} bid уровней, "
                                    f"нужно ≥{MIN_DEPTH_LEVELS})")

    valid_bids = [v["bid"] for v in row["exchanges"].values() if v]
    if len(valid_bids) >= 2:
        spread_pct = round((max(valid_bids) - min(valid_bids)) / min(valid_bids) * 100, 2)
        row["cross_spread"] = spread_pct
        if spread_pct > SUSPICIOUS_SPREAD_PCT:
            row["ok"] = False
            row["reasons"].append(
                f"цены между биржами расходятся на {spread_pct}% — подозрительно "
                f"(рассинхрон котировки или разная стадия миграции токена, не реальный арбитраж)"
            )
    return row


def get_quote_usdt_rate(all_data, quote):
    if quote == "USDT":
        return 1.0
    entry = all_data.get((quote, "USDT"))
    if not entry:
        return None
    asks = [d["ask"] for d in entry.values() if d.get("ask", 0) > 0]
    if not asks:
        return None
    return sum(asks) / len(asks)


def find_arbitrage(all_data: Dict[tuple, Dict]) -> List[dict]:
    results = []
    min_pct = config["min_profit_pct"]
    lot_usdt = config["lot_usdt"]

    for (base, quote), exchanges in all_data.items():
        ex_list = list(exchanges.items())
        if len(ex_list) < 2:
            continue
        quote_rate = get_quote_usdt_rate(all_data, quote)
        if quote_rate is None:
            continue
        vol_quote = lot_usdt / quote_rate

        for i in range(len(ex_list)):
            for j in range(len(ex_list)):
                if i == j:
                    continue
                buy_ex,  buy_d  = ex_list[i]
                sell_ex, sell_d = ex_list[j]
                buy_price  = buy_d.get("ask", 0)
                sell_price = sell_d.get("bid", 0)
                if buy_price <= 0 or sell_price <= buy_price:
                    continue
                buy_fee  = FEES.get(buy_ex,  0.1) / 100
                sell_fee = FEES.get(sell_ex, 0.1) / 100
                gross_pct = (sell_price - buy_price) / buy_price * 100
                net_pct   = gross_pct - buy_fee * 100 - sell_fee * 100
                if net_pct < min_pct:
                    continue
                coins = vol_quote / buy_price
                profit_quote = coins * sell_price * (1 - sell_fee) - vol_quote * (1 + buy_fee)
                profit_usdt = profit_quote * quote_rate
                results.append({
                    "symbol":       base,
                    "quote":        quote,
                    "buy_ex":       buy_ex,
                    "sell_ex":      sell_ex,
                    "buy_price":    buy_price,
                    "sell_price":   sell_price,
                    "gross_pct":    round(gross_pct, 4),
                    "net_pct":      round(net_pct, 4),
                    "profit_quote": round(profit_quote, 8),
                    "profit_usdt":  round(profit_usdt, 4),
                    "coins":        round(coins, 6),
                    "volume_quote": round(vol_quote, 8),
                    "volume_usdt":  lot_usdt,
                    "quote_rate":   quote_rate,
                    "time":         datetime.now().strftime("%H:%M:%S"),
                })

    results.sort(key=lambda x: x["net_pct"], reverse=True)
    return results


def format_signal(opp: dict) -> str:
    quote = opp["quote"]
    p10 = round(opp["profit_usdt"] * 10, 2)
    p50 = round(opp["profit_usdt"] * 50, 2)
    if quote == "USDT":
        profit_line = f"💰 *Прибыль на лот ({opp['volume_usdt']} USDT):* `~{opp['profit_usdt']} USDT`\n"
    else:
        profit_line = (
            f"💰 *Прибыль на лот (~{opp['volume_usdt']} USDT в {quote}):* "
            f"`~{opp['profit_quote']} {quote}` (`~{opp['profit_usdt']} USDT` по курсу {round(opp['quote_rate'],2)})\n"
            f"   ⏳ Копится в балансе {quote}, конвертация в USDT — по достижении порога (см. /balances)\n"
        )
    warning = ""
    if opp["gross_pct"] >= SUSPICIOUS_SPREAD_PCT:
        warning = (
            f"\n⚠️⚠️ *Спред {opp['gross_pct']}% ОЧЕНЬ большой для этих бирж — скорее всего "
            f"устаревшие/рассинхронные данные (или разная стадия миграции токена), а не настоящая "
            f"возможность.*\n"
            f"Проверь `/verify {opp['symbol']}` прежде чем доверять этой цифре — команда явно "
            f"сверит и глубину стакана, и совпадение цены между биржами.\n"
        )
    vol = get_volatility_pct(opp["symbol"])
    if vol is not None and vol >= 2.0:
        warning += (
            f"\n🌪 *Волатильность {opp['symbol']} за 15 мин: {vol}%* — при таком движении "
            f"спред может быть моментум-эффектом (котировки не успевают синхронизироваться "
            f"между биржами), а не устойчивой возможностью. Перепроверь чуть позже, когда "
            f"рынок успокоится.\n"
        )
    return (
        f"🚨 *АРБИТРАЖ: {opp['buy_ex']} → {opp['sell_ex']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 СКРИНИНГ (только мониторинг, реальных ордеров нет)\n\n"
        f"💱 *{opp['symbol']}/{quote}*\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена: `{opp['buy_price']} {quote}`\n"
        f"   Лот: `{opp['volume_quote']} {quote}` (~{opp['volume_usdt']} USDT)\n"
        f"   Получишь: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена: `{opp['sell_price']} {quote}`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n"
        f"{warning}\n"
        f"{profit_line}"
        f"   x10 лотов → `~{p10} USDT` | x50 лотов → `~{p50} USDT`\n\n"
        f"⚠️ Цена актуальна только сейчас!\n\n"
        f"🕐 {opp['time']}"
    )


async def fetch_all(session):
    results = await asyncio.gather(
        get_binance(session), get_kucoin(session), get_mexc(session),
        return_exceptions=True
    )
    ex_names = ALL_EXCHANGES
    all_data: Dict[tuple, Dict] = {}
    active = []
    counts = {}

    for ex_name, result in zip(ex_names, results):
        if isinstance(result, Exception) or not result:
            counts[ex_name] = 0
            continue
        active.append(ex_name)
        counts[ex_name] = len(result)
        for pair, price_data in result.items():
            all_data.setdefault(pair, {})[ex_name] = price_data

    logger.info(f"Пар с биржи: {counts}")
    return all_data, active


async def scan_cycle(session):
    stats["scans"] += 1
    all_data, active = await fetch_all(session)
    for (base, quote), exchanges in all_data.items():
        if quote != "USDT" or not exchanges:
            continue
        mids = [(d["bid"] + d["ask"]) / 2 for d in exchanges.values()
                if d.get("bid", 0) > 0 and d.get("ask", 0) > 0]
        if mids:
            record_symbol_price(base, sum(mids) / len(mids))
    if len(active) < 2:
        return [], active
    opps = find_arbitrage(all_data)
    if opps:
        stats["signals"] += len(opps)
        hour = datetime.utcnow().hour
        hourly_signals[hour] += len(opps)
        hourly_plausible_signals[hour] += sum(
            1 for o in opps if o["gross_pct"] < SUSPICIOUS_SPREAD_PCT
        )
        for o in opps:
            cs = coin_stats.setdefault(o["symbol"], {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            cs["signals"] += 1
            cs["best_net_pct"] = max(cs["best_net_pct"], o["net_pct"])

            pk = (o["symbol"], o["quote"])
            ps = pair_stats.setdefault(pk, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            ps["signals"] += 1
            ps["best_net_pct"] = max(ps["best_net_pct"], o["net_pct"])

            rk = (o["buy_ex"], o["sell_ex"])
            rs = route_stats.setdefault(rk, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0, "coins": set()})
            rs["signals"] += 1
            rs["best_net_pct"] = max(rs["best_net_pct"], o["net_pct"])
            rs["coins"].add(f"{o['symbol']}/{o['quote']}")

            rck = (o["buy_ex"], o["sell_ex"], o["symbol"])
            rcs = route_coin_stats.setdefault(rck, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            rcs["signals"] += 1
            rcs["best_net_pct"] = max(rcs["best_net_pct"], o["net_pct"])

            hrk = (hour, o["buy_ex"], o["sell_ex"])
            hourly_route_signals[hrk] += 1
            if o["gross_pct"] < SUSPICIOUS_SPREAD_PCT:
                hourly_route_plausible[hrk] += 1

            route_key = (o["buy_ex"], o["sell_ex"])
            if route_key in TARGET_ROUTES and o["quote"] == "USDT":
                auto_route_candidates[route_key].add(o["symbol"])
    return opps, active


async def execute_sim(opp: dict, session=None):
    if not check_trade_limit():
        logger.info(f"Trade limit reached ({config['max_trades_per_min']}/min), skipping")
        return

    quote = opp["quote"]
    trade = {
        "id":          len(trade_history) + 1,
        "time":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol":      opp["symbol"],
        "quote":       quote,
        "buy_ex":      opp["buy_ex"],
        "sell_ex":     opp["sell_ex"],
        "buy_price":   opp["buy_price"],
        "sell_price":  opp["sell_price"],
        "net_pct":     opp["net_pct"],
        "profit_usdt": opp["profit_usdt"],
        "profit_quote": opp["profit_quote"],
    }
    trade_history.append(trade)
    stats["trades_sim"]        += 1
    stats["trades_this_minute"] += 1

    cs = coin_stats.setdefault(opp["symbol"], {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
    cs["trades"] += 1
    cs["profit_usdt"] += opp["profit_usdt"]

    pk = (opp["symbol"], quote)
    ps = pair_stats.setdefault(pk, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
    ps["trades"] += 1
    ps["profit_usdt"] += opp["profit_usdt"]

    rk = (opp["buy_ex"], opp["sell_ex"])
    rs = route_stats.setdefault(rk, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0, "coins": set()})
    rs["trades"] += 1
    rs["profit_usdt"] += opp["profit_usdt"]
    rs["coins"].add(f"{opp['symbol']}/{quote}")

    rck = (opp["buy_ex"], opp["sell_ex"], opp["symbol"])
    rcs = route_coin_stats.setdefault(rck, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
    rcs["trades"] += 1
    rcs["profit_usdt"] += opp["profit_usdt"]

    if quote == "USDT":
        stats["profit_sim"] += opp["profit_usdt"]
        stats["profit_since_alert"] += opp["profit_usdt"]
    else:
        currency_balances[quote] = currency_balances.get(quote, 0.0) + opp["profit_quote"]
        pending_value_usdt = currency_balances[quote] * opp["quote_rate"]
        logger.info(f"Накоплено в {quote}: {round(currency_balances[quote], 8)} (~{round(pending_value_usdt, 2)} USDT)")

        if pending_value_usdt >= config["convert_threshold_usdt"]:
            converted_amount = currency_balances[quote]
            converted_usdt = pending_value_usdt
            currency_balances[quote] = 0.0
            stats["profit_sim"] += converted_usdt
            stats["profit_since_alert"] += converted_usdt
            conversions_log.append({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "currency": quote, "amount": round(converted_amount, 8),
                "usdt_value": round(converted_usdt, 4), "rate": opp["quote_rate"],
            })
            logger.info(f"КОНВЕРТАЦИЯ: {round(converted_amount,8)} {quote} → {round(converted_usdt,4)} USDT")
            if session is not None:
                await send_tg(session,
                    f"💱 *Автоконвертация*\n"
                    f"Накопилось `{round(converted_amount, 8)} {quote}` (~`{round(converted_usdt, 2)} USDT`) — "
                    f"порог `{config['convert_threshold_usdt']} USDT` достигнут, конвертировано в USDT.\n"
                    f"Курс: `1 {quote} = {round(opp['quote_rate'], 2)} USDT`"
                )

    logger.info(
        f"SIM #{trade['id']}: {opp['symbol']}/{quote} {opp['buy_ex']}→{opp['sell_ex']} "
        f"+{opp['net_pct']}% +{opp['profit_quote']} {quote} (~{opp['profit_usdt']} USDT) "
        f"[{stats['trades_this_minute']}/{config['max_trades_per_min']} этой минуты]"
    )

    if stats["profit_since_alert"] <= -config["stop_loss_usdt"]:
        logger.warning(f"Стоп-лосс достигнут (P&L с последнего уведомления: {round(stats['profit_since_alert'], 2)})")
        if session is not None:
            await send_tg(session,
                f"⚠️ *Стоп-лосс достигнут* (бот работает дальше без остановки — это только мониторинг)\n"
                f"P&L с последнего уведомления: `{round(stats['profit_since_alert'], 2)} USDT` (порог: -{config['stop_loss_usdt']} USDT)\n"
                f"Общий P&L за всё время: `{round(stats['profit_sim'], 2)} USDT`"
            )
        stats["profit_since_alert"] = 0.0


# ═══════════════════════════════════════════════════════════════
# ТРЕУГОЛЬНЫЙ АРБИТРАЖ — на КАЖДОЙ бирже отдельно.
#
# ИЗМЕНЕНО (по прямому запросу пользователя — треугольник по ВСЕМ
# монетам, а не отдельному маленькому списку): раньше здесь был отдельный
# TRIANGLE_SYMBOLS (~12 монет специально отобранных как ликвидные). Такой
# отдельный список больше не используется в сканировании — используется
# полный SYMBOLS (~110 монет). Переменная оставлена только как алиас для
# обратной совместимости (на случай, если где-то ещё используется имя).
# ═══════════════════════════════════════════════════════════════
TRIANGLE_SYMBOLS: List[str] = list(SYMBOLS)
TRIANGLE_BRIDGE = "BTC"


def fmt_pair(ex: str, base: str, quote: str) -> str:
    if ex == "KuCoin":
        return f"{base}-{quote}"
    elif ex == "Gate":
        return f"{base}_{quote}"
    elif ex == "HTX":
        return f"{base.lower()}{quote.lower()}"
    else:
        return f"{base}{quote}"


async def calc_triangle_on_exchange(session, ex: str, alt: str, bridge: str,
                                     lot_usdt: float) -> List[dict]:
    ob_fn = ORDERBOOK_FN.get(ex)
    if not ob_fn:
        return []
    if alt == bridge or alt == "USDT":
        return []

    bridge_usdt_sym = fmt_pair(ex, bridge, "USDT")
    alt_bridge_sym = fmt_pair(ex, alt, bridge)
    alt_usdt_sym = fmt_pair(ex, alt, "USDT")

    book_bridge_usdt, book_alt_bridge, book_alt_usdt = await asyncio.gather(
        ob_fn(session, bridge_usdt_sym),
        ob_fn(session, alt_bridge_sym),
        ob_fn(session, alt_usdt_sym),
        return_exceptions=True,
    )
    for b in (book_bridge_usdt, book_alt_bridge, book_alt_usdt):
        if isinstance(b, Exception) or not b or not b.get("bids") or not b.get("asks"):
            return []

    fee = FEES.get(ex, 0.1) / 100
    found = []

    bridge_qty, _, _, full1 = _walk_by_notional(book_bridge_usdt["asks"], lot_usdt)
    if full1 and bridge_qty > 0:
        bridge_after_fee = bridge_qty * (1 - fee)
        alt_qty, _, _, full2 = _walk_by_notional(book_alt_bridge["asks"], bridge_after_fee)
        if full2 and alt_qty > 0:
            alt_after_fee = alt_qty * (1 - fee)
            usdt_out, _, full3 = _walk_by_qty(book_alt_usdt["bids"], alt_after_fee)
            if full3 and usdt_out > 0:
                final_usdt = usdt_out * (1 - fee)
                profit = final_usdt - lot_usdt
                net_pct = profit / lot_usdt * 100
                if net_pct >= config["min_profit_pct"]:
                    found.append({
                        "exchange": ex, "symbol": alt, "bridge": bridge,
                        "path": f"USDT→{bridge}→{alt}→USDT",
                        "net_pct": round(net_pct, 4), "profit_usdt": round(profit, 4),
                        "levels": 3, "time": datetime.now().strftime("%H:%M:%S"),
                    })

    alt_qty2, _, _, full1b = _walk_by_notional(book_alt_usdt["asks"], lot_usdt)
    if full1b and alt_qty2 > 0:
        alt_after_fee2 = alt_qty2 * (1 - fee)
        bridge_out, _, full2b = _walk_by_qty(book_alt_bridge["bids"], alt_after_fee2)
        if full2b and bridge_out > 0:
            bridge_after_fee2 = bridge_out * (1 - fee)
            usdt_out2, _, full3b = _walk_by_qty(book_bridge_usdt["bids"], bridge_after_fee2)
            if full3b and usdt_out2 > 0:
                final_usdt2 = usdt_out2 * (1 - fee)
                profit2 = final_usdt2 - lot_usdt
                net_pct2 = profit2 / lot_usdt * 100
                if net_pct2 >= config["min_profit_pct"]:
                    found.append({
                        "exchange": ex, "symbol": alt, "bridge": bridge,
                        "path": f"USDT→{alt}→{bridge}→USDT",
                        "net_pct": round(net_pct2, 4), "profit_usdt": round(profit2, 4),
                        "levels": 3, "time": datetime.now().strftime("%H:%M:%S"),
                    })

    return found


async def scan_all_triangles(session) -> List[dict]:
    """ИЗМЕНЕНО (по прямому запросу пользователя — треугольник по ВСЕМ
    монетам, только KuCoin и MEXC): раньше проверялся маленький список
    TRIANGLE_SYMBOLS (~12 монет) на ВСЕХ биржах включая Binance. Теперь —
    полный список SYMBOLS (~110 монет), только на KuCoin и MEXC (Binance
    заблокирован по API на территории пользователя, и не нужен для этой
    схемы, работающей внутри одной биржи).

    ВНИМАНИЕ: это ~660 запросов за один вызов (2 биржи × ~108 монет × 3
    пары) — заметно больше, чем раньше (~70). Разумно для разовой,
    ручной команды /triangle, но НЕ рекомендуется запускать это часто
    в фоновом автоматическом цикле — риск упереться в ограничения
    скорости запросов (rate limit) бирж."""
    tasks = []
    for ex in TRIANGLE_EXCHANGES:
        for alt in SYMBOLS:
            if alt == TRIANGLE_BRIDGE:
                continue
            tasks.append(calc_triangle_on_exchange(session, ex, alt, TRIANGLE_BRIDGE, config["lot_usdt"]))
    results_nested = await asyncio.gather(*tasks, return_exceptions=True)
    found = []
    for r in results_nested:
        if isinstance(r, Exception):
            continue
        found.extend(r)
    found.sort(key=lambda x: x["net_pct"], reverse=True)
    return found


GRID_FEE_PCT = float(os.environ.get("GRID_FEE_PCT", "0.1"))
grids: Dict[str, dict] = {}


async def get_price_binance_simple(session, symbol: str) -> Optional[dict]:
    try:
        async with session.get(f"{BINANCE_MARKET_BASE}/api/v3/ticker/bookTicker",
                                params={"symbol": f"{symbol}USDT"},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            bid = float(d.get("bidPrice", 0) or 0)
            ask = float(d.get("askPrice", 0) or 0)
            if bid <= 0 or ask <= 0:
                return None
            return {"bid": bid, "ask": ask}
    except Exception as e:
        logger.error(f"Grid price fetch {symbol}: {e}")
        return None


def make_grid(symbol: str, low: float, high: float, levels: int, lot_usdt: float,
               start_price: Optional[float] = None) -> dict:
    step = (high - low) / (levels - 1)
    lines = [round(low + step * i, 8) for i in range(levels)]
    cells = []
    for i in range(levels - 1):
        buy_line = lines[i]
        armed = (start_price is None) or (start_price > buy_line)
        cells.append({
            "buy_line": buy_line, "sell_line": lines[i + 1],
            "held": False, "bought_at": None, "armed": armed,
            "qty": lot_usdt / buy_line,
        })
    return {
        "symbol": symbol, "low": low, "high": high, "levels": levels, "lot_usdt": lot_usdt,
        "cells": cells, "started_at": datetime.now(), "trades": 0, "profit_usdt": 0.0,
        "trade_log": [], "current_price": None,
        "price_touches_below_range": 0, "price_touches_above_range": 0,
    }


async def check_grid(session, symbol: str):
    grid = grids.get(symbol)
    if not grid:
        return
    price = await get_price_binance_simple(session, symbol)
    if not price:
        return
    grid["current_price"] = price

    if price["bid"] > grid["high"]:
        grid["price_touches_above_range"] += 1
    elif price["ask"] < grid["low"]:
        grid["price_touches_below_range"] += 1

    for cell in grid["cells"]:
        fee = GRID_FEE_PCT / 100
        if not cell["held"]:
            if cell["armed"] and price["ask"] <= cell["buy_line"]:
                cell["held"] = True
                cell["armed"] = False
                cell["bought_at"] = price["ask"]
                grid["trades"] += 1
                grid["trade_log"].append({"time": datetime.now().strftime("%H:%M:%S"),
                                            "side": "BUY", "price": price["ask"],
                                            "level": f"{cell['buy_line']}->{cell['sell_line']}"})
            elif not cell["armed"] and price["ask"] > cell["buy_line"]:
                cell["armed"] = True
        elif cell["held"] and price["bid"] >= cell["sell_line"]:
            buy_price = cell["bought_at"]
            sell_price = price["bid"]
            qty = cell["qty"]
            net_profit = qty * (sell_price - buy_price) - fee * qty * (buy_price + sell_price)
            cell["held"] = False
            cell["bought_at"] = None
            grid["trades"] += 1
            grid["profit_usdt"] += net_profit
            grid["trade_log"].append({"time": datetime.now().strftime("%H:%M:%S"),
                                        "side": "SELL", "price": sell_price,
                                        "level": f"{cell['buy_line']}->{cell['sell_line']}",
                                        "profit": round(net_profit, 4)})
    if len(grid["trade_log"]) > 200:
        grid["trade_log"] = grid["trade_log"][-200:]


def format_grid_stats(grid: dict) -> str:
    uptime = datetime.now() - grid["started_at"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    held_cells = sum(1 for c in grid["cells"] if c["held"])
    unrealized = sum(
        c["qty"] * ((grid["current_price"]["bid"] if grid["current_price"] else c["bought_at"]) - c["bought_at"])
        for c in grid["cells"] if c["held"] and c["bought_at"]
    )
    price_line = ""
    if grid["current_price"]:
        price_line = f"Текущая цена: `{grid['current_price']['bid']}` / `{grid['current_price']['ask']}` (bid/ask)\n"
    return (
        f"📊 *GRID — {grid['symbol']}/USDT*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Работает: {h}ч {m}м\n"
        f"Диапазон: `{grid['low']}` — `{grid['high']}` ({grid['levels']} уровней, {len(grid['cells'])} ячеек)\n"
        f"Лот на уровень: `${grid['lot_usdt']}`\n{price_line}\n"
        f"✅ Сделок всего: {grid['trades']}\n"
        f"💰 Реализованная прибыль: `{round(grid['profit_usdt'], 4)} USDT`\n"
        f"📦 Занятых ячеек сейчас: {held_cells}/{len(grid['cells'])}\n"
        f"📈 Незафиксированный P&L по открытым ячейкам: `{round(unrealized, 4)} USDT`\n\n"
        f"⚠️ Цена выходила НИЖЕ диапазона: {grid['price_touches_below_range']} раз(а)\n"
        f"⚠️ Цена выходила ВЫШЕ диапазона: {grid['price_touches_above_range']} раз(а)\n"
        f"_(если эти числа растут — сетка простаивает, цена ушла за границы)_\n\n"
        f"Это СИМУЛЯЦИЯ на реальных ценах Binance — реальных денег бот не касается."
    )


async def grid_loop(session):
    while True:
        try:
            for symbol in list(grids.keys()):
                await check_grid(session, symbol)
        except Exception as e:
            logger.error(f"Grid loop error: {e}")
        await asyncio.sleep(5)


GATE_FUTURES_BASE = "https://api.gateio.ws/api/v4"
funding_positions: Dict[str, dict] = {}
_funding_cache: dict = {"data": None, "ts": 0.0}


async def _fetch_gate_futures_tickers(session) -> List[dict]:
    now = time.time()
    if _funding_cache["data"] is not None and now - _funding_cache["ts"] < 15:
        return _funding_cache["data"]
    try:
        async with session.get(f"{GATE_FUTURES_BASE}/futures/usdt/tickers",
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if isinstance(data, list):
                _funding_cache["data"] = data
                _funding_cache["ts"] = now
                return data
            return []
    except Exception as e:
        logger.error(f"Gate futures tickers fetch: {e}")
        return []


_funding_interval_cache: Dict[str, int] = {}


async def get_funding_interval_sec(session, symbol: str) -> int:
    if symbol in _funding_interval_cache:
        return _funding_interval_cache[symbol]
    try:
        async with session.get(f"{GATE_FUTURES_BASE}/futures/usdt/contracts/{symbol}_USDT",
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            interval = int(d.get("funding_interval", 28800) or 28800)
            _funding_interval_cache[symbol] = interval
            return interval
    except Exception as e:
        logger.error(f"Funding interval fetch {symbol}: {e}")
        return 28800


async def get_funding_rate(session, symbol: str) -> Optional[dict]:
    tickers = await _fetch_gate_futures_tickers(session)
    contract = f"{symbol}_USDT"
    for item in tickers:
        if item.get("contract") == contract:
            rate = item.get("funding_rate")
            if rate is None:
                return None
            try:
                interval_sec = await get_funding_interval_sec(session, symbol)
                return {
                    "rate": float(rate),
                    "mark_price": float(item.get("mark_price", 0) or 0),
                    "next_funding_time": int(item.get("funding_next_apply", 0) or 0) * 1000,
                    "interval_hours": interval_sec / 3600,
                }
            except (TypeError, ValueError):
                return None
    return None


async def get_all_funding_rates(session) -> List[dict]:
    tickers = await _fetch_gate_futures_tickers(session)
    out = []
    for item in tickers:
        contract = item.get("contract", "")
        if not contract.endswith("_USDT"):
            continue
        base = contract[:-5]
        rate = item.get("funding_rate")
        if rate is None:
            continue
        try:
            rate = float(rate)
        except (TypeError, ValueError):
            continue
        out.append({"symbol": base, "rate": rate,
                     "mark_price": float(item.get("mark_price", 0) or 0)})
    return out


async def get_funding_rate_history(session, symbol: str, limit: int = 12) -> Optional[List[dict]]:
    try:
        async with session.get(f"{GATE_FUTURES_BASE}/futures/usdt/funding_rate",
                                params={"contract": f"{symbol}_USDT", "limit": limit},
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if not isinstance(data, list) or not data:
                logger.error(f"Funding history {symbol}: неожиданный формат ответа: {data}")
                return None
            out = []
            for item in data:
                ts = item.get("t") or item.get("time") or item.get("timestamp")
                rate = item.get("r")
                if rate is None:
                    rate = item.get("rate") or item.get("funding_rate")
                if ts is None or rate is None:
                    logger.error(f"Funding history {symbol}: не нашёл поля времени/ставки "
                                  f"в записи: {item}")
                    continue
                try:
                    out.append({"time": int(ts), "rate": float(rate)})
                except (TypeError, ValueError):
                    continue
            return out if out else None
    except Exception as e:
        logger.error(f"Funding history fetch {symbol}: {e}")
        return None


async def check_funding_position(session, symbol: str):
    pos = funding_positions.get(symbol)
    if not pos:
        return
    info = await get_funding_rate(session, symbol)
    if not info:
        return

    now = datetime.now()
    elapsed_hours = (now - pos["last_check"]).total_seconds() / 3600
    pos["last_check"] = now
    pos["last_rate"] = info["rate"]
    pos["last_mark_price"] = info["mark_price"]
    pos["interval_hours"] = info["interval_hours"]

    accrued = pos["capital_usdt"] * info["rate"] * (elapsed_hours / info["interval_hours"])
    pos["accrued_usdt"] += accrued
    pos["checks"] += 1


def format_funding_stats(pos: dict) -> str:
    uptime = datetime.now() - pos["started_at"]
    h = int(uptime.total_seconds() // 3600)
    m = int((uptime.total_seconds() % 3600) // 60)
    rate_pct = pos.get("last_rate", 0) * 100
    interval_h = pos.get("interval_hours", 8)
    payments_per_day = 24 / interval_h
    daily_estimate = pos["capital_usdt"] * pos.get("last_rate", 0) * payments_per_day
    monthly_estimate = daily_estimate * 30
    return (
        f"💸 *ФАНДИНГ-ПОЗИЦИЯ — {pos['symbol']}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Схема: лонг спот + шорт фьючерс, капитал `${pos['capital_usdt']}`\n"
        f"Работает: {h}ч {m}м | Проверок: {pos['checks']}\n\n"
        f"📊 Текущая ставка фандинга: `{rate_pct:.4f}%` за {interval_h:.0f}ч "
        f"(у этой монеты именно такой интервал, не всегда 8ч!)\n"
        f"   {'🟢 Вы ПОЛУЧАЕТЕ (шорт при положительной ставке)' if pos.get('last_rate',0) >= 0 else '🔴 Вы ПЛАТИТЕ (ставка отрицательная)'}\n\n"
        f"💰 Накоплено (симуляция, с начала): `{round(pos['accrued_usdt'], 4)} USDT`\n\n"
        f"📈 Оценка при ТЕКУЩЕЙ ставке (может измениться):\n"
        f"   В день: ~`{round(daily_estimate, 4)} USDT`\n"
        f"   В месяц: ~`{round(monthly_estimate, 2)} USDT`\n\n"
        f"⚠️ Это СИМУЛЯЦИЯ — реальных позиций на споте/фьючерсах бот не открывает.\n"
        f"⚠️ Не учитывает: ценовой риск при неидеальном хедже, комиссии за открытие "
        f"позиций, риск ликвидации на плече, изменение ставки между проверками."
    )


async def funding_loop(session):
    while True:
        try:
            for symbol in list(funding_positions.keys()):
                await check_funding_position(session, symbol)
        except Exception as e:
            logger.error(f"Funding loop error: {e}")
        await asyncio.sleep(60)


async def daily_digest_loop(session):
    await asyncio.sleep(3600)
    while True:
        try:
            if CHAT_ID and (grids or funding_positions):
                msg = "🌅 *ЕЖЕДНЕВНЫЙ ДАЙДЖЕСТ — Grid и Фандинг*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                if grids:
                    msg += "*Grid-сетки:*\n"
                    for sym, g in grids.items():
                        held = sum(1 for c in g["cells"] if c["held"])
                        uptime_h = (datetime.now() - g["started_at"]).total_seconds() / 3600
                        daily_rate = g["profit_usdt"] / uptime_h * 24 if uptime_h > 0 else 0
                        msg += (f"  *{sym}*: {g['trades']} сделок за {uptime_h:.0f}ч, "
                                f"прибыль `{round(g['profit_usdt'],4)} USDT` "
                                f"(~{round(daily_rate,4)}/сутки при текущем темпе), "
                                f"занято {held}/{len(g['cells'])}\n")
                    msg += "\n"
                if funding_positions:
                    msg += "*Фандинг-позиции:*\n"
                    for sym, pos in funding_positions.items():
                        uptime_h = (datetime.now() - pos["started_at"]).total_seconds() / 3600
                        daily_rate = pos["accrued_usdt"] / uptime_h * 24 if uptime_h > 0 else 0
                        msg += (f"  *{sym}*: накоплено `{round(pos['accrued_usdt'],4)} USDT` "
                                f"за {uptime_h:.0f}ч (~{round(daily_rate,4)}/сутки при текущем темпе)\n")
                msg += "\n_Подробнее — /gridstats, /fundingstats. Это симуляция, не реальные ордера._"
                await send_tg(session, msg)
        except Exception as e:
            logger.error(f"Daily digest error: {e}")
        await asyncio.sleep(86400)


# НОВОЕ (по прямому запросу пользователя — "чтобы в автомате искал нужные
# монеты на треугольный арбитраж на площадке KuCoin и MEXC"): фоновый цикл,
# который сам, без ручного /triangle, регулярно сканирует ВСЕ монеты на
# обеих биржах и уведомляет, если найдёт реальную возможность выше порога.
#
# Интервал НАМЕРЕННО больше, чем у основного скана (config["scan_interval"],
# по умолчанию 6 сек) — один проход /triangle это ~660 запросов (2 биржи ×
# ~108 монет × 3 пары), и делать это каждые несколько секунд рискует
# упереться в ограничения скорости запросов (rate limit) обеих бирж.
config["triangle_scan_interval_sec"] = 180  # раз в 3 минуты по умолчанию,
    # настраивается командой /settriangleinterval
TRIANGLE_SCAN_COOLDOWN_SEC = 600  # не спамить одной и той же находкой чаще раза в 10 минут
_last_triangle_alert: Dict[Tuple[str, str, str], float] = {}


async def triangle_scan_loop(session):
    """Фоновый аналог /triangle — сканирует автоматически, на паузу
    (глобальная кнопка) не завязан, т.к. это отдельная, независимая от
    основного арбитражного скрининга функция без реальных сделок."""
    await asyncio.sleep(45)  # даём боту время подняться, не сразу тяжёлый скан
    while True:
        try:
            results = await scan_all_triangles(session)
            now_ts = time.time()
            fresh = []
            for r in results:
                key = (r["exchange"], r["symbol"], r["path"])
                if now_ts - _last_triangle_alert.get(key, 0) < TRIANGLE_SCAN_COOLDOWN_SEC:
                    continue
                _last_triangle_alert[key] = now_ts
                fresh.append(r)
            if fresh and CHAT_ID:
                msg = (f"🔺 *АВТО-НАЙДЕНО: треугольный арбитраж на "
                       f"{', '.join(TRIANGLE_EXCHANGES)}!*\n━━━━━━━━━━━━━━━━━━━━━━\n\n")
                for r in fresh[:10]:
                    msg += (f"*{r['exchange']}* — {r['symbol']} via {r['path']}\n"
                            f"   Чистая: `{r['net_pct']}%` | Профит на лот "
                            f"(${config['lot_usdt']}): `{r['profit_usdt']} USDT`\n\n")
                await send_tg(session, msg)
            logger.info(f"Triangle auto-scan: {len(results)} возможностей выше порога "
                         f"{config['min_profit_pct']}%, {len(fresh)} новых (не в кулдауне)")
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Triangle scan loop error: {e}")
        await asyncio.sleep(config.get("triangle_scan_interval_sec", 180))


async def handle_command(session, text, chat_id):
    global CHAT_ID
    CHAT_ID = chat_id
    parts = text.strip().split()
    cmd = parts[0].lower()

    if cmd == "/start":
        await send_tg(session,
            f"✅ *ArbScreenerBot — МОНИТОР КАНДИДАТОВ (без реальной торговли)*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Назначение: находить и проверять монеты-кандидаты для реальной торговли "
            f"в другом боте (WorkerArbBot). Этот бот НИКОГДА не отправляет реальные "
            f"ордера — такой возможности физически нет в коде.\n\n"
            f"Мониторинг: {len(SYMBOLS)} монет: {', '.join(SYMBOLS)}\n"
            f"Площадки: {', '.join(ALL_EXCHANGES)}\n"
            f"Валюты котировки: {', '.join(QUOTE_CURRENCIES)}\n\n"
            f"⚙️ Лот/шаг сделки (для расчётов): `{config['lot_usdt']} USDT`-эквивалент\n"
            f"⚙️ Порог маржи: `{config['min_profit_pct']}%`\n"
            f"⚙️ Порог подозрительного спреда: `{SUSPICIOUS_SPREAD_PCT}%`\n"
            f"⚙️ Минимум уровней стакана для доверия: `{MIN_DEPTH_LEVELS}`\n\n"
            f"*Главные команды:*\n"
            f"/verify МОНЕТА1 МОНЕТА2 ... — проверить кандидатов по-настоящему "
            f"(глубина стакана + совпадение цены между биржами, до 8 монет)\n"
            f"/triangle — треугольный арбитраж по ВСЕМ {len(SYMBOLS)} монетам на "
            f"{', '.join(TRIANGLE_EXCHANGES)} (без Binance)\n"
            f"/startgrid СИМВОЛ НИЖЕ ВЫШЕ УРОВНЕЙ ЛОТ — учебный grid-симулятор (новое!)\n"
            f"/fundingtop — топ ставок фандинга на Gate.io Futures (новое!)\n"
            f"/startfunding СИМВОЛ КАПИТАЛ — симуляция фандинг-арбитража (новое!)\n"
            f"/fundinghistory СИМВОЛ — история ставки за последние сутки (новое!)\n"
            f"/dashboard — общая сводка по ВСЕМУ разом (новое!)\n"
            f"/report — топ-5 монет по сигналам за сессию\n"
            f"/depthcheck SYMBOL — подробная глубина стакана одной монеты\n"
            f"/leaderboard — рейтинг монет по числу сигналов\n\n"
            f"*Остальные:*\n"
            f"/scan — скан сейчас | /top — топ пар по спреду прямо сейчас\n"
            f"/prices SYMBOL — цены на всех биржах и валютах котировки\n"
            f"/exchanges — диагностика бирж\n"
            f"/pairs — рейтинг связок монета/валюта\n"
            f"/routes — рейтинг маршрутов биржа→биржа\n"
            f"/balances — накопленные BTC/ETH, ожидающие конвертации\n"
            f"/stats — статистика | /history — последние сигналы\n"
            f"/hours [БИРЖА1 БИРЖА2] — сигналы по часам UTC, можно с фильтром по маршруту (новое!)\n"
            f"/autoroutes — автоматический анализ узкого маршрута, сам присылает карточки (новое!)\n"
            f"/setprofit 0.15 — порог маржи | /setlot 100 — размер лота\n"
        )

    elif cmd == "/triangle":
        total_requests = len(TRIANGLE_EXCHANGES) * len(SYMBOLS) * 3
        await send_tg(session,
            f"🔺 Сканирую треугольный арбитраж на {', '.join(TRIANGLE_EXCHANGES)} "
            f"по ВСЕМ {len(SYMBOLS)} монетам, мост: {TRIANGLE_BRIDGE} "
            f"(~{total_requests} запросов, может занять 15-30 сек)...")
        results = await scan_all_triangles(session)
        if not results:
            await send_tg(session,
                f"😔 Нет треугольных возможностей выше порога {config['min_profit_pct']}% "
                f"ни на {', '.join(TRIANGLE_EXCHANGES)} прямо сейчас.\n"
                f"(Либо пары ALT/{TRIANGLE_BRIDGE} не существуют для части монет на "
                f"части бирж — это нормально, такие комбинации просто пропускаются.)\n\n"
                f"Хочешь увидеть, насколько БЛИЗКО рынок подходил к порогу — "
                f"`/triangletop` покажет лучшие результаты без фильтра."
            )
        else:
            msg = (f"🔺 *ТРЕУГОЛЬНЫЙ АРБИТРАЖ — {', '.join(TRIANGLE_EXCHANGES)}*\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
            for r in results[:10]:
                msg += (f"*{r['exchange']}* — {r['symbol']} via {r['path']}\n"
                        f"   Чистая: `{r['net_pct']}%` | Профит на лот "
                        f"(${config['lot_usdt']}): `{r['profit_usdt']} USDT`\n\n")
            await send_tg(session, msg)

    elif cmd == "/triangletop":
        await send_tg(session,
            f"🔺 Сканирую БЕЗ ПОРОГА по всем {len(SYMBOLS)} монетам на "
            f"{', '.join(TRIANGLE_EXCHANGES)} — показываю лучшие результаты, даже "
            f"отрицательные, чтобы увидеть, насколько рынок реально близок к "
            f"возможности (это может занять 15-30 сек)...")
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        results = await scan_all_triangles(session)
        config["min_profit_pct"] = saved
        if not results:
            await send_tg(session,
                "❌ Не удалось посчитать ни одной комбинации — либо нет пар ALT/BTC "
                "на выбранных биржах, либо сеть недоступна.")
            return
        msg = (f"🔺 *ТРЕУГОЛЬНИК — ТОП-15 БЕЗ ФИЛЬТРА (порог обычно {saved}%)*\n"
               f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for r in results[:15]:
            icon = "🟢" if r["net_pct"] >= saved else "🔴"
            msg += (f"{icon} *{r['exchange']}* — {r['symbol']} via {r['path']}\n"
                    f"   Чистая: `{r['net_pct']}%`\n\n")
        await send_tg(session, msg)

    elif cmd == "/settriangleinterval":
        # НОВОЕ (по прямому запросу пользователя — автоматический фоновый
        # поиск треугольника на KuCoin и MEXC): как часто фоновый цикл
        # сам сканирует ВСЕ монеты, без ручного /triangle.
        if len(parts) < 2:
            cur = config.get("triangle_scan_interval_sec", 180)
            await send_tg(session,
                f"Текущий интервал автоматического сканирования треугольника: "
                f"{cur} сек ({cur//60} мин)\n\n"
                f"⚠️ Не рекомендуется ставить меньше 60 сек — один проход это "
                f"~660 запросов к биржам (2 биржи × ~108 монет × 3 пары), "
                f"слишком частый опрос рискует упереться в rate limit.\n\n"
                f"Пример: `/settriangleinterval 120`"
            )
            return
        try:
            val = int(parts[1])
            if val < 60:
                await send_tg(session, "❌ Не менее 60 сек — иначе риск блокировки биржами "
                                        "за слишком частые запросы.")
                return
            config["triangle_scan_interval_sec"] = val
            await send_tg(session, f"✅ Интервал автосканирования треугольника: {val} сек")
        except ValueError:
            await send_tg(session, "❌ Пример: `/settriangleinterval 120`")

    elif cmd == "/fundingtop":
        await send_tg(session, "💸 Получаю ставки фандинга со всех фьючерсов Gate.io...")
        rates = await get_all_funding_rates(session)
        if not rates:
            await send_tg(session, "❌ Не удалось получить данные.")
            return
        rates.sort(key=lambda x: abs(x["rate"]), reverse=True)
        msg = "💸 *ТОП-20 СТАВОК ФАНДИНГА (Gate.io Futures)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for r in rates[:20]:
            annual_pct = r["rate"] * 3 * 365 * 100
            icon = "🟢" if r["rate"] >= 0 else "🔴"
            msg += (f"{icon} *{r['symbol']}*: `{r['rate']*100:.4f}%`/8ч "
                    f"(~{annual_pct:.0f}% годовых при неизменной ставке)\n")
        msg += ("\n_🟢 положительная ставка = шорт получает деньги | 🔴 отрицательная = "
                "шорт платит, лонг получает.\n"
                "Годовая цифра — грубая экстраполяция ТЕКУЩЕЙ ставки, реальная ставка "
                "меняется каждые 8 часов, не воспринимай как гарантию.\n"
                "`/startfunding СИМВОЛ КАПИТАЛ` — запустить симуляцию по конкретной монете._")
        await send_tg(session, msg)

    elif cmd == "/dashboard":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)

        msg = (
            f"🎛 *ОБЩАЯ СВОДКА — всё разом*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Аптайм бота: {h}ч {m}м\n\n"
        )

        msg += (
            f"*1️⃣ Обычный арбитраж (скрининг)*\n"
            f"   Сканов: {stats['scans']} | Сигналов: {stats['signals']} | "
            f"Сделок (сим.): {stats['trades_sim']}\n"
            f"   Прибыль (сим., за всё время): `{round(stats['profit_sim'], 2)} USDT`\n\n"
        )

        msg += (
            f"*2️⃣ Треугольный арбитраж*\n"
            f"   Монет в скане: {len(SYMBOLS)} (полный список)\n"
            f"   Биржи: {', '.join(TRIANGLE_EXCHANGES)} | Мост: {TRIANGLE_BRIDGE}\n"
            f"   `/triangle` — проверить прямо сейчас (займёт 15-30 сек)\n\n"
        )

        msg += f"*3️⃣ Grid-сетки* ({len(grids)} активных)\n"
        if not grids:
            msg += "   Нет активных сеток. `/startgrid СИМВОЛ НИЖЕ ВЫШЕ УРОВНЕЙ ЛОТ`\n\n"
        else:
            for sym, g in grids.items():
                held = sum(1 for c in g["cells"] if c["held"])
                msg += (f"   *{sym}*: {g['trades']} сделок, прибыль "
                        f"`{round(g['profit_usdt'],4)} USDT`, занято {held}/{len(g['cells'])} ячеек\n")
            msg += "\n"

        msg += f"*4️⃣ Фандинг-позиции* ({len(funding_positions)} активных)\n"
        if not funding_positions:
            msg += "   Нет активных позиций. `/startfunding СИМВОЛ КАПИТАЛ`\n"
        else:
            for sym, pos in funding_positions.items():
                rate = pos.get("last_rate", 0) * 100
                msg += (f"   *{sym}*: ставка `{rate:.4f}%`/8ч, накоплено "
                        f"`{round(pos['accrued_usdt'],4)} USDT` за {pos['checks']} проверок\n")

        threshold = config.get('worker_honest_threshold_pct', 4.0)
        routes_str = ", ".join(f"{b}→{s}" for b, s in TARGET_ROUTES) or "(пусто)"
        suspicious_count = stats.get("auto_signal_suspicious_skipped", 0)
        msg += (
            f"\n\n*5️⃣ Автоанализ узкого маршрута*\n"
            f"   Маршруты: {routes_str} | Текущая монета WorkerArbBot: "
            f"*{config.get('current_real_coin', '?')}*\n"
            f"   Боевой порог: *{threshold}%* | Отсеяно как аномалия: {suspicious_count}\n"
            f"   🎯 Сигналов прошло боевой порог: {len(qualified_signals_log)} "
            f"| `/qualifiedsignals` — посмотреть список\n"
        )

        msg += (f"\n\n_Подробности по каждому пункту: /stats, /triangle, /gridstats, "
                f"/fundingstats, /qualifiedsignals — эта сводка их не заменяет, только даёт быстрый обзор._")
        await send_tg(session, msg)

    elif cmd == "/fundinghistory":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/fundinghistory BTC`")
            return
        symbol = parts[1].upper()
        await send_tg(session, f"📜 Смотрю историю ставки фандинга для {symbol}...")
        history = await get_funding_rate_history(session, symbol, limit=12)
        if not history:
            await send_tg(session,
                f"❌ Не удалось получить историю для {symbol} — либо такого контракта "
                f"нет на Gate.io, либо формат ответа изменился (проверь Railway-логи, "
                f"там будет сырой ответ биржи для диагностики).")
            return
        msg = f"📜 *ИСТОРИЯ ФАНДИНГА — {symbol}* (последние {len(history)} интервалов по 8ч)\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for h in history:
            dt = datetime.fromtimestamp(h["time"]).strftime("%d.%m %H:%M")
            icon = "🟢" if h["rate"] >= 0 else "🔴"
            msg += f"{icon} {dt}: `{h['rate']*100:.4f}%`\n"
        rates_only = [h["rate"] for h in history]
        avg = sum(rates_only) / len(rates_only)
        msg += f"\n_Средняя ставка за период: {avg*100:.4f}% — так честнее оценивать долгосрочный доход, чем по одной текущей секунде._"
        await send_tg(session, msg)

    elif cmd == "/startfunding":
        if len(parts) < 3:
            await send_tg(session,
                "Пример: `/startfunding BTC 1000`\n"
                "(монета, условный капитал в USDT для симуляции схемы "
                "лонг-спот + шорт-фьючерс)")
            return
        symbol = parts[1].upper()
        try:
            capital = float(parts[2])
        except Exception:
            await send_tg(session, "❌ Капитал должен быть числом. Пример: `/startfunding BTC 1000`")
            return
        info = await get_funding_rate(session, symbol)
        if not info:
            await send_tg(session, f"❌ Не нашёл фьючерс {symbol}USDT на Gate.io Futures.")
            return
        funding_positions[symbol] = {
            "symbol": symbol, "capital_usdt": capital,
            "started_at": datetime.now(), "last_check": datetime.now(),
            "last_rate": info["rate"], "last_mark_price": info["mark_price"],
            "interval_hours": info["interval_hours"],
            "accrued_usdt": 0.0, "checks": 0,
        }
        await send_tg(session,
            f"✅ Фандинг-позиция запущена: *{symbol}*, капитал ${capital}\n"
            f"Текущая ставка: {info['rate']*100:.4f}%/{info['interval_hours']:.0f}ч\n\n"
            f"`/fundingstats {symbol}` — посмотреть накопленный результат."
        )

    elif cmd == "/fundingstats":
        if len(parts) < 2:
            if not funding_positions:
                await send_tg(session, "Нет активных фандинг-позиций. `/startfunding СИМВОЛ КАПИТАЛ`")
                return
            for sym, pos in funding_positions.items():
                await send_tg(session, format_funding_stats(pos))
            return
        symbol = parts[1].upper()
        if symbol not in funding_positions:
            await send_tg(session, f"❌ Нет активной позиции для {symbol}.")
            return
        await send_tg(session, format_funding_stats(funding_positions[symbol]))

    elif cmd == "/stopfunding":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/stopfunding BTC`")
            return
        symbol = parts[1].upper()
        if symbol not in funding_positions:
            await send_tg(session, f"❌ Нет активной позиции для {symbol}.")
            return
        pos = funding_positions.pop(symbol)
        await send_tg(session,
            f"⏹ Позиция {symbol} остановлена.\n"
            f"Итог: накоплено {round(pos['accrued_usdt'],4)} USDT за {pos['checks']} проверок."
        )

    elif cmd == "/startgrid":
        if len(parts) < 6:
            await send_tg(session,
                "Пример: `/startgrid SOL 140 160 10 20`\n"
                "(монета, нижняя граница, верхняя граница, число уровней, лот в USDT на уровень)\n\n"
                "Учебный grid-симулятор — реальных ордеров не отправляет, только считает "
                "на реальных ценах Binance.")
            return
        try:
            symbol = parts[1].upper()
            low = float(parts[2])
            high = float(parts[3])
            levels = int(parts[4])
            lot = float(parts[5])
            if high <= low or levels < 2 or lot <= 0:
                await send_tg(session, "❌ Проверь параметры: верхняя граница больше нижней, "
                                        "уровней минимум 2, лот больше нуля.")
                return
        except Exception:
            await send_tg(session, "❌ Не смог разобрать числа. Пример: `/startgrid SOL 140 160 10 20`")
            return
        price = await get_price_binance_simple(session, symbol)
        if not price:
            await send_tg(session, f"❌ Не нашёл цену {symbol}/USDT на Binance.")
            return
        grids[symbol] = make_grid(symbol, low, high, levels, lot, start_price=price["bid"])
        in_range = "✅ текущая цена ВНУТРИ диапазона" if low <= price["bid"] <= high else \
                   "⚠️ текущая цена СЕЙЧАС ВНЕ диапазона — сетка подождёт, пока цена зайдёт внутрь"
        await send_tg(session,
            f"✅ Сетка запущена: *{symbol}/USDT*\n"
            f"Диапазон: {low} — {high}, {levels} уровней, ${lot} на уровень\n"
            f"Текущая цена: {price['bid']}/{price['ask']}\n{in_range}\n\n"
            f"`/gridstats {symbol}` — посмотреть прогресс."
        )

    elif cmd == "/gridstats":
        if len(parts) < 2:
            if not grids:
                await send_tg(session, "Нет активных сеток. `/startgrid СИМВОЛ НИЖЕ ВЫШЕ УРОВНЕЙ ЛОТ`")
                return
            for sym, g in grids.items():
                await send_tg(session, format_grid_stats(g))
            return
        symbol = parts[1].upper()
        if symbol not in grids:
            await send_tg(session, f"❌ Нет активной сетки для {symbol}. `/listgrids`")
            return
        await send_tg(session, format_grid_stats(grids[symbol]))

    elif cmd == "/gridhistory":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/gridhistory SOL`")
            return
        symbol = parts[1].upper()
        if symbol not in grids:
            await send_tg(session, f"❌ Нет активной сетки для {symbol}.")
            return
        log = grids[symbol]["trade_log"][-15:][::-1]
        if not log:
            await send_tg(session, "Пока нет сделок в этой сетке.")
            return
        msg = f"📋 *Последние сделки — {symbol}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in log:
            if t["side"] == "BUY":
                msg += f"🟢 {t['time']} КУПИЛ @ `{t['price']}` (ячейка {t['level']})\n"
            else:
                msg += f"🔴 {t['time']} ПРОДАЛ @ `{t['price']}` — прибыль `{t['profit']} USDT` (ячейка {t['level']})\n"
        await send_tg(session, msg)

    elif cmd == "/stopgrid":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/stopgrid SOL`")
            return
        symbol = parts[1].upper()
        if symbol not in grids:
            await send_tg(session, f"❌ Нет активной сетки для {symbol}.")
            return
        g = grids.pop(symbol)
        await send_tg(session,
            f"⏹ Сетка {symbol} остановлена. Итог: {g['trades']} сделок, "
            f"прибыль {round(g['profit_usdt'],4)} USDT.")

    elif cmd == "/listgrids":
        if not grids:
            await send_tg(session, "Нет активных сеток.")
            return
        msg = "📋 *Активные сетки*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for sym, g in grids.items():
            msg += f"*{sym}*: {g['low']}-{g['high']}, {g['trades']} сделок, {round(g['profit_usdt'],4)} USDT\n"
        await send_tg(session, msg)

    elif cmd == "/verify":
        if len(parts) < 2:
            await send_tg(session,
                "Проверяет кандидатов по-настоящему: реальная глубина стакана "
                "(не top-of-book) + явное сравнение цены между биржами. Именно "
                "такая проверка поймала бы ложный сигнал по ZIL (цены разошлись "
                "на 10% при формально неплохой глубине).\n\n"
                "Пример: `/verify TRX DOGE XRP ADA LTC TON` (до 8 монет)"
            )
            return
        raw_candidates = " ".join(parts[1:]).replace(",", " ").replace(";", " ").split()
        candidates = [p.upper() for p in raw_candidates if p][:8]
        if not candidates:
            await send_tg(session, "Не нашёл ни одной монеты в команде. Пример: `/verify TRX DOGE XRP`")
            return
        await send_tg(session, f"🔍 Проверяю глубину и согласованность цены для: {', '.join(candidates)}...")

        results = []
        for sym in candidates:
            row = await verify_candidate(session, sym, config["lot_usdt"])
            results.append(row)

        msg = "📊 *ПРОВЕРКА КАНДИДАТОВ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for row in sorted(results, key=lambda r: (not r["ok"], r.get("cross_spread") or 999)):
            icon = "✅" if row["ok"] else "❌"
            msg += f"{icon} *{row['symbol']}*"
            if row["cross_spread"] is not None:
                msg += f" (разброс цены между биржами: {row['cross_spread']}%)"
            msg += "\n"
            for ex, d in row["exchanges"].items():
                if d is None:
                    msg += f"   {ex}: нет данных\n"
                else:
                    msg += (f"   {ex}: {d['ask_levels']}/{d['bid_levels']} уровней, "
                            f"проскальз. на лот: {d['slippage_pct']}%\n")
            if row["reasons"]:
                msg += f"   ⚠️ {'; '.join(row['reasons'])}\n"
            msg += "\n"
        msg += (f"_✅ = минимум {MIN_DEPTH_LEVELS} уровней с обеих сторон на обеих биржах, цены не "
                f"расходятся сильнее {SUSPICIOUS_SPREAD_PCT}% — только такие кандидаты стоит "
                f"рассматривать для реальной торговли в WorkerArbBot._")
        await send_tg(session, msg)

    elif cmd == "/scan":
        await send_tg(session, f"🔍 Сканирую {len(ALL_EXCHANGES)} бирж, {len(SYMBOLS)} монет...")
        opps, active = await scan_cycle(session)
        if not opps:
            await send_tg(session,
                f"😔 Нет сигналов (порог {config['min_profit_pct']}%).\n\n"
                f"Активных бирж: {len(active)} ({', '.join(active)})\n"
                f"Сканов: {stats['scans']}\n"
                f"/top — лучшие пары ниже порога, /exchanges — диагностика"
            )
        else:
            await send_tg(session, f"✅ Найдено {len(opps)} сигналов! Топ-3:")
            for opp in opps[:3]:
                await send_tg(session, format_signal(opp))
                if opp["gross_pct"] < SUSPICIOUS_SPREAD_PCT:
                    verified = await reverify_with_depth(session, opp)
                    if verified:
                        await execute_sim(verified, session)
                    else:
                        stats["depth_reverify_failed"] = stats.get("depth_reverify_failed", 0) + 1
                else:
                    stats["signals_suspicious_skipped"] = stats.get("signals_suspicious_skipped", 0) + 1

    elif cmd == "/exchanges":
        await send_tg(session, "🔍 Проверяю каждую биржу отдельно...")
        results = await asyncio.gather(
            get_binance(session), get_kucoin(session), get_mexc(session),
            return_exceptions=True
        )
        ex_names = ALL_EXCHANGES
        msg = "📡 *ДИАГНОСТИКА БИРЖ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for name, r in zip(ex_names, results):
            if isinstance(r, Exception):
                msg += f"❌ {name}: исключение `{r}`\n"
            elif not r:
                msg += f"⚠️ {name}: 0 пар (проверь сеть/гео-блок)\n"
            else:
                by_quote = {}
                for (base, quote) in r.keys():
                    by_quote[quote] = by_quote.get(quote, 0) + 1
                breakdown = ", ".join(f"{q}:{n}" for q, n in by_quote.items())
                msg += f"✅ {name}: {len(r)} пар всего ({breakdown})\n"
        await send_tg(session, msg)

    elif cmd == "/top":
        await send_tg(session, "📊 Ищу лучшие пары по обеим биржам и всем валютам котировки...")
        all_data, active = await fetch_all(session)
        if len(active) < 2:
            await send_tg(session, "❌ Недостаточно активных бирж.")
            return
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        opps = find_arbitrage(all_data)
        config["min_profit_pct"] = saved
        if not opps:
            await send_tg(session, "❌ Нет данных вообще ни по одной паре.")
            return
        msg = f"📊 *ТОП-20 — {datetime.now().strftime('%H:%M:%S')}*\n"
        msg += f"Бирж: {', '.join(active)} | Пар с данными: {len(all_data)}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, opp in enumerate(opps[:20], 1):
            icon = "🟢" if opp["net_pct"] >= config["min_profit_pct"] else "🔴"
            suspicious = " ⚠️" if opp["gross_pct"] >= SUSPICIOUS_SPREAD_PCT else ""
            msg += (
                f"{icon} *{i}. {opp['symbol']}/{opp['quote']}* {opp['buy_ex']}→{opp['sell_ex']}\n"
                f"   Спред: `{opp['gross_pct']}%` | Чистая: `{opp['net_pct']}%`{suspicious}\n"
            )
        msg += (f"\n_Порог сигнала: {saved}% | ⚠️ = спред ≥{SUSPICIOUS_SPREAD_PCT}%, вероятно "
                f"рассинхрон/устаревание данных, не настоящая возможность — сверяй через /verify_")
        await send_tg(session, msg)

    elif cmd == "/prices":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/prices BTC`")
            return
        sym = parts[1].upper()
        if sym not in SYMBOLS:
            await send_tg(session, f"❌ `{sym}` нет в списке скрининга.")
            return
        await send_tg(session, f"📊 Получаю цены по {sym}...")
        all_data, active = await fetch_all(session)
        msg = f"📊 *{sym} — {datetime.now().strftime('%H:%M:%S')}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for quote in QUOTE_CURRENCIES:
            if quote == sym:
                continue
            ex_data = all_data.get((sym, quote), {})
            msg += f"*{sym}/{quote}:*\n"
            for ex in ALL_EXCHANGES:
                if ex in ex_data:
                    d = ex_data[ex]
                    msg += f"  {ex}: bid `{d['bid']}` / ask `{d['ask']}`\n"
                else:
                    msg += f"  ⚠️ {ex}: нет данных\n"
            msg += "\n"
        await send_tg(session, msg)

    elif cmd == "/depthcheck":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/depthcheck TRX`\nПоказывает реальную глубину стакана "
                                    "(не только top-of-book) и явно сверяет цену между биржами.")
            return
        sym = parts[1].upper()
        if sym not in SYMBOLS:
            await send_tg(session, f"❌ `{sym}` нет в списке скрининга.")
            return
        await send_tg(session, f"🔍 Смотрю реальную глубину стакана {sym}/USDT на обеих биржах...")

        row = await verify_candidate(session, sym, config["lot_usdt"])
        lot = config["lot_usdt"]
        msg = f"📏 *ГЛУБИНА СТАКАНА — {sym}/USDT*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex, d in row["exchanges"].items():
            if d is None:
                msg += f"⚠️ *{ex}*: нет данных стакана\n\n"
                continue
            msg += (
                f"*{ex}*\n"
                f"  Лучший bid/ask: `{d['bid']}` / `{d['ask']}`\n"
                f"  Уровней в стакане: {d['bid_levels']} bid / {d['ask_levels']} ask\n"
                f"  Покупка лота (~${lot:.0f}): проскальзывание `{d['slippage_pct']}%`"
                f"{' ⚠️ не хватило глубины' if not d['book_full'] else ''}\n\n"
            )
        if row["cross_spread"] is not None:
            icon = "✅" if row["ok"] else "❌"
            msg += f"{icon} *Разброс цены между биржами: {row['cross_spread']}%*\n"
        if row["reasons"]:
            msg += "⚠️ " + "; ".join(row["reasons"]) + "\n"
        msg += ("\n_Проскальзывание — насколько твоя средняя цена хуже, чем показывает верхний "
                "уровень стакана, если купить лот целиком одним рыночным ордером._")
        await send_tg(session, msg)

    elif cmd == "/report":
        await send_tg(session, "📊 Собираю отчёт по топ-5 монетам, которые лучше всего показывают сигналы...")
        all_data, active = await fetch_all(session)
        saved_threshold = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        all_opps = find_arbitrage(all_data)
        config["min_profit_pct"] = saved_threshold

        by_trades = sorted(
            [(c, cs) for c, cs in coin_stats.items() if cs["trades"] > 0],
            key=lambda kv: kv[1]["profit_usdt"], reverse=True
        )
        top_coins = [c for c, _ in by_trades][:5]
        by_signals = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        for c, cs in by_signals:
            if c not in top_coins and cs["signals"] > 0:
                top_coins.append(c)
            if len(top_coins) >= 5:
                break
        if len(top_coins) < 5:
            live_ranked = sorted(SYMBOLS, key=lambda c: max(
                [o["net_pct"] for o in all_opps if o["symbol"] == c], default=-999
            ), reverse=True)
            for c in live_ranked:
                if c not in top_coins:
                    top_coins.append(c)
                if len(top_coins) >= 5:
                    break

        msg = (
            f"📈 *ТОП-5 МОНЕТ ПО СИГНАЛАМ — {datetime.now().strftime('%H:%M:%S')}*\n"
            f"Бирж активно: {len(active)} ({', '.join(active)})\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )

        for coin in top_coins:
            msg += f"💱 *{coin}*\n"
            usdt_data = all_data.get((coin, "USDT"), {})
            if usdt_data:
                prices = " | ".join(f"{ex}: `{d['bid']}`/`{d['ask']}`" for ex, d in usdt_data.items())
                msg += f"   Цены (bid/ask): {prices}\n"
            else:
                msg += "   ⚠️ Нет данных USDT ни с одной биржи прямо сейчас\n"

            coin_opps = [o for o in all_opps if o["symbol"] == coin]
            if coin_opps:
                best = coin_opps[0]
                icon = "🟢" if best["net_pct"] >= saved_threshold else "🔴"
                suspicious = (" ⚠️ *подозрительно большой спред — проверь `/verify " + coin + "` "
                              "прежде чем доверять*") if best["gross_pct"] >= SUSPICIOUS_SPREAD_PCT else ""
                msg += (f"   {icon} Лучший спред сейчас: {best['buy_ex']}→{best['sell_ex']} "
                        f"через {best['quote']}, чистая маржа `{best['net_pct']}%`{suspicious}\n")
            else:
                msg += "   Спреда сейчас не найдено ни по одной паре котировки\n"

            cs = coin_stats.get(coin, {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            msg += (f"   За сессию: сигналов `{cs['signals']}`, сделок `{cs['trades']}`, "
                    f"P&L `{round(cs['profit_usdt'],4)} USDT`, лучшая маржа `{cs['best_net_pct']}%`\n\n")

        msg += (f"_Порог сигнала: {saved_threshold}% | Полный список ({len(SYMBOLS)} монет) — "
                f"/leaderboard, /pairs, /routes | Перед добавлением в реальную торговлю — /verify_")
        await send_tg(session, msg)

    elif cmd == "/leaderboard":
        route_filter = None
        if len(parts) >= 3:
            buy_ex, sell_ex = parts[1], parts[2]
            rs = route_stats.get((buy_ex, sell_ex))
            if not rs or not rs.get("coins"):
                await send_tg(session,
                    f"Пока нет данных по маршруту {buy_ex} → {sell_ex}. Проверь написание "
                    f"бирж (см. /routes) или посмотри без фильтра: `/leaderboard`")
                return
            route_filter = {entry.split("/")[0] for entry in rs["coins"]}

        ranked = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        if route_filter is not None:
            ranked = [r for r in ranked if r[0] in route_filter]
        ranked = [r for r in ranked if r[1]["signals"] > 0][:20]
        if not ranked:
            await send_tg(session, "Пока нет ни одного сигнала ни по одной монете. Дай боту поработать подольше или снизь /setprofit.")
            return
        header_route = f" — маршрут {parts[1]} → {parts[2]}" if route_filter is not None else ""
        msg = (f"🏆 *РЕЙТИНГ КАНДИДАТОВ{header_route}*\n(сортировка по числу "
               "сигналов — но перед добавлением в реальную торговлю каждого прогони через /verify)\n"
               "━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for i, (sym, cs) in enumerate(ranked, 1):
            if route_filter is not None:
                display_stats = route_coin_stats.get((parts[1], parts[2], sym),
                                                        {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            else:
                display_stats = cs
            msg += (
                f"{i}. *{sym}* — сигналов: `{display_stats['signals']}` | сделок: `{display_stats['trades']}` | "
                f"P&L: `{round(display_stats['profit_usdt'],3)} USDT` | лучшая маржа: `{display_stats['best_net_pct']}%`\n"
            )
        if route_filter is None:
            msg += "\n_Через какую именно валюту (USDT/BTC/ETH) — смотри /pairs. Фильтр по маршруту: `/leaderboard БИРЖА1 БИРЖА2`_"
        await send_tg(session, msg)

    elif cmd == "/pairs":
        ranked = sorted(pair_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        ranked = [r for r in ranked if r[1]["signals"] > 0][:25]
        if not ranked:
            await send_tg(session, "Пока нет сигналов ни по одной связке монета/валюта.")
            return
        msg = "🔗 *РЕЙТИНГ СВЯЗОК МОНЕТА/ВАЛЮТА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, ((sym, quote), ps) in enumerate(ranked, 1):
            msg += (
                f"{i}. *{sym}/{quote}* — сигналов: `{ps['signals']}` | сделок: `{ps['trades']}` | "
                f"P&L: `{round(ps['profit_usdt'],3)} USDT` | лучшая маржа: `{ps['best_net_pct']}%`\n"
            )
        await send_tg(session, msg)

    elif cmd == "/routes":
        ranked = sorted(route_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        ranked = [r for r in ranked if r[1]["signals"] > 0]
        if not ranked:
            await send_tg(session, "Пока нет сигналов ни по одному маршруту биржа→биржа.")
            return
        msg = "🛣 *РЕЙТИНГ МАРШРУТОВ (биржа → биржа)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, ((buy_ex, sell_ex), rs) in enumerate(ranked, 1):
            example_coins = ", ".join(list(rs["coins"])[:5])
            more = f" и ещё {len(rs['coins'])-5}" if len(rs["coins"]) > 5 else ""
            msg += (
                f"{i}. *{buy_ex} → {sell_ex}*\n"
                f"   Сигналов: `{rs['signals']}` | Сделок: `{rs['trades']}` | "
                f"P&L: `{round(rs['profit_usdt'],3)} USDT` | Лучшая маржа: `{rs['best_net_pct']}%`\n"
                f"   Монеты: {example_coins}{more}\n\n"
            )
        msg += "_Показывает, между какими конкретно биржами арбитраж встречается чаще всего — независимо от монеты._"
        await send_tg(session, msg)

    elif cmd == "/routecoins":
        if len(parts) < 3:
            await send_tg(session,
                "Полный список монет с историей именно на этом маршруте (без обрезки "
                "до топ-5, как в /routes).\n\n"
                "Пример: `/routecoins KuCoin MEXC`"
            )
            return
        buy_ex, sell_ex = parts[1], parts[2]
        rk = (buy_ex, sell_ex)
        rs = route_stats.get(rk)
        if not rs or not rs.get("coins"):
            await send_tg(session,
                f"Пока нет данных по маршруту {buy_ex} → {sell_ex}. "
                f"Проверь точное написание бирж (см. /routes) или дай боту поработать дольше."
            )
            return

        rows = []
        for entry in rs["coins"]:
            sym = entry.split("/")[0]
            rcs = route_coin_stats.get((buy_ex, sell_ex, sym),
                                        {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
            rows.append((entry, rcs["trades"], rcs["profit_usdt"], rcs["best_net_pct"]))
        rows.sort(key=lambda r: (r[1] > 0, r[2], r[3]), reverse=True)

        msg = (f"🪙 *ВСЕ МОНЕТЫ — {buy_ex} → {sell_ex}* ({len(rows)} шт)\n"
               f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for entry, trades, profit, best in rows[:40]:
            mark = "✅" if trades > 0 else "➖"
            msg += f"{mark} *{entry}* — сделок: `{trades}` | P&L: `{round(profit,3)}` | лучшая маржа: `{best}%`\n"
        if len(rows) > 40:
            msg += f"\n_...и ещё {len(rows)-40}, показаны первые 40 по релевантности._"
        msg += ("\n\n_✅ = хотя бы раз реально исполнялась (в симуляции) на ЭТОМ маршруте. "
                "Цифры теперь именно по этому маршруту, не глобальные. "
                "Перед добавлением в WorkerArbBot — обязательно `/verify МОНЕТА`._")
        await send_tg(session, msg)

    elif cmd == "/balances":
        pending = {q: b for q, b in currency_balances.items() if b > 0}
        msg = "💰 *НАКОПЛЕННЫЕ БАЛАНСЫ (ждут конвертации в USDT)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        if not pending:
            msg += "Пусто — либо ещё не было сделок в BTC/ETH-парах, либо всё уже сконвертировано.\n"
        else:
            for q, amt in pending.items():
                msg += f"*{q}:* `{round(amt, 8)}` (порог конвертации: {config['convert_threshold_usdt']} USDT)\n"
        if conversions_log:
            msg += f"\n📜 *Последние конвертации ({len(conversions_log)} всего):*\n"
            for c in conversions_log[-5:][::-1]:
                msg += f"  {c['time']}: {c['amount']} {c['currency']} → {c['usdt_value']} USDT\n"
        await send_tg(session, msg)

    elif cmd == "/stats":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)
        pending = {q: b for q, b in currency_balances.items() if b > 0}
        pending_line = ", ".join(f"{round(b,6)} {q}" for q, b in pending.items()) if pending else "нет"
        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔵 ТОЛЬКО МОНИТОРИНГ — реальных ордеров эта система не отправляет никогда\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок (симуляция): {stats['trades_sim']}\n"
            f"💰 Прибыль реализованная (за всё время): {round(stats['profit_sim'], 4)} USDT\n"
            f"💰 P&L с последнего уведомления о стоп-лоссе: {round(stats['profit_since_alert'], 4)} USDT\n"
            f"⏳ Ожидает конвертации: {pending_line}\n"
            f"🔄 Конвертаций всего: {len(conversions_log)}\n"
            f"❌ Ошибок: {stats['errors']}\n"
            f"🚫 Пропущено как неправдоподобные (не засчитаны в статистику монет): "
            f"{stats.get('signals_suspicious_skipped', 0)}\n"
            f"🚫 Не подтвердились при честном пересчёте по глубине стакана: "
            f"{stats.get('depth_reverify_failed', 0)}\n\n"
            f"⏱ Сделок этой минуты: {stats['trades_this_minute']}/{config['max_trades_per_min']}\n\n"
            f"⚙️ Стартовый капитал (справочно): {config['start_capital']} USDT\n"
            f"⚙️ Лот: {config['lot_usdt']} USDT-эквивалент\n"
            f"⚙️ Стоп-лосс (уведомление): -{config['stop_loss_usdt']} USDT\n"
            f"⚙️ Порог маржи: {config['min_profit_pct']}%\n"
            f"⚙️ Порог подозрительного спреда: {SUSPICIOUS_SPREAD_PCT}%\n"
            f"⚙️ Минимум уровней стакана: {MIN_DEPTH_LEVELS}\n"
            f"⚙️ Порог автоконвертации: {config['convert_threshold_usdt']} USDT\n"
            f"⚙️ Монет в скрининге: {len(SYMBOLS)}\n"
            f"⚙️ Валюты котировки: {', '.join(QUOTE_CURRENCIES)}\n"
            f"⚙️ Бирж: {len(ALL_EXCHANGES)} ({'/'.join(ALL_EXCHANGES)})\n"
            f"⚙️ Треугольник: {len(SYMBOLS)} монет на {', '.join(TRIANGLE_EXCHANGES)}\n\n"
            f"/leaderboard — какие монеты реально сработали | /verify — проверить кандидатов по-настоящему"
        )

    elif cmd == "/hours":
        if len(parts) >= 3:
            buy_ex, sell_ex = parts[1], parts[2]
            route_hour_data = [
                (h, hourly_route_signals.get((h, buy_ex, sell_ex), 0),
                 hourly_route_plausible.get((h, buy_ex, sell_ex), 0))
                for h in range(24)
            ]
            route_hour_data = [r for r in route_hour_data if r[1] > 0]
            if not route_hour_data:
                await send_tg(session,
                    f"Пока нет данных по маршруту {buy_ex} → {sell_ex}. "
                    f"Проверь написание бирж (см. /routes) или дай боту поработать дольше."
                )
                return
            route_hour_data.sort(key=lambda x: x[2], reverse=True)
            msg = f"⏰ *СИГНАЛЫ ПО ЧАСАМ (UTC) — {buy_ex} → {sell_ex}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            for h, total, plausible in route_hour_data[:12]:
                bar = "█" * min(10, plausible // 3 + (1 if plausible else 0))
                msg += f"*{h:02d}:00* {bar}\n   Всего: {total} | Правдоподобных: {plausible}\n\n"
            best = route_hour_data[0]
            if best[2] > 0:
                msg += f"🏆 Лучший час на этом маршруте: *{best[0]:02d}:00 UTC*"
            else:
                msg += f"_Ни одного правдоподобного сигнала на {buy_ex}→{sell_ex} ни в один из часов пока._"
            await send_tg(session, msg)
            return

        if not hourly_signals:
            await send_tg(session, "Пока нет данных — дай боту поработать подольше.")
            return
        hour_data = [(h, hourly_signals.get(h, 0), hourly_plausible_signals.get(h, 0))
                     for h in range(24) if hourly_signals.get(h, 0) > 0]
        hour_data.sort(key=lambda x: x[2], reverse=True)
        msg = "⏰ *СИГНАЛЫ ПО ЧАСАМ (UTC)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for h, total, plausible in hour_data[:12]:
            bar = "█" * min(10, plausible // 3 + (1 if plausible else 0))
            msg += f"*{h:02d}:00* {bar}\n   Всего сигналов: {total} | Правдоподобных (<{SUSPICIOUS_SPREAD_PCT}%): {plausible}\n\n"
        best = max(hour_data, key=lambda x: x[2], default=None)
        if best and best[2] > 0:
            msg += f"🏆 Лучший час по правдоподобным сигналам: *{best[0]:02d}:00 UTC*"
        else:
            msg += "_Пока ни одного правдоподобного сигнала ни в один из часов — данных ещё мало, либо рынок сейчас действительно спокоен на всех 4 биржах._"
        await send_tg(session, msg)

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "📋 Нет сделок в этой сессии.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in trade_history[-10:][::-1]:
            msg += (
                f"#{t['id']} *{t['symbol']}/{t['quote']}* {t['buy_ex']}→{t['sell_ex']}\n"
                f"   +{t['net_pct']}% | +{t['profit_quote']} {t['quote']} (~{t['profit_usdt']} USDT) | {t['time']}\n\n"
            )
        await send_tg(session, msg)

    elif cmd == "/setprofit":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setprofit 0.15`")
            return
        try:
            config["min_profit_pct"] = float(parts[1])
            await send_tg(session, f"✅ Порог маржи: `{config['min_profit_pct']}%`")
        except Exception:
            await send_tg(session, "❌ Пример: `/setprofit 0.15`")

    elif cmd == "/setlot":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setlot 100`")
            return
        try:
            config["lot_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Лот: `{config['lot_usdt']} USDT`")
        except Exception:
            await send_tg(session, "❌ Пример: `/setlot 100`")

    elif cmd == "/autoroutes":
        routes_str = ", ".join(f"{b}→{s}" for b, s in TARGET_ROUTES) or "(пусто)"
        suspicious_count = stats.get("auto_signal_suspicious_skipped", 0)
        await send_tg(session,
            f"🤖 *Автоматический анализ узкого маршрута*\n\n"
            f"Отслеживаемые маршруты: {routes_str}\n"
            f"Порог для карточки (чистый спред): {config['auto_signal_min_pct']}%\n"
            f"Интервал проверки: {config['auto_check_interval_sec']} сек\n"
            f"Кулдаун на монету: {AUTO_SIGNAL_COOLDOWN_SEC} сек\n"
            f"🚫 Отсеяно как аномальный спред (≥{SUSPICIOUS_SPREAD_PCT}%, даже между "
            f"двумя целевыми биржами — как было с RVN на MEXC): {suspicious_count}\n\n"
            f"`/addautoroute БИРЖА1 БИРЖА2` — добавить маршрут\n"
            f"`/removeautoroute БИРЖА1 БИРЖА2` — убрать маршрут\n"
            f"`/setautothreshold N` — порог чистого спреда, %\n\n"
            f"Как только кандидат на отслеживаемом маршруте покажет чистый "
            f"спред выше порога (и НЕ аномальный) — прилетит карточка с "
            f"разбором и трендом автоматически, без ручных проверок.\n\n"
            f"`/routetrend МОНЕТА` — посмотреть накопленную историю спреда "
            f"и тренд ПРЯМО СЕЙЧАС, не дожидаясь новой карточки.\n\n"
            f"`/dexsearch МОНЕТА` — найти ВСЕ пулы в BSC по названию, "
            f"с ликвидностью каждого (защита от поддельных токенов).\n\n"
            f"`/dexprice МОНЕТА` — сравнить CEX-цену с реальной DEX-ценой "
            f"в сети BSC (через GeckoTerminal), полезно перед любой схемой "
            f"через TrustWallet/DEX-своп."
        )

    elif cmd == "/dexsearch":
        if len(parts) < 2:
            await send_tg(session,
                "Ищет ВСЕ пулы в сети BSC, подходящие под запрос (обычно тикер) "
                "— показывает ликвидность каждого, чтобы отличить настоящий "
                "проект от подделки с тем же названием.\n\n"
                "⚠️ НИКОГДА не доверяй результату с первого взгляда — сверяй "
                "адрес с официальным сайтом проекта или bscscan.com перед "
                "любым реальным переводом.\n\n"
                "Пример: `/dexsearch CAKE`"
            )
            return
        query = " ".join(parts[1:])
        await send_tg(session, f"🔍 Ищу пулы '{query}' в сети BSC...")
        results = await search_dex_pools(session, query, "bsc")
        if results is None:
            await send_tg(session,
                "🔴 Не удалось выполнить поиск — GeckoTerminal сейчас недоступен. "
                "Проверьте логи Railway (поиск \"GeckoTerminal search\")."
            )
            return
        if not results:
            await send_tg(session, f"Ничего не найдено по запросу '{query}' в сети BSC.")
            return

        msg = f"🔍 *НАЙДЕНО ПУЛОВ ДЛЯ '{query}' (BSC)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, r in enumerate(results, 1):
            liq = r["liquidity_usd"]
            warn = "⚠️ ОЧЕНЬ низкая ликвидность — высокий риск подделки/мёртвого пула" if liq < 5000 else ""
            msg += (
                f"{i}. *{r['pool_name']}*\n"
                f"   Токен: {r['token_symbol']} | Адрес: `{r['token_address']}`\n"
                f"   Цена: `{r['price_usd']}` USD | Ликвидность пула: `${liq:,.0f}` {warn}\n\n"
            )
        msg += (
            "⚠️ *Реальный проект обычно имеет ликвидность на порядки больше "
            "поддельных клонов — но это ориентир, не гарантия.* Обязательно "
            "сверьте выбранный адрес с bscscan.com или официальным сайтом "
            "проекта, прежде чем использовать `/dexprice СИМВОЛ 0xАДРЕС` "
            "или тем более переводить реальные деньги."
        )
        await send_tg(session, msg)


    elif cmd == "/dexprice":
        if len(parts) < 2:
            known = ", ".join(BSC_TOKEN_ADDRESSES.keys())
            await send_tg(session,
                f"Сравнивает CEX-цену с реальной DEX-ценой в сети BSC "
                f"(через GeckoTerminal — берёт лучший по ликвидности пул, "
                f"обычно PancakeSwap).\n\n"
                f"Известные токены (проверенные адреса): {known}\n"
                f"Для других монет нужно вручную найти и проверить BEP-20 адрес "
                f"на bscscan.com — просто напишите: `/dexprice SYMBOL 0xАДРЕС`\n\n"
                f"⚠️ Это ориентировочная цена ПУЛА, без учёта газа сети и "
                f"проскальзывания именно вашей суммы — не точная копия "
                f"реального свопа в TrustWallet.\n\n"
                f"Пример: `/dexprice CAKE`"
            )
            return
        symbol = parts[1].upper()
        if len(parts) >= 3 and parts[2].startswith("0x"):
            token_address = parts[2]
        elif symbol in BSC_TOKEN_ADDRESSES:
            token_address = BSC_TOKEN_ADDRESSES[symbol]
        else:
            await send_tg(session,
                f"❌ Не знаю адрес контракта для {symbol} на BSC. "
                f"Найдите и проверьте адрес на bscscan.com, затем: "
                f"`/dexprice {symbol} 0xАДРЕС`"
            )
            return

        await send_tg(session, f"🔍 Смотрю DEX-цену {symbol} в сети BSC (через GeckoTerminal) и сравниваю с биржами...")
        dex_price = await get_pancakeswap_price_usd(session, token_address)
        if dex_price is None:
            await send_tg(session,
                f"🔴 Не удалось получить DEX-цену для {symbol}. Либо API "
                f"GeckoTerminal сейчас недоступен, либо адрес контракта неверный, "
                f"либо у токена нет достаточно ликвидного пула на BSC. "
                f"Проверьте детали в логах Railway (поиск \"GeckoTerminal\")."
            )
            return

        cex_line = "_(монета не в списке CEX-скрининга — сравнение только с DEX-ценой)_"
        if symbol in SYMBOLS:
            all_data, active = await fetch_all(session)
            usdt_data = all_data.get((symbol, "USDT"), {})
            if usdt_data:
                bids = [d["bid"] for d in usdt_data.values() if d.get("bid", 0) > 0]
                if bids:
                    best_cex_bid = max(bids)
                    diff_pct = (dex_price - best_cex_bid) / best_cex_bid * 100
                    cex_line = (
                        f"Лучший CEX bid: `{best_cex_bid}` USDT\n"
                        f"Разница DEX vs CEX: `{diff_pct:+.2f}%`\n\n"
                        f"⚠️ Это БЕЗ учёта газа сети BSC (обычно $0.1-0.5) и БЕЗ "
                        f"учёта комиссии биржи за покупку — вычтите их из разницы, "
                        f"прежде чем считать это реальной прибылью."
                    )
        await send_tg(session,
            f"💱 *{symbol} — CEX vs DEX (BSC, через GeckoTerminal)*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"DEX-цена: `{dex_price}` USD\n\n"
            f"{cex_line}"
        )


    elif cmd == "/routetrend":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/routetrend LRC`")
            return
        symbol = parts[1].upper()
        found_any = False
        msg = f"📈 *ИСТОРИЯ СПРЕДА — {symbol}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for buy_ex, sell_ex in TARGET_ROUTES:
            hist = route_symbol_spread_history.get((buy_ex, sell_ex, symbol))
            if not hist:
                continue
            found_any = True
            trend = get_route_spread_trend(buy_ex, sell_ex, symbol)
            verdict_obj = get_route_spread_verdict(buy_ex, sell_ex, symbol)
            points_str = " → ".join(f"{pct:+.2f}%" for _, pct in hist[-8:])
            msg += (
                f"*{buy_ex} → {sell_ex}*\n"
                f"   Точек в окне (20 мин): {len(hist)}\n"
                f"   Последние: {points_str}\n"
                f"   Тренд: {trend}\n"
                f"   *Заключение:* {verdict_obj['text']}\n\n"
            )
        if not found_any:
            msg += ("Пока нет накопленной истории — либо монета ещё не "
                    "появлялась как кандидат на отслеживаемых маршрутах, "
                    "либо прошло меньше минуты с первого появления.")
        await send_tg(session, msg)

    elif cmd == "/addautoroute":
        if len(parts) < 3:
            await send_tg(session, "Пример: `/addautoroute KuCoin MEXC`")
            return
        route = (parts[1], parts[2])
        if route in TARGET_ROUTES:
            await send_tg(session, f"⚠️ Маршрут {route[0]}→{route[1]} уже отслеживается.")
            return
        TARGET_ROUTES.append(route)
        await send_tg(session, f"✅ Добавлен в автоанализ: {route[0]}→{route[1]}")

    elif cmd == "/removeautoroute":
        if len(parts) < 3:
            await send_tg(session, "Пример: `/removeautoroute KuCoin MEXC`")
            return
        route = (parts[1], parts[2])
        if route not in TARGET_ROUTES:
            await send_tg(session, f"⚠️ Маршрут {route[0]}→{route[1]} не отслеживается.")
            return
        TARGET_ROUTES.remove(route)
        await send_tg(session, f"✅ Убран из автоанализа: {route[0]}→{route[1]}")

    elif cmd == "/setautothreshold":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущий порог: {config['auto_signal_min_pct']}% (чистый спред после комиссий)\n"
                f"Пример: `/setautothreshold 0.5`")
            return
        try:
            val = float(parts[1])
            config["auto_signal_min_pct"] = val
            await send_tg(session, f"✅ Порог автосигнала: {val}%")
        except Exception:
            await send_tg(session, "❌ Пример: `/setautothreshold 0.5`")

    elif cmd == "/setrealcoin":
        if len(parts) < 2:
            await send_tg(session,
                f"Текущая монета WorkerArbBot (по данным TrialArbBot): "
                f"*{config.get('current_real_coin', '?')}*\n\n"
                f"Обнови её здесь, если менял монету в рабочем боте вручную:\n"
                f"Пример: `/setrealcoin ONE`")
            return
        config["current_real_coin"] = parts[1].upper()
        await send_tg(session, f"✅ Текущая монета WorkerArbBot: {config['current_real_coin']}")

    elif cmd == "/setworkerthreshold":
        if len(parts) < 2:
            cur = config.get("worker_honest_threshold_pct", 4.0)
            await send_tg(session,
                f"Текущий боевой порог WorkerArbBot (по данным TrialArbBot): {cur}%\n\n"
                f"Сигналы выше этого порога отдельно помечаются 🎯 и логируются "
                f"в `/qualifiedsignals` — это не просто «правдоподобные», а "
                f"реально проходные для рабочего бота.\n\n"
                f"Сверься с настоящим значением в WorkerArbBot (`/stats` → "
                f"«честный» порог) и обнови здесь при изменении.\n\n"
                f"Пример: `/setworkerthreshold 4.06`"
            )
            return
        try:
            val = float(parts[1])
            config["worker_honest_threshold_pct"] = val
            await send_tg(session, f"✅ Боевой порог WorkerArbBot: {val}%")
        except ValueError:
            await send_tg(session, "❌ Пример: `/setworkerthreshold 4.06`")

    elif cmd == "/qualifiedsignals":
        if not qualified_signals_log:
            await send_tg(session,
                f"Пока ни одного сигнала не прошло боевой порог "
                f"({config.get('worker_honest_threshold_pct', 4.0)}%). "
                f"Это ожидаемо — порог высокий и специально откалиброван "
                f"так, чтобы редко, но надёжно.")
            return
        msg = (f"🎯 *СИГНАЛЫ, ПРОШЕДШИЕ БОЕВОЙ ПОРОГ* "
               f"({config.get('worker_honest_threshold_pct', 4.0)}%)\n"
               f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for entry in qualified_signals_log[-20:][::-1]:
            msg += (f"{entry['time']} — *{entry['symbol']}* "
                    f"{entry['buy_ex']}→{entry['sell_ex']}: `{entry['net_pct']}%` "
                    f"(вердикт: {entry['verdict']})\n")
        msg += f"\n_Всего в истории: {len(qualified_signals_log)}_"
        await send_tg(session, msg)

    else:
        await send_tg(session,
            "/start /verify /scan /top /prices SYMBOL /depthcheck SYMBOL /exchanges\n"
            "/report /leaderboard /pairs /routes /balances\n"
            "/stats /history /triangle /triangletop\n"
            "/setprofit 0.15 /setlot 100\n\n"
            "Это только монитор — реальных ордеров тут нет и не будет."
        )


async def polling_loop(session):
    offset = 0
    while True:
        updates = await get_updates(session, offset)
        for update in updates:
            offset = update["update_id"] + 1
            msg = update.get("message", {})
            if msg:
                chat_id = msg["chat"]["id"]
                text = msg.get("text", "")
                if text.startswith("/"):
                    try:
                        await handle_command(session, text, chat_id)
                    except Exception as e:
                        logger.error(f"handle_command упал на '{text}': {e}")
                        try:
                            await send_tg(session, f"⚠️ Ошибка при обработке `{text.split()[0]}`: `{e}`\nБот продолжает работать, напиши другую команду.")
                        except Exception:
                            pass
        await asyncio.sleep(1)


async def scan_loop(session):
    await asyncio.sleep(15)
    while True:
        try:
            opps, active = await scan_cycle(session)
            logger.info(f"Scan #{stats['scans']}: {len(active)} бирж, {len(opps)} сигналов")
            for opp in opps[:5]:
                key = f"{opp['symbol']}-{opp['quote']}-{opp['buy_ex']}-{opp['sell_ex']}"
                now = datetime.now().timestamp()
                if now - last_signal_time.get(key, 0) > 120:
                    last_signal_time[key] = now
                    if CHAT_ID:
                        await send_tg(session, format_signal(opp))
                        await asyncio.sleep(0.7)
                    if opp["gross_pct"] < SUSPICIOUS_SPREAD_PCT:
                        verified = await reverify_with_depth(session, opp)
                        if verified:
                            await execute_sim(verified, session)
                        else:
                            stats["depth_reverify_failed"] = stats.get("depth_reverify_failed", 0) + 1
                    else:
                        stats["signals_suspicious_skipped"] = stats.get("signals_suspicious_skipped", 0) + 1
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Scan error: {e}")
        await asyncio.sleep(config["scan_interval"])


def format_auto_signal(check: dict, trend: str, route_hist: dict) -> str:
    buy_ex, sell_ex, symbol = check["buy_ex"], check["sell_ex"], check["symbol"]
    net_pct = check["net_pct"]
    buy_ask_lv, buy_bid_lv = check.get("buy_levels", (0, 0))
    sell_ask_lv, sell_bid_lv = check.get("sell_levels", (0, 0))

    trades = route_hist.get("trades", 0)
    profit = route_hist.get("profit_usdt", 0.0)
    history_line = (
        f"📜 История на этом маршруте: {trades} сделок, P&L {round(profit,3)} USDT"
        if trades > 0 else
        "📜 История на этом маршруте: ещё не исполнялась ни разу"
    )

    verdict_obj = get_route_spread_verdict(buy_ex, sell_ex, symbol)
    verdict = verdict_obj["text"]

    hist = route_symbol_spread_history.get((buy_ex, sell_ex, symbol), [])
    commands_block = ""
    if verdict_obj["level"] == "green":
        current_coin = config.get("current_real_coin", "")
        if symbol == current_coin:
            commands_block = (
                f"\n✅ *{symbol} уже торгуется в WorkerArbBot — переключать не нужно.*\n"
            )
        elif len(hist) < 5:
            commands_block = (
                f"\n⏳ *Ещё рано формировать команды на переключение* — только "
                f"{len(hist)} точек истории, для решения нужно минимум 5. "
                f"Дождись следующих карточек.\n"
            )
        else:
            commands_block = (
                f"\n📋 *Готовые команды для WorkerArbBot (проверь сам перед отправкой!):*\n"
                f"```\n"
                f"/pause\n"
                f"/addcoin {symbol}\n"
                f"/removecoin {current_coin}\n"
                f"/listcoins\n"
                f"/realbalance\n"
                f"/go\n"
                f"```\n"
            )

    return (
        f"🤖 *АВТО-АНАЛИЗ: {buy_ex} → {sell_ex} | {symbol}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📥 Купить на {buy_ex}: `{check['buy_price']}` ({buy_ask_lv} ask / {buy_bid_lv} bid уровней)\n"
        f"📤 Продать на {sell_ex}: `{check['sell_price']}` ({sell_ask_lv} ask / {sell_bid_lv} bid уровней)\n\n"
        f"📊 Спред: `{check['gross_pct']}%` | После комиссий: `{net_pct}%`\n"
        f"✅ Глубина в порядке на обеих биржах, спред НЕ аномальный (<{SUSPICIOUS_SPREAD_PCT}%) "
        f"— это уже полная проверка именно для этого маршрута, отдельный `/verify` не нужен "
        f"(он может ложно отказать из-за биржи, которая тут вообще не участвует).\n\n"
        f"📈 Тренд за окно: {trend}\n\n"
        f"{history_line}\n\n"
        f"*ЗАКЛЮЧЕНИЕ:* {verdict}\n"
        f"{commands_block}"
        f"_Это всё равно не гарантия результата — рынок может измениться "
        f"за секунды между этой карточкой и реальным исполнением._\n\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
    )


async def auto_signal_loop(session):
    await asyncio.sleep(30)
    while True:
        try:
            for route_key in list(TARGET_ROUTES):
                buy_ex, sell_ex = route_key
                candidates = list(auto_route_candidates.get(route_key, set()))[:20]
                for symbol in candidates:
                    check = await check_narrow_route(session, buy_ex, sell_ex, symbol, config["lot_usdt"])
                    if not check:
                        continue
                    record_route_spread(buy_ex, sell_ex, symbol, check["net_pct"])

                    if check.get("suspicious"):
                        stats["auto_signal_suspicious_skipped"] = stats.get("auto_signal_suspicious_skipped", 0) + 1
                        continue

                    if check["net_pct"] < config["auto_signal_min_pct"]:
                        continue

                    cd_key = (buy_ex, sell_ex, symbol)
                    now_ts = time.time()
                    if now_ts - _last_auto_signal_time.get(cd_key, 0) < AUTO_SIGNAL_COOLDOWN_SEC:
                        continue
                    _last_auto_signal_time[cd_key] = now_ts

                    trend = get_route_spread_trend(buy_ex, sell_ex, symbol)
                    route_hist = route_coin_stats.get((buy_ex, sell_ex, symbol),
                                                        {"trades": 0, "profit_usdt": 0.0})
                    if CHAT_ID:
                        card_text = format_auto_signal(check, trend, route_hist)
                        verdict_obj = get_route_spread_verdict(buy_ex, sell_ex, symbol)

                        qualifies_real = check["net_pct"] >= config.get("worker_honest_threshold_pct", 4.0)
                        if qualifies_real:
                            qualified_signals_log.append({
                                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "buy_ex": buy_ex, "sell_ex": sell_ex, "symbol": symbol,
                                "net_pct": check["net_pct"], "verdict": verdict_obj["level"],
                            })
                            if len(qualified_signals_log) > QUALIFIED_SIGNALS_LOG_MAXLEN:
                                qualified_signals_log.pop(0)
                            card_text = (
                                f"🎯 *ПРЕВЫШАЕТ БОЕВОЙ ПОРОГ WorkerArbBot "
                                f"({config.get('worker_honest_threshold_pct', 4.0)}%)!*\n\n" + card_text
                            )

                        if verdict_obj["level"] == "green" or qualifies_real:
                            await send_tg_pinned(session, card_text)
                        else:
                            await send_tg(session, card_text)
                    await asyncio.sleep(0.7)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Auto signal loop error: {e}")
        await asyncio.sleep(config["auto_check_interval_sec"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"ArbScreenerBot (только мониторинг) | {len(SYMBOLS)} монет | {len(ALL_EXCHANGES)} бирж ({'/'.join(ALL_EXCHANGES)}) | "
        f"лот {config['lot_usdt']} USDT | порог {config['min_profit_pct']}% | "
        f"подозрительный спред >{SUSPICIOUS_SPREAD_PCT}% | мин. уровней стакана {MIN_DEPTH_LEVELS} | "
        f"треугольник: {len(SYMBOLS)} монет на {', '.join(TRIANGLE_EXCHANGES)} через {TRIANGLE_BRIDGE}"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(
            polling_loop(session),
            scan_loop(session),
            grid_loop(session),
            funding_loop(session),
            daily_digest_loop(session),
            auto_signal_loop(session),
            triangle_scan_loop(session),
            return_exceptions=True,
        )
        names = ["polling_loop", "scan_loop", "grid_loop", "funding_loop", "daily_digest_loop",
                  "auto_signal_loop", "triangle_scan_loop"]
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error(f"Фоновая задача {name} упала с исключением: {result}")


if __name__ == "__main__":
    asyncio.run(main())
