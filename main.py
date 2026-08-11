import asyncio
import aiohttp
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

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


async def send_tg(session, text):
    if not CHAT_ID:
        return
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
    except Exception as e:
        logger.error(f"TG: {e}")


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

ALL_EXCHANGES = ["Binance", "KuCoin", "Gate", "Bitget", "MEXC"]


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
        get_binance(session), get_kucoin(session),
        get_gate(session), get_bitget(session), get_mexc(session),
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
    if len(active) < 2:
        return [], active
    opps = find_arbitrage(all_data)
    if opps:
        stats["signals"] += len(opps)
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
# НОВОЕ 11.08: ТРЕУГОЛЬНЫЙ АРБИТРАЖ — на КАЖДОЙ бирже отдельно (не между
# биржами — треугольник в принципе внутри одной площадки, монета не может
# телепортироваться между биржами бесплатно). Путь: USDT -> МОСТ (BTC) ->
# АЛЬТ -> USDT, и обратное направление. Считаем по РЕАЛЬНОЙ глубине
# стакана (walk-the-book), теми же проверенными функциями _walk_by_notional
# и _walk_by_qty, что уже используются для честной проверки обычного
# арбитража — не top-of-book, который может обмануть на тонком стакане.
#
# Список монет для треугольника — ОТДЕЛЬНЫЙ от SYMBOLS, чтобы не сканировать
# все ~110 монет x 5 бирж x 3 стакана сразу (это было бы полторы тысячи
# запросов за один /triangle — слишком медленно и рискует упереться в
# рейт-лимиты). По умолчанию — ликвидные, крупные монеты, у которых
# реально есть и ALT/BTC, и ALT/USDT пара почти на любой бирже.
# ═══════════════════════════════════════════════════════════════
TRIANGLE_SYMBOLS: List[str] = ["ETH", "BNB", "SOL", "XRP", "ADA", "DOGE",
                                "LTC", "TRX", "DOT", "AVAX", "LINK", "TON"]
TRIANGLE_BRIDGE = "BTC"


def fmt_pair(ex: str, base: str, quote: str) -> str:
    """Обобщённый форматтер символа пары под конкретную биржу — то же
    самое, что SYMBOL_FMT, но принимает ЛЮБУЮ котируемую валюту (не
    только USDT), нужно для ноги ALT/BTC внутри треугольника."""
    if ex == "KuCoin":
        return f"{base}-{quote}"
    elif ex == "Gate":
        return f"{base}_{quote}"
    elif ex == "HTX":
        return f"{base.lower()}{quote.lower()}"
    else:  # Binance, Bitget, MEXC — слитно, заглавными
        return f"{base}{quote}"


async def calc_triangle_on_exchange(session, ex: str, alt: str, bridge: str,
                                     lot_usdt: float) -> List[dict]:
    """Считает ОБА направления треугольника USDT<->BRIDGE<->ALT на ОДНОЙ
    бирже, по реальной глубине стакана. Возвращает список найденных
    возможностей (обычно 0, 1 или 2 — по одной на направление)."""
    ob_fn = ORDERBOOK_FN.get(ex)
    if not ob_fn:
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
            return []  # хотя бы одной из трёх пар нет на этой бирже — треугольник не посчитать

    fee = FEES.get(ex, 0.1) / 100
    found = []

    # --- Путь 1: USDT -> BRIDGE -> ALT -> USDT ---
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

    # --- Путь 2: USDT -> ALT -> BRIDGE -> USDT (обратное направление) ---
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
    """Проверяет ВСЕ монеты из TRIANGLE_SYMBOLS на ВСЕХ биржах из
    ALL_EXCHANGES разом — именно то, что попросили: полная картина по
    всем площадкам одним запросом."""
    tasks = []
    for ex in ALL_EXCHANGES:
        for alt in TRIANGLE_SYMBOLS:
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


# ══════════════════════════════════════════════════════════════
# НОВОЕ 11.08: УЧЕБНЫЙ GRID-СИМУЛЯТОР — объединён в один бот с монитором
# арбитража и треугольника (по вашему запросу, вместо отдельного третьего
# бота). Та же гарантия безопасности: только симуляция на реальных ценах
# Binance, физически не может отправить реальный ордер.
#
# Логика: диапазон цены [low, high], N уровней -> N-1 независимых ячеек
# (купить на уровне i, продать на уровне i+1). После продажи ячейка сразу
# "перевзводится" — снова готова купить, если цена опустится обратно.
# ══════════════════════════════════════════════════════════════

GRID_FEE_PCT = float(os.environ.get("GRID_FEE_PCT", "0.1"))  # Binance стандартная
grids: Dict[str, dict] = {}


async def get_price_binance_simple(session, symbol: str) -> Optional[dict]:
    """Best bid/ask — переиспользуем ту же логику, что и в остальном
    файле, но без привязки к SYMBOLS (grid должен уметь взять ЛЮБОЙ
    тикер, не только те 110+ монет из основного скрининга)."""
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


def make_grid(symbol: str, low: float, high: float, levels: int, lot_usdt: float) -> dict:
    step = (high - low) / (levels - 1)
    lines = [round(low + step * i, 8) for i in range(levels)]
    cells = []
    for i in range(levels - 1):
        cells.append({
            "buy_line": lines[i], "sell_line": lines[i + 1],
            "held": False, "bought_at": None,
            "qty": lot_usdt / lines[i],
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
        if not cell["held"] and price["ask"] <= cell["buy_line"]:
            cell["held"] = True
            cell["bought_at"] = price["ask"]
            grid["trades"] += 1
            grid["trade_log"].append({"time": datetime.now().strftime("%H:%M:%S"),
                                        "side": "BUY", "price": price["ask"],
                                        "level": f"{cell['buy_line']}->{cell['sell_line']}"})
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
            f"/triangle — треугольный арбитраж на ВСЕХ биржах разом (новое!)\n"
            f"/startgrid СИМВОЛ НИЖЕ ВЫШЕ УРОВНЕЙ ЛОТ — учебный grid-симулятор (новое!)\n"
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
            f"/setprofit 0.15 — порог маржи | /setlot 100 — размер лота\n"
            f"/addtriangle SYM /removetriangle SYM — список монет для /triangle\n"
        )

    elif cmd == "/triangle":
        await send_tg(session,
            f"🔺 Сканирую треугольный арбитраж на ВСЕХ биржах "
            f"({', '.join(ALL_EXCHANGES)}), монеты: {', '.join(TRIANGLE_SYMBOLS)}, "
            f"мост: {TRIANGLE_BRIDGE}...")
        results = await scan_all_triangles(session)
        if not results:
            await send_tg(session,
                f"😔 Нет треугольных возможностей выше порога {config['min_profit_pct']}% "
                f"ни на одной из {len(ALL_EXCHANGES)} бирж прямо сейчас.\n"
                f"(Либо пары ALT/{TRIANGLE_BRIDGE} не существуют для части монет на "
                f"части бирж — это нормально, такие комбинации просто пропускаются.)\n\n"
                f"Хочешь увидеть, насколько БЛИЗКО рынок подходил к порогу — "
                f"`/triangletop` покажет лучшие результаты без фильтра."
            )
        else:
            msg = (f"🔺 *ТРЕУГОЛЬНЫЙ АРБИТРАЖ — ВСЕ БИРЖИ*\n"
                   f"━━━━━━━━━━━━━━━━━━━━━━\n\n")
            for r in results[:10]:
                msg += (f"*{r['exchange']}* — {r['symbol']} via {r['path']}\n"
                        f"   Чистая: `{r['net_pct']}%` | Профит на лот "
                        f"(${config['lot_usdt']}): `{r['profit_usdt']} USDT`\n\n")
            await send_tg(session, msg)

    elif cmd == "/triangletop":
        await send_tg(session,
            f"🔺 Сканирую БЕЗ ПОРОГА — показываю лучшие результаты, даже отрицательные, "
            f"чтобы увидеть, насколько рынок реально близок к возможности...")
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
        grids[symbol] = make_grid(symbol, low, high, levels, lot)
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

    elif cmd == "/addtriangle":
        if len(parts) < 2:
            await send_tg(session,
                f"Добавляет монету в список для /triangle.\n"
                f"Сейчас: {', '.join(TRIANGLE_SYMBOLS)}\n"
                f"Пример: `/addtriangle MATIC`")
            return
        sym = parts[1].upper()
        if sym in TRIANGLE_SYMBOLS:
            await send_tg(session, f"⚠️ {sym} уже в списке треугольника.")
            return
        TRIANGLE_SYMBOLS.append(sym)
        await send_tg(session, f"✅ Добавлено: {sym}\nСписок: {', '.join(TRIANGLE_SYMBOLS)}")

    elif cmd == "/removetriangle":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/removetriangle MATIC`")
            return
        sym = parts[1].upper()
        if sym not in TRIANGLE_SYMBOLS:
            await send_tg(session, f"⚠️ {sym} не в списке треугольника.")
            return
        TRIANGLE_SYMBOLS.remove(sym)
        await send_tg(session, f"✅ Удалено: {sym}\nСписок: {', '.join(TRIANGLE_SYMBOLS)}")

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
            get_binance(session), get_kucoin(session),
            get_gate(session), get_bitget(session), get_mexc(session),
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

        by_signals = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        top_coins = [c for c, cs in by_signals if cs["signals"] > 0][:5]
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
        ranked = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        ranked = [r for r in ranked if r[1]["signals"] > 0][:20]
        if not ranked:
            await send_tg(session, "Пока нет ни одного сигнала ни по одной монете. Дай боту поработать подольше или снизь /setprofit.")
            return
        msg = ("🏆 *РЕЙТИНГ КАНДИДАТОВ*\n(агрегат по всем валютам котировки, сортировка по числу "
               "сигналов — но перед добавлением в реальную торговлю каждого прогони через /verify)\n"
               "━━━━━━━━━━━━━━━━━━━━━━\n\n")
        for i, (sym, cs) in enumerate(ranked, 1):
            msg += (
                f"{i}. *{sym}* — сигналов: `{cs['signals']}` | сделок: `{cs['trades']}` | "
                f"P&L: `{round(cs['profit_usdt'],3)} USDT` | лучшая маржа: `{cs['best_net_pct']}%`\n"
            )
        msg += "\n_Через какую именно валюту (USDT/BTC/ETH) — смотри /pairs_"
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
            f"⚙️ Бирж: {len(ALL_EXCHANGES)} ({'/'.join(ALL_EXCHANGES)})\n\n"
            f"/leaderboard — какие монеты реально сработали | /verify — проверить кандидатов по-настоящему"
        )

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

    else:
        await send_tg(session,
            "/start /verify /scan /top /prices SYMBOL /depthcheck SYMBOL /exchanges\n"
            "/report /leaderboard /pairs /routes /balances\n"
            "/stats /history /triangle /addtriangle /removetriangle\n"
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


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"ArbScreenerBot (только мониторинг) | {len(SYMBOLS)} монет | {len(ALL_EXCHANGES)} бирж ({'/'.join(ALL_EXCHANGES)}) | "
        f"лот {config['lot_usdt']} USDT | порог {config['min_profit_pct']}% | "
        f"подозрительный спред >{SUSPICIOUS_SPREAD_PCT}% | мин. уровней стакана {MIN_DEPTH_LEVELS} | "
        f"треугольник: {len(TRIANGLE_SYMBOLS)} монет через {TRIANGLE_BRIDGE}"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(
            polling_loop(session),
            scan_loop(session),
            grid_loop(session),
            return_exceptions=True,
        )
        names = ["polling_loop", "scan_loop", "grid_loop"]
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error(f"Фоновая задача {name} упала с исключением: {result}")


if __name__ == "__main__":
    asyncio.run(main())
