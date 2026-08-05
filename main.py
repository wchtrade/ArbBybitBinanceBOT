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

# ══════════════════════════════════════════════════════════════
# НАЗНАЧЕНИЕ БОТА: ТОЛЬКО СКРИНИНГ / МОНИТОРИНГ, НАВСЕГДА
#
# ИСПРАВЛЕНО 05.08: раньше в этом же файле был встроен ВТОРОЙ слой —
# WorkerArbBot с реальными подписанными ордерами. По вашему запросу этот
# слой ПОЛНОСТЬЮ УДАЛЁН из файла, а не просто отключён гейтом — ни одной
# функции, которая могла бы подписать и отправить реальный ордер на
# биржу, в этом файле больше нет. Раз назначение бота — искать и
# проверять новые монеты-кандидаты для WorkerArbBot (другого, отдельного
# бота), у этого скрипта не должно быть физической возможности тронуть
# реальные деньги, даже случайно, даже через баг в конфиге.
#
# Если понадобится реальная торговля по монете, которую здесь нашли —
# она добавляется в WorkerArbBot (`/addcoin`), а не сюда.
#
# ВТОРОЕ ИСПРАВЛЕНИЕ 05.08 (причина: ложный сигнал по ZIL, где Binance
# показал цену на ~10% ниже реальной при формально неплохой глубине):
# раньше большой спред между биржами просто помечался предупреждением
# внутри текста сигнала (легко пропустить). Теперь /depthcheck и новая
# команда /verify явно и структурно проверяют: (а) совпадает ли цена
# между биржами в разумных пределах, (б) достаточна ли глубина стакана с
# обеих сторон — и только если ОБА условия выполнены, кандидат
# помечается как заслуживающий доверия.
# ══════════════════════════════════════════════════════════════

config = {
    "min_profit_pct":  float(os.environ.get("MIN_PROFIT_PCT", "0.3")),
    "lot_usdt":        float(os.environ.get("LOT_USDT", "100")),      # шаг лота, в USDT-эквиваленте для любой валюты котировки
    "start_capital":   float(os.environ.get("START_CAPITAL", "10000")),
    "stop_loss_usdt":  float(os.environ.get("STOP_LOSS_USDT", "50")),
    "scan_interval":   6,
    "max_trades_per_min": int(os.environ.get("MAX_TRADES_PER_MIN", "5")),
    "convert_threshold_usdt": float(os.environ.get("CONVERT_THRESHOLD_USDT", "20")),
}

# Подозрительно большой спред (в %) — почти наверняка рассинхрон/задержка
# данных между биржами (или, как в случае с ZIL, разная стадия миграции
# токена), а не настоящая возможность.
SUSPICIOUS_SPREAD_PCT = 5.0

# Минимум уровней стакана с ОБЕИХ сторон на КАЖДОЙ бирже, ниже которого
# кандидат считается непроверенным (то же правило, что в /scancandidates
# у WorkerArbBot) — тонкий стакан слишком легко даёт обманчивую картину.
MIN_DEPTH_LEVELS = 15

# Валюты-мосты для арбитража: сравниваются не только пары COIN/USDT, но и
# COIN/BTC, COIN/ETH — это независимые от USDT-рынка стаканы.
QUOTE_CURRENCIES = ["USDT", "BTC", "ETH"]

# Bybit не используется — подтверждённо блокирует облачные IP
# (Railway/AWS/GCP) через CloudFront (403), без VPS/прокси не лечится.
# HTX убрана по опыту WorkerArbBot — там же выяснилось, что HTX почти
# всегда даёт самую тонкую/оторванную от рынка ликвидность из трёх бирж.
FEES = {
    "Binance": 0.10,
    "KuCoin":  0.10,
}

# ══════════════════════════════════════════════════════════════
# ШИРОКИЙ СПИСОК МОНЕТ ДЛЯ СКРИНИНГА — максимальный охват. Часть монет
# может отсутствовать на одной или нескольких биржах — это нормально,
# такие просто не попадут в сравнение (см. find_arbitrage: нужно >=2 биржи).
# ══════════════════════════════════════════════════════════════
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
    "CELO", "ZIL", "QTUM", "WAVES", "KSM", "ICP", "KAS", "EGLD", "FLOW",
    "XTZ", "NEO", "IOTA", "IOST", "ONT", "CKB",
    "GRT", "ANKR", "SKL", "STORJ", "FIL", "AR",
    "CHZ", "GMT", "RVN", "THETA", "MASK", "GAL", "PYTH", "JUP", "JTO",
    "TON", "ORDI", "WOO", "PERP", "LRC", "BAT", "COTI",
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

# ИСПРАВЛЕНИЕ 05.08: для top-of-book (bid/ask) по-прежнему используем
# публичный data-api.binance.vision — это официальный, документированный
# Binance-эндпоинт для рыночных данных (тот же Matching Engine, что и
# основной API), не самодельная замена. Расхождение цены по ZIL, скорее
# всего, было связано с миграцией токена на блокчейне, а не с качеством
# самого источника данных — но на всякий случай /depthcheck и /verify
# теперь ВСЕГДА перепроверяют реальную глубину стакана (а не только
# top-of-book) перед тем, как показать монету как заслуживающую доверия.
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


# ═══════════════════════════════════════
# РЕАЛЬНАЯ ГЛУБИНА СТАКАНА (не только top-of-book) — общие функции для
# /depthcheck и /verify. Раньше жили только во "втором слое" (WorkerArbBot),
# теперь это основной инструмент проверки качества любого кандидата.
# ═══════════════════════════════════════

def sym_binance(base: str) -> str: return f"{base}USDT"
def sym_kucoin(base: str) -> str: return f"{base}-USDT"


SYMBOL_FMT = {"Binance": sym_binance, "KuCoin": sym_kucoin}


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


ORDERBOOK_FN = {"Binance": get_orderbook_binance, "KuCoin": get_orderbook_kucoin}


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


async def verify_candidate(session, sym: str, lot_usdt: float) -> dict:
    """ГЛАВНАЯ проверочная функция — общая для /depthcheck и /verify.
    Проверяет РЕАЛЬНУЮ глубину стакана (не top-of-book) на Binance и
    KuCoin, и явно сверяет цену между биржами. Кандидат считается
    надёжным (ok=True), только если ОДНОВРЕМЕННО:
      1) на обеих биржах есть данные,
      2) на обеих биржах >= MIN_DEPTH_LEVELS уровней с ОБЕИХ сторон
         (bid и ask),
      3) цены (best bid) между биржами не расходятся сильнее
         SUSPICIOUS_SPREAD_PCT.
    Именно комбинация (2)+(3) ловит и тонкий стакан (как было с ZK/RVN
    на HTX), и рассинхрон/устаревание котировки при формально неплохой
    глубине (как оказалось с ZIL на Binance)."""
    row = {"symbol": sym, "exchanges": {}, "ok": True, "reasons": [], "cross_spread": None}

    for ex in ("Binance", "KuCoin"):
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
        return_exceptions=True
    )
    ex_names = ["Binance", "KuCoin"]
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
            f"Площадки: Binance, KuCoin\n"
            f"Валюты котировки: {', '.join(QUOTE_CURRENCIES)}\n\n"
            f"⚙️ Лот/шаг сделки (для расчётов): `{config['lot_usdt']} USDT`-эквивалент\n"
            f"⚙️ Порог маржи: `{config['min_profit_pct']}%`\n"
            f"⚙️ Порог подозрительного спреда: `{SUSPICIOUS_SPREAD_PCT}%`\n"
            f"⚙️ Минимум уровней стакана для доверия: `{MIN_DEPTH_LEVELS}`\n\n"
            f"*Главные команды:*\n"
            f"/verify МОНЕТА1 МОНЕТА2 ... — проверить кандидатов по-настоящему "
            f"(глубина стакана + совпадение цены между биржами, до 8 монет)\n"
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
        )

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
        # ИСПРАВЛЕНИЕ: раньше "/verify ZIL, COTI, YFI" (с запятыми, как
        # люди обычно и пишут через запятую) ломался — "ZIL," с запятой
        # воспринималось как отдельный, несуществующий тикер, и биржи
        # честно отвечали "нет данных". Теперь запятые/точки с запятой
        # убираются, пустые токены после этого пропускаются.
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
        await send_tg(session, f"🔍 Сканирую 2 биржи, {len(SYMBOLS)} монет...")
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
                await execute_sim(opp, session)

    elif cmd == "/exchanges":
        await send_tg(session, "🔍 Проверяю каждую биржу отдельно...")
        results = await asyncio.gather(
            get_binance(session), get_kucoin(session),
            return_exceptions=True
        )
        ex_names = ["Binance", "KuCoin"]
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
            for ex in ("Binance", "KuCoin"):
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
            f"❌ Ошибок: {stats['errors']}\n\n"
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
            f"⚙️ Бирж: 2 (Binance/KuCoin)\n\n"
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
            "/stats /history\n"
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
                    await execute_sim(opp, session)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Scan error: {e}")
        await asyncio.sleep(config["scan_interval"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"ArbScreenerBot (только мониторинг) | {len(SYMBOLS)} монет | 2 биржи (Binance/KuCoin) | "
        f"лот {config['lot_usdt']} USDT | порог {config['min_profit_pct']}% | "
        f"подозрительный спред >{SUSPICIOUS_SPREAD_PCT}% | мин. уровней стакана {MIN_DEPTH_LEVELS}"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await asyncio.gather(
            polling_loop(session),
            scan_loop(session),
            return_exceptions=True,
        )
        names = ["polling_loop", "scan_loop"]
        for name, result in zip(names, results):
            if isinstance(result, Exception):
                logger.error(f"Фоновая задача {name} упала с исключением: {result}")


if __name__ == "__main__":
    asyncio.run(main())
