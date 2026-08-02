import asyncio
import aiohttp
import logging
import os
from datetime import datetime
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TG_TOKEN = os.environ.get("ARB_BOT_TOKEN", "")
CHAT_ID = None

# ══════════════════════════════════════════════════════════════
# НАЗНАЧЕНИЕ БОТА: СКРИНИНГ, НЕ БОЕВАЯ ТОРГОВЛЯ
# Задача — прогнать широкий список монет через 3 биржи, накопить
# статистику по каждой монете (частота сигналов, средняя маржа,
# симулированный P&L) и по итогам нескольких дней работы выбрать
# кандидатов на реальную торговлю через /leaderboard.
# Реальных ордеров бот не отправляет ни в каком режиме.
# ══════════════════════════════════════════════════════════════

config = {
    "min_profit_pct":  float(os.environ.get("MIN_PROFIT_PCT", "0.15")),
    "lot_usdt":        float(os.environ.get("LOT_USDT", "100")),      # шаг лота, в USDT-эквиваленте для любой валюты котировки
    "start_capital":   float(os.environ.get("START_CAPITAL", "10000")),
    "stop_loss_usdt":  float(os.environ.get("STOP_LOSS_USDT", "50")),
    "scan_interval":   6,
    "simulation_mode": True,   # бот только симулирует, реальных ордеров нет — см. шапку файла
    "max_trades_per_min": int(os.environ.get("MAX_TRADES_PER_MIN", "5")),
    "convert_threshold_usdt": float(os.environ.get("CONVERT_THRESHOLD_USDT", "20")),
}

# Стоп-лосс: при накопленном P&L <= -stop_loss_usdt торговля (запись в
# P&L) приостанавливается, пока не отправишь /resume вручную.
trading_paused = False
pause_reason = ""

# Валюты-мосты для арбитража: раньше сравнивались только пары COIN/USDT.
# Теперь дополнительно сравниваются COIN/BTC и COIN/ETH между биржами —
# это независимые от USDT-рынка стаканы, там тоже бывает рассинхрон.
QUOTE_CURRENCIES = ["USDT", "BTC", "ETH"]

# Bybit не используется — подтверждённо блокирует облачные IP
# (Railway/AWS/GCP) через CloudFront (403), без VPS/прокси не лечится.
FEES = {
    "Binance": 0.10,
    "KuCoin":  0.10,
    "HTX":     0.20,
}

# ══════════════════════════════════════════════════════════════
# ШИРОКИЙ СПИСОК МОНЕТ ДЛЯ СКРИНИНГА (~130 шт)
# Цель — максимальный охват, а не точечный выбор. Часть монет может
# отсутствовать на одной или нескольких биржах — это нормально, такие
# просто не попадут в сравнение (см. find_arbitrage: нужно >=2 биржи).
# ══════════════════════════════════════════════════════════════
SYMBOLS = [
    # Топ / майоры
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "TRX", "DOT", "AVAX",
    "LINK", "NEAR", "ATOM", "LTC", "BCH", "ETC", "BNB",
    # L2 / новые сети
    "MATIC", "ARB", "OP", "SUI", "APT", "ZK", "STRK", "MANTA", "SEI",
    "TIA", "INJ", "WLD", "IMX", "METIS", "BLAST",
    # DeFi
    "UNI", "AAVE", "CRV", "COMP", "MKR", "SNX", "YFI", "SUSHI", "CAKE",
    "DYDX", "LDO", "GMX", "RUNE", "1INCH", "BAL", "ZRX",
    # AI-токены
    "FET", "AGIX", "OCEAN", "RENDER", "TAO", "ARKM", "RLC",
    # Мем-коины
    "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "BOME", "MEME",
    # Игры / NFT
    "SAND", "MANA", "AXS", "GALA", "ENJ", "APE", "ILV", "MAGIC",
    # Другие L1
    "VET", "HBAR", "ALGO", "XLM", "EOS", "FTM", "ROSE", "ONE", "KAVA",
    "CELO", "ZIL", "QTUM", "WAVES", "KSM", "ICP", "KAS", "EGLD", "FLOW",
    "XTZ", "NEO", "IOTA", "IOST", "ONT", "CKB",
    # Инфраструктура / индексация / storage
    "GRT", "ANKR", "SKL", "STORJ", "FIL", "AR",
    # Прочее
    "CHZ", "GMT", "RVN", "THETA", "MASK", "GAL", "PYTH", "JUP", "JTO",
    "TON", "ORDI", "WOO", "PERP", "LRC", "BAT", "COTI",
]
QUOTE = "USDT"

# Статистика по каждой монете (агрегат по всем валютам котировки) — для /leaderboard
coin_stats: Dict[str, dict] = {
    s: {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0}
    for s in SYMBOLS
}

# Статистика по конкретным связкам (монета, валюта котировки) — для /pairs,
# отвечает на вопрос "через какую валюту конкретно нашёлся арбитраж"
pair_stats: Dict[tuple, dict] = {}

# Статистика по маршрутам биржа→биржа (независимо от монеты) — отвечает
# на вопрос "между какими конкретно биржами арбитраж встречается чаще
# и с какой маржой", для /routes
route_stats: Dict[tuple, dict] = {}

# Накопители прибыли в НЕ-USDT валютах, ожидающие конвертации.
# Как только эквивалент в USDT достигает convert_threshold_usdt — конвертируем
# (прибавляем к stats["profit_sim"], сбрасываем накопитель, шлём уведомление).
currency_balances: Dict[str, float] = {q: 0.0 for q in QUOTE_CURRENCIES if q != "USDT"}
conversions_log: List[dict] = []

stats = {
    "scans": 0, "signals": 0,
    "trades_sim": 0, "profit_sim": 0.0,
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
        await session.post(url, json={
            "chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"
        }, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logger.error(f"TG: {e}")


async def get_updates(session, offset=0):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    try:
        async with session.get(url,
            params={"offset": offset, "timeout": 30},
            timeout=aiohttp.ClientTimeout(total=35)) as r:
            return (await r.json()).get("result", [])
    except:
        return []


def check_trade_limit() -> bool:
    now = datetime.now()
    elapsed = (now - stats["minute_start"]).total_seconds()
    if elapsed >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


# ═══════════════════════════════════════
# БИРЖИ: Binance, KuCoin, HTX
# Возвращают Dict[(base, quote), {"bid":.., "ask":..}] — теперь не только
# .../USDT, но и .../BTC, .../ETH (см. QUOTE_CURRENCIES)
# ═══════════════════════════════════════

_QUOTES_SORTED = sorted(QUOTE_CURRENCIES, key=len, reverse=True)  # длинные суффиксы (USDT) проверяем раньше коротких (BTC/ETH)


async def get_binance(session) -> Dict:
    try:
        async with session.get(
            "https://data-api.binance.vision/api/v3/ticker/bookTicker",
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
                        break  # суффикс распознан (даже если base не в списке) — дальше не проверяем
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
            quotes_lower = [(q, q.lower()) for q in _QUOTES_SORTED]
            for item in (await r.json()).get("data", []):
                sym = item.get("symbol", "")
                for q, q_lower in quotes_lower:
                    if sym.endswith(q_lower):
                        base = sym[:-len(q_lower)].upper()
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


# ═══════════════════════════════════════
# АРБИТРАЖ
# ═══════════════════════════════════════

def get_quote_usdt_rate(all_data, quote):
    """Курс валюты котировки к USDT. Для USDT — всегда 1. Для BTC/ETH берём
    среднюю ask-цену по парам (BTC,USDT)/(ETH,USDT) из уже собранных данных —
    без отдельных запросов, эти монеты и так есть в основном списке SYMBOLS."""
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
            continue  # нет курса quote→USDT в этом цикле — не можем размерить лот, пропускаем
        vol_quote = lot_usdt / quote_rate  # лот в единицах валюты котировки, эквивалент ~lot_usdt

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
    return (
        f"🚨 *АРБИТРАЖ: {opp['buy_ex']} → {opp['sell_ex']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 СКРИНИНГ (симуляция)\n\n"
        f"💱 *{opp['symbol']}/{quote}*\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена: `{opp['buy_price']} {quote}`\n"
        f"   Лот: `{opp['volume_quote']} {quote}` (~{opp['volume_usdt']} USDT)\n"
        f"   Получишь: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена: `{opp['sell_price']} {quote}`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n\n"
        f"{profit_line}"
        f"   x10 лотов → `~{p10} USDT` | x50 лотов → `~{p50} USDT`\n\n"
        f"⚠️ Цена актуальна только сейчас!\n\n"
        f"🕐 {opp['time']}"
    )


# ═══════════════════════════════════════
# СКАН
# ═══════════════════════════════════════

async def fetch_all(session):
    results = await asyncio.gather(
        get_binance(session), get_kucoin(session), get_htx(session),
        return_exceptions=True
    )
    ex_names = ["Binance", "KuCoin", "HTX"]
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
    global trading_paused, pause_reason
    if trading_paused:
        logger.info(f"Пропуск сделки — торговля на паузе ({pause_reason})")
        return
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
    cs["profit_usdt"] += opp["profit_usdt"]  # для лидерборда всегда в USDT-эквиваленте, для сравнимости монет между собой

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
        # прибыль уже в USDT — сразу в реализованный P&L, без накопителя
        stats["profit_sim"] += opp["profit_usdt"]
    else:
        # прибыль в BTC/ETH — копится в отдельном балансе до порога конвертации
        currency_balances[quote] = currency_balances.get(quote, 0.0) + opp["profit_quote"]
        pending_value_usdt = currency_balances[quote] * opp["quote_rate"]
        logger.info(f"Накоплено в {quote}: {round(currency_balances[quote], 8)} (~{round(pending_value_usdt, 2)} USDT)")

        if pending_value_usdt >= config["convert_threshold_usdt"]:
            converted_amount = currency_balances[quote]
            converted_usdt = pending_value_usdt
            currency_balances[quote] = 0.0
            stats["profit_sim"] += converted_usdt
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

    if stats["profit_sim"] <= -config["stop_loss_usdt"]:
        trading_paused = True
        pause_reason = f"стоп-лосс {config['stop_loss_usdt']} USDT достигнут (P&L: {round(stats['profit_sim'], 2)})"
        logger.warning(f"СТОП-ЛОСС СРАБОТАЛ: {pause_reason}")
        if session is not None:
            await send_tg(session,
                f"🛑 *СТОП-ЛОСС СРАБОТАЛ*\n"
                f"Накопленный P&L: `{round(stats['profit_sim'], 2)} USDT` (лимит: -{config['stop_loss_usdt']} USDT)\n\n"
                f"Исполнение сделок приостановлено. Сигналы дальше — только справочно.\n"
                f"Включить обратно — команда `/resume`."
            )


# ═══════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════

async def handle_command(session, text, chat_id):
    global CHAT_ID, trading_paused, pause_reason
    CHAT_ID = chat_id
    parts = text.strip().split()
    cmd = parts[0].lower()

    if cmd == "/start":
        await send_tg(session,
            f"✅ *ArbScreenerBot запущен!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Назначение: СКРИНИНГ — искать новые монеты для будущей реальной торговли.\n"
            f"Площадки: Binance, KuCoin, HTX\n"
            f"Монет в скрининге: {len(SYMBOLS)}\n"
            f"Валюты котировки: {', '.join(QUOTE_CURRENCIES)} (не только USDT — ещё COIN/BTC и COIN/ETH)\n\n"
            f"⚙️ Стартовый капитал (справочно): `{config['start_capital']} USDT`\n"
            f"⚙️ Лот/шаг сделки: `{config['lot_usdt']} USDT`-эквивалент\n"
            f"⚙️ Стоп-лосс: `-{config['stop_loss_usdt']} USDT` (только `/resume` включает обратно)\n"
            f"⚙️ Порог маржи: `{config['min_profit_pct']}%`\n"
            f"⚙️ Порог автоконвертации BTC/ETH→USDT: `{config['convert_threshold_usdt']} USDT`\n"
            f"⚙️ Лимит: `{config['max_trades_per_min']} сделок/мин`\n\n"
            f"/scan — скан сейчас\n"
            f"/top — топ пар по спреду (по всем валютам котировки)\n"
            f"/prices SYMBOL — цены по монете на всех биржах и валютах котировки\n"
            f"/exchanges — диагностика бирж\n"
            f"/leaderboard — рейтинг монет-кандидатов на реал (агрегат по всем валютам)\n"
            f"/pairs — рейтинг конкретных связок монета/валюта\n"
            f"/routes — рейтинг маршрутов биржа→биржа (где арбитраж чаще всего)\n"
            f"/balances — накопленные BTC/ETH, ожидающие конвертации\n"
            f"/stats — статистика\n"
            f"/history — последние сделки\n"
            f"/pause — приостановить торговлю вручную\n"
            f"/resume — снять паузу (ручную или после стоп-лосса)\n"
            f"/setprofit 0.15 — порог маржи\n"
            f"/setlot 100 — изменить размер лота\n"
        )

    elif cmd == "/scan":
        await send_tg(session, f"🔍 Сканирую 3 биржи, {len(SYMBOLS)} монет...")
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
            get_binance(session), get_kucoin(session), get_htx(session),
            return_exceptions=True
        )
        ex_names = ["Binance", "KuCoin", "HTX"]
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
        await send_tg(session, "📊 Ищу лучшие пары по всем 3 биржам и всем валютам котировки...")
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
            msg += (
                f"{icon} *{i}. {opp['symbol']}/{opp['quote']}* {opp['buy_ex']}→{opp['sell_ex']}\n"
                f"   Спред: `{opp['gross_pct']}%` | Чистая: `{opp['net_pct']}%`\n"
            )
        msg += f"\n_Порог сигнала: {config['min_profit_pct']}%_"
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
            for ex in ("Binance", "KuCoin", "HTX"):
                if ex in ex_data:
                    d = ex_data[ex]
                    msg += f"  {ex}: bid `{d['bid']}` / ask `{d['ask']}`\n"
                else:
                    msg += f"  ⚠️ {ex}: нет данных\n"
            msg += "\n"
        await send_tg(session, msg)

    elif cmd == "/leaderboard":
        ranked = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        ranked = [r for r in ranked if r[1]["signals"] > 0][:20]
        if not ranked:
            await send_tg(session, "Пока нет ни одного сигнала ни по одной монете. Дай боту поработать подольше или снизь /setprofit.")
            return
        msg = "🏆 *РЕЙТИНГ КАНДИДАТОВ НА РЕАЛ*\n(агрегат по всем валютам котировки, сортировка по числу сигналов)\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
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
            f"⏸ Пауза: {('*ДА* — ' + pause_reason) if trading_paused else 'нет'}\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок (симуляция): {stats['trades_sim']}\n"
            f"💰 Прибыль реализованная (сконвертирована в USDT): {round(stats['profit_sim'], 4)} USDT\n"
            f"⏳ Ожидает конвертации: {pending_line}\n"
            f"🔄 Конвертаций всего: {len(conversions_log)}\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"⏱ Сделок этой минуты: {stats['trades_this_minute']}/{config['max_trades_per_min']}\n\n"
            f"⚙️ Стартовый капитал: {config['start_capital']} USDT\n"
            f"⚙️ Лот: {config['lot_usdt']} USDT-эквивалент\n"
            f"⚙️ Стоп-лосс: -{config['stop_loss_usdt']} USDT (считается по реализованному P&L)\n"
            f"⚙️ Порог маржи: {config['min_profit_pct']}%\n"
            f"⚙️ Порог автоконвертации: {config['convert_threshold_usdt']} USDT\n"
            f"⚙️ Монет в скрининге: {len(SYMBOLS)}\n"
            f"⚙️ Валюты котировки: {', '.join(QUOTE_CURRENCIES)}\n"
            f"⚙️ Бирж: 3 (Binance/KuCoin/HTX)\n\n"
            f"/leaderboard — какие монеты реально сработали | /pairs — через какую валюту | /balances — детали накоплений"
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

    elif cmd == "/pause":
        if trading_paused:
            await send_tg(session, f"⏸ Уже на паузе: {pause_reason}\nВключить обратно — /resume.")
        else:
            trading_paused = True
            pause_reason = "ручная пауза (/pause)"
            await send_tg(session,
                "⏸ *Торговля приостановлена вручную*\n"
                "Сканирование и сигналы продолжаются как обычно, но новые сделки в P&L не пишутся.\n"
                "Включить обратно — `/resume`."
            )

    elif cmd == "/resume":
        if not trading_paused:
            await send_tg(session, "✅ Торговля и так не на паузе — стоп-лосс не срабатывал.")
        else:
            trading_paused = False
            old_reason = pause_reason
            pause_reason = ""
            await send_tg(session,
                f"▶️ *Торговля возобновлена вручную*\nБыла на паузе из-за: {old_reason}\n"
                f"P&L симуляции НЕ сброшен."
            )

    elif cmd == "/setprofit":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setprofit 0.15`")
            return
        try:
            config["min_profit_pct"] = float(parts[1])
            await send_tg(session, f"✅ Порог маржи: `{config['min_profit_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setprofit 0.15`")

    elif cmd == "/setlot":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setlot 100`")
            return
        try:
            config["lot_usdt"] = float(parts[1])
            await send_tg(session, f"✅ Лот: `{config['lot_usdt']} USDT`")
        except:
            await send_tg(session, "❌ Пример: `/setlot 100`")

    else:
        await send_tg(session,
            "/start /scan /top /prices SYMBOL /exchanges\n"
            "/leaderboard /pairs /routes /balances\n"
            "/stats /history /pause /resume\n"
            "/setprofit 0.15 /setlot 100"
        )


# ═══════════════════════════════════════
# ЦИКЛЫ
# ═══════════════════════════════════════

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
                    await handle_command(session, text, chat_id)
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
        f"ArbScreenerBot | {len(SYMBOLS)} монет | 3 биржи (Binance/KuCoin/HTX) | "
        f"лот {config['lot_usdt']} USDT | стоп-лосс -{config['stop_loss_usdt']} USDT | "
        f"порог {config['min_profit_pct']}%"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
