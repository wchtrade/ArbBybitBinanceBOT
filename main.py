import asyncio
import aiohttp
import logging
import os
import hashlib
import hmac
import base64
import time
import json
import math
import uuid
from datetime import datetime
from urllib.parse import urlencode
from typing import Dict, List, Optional, Tuple

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
                f"Сканирование остановлено полностью, новых сигналов не будет.\n"
                f"Включить обратно — команда `/resume`."
            )


# ═══════════════════════════════════════
# КОМАНДЫ
# ═══════════════════════════════════════

async def handle_command(session, text, chat_id):
    global CHAT_ID, trading_paused, pause_reason
    global real_trading_paused, real_pause_reason, _confirm_real_runtime
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
        if trading_paused:
            await send_tg(session, f"⏸ Бот на паузе: {pause_reason}\nСигналы не показываются. Включить обратно — /resume.")
            return
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
                "Сканирование остановлено полностью — новых сигналов в чат не будет, пока не снимешь паузу.\n"
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

    # ── КОМАНДЫ РЕАЛЬНОЙ ТОРГОВЛИ ────────────────────────────────────────
    elif cmd == "/addcoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/addcoin ZIL`")
            return
        base = parts[1].upper()
        if base in REAL_SYMBOLS:
            await send_tg(session, f"{base} уже в реальной торговле.")
            return
        REAL_SYMBOLS.append(base)
        await send_tg(session, f"🔍 Проверяю фильтры {base} на всех биржах...")
        for ex in sorted(set(get_buy_exchanges()) | set(get_sell_exchanges())):
            f = await get_real_filters(session, ex, base)
            await send_tg(session, f"{ex} {base}: шаг `{f['step']}`, мин. ордер `{f['min_notional']}`")
        await send_tg(session, f"✅ {base} добавлена в реальную торговлю: {', '.join(REAL_SYMBOLS)}")

    elif cmd == "/removecoin":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/removecoin ZIL`")
            return
        base = parts[1].upper()
        if base not in REAL_SYMBOLS:
            await send_tg(session, f"{base} и так не в списке.")
            return
        REAL_SYMBOLS.remove(base)
        await send_tg(session, f"✅ {base} убрана из реальной торговли. Осталось: {', '.join(REAL_SYMBOLS) or '(пусто)'}")

    elif cmd == "/setreallot":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setreallot 12`")
            return
        try:
            val = min(float(parts[1]), 15.0)  # жёсткий потолок
            real_config["max_real_order_usdt"] = val
            await send_tg(session, f"✅ Лимит на ордер: `{val} USDT` (жёсткий потолок 15 USDT)")
        except:
            await send_tg(session, "❌ Пример: `/setreallot 12`")

    elif cmd == "/setbalancebuffer":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setbalancebuffer 5`")
            return
        try:
            real_config["balance_safety_buffer_pct"] = float(parts[1])
            await send_tg(session, f"✅ Буфер preflight-проверки: `{real_config['balance_safety_buffer_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setbalancebuffer 5`")

    elif cmd == "/setheadroom":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setheadroom 20`")
            return
        try:
            real_config["rebalance_headroom_pct"] = float(parts[1])
            await send_tg(session, f"✅ Headroom ребаланса: `{real_config['rebalance_headroom_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setheadroom 20`")

    elif cmd == "/setrebalance":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setrebalance 1` (в лотах)")
            return
        try:
            real_config["rebalance_target_lots"] = float(parts[1])
            await send_tg(session, f"✅ Целевой резерв: `{real_config['rebalance_target_lots']}` лотов")
        except:
            await send_tg(session, "❌ Пример: `/setrebalance 1`")

    elif cmd == "/rebalancelive":
        if len(parts) < 2 or parts[1].lower() not in ("on", "off"):
            await send_tg(session, "Пример: `/rebalancelive on` или `/rebalancelive off`")
            return
        real_config["rebalance_live"] = parts[1].lower() == "on"
        await send_tg(session, f"✅ Реальные ордера ребаланса: `{'включены' if real_config['rebalance_live'] else 'выключены'}`")

    elif cmd in ("/rebalance", "/autorebalance"):
        await send_tg(session, "⚖️ Считаю план ребаланса...")
        result = await run_auto_rebalance(session, live=real_config["rebalance_live"])
        await send_tg(session, f"⚖️ *РЕБАЛАНС*\n{result}")

    elif cmd == "/realbalance":
        await send_tg(session, "📊 Собираю реальные балансы по всем биржам...")
        plan = await real_exchange_rebalance_plan(session)
        msg = "📊 *РЕАЛЬНЫЙ БАЛАНС И ПЛАН*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for entry in plan:
            roles = []
            if entry["role_buy"]:
                roles.append("покупка")
            if entry["role_sell"]:
                roles.append("продажа")
            msg += f"*{entry['exchange']}* ({', '.join(roles) or 'нет роли'})\n"
            usdt_bal = entry["usdt_balance"]
            msg += f"  USDT: `{round(usdt_bal,4) if usdt_bal is not None else '?'}`"
            if entry["role_buy"]:
                delta = entry["usdt_delta"]
                msg += f" (цель `{round(entry['usdt_target'],2)}`, дельта `{round(delta,2) if delta is not None else '?'}`)"
            msg += "\n"
            for base, info in entry["coins"].items():
                bal = info["balance"]
                msg += f"  {base}: `{round(bal,6) if bal is not None else '?'}`"
                if entry["role_sell"] and info.get("target"):
                    delta = info["delta"]
                    msg += f" (цель `{round(info['target'],6)}`, дельта `{round(delta,6) if delta is not None else '?'}`)"
                msg += "\n"
            msg += "\n"
        await send_tg(session, msg)

    elif cmd == "/confirmreal":
        phrase = text[len("/confirmreal"):].strip()
        if phrase == CONFIRM_REAL_PHRASE:
            _confirm_real_runtime = True
            await send_tg(session, f"✅ Подтверждение принято для этой сессии.\n\n{real_trading_status_text()}")
        else:
            await send_tg(session, f"❌ Фраза не совпадает. Нужна ровно: `{CONFIRM_REAL_PHRASE}`")

    elif cmd == "/realpause":
        if real_trading_paused:
            await send_tg(session, f"⏸ Реальная торговля уже на паузе: {real_pause_reason}")
        else:
            real_trading_paused = True
            real_pause_reason = "ручная пауза (/realpause)"
            await send_tg(session, "⏸ Реальная торговля приостановлена вручную. Включить — /realresume.")

    elif cmd == "/realresume":
        if not real_trading_paused:
            await send_tg(session, "✅ Реальная торговля и так не на паузе.")
        else:
            real_trading_paused = False
            old = real_pause_reason
            real_pause_reason = ""
            await send_tg(session, f"▶️ Реальная торговля возобновлена. Была на паузе: {old}")

    elif cmd == "/realstatus":
        reset_real_day_if_needed()
        await send_tg(session,
            f"🔴 *РЕАЛЬНАЯ ТОРГОВЛЯ — СТАТУС*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Готовность: {'✅ РАЗБЛОКИРОВАНА' if real_trading_ready() else '❌ заблокирована'}\n"
            f"{real_trading_status_text()}\n\n"
            f"⏸ Пауза: {('да — ' + real_pause_reason) if real_trading_paused else 'нет'}\n\n"
            f"Монеты: {', '.join(REAL_SYMBOLS) or '(пусто)'}\n"
            f"Маршруты: {', '.join(f'{b}→{s}' for b, s in PAIRS)}\n\n"
            f"Сделок сегодня: {real_stats['trades_today']}/{real_config['max_trades_per_day']}\n"
            f"P&L сегодня: {round(real_stats['pnl_today_usdt'],4)} USDT (стоп-лосс -{real_config['daily_stop_loss_usdt']})\n"
            f"Автодокупок: {real_stats['topups']} | Ошибок: {real_stats['errors']}\n\n"
            f"Лот: {real_config['max_real_order_usdt']} USDT | Буфер: {real_config['balance_safety_buffer_pct']}% | "
            f"Headroom: {real_config['rebalance_headroom_pct']}%\n"
            f"Ребаланс: цель {real_config['rebalance_target_lots']} лот(ов), live={'on' if real_config['rebalance_live'] else 'off'}"
        )

    elif cmd == "/realhistory":
        if not real_trade_history:
            await send_tg(session, "📋 Реальных сделок ещё не было.")
            return
        msg = "📋 *ПОСЛЕДНИЕ РЕАЛЬНЫЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in real_trade_history[-10:][::-1]:
            msg += (
                f"#{t['id']} *{t['base']}* {t['buy_ex']}→{t['sell_ex']}\n"
                f"   {t['qty']} монет | лот {t['lot_usdt']} USDT | факт. профит {t['profit_usdt']} USDT | {t['time']}\n\n"
            )
        await send_tg(session, msg)

    else:
        await send_tg(session,
            "/start /scan /top /prices SYMBOL /exchanges\n"
            "/leaderboard /pairs /routes /balances\n"
            "/stats /history /pause /resume\n"
            "/setprofit 0.15 /setlot 100\n\n"
            "🔴 Реальная торговля:\n"
            "/addcoin /removecoin /setreallot /setbalancebuffer\n"
            "/setheadroom /setrebalance /rebalancelive on|off\n"
            "/rebalance /realbalance /confirmreal <фраза>\n"
            "/realpause /realresume /realstatus /realhistory"
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
        if trading_paused:
            await asyncio.sleep(config["scan_interval"])
            continue
        try:
            opps, active = await scan_cycle(session)
            logger.info(f"Scan #{stats['scans']}: {len(active)} бирж, {len(opps)} сигналов")
            for opp in opps[:5]:
                if trading_paused:  # могло смениться прямо во время обработки списка сигналов
                    break
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


# ══════════════════════════════════════════════════════════════════════════
# WorkerArbBot — РЕАЛЬНАЯ АРБИТРАЖНАЯ ТОРГОВЛЯ (Binance/KuCoin/HTX)
# ══════════════════════════════════════════════════════════════════════════
# Отдельный слой поверх скринера выше — не трогает ни одну из старых функций
# (/leaderboard, /pairs, /routes, /balances, /top, /prices, /exchanges,
# мультивалютный арбитраж, /pause, /resume, стоп-лосс симуляции). Здесь —
# реальные подписанные ордера, отдельные лимиты, отдельные команды.
#
# ⚠️ ЭТОТ КОД НЕ ПРОВЕРЕН НА ЖИВЫХ API — писался по детальной спецификации,
# без доступа к боевым ключам и без сети до бирж из песочницы, где я его
# собирал. Прежде чем включать реальные деньги: (1) прогони в SIMULATION
# минимум несколько дней, (2) включай REAL с минимальным /setreallot
# (10-15 USDT) и проверь на паре сделок, что подпись запросов и парсинг
# ответов бирж работают как ожидается — особенно KuCoin (пасфраза) и HTX
# (account-id, специфичная схема подписи).

# ── РОЛИ БИРЖ И АКТИВНЫЕ МАРШРУТЫ ──────────────────────────────────────────
# Binance и KuCoin — только ПОКУПАЮТ. HTX — только ПРОДАЁТ.
# Пара HTX→KuCoin намеренно отключена: при небольшом капитале HTX не может
# одновременно держать резерв USDT под покупку И резерв монеты под продажу —
# регулярно приводило к отказам "insufficient balance".
PAIRS: List[Tuple[str, str]] = [("KuCoin", "HTX"), ("Binance", "HTX")]


def get_buy_exchanges() -> List[str]:
    return sorted({p[0] for p in PAIRS})


def get_sell_exchanges() -> List[str]:
    return sorted({p[1] for p in PAIRS})


# Монеты в реальной торговле — отдельный список от скринингового SYMBOLS,
# управляется командами /addcoin /removecoin. Начинаем с одной монеты.
REAL_SYMBOLS: List[str] = ["ZIL"]

real_config = {
    "max_real_order_usdt":       float(os.environ.get("MAX_REAL_ORDER_USDT", "15")),
    "min_profit_pct":            float(os.environ.get("REAL_MIN_PROFIT_PCT", "0.30")),
    "scan_interval":             int(os.environ.get("REAL_SCAN_INTERVAL", "10")),
    "balance_safety_buffer_pct": float(os.environ.get("BALANCE_SAFETY_BUFFER_PCT", "5")),
    "rebalance_headroom_pct":    float(os.environ.get("REBALANCE_HEADROOM_PCT", "20")),
    "rebalance_target_lots":     float(os.environ.get("REBALANCE_TARGET_LOTS", "1")),
    "rebalance_live":            os.environ.get("REBALANCE_LIVE", "off").lower() == "on",
    "max_trades_per_day":        int(os.environ.get("MAX_REAL_TRADES_PER_DAY", "20")),
    "daily_stop_loss_usdt":      float(os.environ.get("REAL_DAILY_STOP_LOSS_USDT", "5")),
}

real_trading_paused = False
real_pause_reason = ""

real_stats = {
    "day": datetime.now().strftime("%Y-%m-%d"),
    "trades_today": 0,
    "pnl_today_usdt": 0.0,
    "topups": 0,
    "errors": 0,
    "trades_this_minute": 0,
    "minute_start": datetime.now(),
}
real_trade_history: List[dict] = []
_last_exchange_error = ""

# Кэш точности/минимального ордера по (биржа, монета) — заполняется при
# старте и при /addcoin
REAL_FILTERS: Dict[Tuple[str, str], dict] = {}

# Backoff при 429/418 от конкретной биржи
_exchange_backoff_until: Dict[str, float] = {}


# ── ГЕЙТ РЕАЛЬНОЙ ТОРГОВЛИ ──────────────────────────────────────────────────
REAL_TRADING_UNLOCKED_ENV = os.environ.get("REAL_TRADING_UNLOCKED", "").upper() == "YES"
CONFIRM_REAL_PHRASE = os.environ.get("CONFIRM_REAL_PHRASE", "CONFIRM REAL TRADING")
_confirm_real_runtime = False

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
KUCOIN_API_KEY = os.environ.get("KUCOIN_API_KEY", "")
KUCOIN_API_SECRET = os.environ.get("KUCOIN_API_SECRET", "")
KUCOIN_API_PASSPHRASE = os.environ.get("KUCOIN_API_PASSPHRASE", "")
HTX_ACCESS_KEY = os.environ.get("HTX_ACCESS_KEY", "")
HTX_SECRET_KEY = os.environ.get("HTX_SECRET_KEY", "")


def real_trading_ready() -> bool:
    keys_present = all([
        BINANCE_API_KEY, BINANCE_API_SECRET,
        KUCOIN_API_KEY, KUCOIN_API_SECRET, KUCOIN_API_PASSPHRASE,
        HTX_ACCESS_KEY, HTX_SECRET_KEY,
    ])
    return REAL_TRADING_UNLOCKED_ENV and _confirm_real_runtime and keys_present


def real_trading_status_text() -> str:
    checks = [
        ("Переменная REAL_TRADING_UNLOCKED=YES", REAL_TRADING_UNLOCKED_ENV),
        ("Подтверждение /confirmreal в этой сессии", _confirm_real_runtime),
        ("Binance ключи", bool(BINANCE_API_KEY and BINANCE_API_SECRET)),
        ("KuCoin ключи (+ passphrase)", bool(KUCOIN_API_KEY and KUCOIN_API_SECRET and KUCOIN_API_PASSPHRASE)),
        ("HTX ключи", bool(HTX_ACCESS_KEY and HTX_SECRET_KEY)),
    ]
    return "\n".join(f"{'✅' if ok else '❌'} {label}" for label, ok in checks)


def check_backoff(exchange: str) -> Optional[float]:
    until = _exchange_backoff_until.get(exchange, 0)
    remaining = until - time.time()
    return remaining if remaining > 0 else None


def set_backoff(exchange: str, status: int):
    seconds = 300 if status == 418 else 120
    _exchange_backoff_until[exchange] = time.time() + seconds
    logger.warning(f"{exchange}: backoff {seconds}s после HTTP {status}")


def floor_to_step(qty: float, step: float) -> float:
    if step <= 0:
        return qty
    return math.floor(qty / step) * step


# ── ПОДПИСЬ И ЗАПРОСЫ: BINANCE ──────────────────────────────────────────────
BINANCE_SIGNED_BASE = "https://api.binance.com"
BINANCE_MARKET_BASE = "https://data-api.binance.vision"


def binance_sign(params: dict) -> str:
    query = urlencode(params)
    sig = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    return query + f"&signature={sig}"


async def binance_signed_request(session, method: str, path: str, params: dict):
    global _last_exchange_error
    if check_backoff("Binance"):
        return 429, {"error": "backoff"}
    params = {**params, "timestamp": int(time.time() * 1000), "recvWindow": 10000}
    query = binance_sign(params)
    url = f"{BINANCE_SIGNED_BASE}{path}?{query}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    try:
        async with session.request(method, url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if r.status in (418, 429):
                set_backoff("Binance", r.status)
            if r.status != 200:
                _last_exchange_error = f"Binance {r.status}: {data}"
            return r.status, data
    except Exception as e:
        _last_exchange_error = f"Binance exception: {e}"
        return 0, {"error": str(e)}


async def binance_get_balance(session, asset: str) -> Optional[float]:
    status, d = await binance_signed_request(session, "GET", "/api/v3/account", {})
    if status != 200:
        return None
    for b in d.get("balances", []):
        if b["asset"] == asset:
            return float(b["free"])
    return None


async def binance_market_buy(session, symbol: str, quote_qty: float):
    return await binance_signed_request(session, "POST", "/api/v3/order", {
        "symbol": symbol, "side": "BUY", "type": "MARKET", "quoteOrderQty": round(quote_qty, 8),
    })


async def binance_market_sell(session, symbol: str, quantity: float):
    return await binance_signed_request(session, "POST", "/api/v3/order", {
        "symbol": symbol, "side": "SELL", "type": "MARKET", "quantity": quantity,
    })


async def binance_get_filters(session, symbol: str):
    try:
        async with session.get(f"{BINANCE_MARKET_BASE}/api/v3/exchangeInfo",
                                params={"symbol": symbol}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            info = d["symbols"][0]
            step, min_notional = 0.000001, 10.0
            for f in info["filters"]:
                if f["filterType"] == "LOT_SIZE":
                    step = float(f["stepSize"])
                if f["filterType"] in ("MIN_NOTIONAL", "NOTIONAL"):
                    min_notional = float(f.get("minNotional", f.get("minNotionalValue", 10.0)))
            return {"step": step, "min_notional": min_notional}
    except Exception as e:
        logger.error(f"Binance filters {symbol}: {e}")
        return {"step": 0.000001, "min_notional": 10.0}


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


# ── ПОДПИСЬ И ЗАПРОСЫ: KUCOIN ────────────────────────────────────────────────
KUCOIN_BASE = "https://api.kucoin.com"


def kucoin_headers(method: str, endpoint_with_query: str, body_str: str = "") -> dict:
    timestamp = str(int(time.time() * 1000))
    str_to_sign = timestamp + method + endpoint_with_query + body_str
    signature = base64.b64encode(
        hmac.new(KUCOIN_API_SECRET.encode(), str_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    passphrase_signed = base64.b64encode(
        hmac.new(KUCOIN_API_SECRET.encode(), KUCOIN_API_PASSPHRASE.encode(), hashlib.sha256).digest()
    ).decode()
    return {
        "KC-API-KEY": KUCOIN_API_KEY, "KC-API-SIGN": signature, "KC-API-TIMESTAMP": timestamp,
        "KC-API-PASSPHRASE": passphrase_signed, "KC-API-KEY-VERSION": "2",
        "Content-Type": "application/json",
    }


async def kucoin_signed_request(session, method: str, endpoint: str, params: dict = None, body: dict = None):
    global _last_exchange_error
    if check_backoff("KuCoin"):
        return 429, {"error": "backoff"}
    query = f"?{urlencode(params)}" if params else ""
    body_str = json.dumps(body) if body else ""
    headers = kucoin_headers(method, endpoint + query, body_str)
    url = f"{KUCOIN_BASE}{endpoint}{query}"
    try:
        async with session.request(method, url, headers=headers,
                                    data=body_str if body else None,
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if r.status in (418, 429):
                set_backoff("KuCoin", r.status)
            if r.status != 200 or str(data.get("code", "200000")) != "200000":
                _last_exchange_error = f"KuCoin {r.status}: {data}"
            return r.status, data
    except Exception as e:
        _last_exchange_error = f"KuCoin exception: {e}"
        return 0, {"error": str(e)}


async def kucoin_get_balance(session, currency: str) -> Optional[float]:
    status, d = await kucoin_signed_request(session, "GET", "/api/v1/accounts",
                                             {"currency": currency, "type": "trade"})
    if status != 200:
        return None
    for acc in d.get("data", []):
        if acc.get("currency") == currency:
            return float(acc["available"])
    return None


async def kucoin_market_buy(session, symbol: str, funds: float):
    body = {"clientOid": str(uuid.uuid4()), "side": "buy", "symbol": symbol, "type": "market",
            "funds": str(round(funds, 8))}
    return await kucoin_signed_request(session, "POST", "/api/v1/orders", body=body)


async def kucoin_market_sell(session, symbol: str, size: float):
    body = {"clientOid": str(uuid.uuid4()), "side": "sell", "symbol": symbol, "type": "market",
            "size": str(size)}
    return await kucoin_signed_request(session, "POST", "/api/v1/orders", body=body)


async def kucoin_get_order(session, order_id: str):
    return await kucoin_signed_request(session, "GET", f"/api/v1/orders/{order_id}")


async def kucoin_get_filters(session, symbol: str):
    try:
        async with session.get(f"{KUCOIN_BASE}/api/v1/symbols", timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            for s in d.get("data", []):
                if s.get("symbol") == symbol:
                    return {
                        "step": float(s.get("baseIncrement", 0.000001)),
                        "min_notional": float(s.get("minFunds", 5.0) or 5.0),
                    }
    except Exception as e:
        logger.error(f"KuCoin filters {symbol}: {e}")
    return {"step": 0.000001, "min_notional": 5.0}


async def get_orderbook_kucoin(session, symbol: str):
    try:
        async with session.get(f"{KUCOIN_BASE}/api/v1/market/orderbook/level2_20",
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


# ── ПОДПИСЬ И ЗАПРОСЫ: HTX ───────────────────────────────────────────────────
HTX_HOST = "api.huobi.pro"
HTX_BASE = f"https://{HTX_HOST}"
_htx_account_id_cache: Optional[str] = None


def htx_sign(method: str, path: str, params: dict) -> dict:
    signed = dict(params)
    signed.update({
        "AccessKeyId": HTX_ACCESS_KEY, "SignatureMethod": "HmacSHA256",
        "SignatureVersion": "2", "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    })
    encoded = urlencode(sorted(signed.items()))
    payload = f"{method}\n{HTX_HOST}\n{path}\n{encoded}"
    signature = base64.b64encode(
        hmac.new(HTX_SECRET_KEY.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()
    signed["Signature"] = signature
    return signed


async def htx_signed_get(session, path: str, params: dict = None):
    global _last_exchange_error
    if check_backoff("HTX"):
        return 429, {"error": "backoff"}
    signed = htx_sign("GET", path, params or {})
    try:
        async with session.get(f"{HTX_BASE}{path}", params=signed,
                                timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if r.status in (418, 429):
                set_backoff("HTX", r.status)
            if r.status != 200 or data.get("status") == "error":
                _last_exchange_error = f"HTX {r.status}: {data}"
            return r.status, data
    except Exception as e:
        _last_exchange_error = f"HTX exception: {e}"
        return 0, {"error": str(e)}


async def htx_signed_post(session, path: str, body: dict = None):
    global _last_exchange_error
    if check_backoff("HTX"):
        return 429, {"error": "backoff"}
    signed_qs = htx_sign("POST", path, {})
    url = f"{HTX_BASE}{path}?{urlencode(signed_qs)}"
    try:
        async with session.post(url, json=body or {}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            data = await r.json()
            if r.status in (418, 429):
                set_backoff("HTX", r.status)
            if r.status != 200 or data.get("status") == "error":
                _last_exchange_error = f"HTX {r.status}: {data}"
            return r.status, data
    except Exception as e:
        _last_exchange_error = f"HTX exception: {e}"
        return 0, {"error": str(e)}


async def htx_get_account_id(session) -> Optional[str]:
    global _htx_account_id_cache
    if _htx_account_id_cache:
        return _htx_account_id_cache
    status, d = await htx_signed_get(session, "/v1/account/accounts")
    if status == 200:
        for acc in d.get("data", []):
            if acc.get("type") == "spot":
                _htx_account_id_cache = str(acc["id"])
                return _htx_account_id_cache
    return None


async def htx_get_balance(session, currency: str) -> Optional[float]:
    acc_id = await htx_get_account_id(session)
    if not acc_id:
        return None
    status, d = await htx_signed_get(session, f"/v1/account/accounts/{acc_id}/balance")
    if status != 200:
        return None
    for item in d.get("data", {}).get("list", []):
        if item.get("currency") == currency.lower() and item.get("type") == "trade":
            return float(item["balance"])
    return None


async def htx_market_buy(session, symbol: str, quote_amount: float):
    acc_id = await htx_get_account_id(session)
    if not acc_id:
        return 0, {"error": "no HTX account id"}
    body = {"account-id": acc_id, "amount": str(round(quote_amount, 8)),
            "symbol": symbol, "type": "buy-market", "source": "spot-api"}
    return await htx_signed_post(session, "/v1/order/orders/place", body)


async def htx_market_sell(session, symbol: str, base_amount: float):
    acc_id = await htx_get_account_id(session)
    if not acc_id:
        return 0, {"error": "no HTX account id"}
    body = {"account-id": acc_id, "amount": str(base_amount),
            "symbol": symbol, "type": "sell-market", "source": "spot-api"}
    return await htx_signed_post(session, "/v1/order/orders/place", body)


async def htx_get_order(session, order_id: str):
    return await htx_signed_get(session, f"/v1/order/orders/{order_id}")


async def htx_get_filters(session, symbol: str):
    try:
        async with session.get(f"{HTX_BASE}/v1/common/symbols", timeout=aiohttp.ClientTimeout(total=10)) as r:
            d = await r.json()
            for s in d.get("data", []):
                if s.get("symbol") == symbol:
                    amt_prec = int(s.get("amount-precision", 4))
                    step = 10 ** (-amt_prec)
                    min_notional = float(s.get("min-order-value", 10.0) or 10.0)
                    return {"step": step, "min_notional": min_notional}
    except Exception as e:
        logger.error(f"HTX filters {symbol}: {e}")
    return {"step": 0.0001, "min_notional": 10.0}


async def get_orderbook_htx(session, symbol: str):
    try:
        async with session.get(f"{HTX_BASE}/market/depth",
                                params={"symbol": symbol, "type": "step0"},
                                timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json()
            tick = d.get("tick", {}) or {}
            return {
                "bids": [[float(p), float(q)] for p, q in tick.get("bids", [])],
                "asks": [[float(p), float(q)] for p, q in tick.get("asks", [])],
            }
    except Exception as e:
        logger.error(f"HTX orderbook {symbol}: {e}")
        return None


# ── ЕДИНЫЕ ОБЁРТКИ ПО БИРЖАМ ──────────────────────────────────────────────────
def sym_binance(base: str) -> str: return f"{base}USDT"
def sym_kucoin(base: str) -> str: return f"{base}-USDT"
def sym_htx(base: str) -> str: return f"{base.lower()}usdt"


SYMBOL_FMT   = {"Binance": sym_binance, "KuCoin": sym_kucoin, "HTX": sym_htx}
ORDERBOOK_FN = {"Binance": get_orderbook_binance, "KuCoin": get_orderbook_kucoin, "HTX": get_orderbook_htx}
BALANCE_FN   = {"Binance": binance_get_balance, "KuCoin": kucoin_get_balance, "HTX": htx_get_balance}
FILTERS_FN   = {"Binance": binance_get_filters, "KuCoin": kucoin_get_filters, "HTX": htx_get_filters}


async def real_market_buy(session, exchange: str, base: str, quote_qty: float):
    symbol = SYMBOL_FMT[exchange](base)
    if exchange == "Binance":
        return await binance_market_buy(session, symbol, quote_qty)
    if exchange == "KuCoin":
        return await kucoin_market_buy(session, symbol, quote_qty)
    if exchange == "HTX":
        return await htx_market_buy(session, symbol, quote_qty)
    raise ValueError(exchange)


async def real_market_sell(session, exchange: str, base: str, base_qty: float):
    symbol = SYMBOL_FMT[exchange](base)
    if exchange == "Binance":
        return await binance_market_sell(session, symbol, base_qty)
    if exchange == "KuCoin":
        return await kucoin_market_sell(session, symbol, base_qty)
    if exchange == "HTX":
        return await htx_market_sell(session, symbol, base_qty)
    raise ValueError(exchange)


async def get_real_filters(session, exchange: str, base: str) -> dict:
    key = (exchange, base)
    if key in REAL_FILTERS:
        return REAL_FILTERS[key]
    symbol = SYMBOL_FMT[exchange](base)
    filters = await FILTERS_FN[exchange](session, symbol)
    REAL_FILTERS[key] = filters
    return filters


async def init_real_filters(session):
    for base in REAL_SYMBOLS:
        for ex in set(get_buy_exchanges()) | set(get_sell_exchanges()):
            f = await get_real_filters(session, ex, base)
            logger.info(f"Фильтры {ex} {base}: {f}")


# ── WALK-THE-BOOK: РЕАЛЬНАЯ ЦЕНА ИСПОЛНЕНИЯ ──────────────────────────────────
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
    notional = 0.0
    for price, qty in levels:
        if price <= 0 or qty <= 0:
            continue
        take = min(qty, remaining)
        notional += take * price
        remaining -= take
        if remaining <= 1e-12:
            break
    filled_qty = target_qty - remaining
    avg_price = notional / filled_qty if filled_qty > 0 else 0.0
    return avg_price, filled_qty, notional, remaining <= 1e-9


def calc_arb_real(buy_book: dict, sell_book: dict, lot_usdt: float,
                   buy_fee_pct: float, sell_fee_pct: float) -> Optional[dict]:
    """Честная средняя цена исполнения по обеим сторонам (walk-the-book),
    а не top-of-book — на тонких монетах top-of-book сильно врёт."""
    if not buy_book or not sell_book:
        return None
    asks = buy_book.get("asks", [])
    bids = sell_book.get("bids", [])
    if not asks or not bids:
        return None

    coin_qty, avg_buy_price, filled_usdt, buy_full = _walk_by_notional(asks, lot_usdt)
    if coin_qty <= 0 or avg_buy_price <= 0:
        return None
    avg_sell_price, sold_qty, sell_notional, sell_full = _walk_by_qty(bids, coin_qty)
    if sold_qty <= 0 or avg_sell_price <= 0:
        return None

    gross_pct = (avg_sell_price - avg_buy_price) / avg_buy_price * 100
    net_pct = gross_pct - buy_fee_pct - sell_fee_pct
    return {
        "avg_buy_price": avg_buy_price, "avg_sell_price": avg_sell_price,
        "coin_qty": coin_qty, "filled_usdt": filled_usdt,
        "gross_pct": round(gross_pct, 4), "net_pct": round(net_pct, 4),
        "buy_book_full": buy_full, "sell_book_full": sell_full,
    }


# ── СКАН РЕАЛЬНЫХ ВОЗМОЖНОСТЕЙ ────────────────────────────────────────────────
async def real_scan_cycle(session) -> List[dict]:
    opps = []
    for base in REAL_SYMBOLS:
        for buy_ex, sell_ex in PAIRS:
            buy_symbol = SYMBOL_FMT[buy_ex](base)
            sell_symbol = SYMBOL_FMT[sell_ex](base)
            try:
                buy_book, sell_book = await asyncio.gather(
                    ORDERBOOK_FN[buy_ex](session, buy_symbol),
                    ORDERBOOK_FN[sell_ex](session, sell_symbol),
                )
            except Exception as e:
                logger.error(f"real_scan {base} {buy_ex}->{sell_ex}: {e}")
                continue
            calc = calc_arb_real(buy_book, sell_book, real_config["max_real_order_usdt"],
                                  FEES.get(buy_ex, 0.1), FEES.get(sell_ex, 0.1))
            if not calc:
                continue
            if calc["net_pct"] < real_config["min_profit_pct"]:
                continue
            opps.append({"base": base, "buy_ex": buy_ex, "sell_ex": sell_ex, **calc})
    opps.sort(key=lambda o: o["net_pct"], reverse=True)
    return opps


def format_real_signal(opp: dict) -> str:
    return (
        f"🔴 *РЕАЛЬНЫЙ АРБИТРАЖ: {opp['buy_ex']} → {opp['sell_ex']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"💱 *{opp['base']}/USDT*\n"
        f"Ср. цена покупки (walk-the-book): `{opp['avg_buy_price']:.8f}`\n"
        f"Ср. цена продажи (walk-the-book): `{opp['avg_sell_price']:.8f}`\n"
        f"Спред: `{opp['gross_pct']}%` | После комиссий: `{opp['net_pct']}%`\n"
        f"Глубина стакана: {'достаточно' if opp['buy_book_full'] and opp['sell_book_full'] else '⚠️ не хватило уровней на весь лот'}\n"
    )


# ── ПРЕДОХРАНИТЕЛИ ────────────────────────────────────────────────────────────
def reset_real_day_if_needed():
    today = datetime.now().strftime("%Y-%m-%d")
    if real_stats["day"] != today:
        real_stats["day"] = today
        real_stats["trades_today"] = 0
        real_stats["pnl_today_usdt"] = 0.0


def check_real_trade_limits() -> Optional[str]:
    reset_real_day_if_needed()
    now = datetime.now()
    elapsed = (now - real_stats["minute_start"]).total_seconds()
    if elapsed >= 60:
        real_stats["trades_this_minute"] = 0
        real_stats["minute_start"] = now
    if real_stats["trades_this_minute"] >= config["max_trades_per_min"]:
        return f"лимит {config['max_trades_per_min']} сделок/мин"
    if real_stats["trades_today"] >= real_config["max_trades_per_day"]:
        return f"дневной лимит {real_config['max_trades_per_day']} сделок"
    if real_stats["pnl_today_usdt"] <= -real_config["daily_stop_loss_usdt"]:
        return f"дневной стоп-лосс -{real_config['daily_stop_loss_usdt']} USDT"
    return None


# ── АВТОДОКУПКА РЕЗЕРВА МОНЕТЫ (top-up) ───────────────────────────────────────
async def top_up_coin_reserve(session, exchange: str, base: str, needed_extra_qty: float,
                               ref_price: float) -> bool:
    """Докупает недостающее количество монеты прямо на бирже, где не хватает
    резерва под продажу (обычно HTX) — рыночной покупкой на этой же бирже,
    с запасом на буфер."""
    buffer_mult = 1 + real_config["balance_safety_buffer_pct"] / 100
    quote_needed = needed_extra_qty * ref_price * buffer_mult
    filters = await get_real_filters(session, exchange, base)
    quote_needed = max(quote_needed, filters["min_notional"])
    logger.info(f"top_up_coin_reserve: {exchange} {base} докупаем на {round(quote_needed,4)} USDT")
    status, resp = await real_market_buy(session, exchange, base, quote_needed)
    ok = status == 200
    if ok:
        real_stats["topups"] += 1
    else:
        real_stats["errors"] += 1
    return ok


# ── ИСПОЛНЕНИЕ РЕАЛЬНОЙ СДЕЛКИ (10 шагов) ─────────────────────────────────────
async def execute_real_arbitrage(session, opp: dict) -> Tuple[bool, str]:
    global _last_exchange_error, real_trading_paused, real_pause_reason
    base, buy_ex, sell_ex = opp["base"], opp["buy_ex"], opp["sell_ex"]

    limit_reason = check_real_trade_limits()
    if limit_reason:
        return False, f"Пропуск — {limit_reason}"

    # Шаг 1: урезаем объём до потолка
    lot_usdt = min(real_config["max_real_order_usdt"], opp["filled_usdt"])

    # Шаг 2: не ниже минимума биржи покупки
    buy_filters = await get_real_filters(session, buy_ex, base)
    sell_filters = await get_real_filters(session, sell_ex, base)
    lot_usdt = max(lot_usdt, buy_filters["min_notional"])

    # Шаг 3: баланс USDT на бирже покупки
    usdt_balance = await BALANCE_FN[buy_ex](session, "USDT")
    if usdt_balance is None:
        return False, f"Не удалось получить баланс USDT на {buy_ex}: {_last_exchange_error}"
    if usdt_balance < lot_usdt:
        if usdt_balance >= buy_filters["min_notional"]:
            logger.info(f"{buy_ex}: не хватает на полный лот ({usdt_balance} < {lot_usdt}), уменьшаем объём")
            lot_usdt = usdt_balance
        else:
            return False, f"Недостаточно USDT на {buy_ex}: есть {round(usdt_balance,4)}, нужно минимум {buy_filters['min_notional']}"

    # Шаг 4: нужное количество монеты считаем от ЦЕНЫ ПОКУПКИ (не продажи!)
    needed_coin_qty = lot_usdt / opp["avg_buy_price"]
    buffer_mult = 1 + real_config["balance_safety_buffer_pct"] / 100
    coin_balance = await BALANCE_FN[sell_ex](session, base)
    if coin_balance is None:
        return False, f"Не удалось получить баланс {base} на {sell_ex}: {_last_exchange_error}"
    if coin_balance < needed_coin_qty * buffer_mult:
        shortfall = needed_coin_qty * buffer_mult - coin_balance
        logger.info(f"{sell_ex}: не хватает {base} ({coin_balance} < {needed_coin_qty*buffer_mult}), автодокупка")
        topped = await top_up_coin_reserve(session, sell_ex, base, shortfall, opp["avg_buy_price"])
        if not topped:
            return False, f"Автодокупка {base} на {sell_ex} не удалась: {_last_exchange_error}"
        coin_balance = await BALANCE_FN[sell_ex](session, base)
        if coin_balance is None or coin_balance < needed_coin_qty * buffer_mult:
            return False, f"После автодокупки всё ещё не хватает {base} на {sell_ex}"

    # Шаг 5: нога 1 — покупка
    status1, resp1 = await real_market_buy(session, buy_ex, base, lot_usdt)
    if status1 != 200:
        return False, f"Нога 1 ({buy_ex} BUY) не прошла: {_last_exchange_error}"

    # Шаг 6: фактическое количество купленного
    actual_qty = None
    if buy_ex == "Binance":
        actual_qty = float(resp1.get("executedQty", 0) or 0)
    else:
        order_id = resp1.get("data", {}).get("orderId") if buy_ex == "KuCoin" else resp1.get("data")
        for _ in range(6):  # до ~3 сек
            await asyncio.sleep(0.5)
            if buy_ex == "KuCoin":
                st, od = await kucoin_get_order(session, order_id)
                if st == 200 and od.get("data", {}).get("dealSize"):
                    actual_qty = float(od["data"]["dealSize"])
                    break
            elif buy_ex == "HTX":
                st, od = await htx_get_order(session, str(order_id))
                if st == 200 and od.get("data", {}).get("field-amount"):
                    actual_qty = float(od["data"]["field-amount"])
                    break
    if not actual_qty or actual_qty <= 0:
        actual_qty = needed_coin_qty  # запасной вариант — расчётное значение, если биржа не отдала факт
        logger.warning(f"{buy_ex}: не удалось получить фактический объём исполнения, используем расчётный")

    actual_qty = floor_to_step(actual_qty, sell_filters["step"])
    if actual_qty <= 0:
        return False, "Фактически купленный объём округлился до нуля — лот слишком мал для шага биржи продажи"

    # Шаг 7: повторная проверка баланса на бирже продажи под ФАКТИЧЕСКОЕ количество
    coin_balance = await BALANCE_FN[sell_ex](session, base)
    if coin_balance is None or coin_balance < actual_qty * buffer_mult:
        shortfall = actual_qty * buffer_mult - (coin_balance or 0)
        topped = await top_up_coin_reserve(session, sell_ex, base, max(shortfall, 0), opp["avg_buy_price"])
        if not topped:
            return False, (
                f"⚠️ Нога 1 куплена ({actual_qty} {base} на {buy_ex}), но на {sell_ex} не хватает "
                f"баланса для продажи и автодокупка не удалась: {_last_exchange_error}. "
                f"Проверь баланс вручную!"
            )

    # Шаг 8: нога 2 — продажа фактического количества
    status2, resp2 = await real_market_sell(session, sell_ex, base, actual_qty)
    if status2 != 200:
        # Шаг 9: аварийное закрытие — пробуем продать обратно на бирже покупки
        emergency_msg = ""
        try:
            e_status, e_resp = await real_market_sell(session, buy_ex, base, actual_qty)
            emergency_msg = (
                f"\n🚑 Аварийная продажа обратно на {buy_ex}: "
                f"{'успех' if e_status == 200 else 'НЕ УДАЛАСЬ — ' + str(_last_exchange_error)}"
            )
        except Exception as e:
            emergency_msg = f"\n🚑 Аварийная продажа обратно на {buy_ex}: исключение {e}"
        return False, (
            f"⚠️ Нога 1 прошла ({actual_qty} {base} куплено на {buy_ex}), "
            f"нога 2 ({sell_ex} SELL) НЕ прошла: {_last_exchange_error}{emergency_msg}"
        )

    # Успех — считаем факт (оценка по walk-the-book цене продажи, т.к. не все
    # биржи мгновенно отдают точный fill по market-ордеру продажи)
    real_sell_notional = actual_qty * opp["avg_sell_price"]
    profit_usdt = round(real_sell_notional * (1 - FEES.get(sell_ex, 0.1) / 100)
                         - lot_usdt * (1 + FEES.get(buy_ex, 0.1) / 100), 4)

    real_stats["trades_today"] += 1
    real_stats["trades_this_minute"] += 1
    real_stats["pnl_today_usdt"] += profit_usdt

    real_trade_history.append({
        "id": len(real_trade_history) + 1,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base": base, "buy_ex": buy_ex, "sell_ex": sell_ex,
        "qty": actual_qty, "lot_usdt": lot_usdt, "profit_usdt": profit_usdt,
        "net_pct_expected": opp["net_pct"],
    })

    if real_stats["pnl_today_usdt"] <= -real_config["daily_stop_loss_usdt"]:
        real_trading_paused = True
        real_pause_reason = f"дневной стоп-лосс -{real_config['daily_stop_loss_usdt']} USDT достигнут"

    return True, f"✅ Исполнено: {actual_qty} {base}, {buy_ex}→{sell_ex}, факт. профит ~{profit_usdt} USDT"


# ── АВТОРЕБАЛАНС ──────────────────────────────────────────────────────────────
async def real_exchange_rebalance_plan(session) -> List[dict]:
    """План по каждой участвующей бирже: излишек/дефицит USDT и монеты
    относительно целевого резерва (с headroom). Учитывает роли — целевой
    резерв считается только там, где биржа реально её играет."""
    plan = []
    buy_exs = set(get_buy_exchanges())
    sell_exs = set(get_sell_exchanges())
    all_exs = buy_exs | sell_exs
    headroom_mult = 1 + real_config["rebalance_headroom_pct"] / 100
    target_usdt = real_config["max_real_order_usdt"] * real_config["rebalance_target_lots"] * headroom_mult

    for ex in sorted(all_exs):
        usdt_bal = await BALANCE_FN[ex](session, "USDT")
        entry = {"exchange": ex, "role_buy": ex in buy_exs, "role_sell": ex in sell_exs,
                  "usdt_balance": usdt_bal, "coins": {}}
        if ex in buy_exs:
            entry["usdt_target"] = target_usdt
            entry["usdt_delta"] = (usdt_bal - target_usdt) if usdt_bal is not None else None
        else:
            entry["usdt_target"] = 0.0
            entry["usdt_delta"] = usdt_bal

        for base in REAL_SYMBOLS:
            coin_bal = await BALANCE_FN[ex](session, base)
            if ex in sell_exs:
                ref_book = await ORDERBOOK_FN[ex](session, SYMBOL_FMT[ex](base))
                ref_price = ref_book["asks"][0][0] if ref_book and ref_book.get("asks") else None
                coin_target = (target_usdt / ref_price) if ref_price else None
            else:
                coin_target = 0.0
            entry["coins"][base] = {
                "balance": coin_bal, "target": coin_target,
                "delta": (coin_bal - coin_target) if (coin_bal is not None and coin_target is not None) else None,
            }
        plan.append(entry)
    return plan


DEFICIT_THRESHOLD_MIN_USDT = 0.10


def _is_deficit(delta: Optional[float], lot_usdt: float) -> bool:
    if delta is None:
        return False
    threshold = max(lot_usdt * 0.05, DEFICIT_THRESHOLD_MIN_USDT)
    return delta < -threshold


async def run_auto_rebalance(session, live: bool) -> str:
    global real_trading_paused, real_pause_reason
    plan = await real_exchange_rebalance_plan(session)
    lot = real_config["max_real_order_usdt"]
    lines = []
    inter_exchange_actions_needed = []

    for entry in plan:
        ex = entry["exchange"]
        if not entry["role_buy"] and entry["usdt_balance"] and entry["usdt_balance"] > lot * 0.2:
            lines.append(f"{ex}: лишний USDT {round(entry['usdt_balance'],2)} (эта биржа не покупает) — "
                          f"{'кандидат на конвертацию в актив' if live else 'кандидат на конвертацию (dry-run)'}")

        if entry["role_buy"] and _is_deficit(entry["usdt_delta"], lot):
            inter_exchange_actions_needed.append(
                f"{ex}: не хватает USDT ({round(entry['usdt_balance'] or 0,2)} из {round(entry['usdt_target'],2)}) — "
                f"переведи USDT на {ex} вручную (межбиржевой перевод бот не делает)"
            )

        for base, info in entry["coins"].items():
            if entry["role_sell"] and _is_deficit(info["delta"], lot):
                shortfall = -info["delta"] if info["delta"] else 0
                ref_price = (lot / info["target"]) if info.get("target") else None
                if live and ref_price:
                    ok = await top_up_coin_reserve(session, ex, base, shortfall, ref_price)
                    lines.append(f"{ex} {base}: дефицит {round(shortfall,6)} — автодокупка {'успех' if ok else 'НЕ удалась'}")
                else:
                    lines.append(f"{ex} {base}: дефицит {round(shortfall,6)} (dry-run, не куплено)")
            elif not entry["role_sell"] and info["balance"] and info["balance"] > 0:
                if live:
                    step = REAL_FILTERS.get((ex, base), {}).get("step", 0.000001)
                    st, resp = await real_market_sell(session, ex, base, floor_to_step(info["balance"], step))
                    lines.append(f"{ex} {base}: мёртвый остаток {round(info['balance'],6)} (эта биржа не продаёт) — "
                                  f"{'продан в USDT' if st == 200 else 'продажа не удалась: ' + str(_last_exchange_error)}")
                else:
                    lines.append(f"{ex} {base}: мёртвый остаток {round(info['balance'],6)} (dry-run, не продано)")

    if inter_exchange_actions_needed:
        real_trading_paused = True
        real_pause_reason = "межбиржевой дефицит — нужен ручной перевод (см. /realbalance)"
        lines.append("\n🛑 Обнаружен межбиржевой дефицит — торговля на паузе, перевод только вручную:")
        lines.extend(inter_exchange_actions_needed)

    return "\n".join(lines) if lines else "Балансы в норме, действий не требуется."


_last_rebalance_attempt = 0.0
REBALANCE_COOLDOWN_SEC = 30


async def maybe_auto_rebalance(session, reason: str = ""):
    global _last_rebalance_attempt
    now = time.time()
    if now - _last_rebalance_attempt < REBALANCE_COOLDOWN_SEC:
        return
    _last_rebalance_attempt = now
    logger.info(f"Авторебаланс ({reason})")
    result = await run_auto_rebalance(session, live=real_config["rebalance_live"])
    if CHAT_ID:
        await send_tg(session, f"⚖️ *Авторебаланс* ({reason})\n{result}")


# ── ЦИКЛ РЕАЛЬНОЙ ТОРГОВЛИ ────────────────────────────────────────────────────
async def real_scan_loop(session):
    await asyncio.sleep(20)
    while True:
        try:
            if real_trading_paused:
                await asyncio.sleep(real_config["scan_interval"])
                continue
            opps = await real_scan_cycle(session)
            for opp in opps[:3]:
                if not real_trading_ready():
                    if CHAT_ID:
                        await send_tg(session,
                            format_real_signal(opp) +
                            f"\n⚠️ REAL не разблокирован — сигнал справочный.\n{real_trading_status_text()}")
                    continue
                ok, msg = await execute_real_arbitrage(session, opp)
                if CHAT_ID:
                    await send_tg(session, format_real_signal(opp) + f"\n{msg}")
                if not ok and ("недостаточно" in msg.lower() or "не хватает" in msg.lower()):
                    await maybe_auto_rebalance(session, reason="ошибка баланса")
        except Exception as e:
            real_stats["errors"] += 1
            logger.error(f"real_scan_loop error: {e}")
        await asyncio.sleep(real_config["scan_interval"])


async def real_rebalance_background_loop(session):
    await asyncio.sleep(60)
    while True:
        await maybe_auto_rebalance(session, reason="плановый (раз в ~30 мин)")
        await asyncio.sleep(1800)



async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"ArbScreenerBot | {len(SYMBOLS)} монет (скрининг) | 3 биржи (Binance/KuCoin/HTX) | "
        f"лот {config['lot_usdt']} USDT | стоп-лосс -{config['stop_loss_usdt']} USDT | "
        f"порог {config['min_profit_pct']}%"
    )
    logger.info(
        f"WorkerArbBot (реал) | монеты {REAL_SYMBOLS} | маршруты {PAIRS} | "
        f"лот {real_config['max_real_order_usdt']} USDT | REAL готов: {real_trading_ready()}"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            await init_real_filters(session)
        except Exception as e:
            logger.error(f"init_real_filters: {e}")
        await asyncio.gather(
            polling_loop(session),
            scan_loop(session),
            real_scan_loop(session),
            real_rebalance_background_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
