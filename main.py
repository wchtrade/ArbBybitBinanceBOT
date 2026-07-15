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
    "lot_usdt":        float(os.environ.get("LOT_USDT", "100")),      # шаг лота
    "start_capital":   float(os.environ.get("START_CAPITAL", "10000")),
    "stop_loss_usdt":  float(os.environ.get("STOP_LOSS_USDT", "50")),
    "scan_interval":   6,
    "simulation_mode": True,   # бот только симулирует, реальных ордеров нет — см. шапку файла
    "max_trades_per_min": int(os.environ.get("MAX_TRADES_PER_MIN", "5")),
}

# Стоп-лосс: при накопленном P&L <= -stop_loss_usdt торговля (запись в
# P&L) приостанавливается, пока не отправишь /resume вручную.
trading_paused = False
pause_reason = ""

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

# Статистика по каждой монете — для /leaderboard (поиск кандидатов на реал)
coin_stats: Dict[str, dict] = {
    s: {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0}
    for s in SYMBOLS
}

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
# ═══════════════════════════════════════

async def get_binance(session) -> Dict:
    try:
        async with session.get(
            "https://data-api.binance.vision/api/v3/ticker/bookTicker",
            timeout=aiohttp.ClientTimeout(total=8)) as r:
            out = {}
            for item in await r.json():
                sym = item.get("symbol", "")
                if sym.endswith(QUOTE):
                    base = sym[:-len(QUOTE)]
                    if base in SYMBOLS:
                        bid = float(item.get("bidPrice", 0) or 0)
                        ask = float(item.get("askPrice", 0) or 0)
                        if bid > 0 and ask > 0:
                            out[base] = {"bid": bid, "ask": ask}
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
                if sym.endswith(f"-{QUOTE}"):
                    base = sym[:-len(f"-{QUOTE}")]
                    if base in SYMBOLS:
                        bid = float(item.get("buy", 0) or 0)
                        ask = float(item.get("sell", 0) or 0)
                        if bid > 0 and ask > 0:
                            out[base] = {"bid": bid, "ask": ask}
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
                sym = item.get("symbol", "")
                if sym.endswith("usdt"):
                    base = sym[:-4].upper()
                    if base in SYMBOLS:
                        bid = float(item.get("bid", 0) or 0)
                        ask = float(item.get("ask", 0) or 0)
                        if bid > 0 and ask > 0:
                            out[base] = {"bid": bid, "ask": ask}
            return out
    except Exception as e:
        logger.error(f"HTX: {e}")
        return {}


# ═══════════════════════════════════════
# АРБИТРАЖ
# ═══════════════════════════════════════

def find_arbitrage(all_data: Dict[str, Dict]) -> List[dict]:
    results = []
    vol = config["lot_usdt"]
    min_pct = config["min_profit_pct"]

    for symbol, exchanges in all_data.items():
        ex_list = list(exchanges.items())
        if len(ex_list) < 2:
            continue
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
                coins  = vol / buy_price
                profit = coins * sell_price * (1 - sell_fee) - vol * (1 + buy_fee)
                results.append({
                    "symbol":      symbol,
                    "buy_ex":      buy_ex,
                    "sell_ex":     sell_ex,
                    "buy_price":   buy_price,
                    "sell_price":  sell_price,
                    "gross_pct":   round(gross_pct, 4),
                    "net_pct":     round(net_pct, 4),
                    "profit_usdt": round(profit, 4),
                    "coins":       round(coins, 6),
                    "volume_usdt": vol,
                    "time":        datetime.now().strftime("%H:%M:%S"),
                })

    results.sort(key=lambda x: x["net_pct"], reverse=True)
    return results


def format_signal(opp: dict) -> str:
    p1000 = round(opp["profit_usdt"] * 10, 2)
    p5000 = round(opp["profit_usdt"] * 50, 2)
    return (
        f"🚨 *АРБИТРАЖ: {opp['buy_ex']} → {opp['sell_ex']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔵 СКРИНИНГ (симуляция)\n\n"
        f"💱 *{opp['symbol']}/USDT*\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена: `{opp['buy_price']} USDT`\n"
        f"   Лот: `{opp['volume_usdt']} USDT`\n"
        f"   Получишь: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена: `{opp['sell_price']} USDT`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n\n"
        f"💰 *Прибыль на лот ({opp['volume_usdt']} USDT):* `~{opp['profit_usdt']} USDT`\n"
        f"   x10 лотов → `~{p1000} USDT`\n"
        f"   x50 лотов → `~{p5000} USDT`\n\n"
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
    all_data: Dict[str, Dict] = {}
    active = []
    counts = {}

    for ex_name, result in zip(ex_names, results):
        if isinstance(result, Exception) or not result:
            counts[ex_name] = 0
            continue
        active.append(ex_name)
        counts[ex_name] = len(result)
        for symbol, price_data in result.items():
            all_data.setdefault(symbol, {})[ex_name] = price_data

    logger.info(f"Монет с биржи: {counts}")
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
    return opps, active


async def execute_sim(opp: dict, session=None):
    global trading_paused, pause_reason
    if trading_paused:
        logger.info(f"Пропуск сделки — торговля на паузе ({pause_reason})")
        return
    if not check_trade_limit():
        logger.info(f"Trade limit reached ({config['max_trades_per_min']}/min), skipping")
        return

    trade = {
        "id":          len(trade_history) + 1,
        "time":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol":      opp["symbol"],
        "buy_ex":      opp["buy_ex"],
        "sell_ex":     opp["sell_ex"],
        "buy_price":   opp["buy_price"],
        "sell_price":  opp["sell_price"],
        "net_pct":     opp["net_pct"],
        "profit_usdt": opp["profit_usdt"],
    }
    trade_history.append(trade)
    stats["trades_sim"]        += 1
    stats["profit_sim"]        += opp["profit_usdt"]
    stats["trades_this_minute"] += 1

    cs = coin_stats.setdefault(opp["symbol"], {"signals": 0, "trades": 0, "profit_usdt": 0.0, "best_net_pct": 0.0})
    cs["trades"] += 1
    cs["profit_usdt"] += opp["profit_usdt"]

    logger.info(
        f"SIM #{trade['id']}: {opp['symbol']} {opp['buy_ex']}→{opp['sell_ex']} "
        f"+{opp['net_pct']}% +{opp['profit_usdt']} USDT "
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
            f"Монет в скрининге: {len(SYMBOLS)}\n\n"
            f"⚙️ Стартовый капитал (справочно): `{config['start_capital']} USDT`\n"
            f"⚙️ Лот/шаг сделки: `{config['lot_usdt']} USDT`\n"
            f"⚙️ Стоп-лосс: `-{config['stop_loss_usdt']} USDT` (только `/resume` включает обратно)\n"
            f"⚙️ Порог маржи: `{config['min_profit_pct']}%`\n"
            f"⚙️ Лимит: `{config['max_trades_per_min']} сделок/мин`\n\n"
            f"/scan — скан сейчас\n"
            f"/top — топ пар по спреду\n"
            f"/prices SYMBOL — цены по конкретной монете на всех биржах\n"
            f"/exchanges — диагностика бирж\n"
            f"/leaderboard — рейтинг монет-кандидатов на реал\n"
            f"/stats — статистика\n"
            f"/history — последние сделки\n"
            f"/resume — снять паузу после стоп-лосса\n"
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
                msg += f"⚠️ {name}: 0 монет (проверь сеть/гео-блок)\n"
            else:
                msg += f"✅ {name}: {len(r)} монет из {len(SYMBOLS)} в списке\n"
        await send_tg(session, msg)

    elif cmd == "/top":
        await send_tg(session, "📊 Ищу лучшие пары по всем 3 биржам...")
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
        msg += f"Бирж: {', '.join(active)} | Монет с данными: {len(all_data)}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, opp in enumerate(opps[:20], 1):
            icon = "🟢" if opp["net_pct"] >= config["min_profit_pct"] else "🔴"
            msg += (
                f"{icon} *{i}. {opp['symbol']}* {opp['buy_ex']}→{opp['sell_ex']}\n"
                f"   Спред: `{opp['gross_pct']}%` | Чистая: `{opp['net_pct']}%`\n"
            )
        msg += f"\n_Порог сигнала: {config['min_profit_pct']}%_"
        await send_tg(session, msg)

    elif cmd == "/prices":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/prices BTC`\nСписок всех монет — /exchanges покажет количество, а не список.")
            return
        sym = parts[1].upper()
        if sym not in SYMBOLS:
            await send_tg(session, f"❌ `{sym}` нет в списке скрининга.")
            return
        await send_tg(session, f"📊 Получаю цены по {sym}...")
        all_data, active = await fetch_all(session)
        ex_data = all_data.get(sym, {})
        msg = f"📊 *{sym}/USDT — {datetime.now().strftime('%H:%M:%S')}*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for ex in ("Binance", "KuCoin", "HTX"):
            if ex in ex_data:
                d = ex_data[ex]
                msg += f"{ex}: bid `{d['bid']}` / ask `{d['ask']}`\n"
            else:
                msg += f"⚠️ {ex}: нет данных по этой монете\n"
        await send_tg(session, msg)

    elif cmd == "/leaderboard":
        ranked = sorted(coin_stats.items(), key=lambda kv: kv[1]["signals"], reverse=True)
        ranked = [r for r in ranked if r[1]["signals"] > 0][:20]
        if not ranked:
            await send_tg(session, "Пока нет ни одного сигнала ни по одной монете. Дай боту поработать подольше или снизь /setprofit.")
            return
        msg = "🏆 *РЕЙТИНГ КАНДИДАТОВ НА РЕАЛ*\n(сортировка по количеству сигналов)\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, (sym, cs) in enumerate(ranked, 1):
            msg += (
                f"{i}. *{sym}* — сигналов: `{cs['signals']}` | сделок: `{cs['trades']}` | "
                f"P&L: `{round(cs['profit_usdt'],3)} USDT` | лучшая маржа: `{cs['best_net_pct']}%`\n"
            )
        await send_tg(session, msg)

    elif cmd == "/stats":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)
        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🛑 Стоп-лосс: {'*АКТИВЕН — торговля на паузе*' if trading_paused else 'не сработал'}\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок (симуляция): {stats['trades_sim']}\n"
            f"💰 Прибыль (симуляция): {round(stats['profit_sim'], 4)} USDT\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"⏱ Сделок этой минуты: {stats['trades_this_minute']}/{config['max_trades_per_min']}\n\n"
            f"⚙️ Стартовый капитал: {config['start_capital']} USDT\n"
            f"⚙️ Лот: {config['lot_usdt']} USDT\n"
            f"⚙️ Стоп-лосс: -{config['stop_loss_usdt']} USDT\n"
            f"⚙️ Порог маржи: {config['min_profit_pct']}%\n"
            f"⚙️ Монет в скрининге: {len(SYMBOLS)}\n"
            f"⚙️ Бирж: 3 (Binance/KuCoin/HTX)\n\n"
            f"/leaderboard — какие монеты реально сработали"
        )

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "📋 Нет сделок в этой сессии.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in trade_history[-10:][::-1]:
            msg += (
                f"#{t['id']} *{t['symbol']}* {t['buy_ex']}→{t['sell_ex']}\n"
                f"   +{t['net_pct']}% | +{t['profit_usdt']} USDT | {t['time']}\n\n"
            )
        await send_tg(session, msg)

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
            "/start /scan /top /prices SYMBOL /exchanges /leaderboard\n"
            "/stats /history /resume\n"
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
                key = f"{opp['symbol']}-{opp['buy_ex']}-{opp['sell_ex']}"
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
