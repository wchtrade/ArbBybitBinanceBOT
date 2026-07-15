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

config = {
    "min_profit_pct":  float(os.environ.get("MIN_PROFIT_PCT", "0.15")),
    "scan_interval":   6,
    "simulation_mode": os.environ.get("SIMULATION_MODE", "true").lower() == "true",
    "max_trades_per_min": int(os.environ.get("MAX_TRADES_PER_MIN", "5")),
    "stop_loss_usdt":  float(os.environ.get("STOP_LOSS_USDT", "20")),
}

# Стоп-лосс: если накопленный P&L падает до -stop_loss_usdt, торговля
# (запись сделок/исполнение) приостанавливается и не возобновляется сама —
# только явной командой /resume от тебя.
trading_paused = False
pause_reason = ""

# Только две биржи: Binance + KuCoin.
# Bybit исключён — подтверждённо блокирует облачные IP (Railway/AWS/GCP)
# через CloudFront (403 "block access from your country"), без VPS/прокси
# не лечится.
FEES = {
    "Binance": 0.10,
    "KuCoin":  0.10,
}

# ФИНАЛЬНЫЙ ОТБОР — обновлено по твоему решению.
# YFI/GRT/COMP — по факту статистики сессии 06.07.2026 (2ч45м, 608
# сигналов, 56 сделок), реально давали сигналы регулярно.
# IMX — добавлена по твоему запросу, без собственной статистики сигналов
# (добавление "на пробу"), поэтому вес и лот меньше проверенной тройки.
# IOST — возвращена в торговлю по твоему явному решению, ПОСЛЕ
# предупреждения: в логе её цена не менялась ~40 минут подряд, что похоже
# на редкий/тонкий тикер, а не на настоящий стабильный спред. Поэтому у
# неё самый маленький вес/лот из всех пяти — если окажется, что сигналы
# по ней не исполняются по показанной цене, проще всего будет уменьшить
# lot_usdt до 0 или убрать монету совсем, не трогая остальные.
#
# Веса: YFI 30% / GRT 25% / COMP 20% / IMX 15% / IOST 10% (сумма 100%).
# Лоты и стоп-лоссы — из активного капитала $800 (капитал $1000, резерв
# 20%), лот = (аллокация/4) * 50% предохранителя на сделку.
COIN_CONFIG = {
    "YFI":  {"lot_usdt": 30.0, "stop_loss_usdt": 6.0, "weight": 0.30},
    "GRT":  {"lot_usdt": 25.0, "stop_loss_usdt": 5.0, "weight": 0.25},
    "COMP": {"lot_usdt": 20.0, "stop_loss_usdt": 4.0, "weight": 0.20},
    "IMX":  {"lot_usdt": 15.0, "stop_loss_usdt": 3.0, "weight": 0.15},
    "IOST": {"lot_usdt": 10.0, "stop_loss_usdt": 2.0, "weight": 0.10},
}
SYMBOLS = list(COIN_CONFIG.keys())

# Пусто — все отслеживаемые монеты сейчас участвуют в торговле.
# Если захочешь добавить монету "на наблюдение" без исполнения — впиши
# её сюда, по образцу того, как раньше был устроен IOST.
WATCHLIST = []
ALL_TRACKED = SYMBOLS + WATCHLIST
QUOTE = "USDT"

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
    """Проверяет лимит сделок в минуту (config['max_trades_per_min'])"""
    now = datetime.now()
    elapsed = (now - stats["minute_start"]).total_seconds()
    if elapsed >= 60:
        stats["trades_this_minute"] = 0
        stats["minute_start"] = now
    return stats["trades_this_minute"] < config["max_trades_per_min"]


# ═══════════════════════════════════════
# БИРЖИ (только 2)
# ═══════════════════════════════════════

async def get_binance(session) -> Dict:
    try:
        async with session.get(
            "https://data-api.binance.vision/api/v3/ticker/bookTicker",
            timeout=aiohttp.ClientTimeout(total=6)) as r:
            out = {}
            for item in await r.json():
                sym = item.get("symbol", "")
                if sym.endswith(QUOTE):
                    base = sym[:-len(QUOTE)]
                    if base in ALL_TRACKED:
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
            timeout=aiohttp.ClientTimeout(total=6)) as r:
            out = {}
            for item in (await r.json()).get("data", {}).get("ticker", []):
                sym = item.get("symbol", "")
                if sym.endswith(f"-{QUOTE}"):
                    base = sym[:-len(f"-{QUOTE}")]
                    if base in ALL_TRACKED:
                        bid = float(item.get("buy", 0) or 0)
                        ask = float(item.get("sell", 0) or 0)
                        if bid > 0 and ask > 0:
                            out[base] = {"bid": bid, "ask": ask}
            return out
    except Exception as e:
        logger.error(f"KuCoin: {e}")
        return {}


# ═══════════════════════════════════════
# АРБИТРАЖ
# ═══════════════════════════════════════

def find_arbitrage(all_data: Dict[str, Dict]) -> List[dict]:
    results = []
    min_pct = config["min_profit_pct"]

    for symbol, exchanges in all_data.items():
        if symbol not in COIN_CONFIG:
            continue  # watchlist-монеты (IOST) не участвуют в поиске арбитража
        vol = COIN_CONFIG[symbol]["lot_usdt"]
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
    mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
    p500  = round(opp["profit_usdt"] * 5,  2)
    p1000 = round(opp["profit_usdt"] * 10, 2)
    return (
        f"🚨 *АРБИТРАЖ: {opp['buy_ex']} → {opp['sell_ex']}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{mode}\n\n"
        f"💱 *{opp['symbol']}/USDT*\n\n"
        f"📥 *КУПИТЬ на {opp['buy_ex']}*\n"
        f"   Цена: `{opp['buy_price']} USDT`\n"
        f"   Объём: `{opp['volume_usdt']} USDT`\n"
        f"   Получишь: `{opp['coins']} {opp['symbol']}`\n\n"
        f"📤 *ПРОДАТЬ на {opp['sell_ex']}*\n"
        f"   Цена: `{opp['sell_price']} USDT`\n\n"
        f"📊 *Расчёт:*\n"
        f"   Спред: `{opp['gross_pct']}%`\n"
        f"   После комиссий: `{opp['net_pct']}%`\n\n"
        f"💰 *Прибыль:*\n"
        f"   100 USDT → `~{opp['profit_usdt']} USDT`\n"
        f"   500 USDT → `~{p500} USDT`\n"
        f"   1000 USDT → `~{p1000} USDT`\n\n"
        f"⚠️ Цена актуальна только сейчас!\n"
        f"⚠️ Проверь баланс перед входом!\n\n"
        f"🕐 {opp['time']}"
    )


# ═══════════════════════════════════════
# СКАН
# ═══════════════════════════════════════

async def fetch_all(session):
    results = await asyncio.gather(
        get_binance(session),
        get_kucoin(session),
        return_exceptions=True
    )

    ex_names = ["Binance", "KuCoin"]
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
            if symbol not in all_data:
                all_data[symbol] = {}
            all_data[symbol][ex_name] = price_data

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
    logger.info(
        f"SIM #{trade['id']}: {opp['symbol']} "
        f"{opp['buy_ex']}→{opp['sell_ex']} "
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
                f"Накопленный P&L: `{round(stats['profit_sim'], 2)} USDT` "
                f"(лимит: -{config['stop_loss_usdt']} USDT)\n\n"
                f"Исполнение сделок приостановлено. Новые сигналы будут "
                f"только показываться, без записи в P&L.\n\n"
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
        mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
        lots_str = " | ".join(f"{c}: {v['lot_usdt']} USDT" for c, v in COIN_CONFIG.items())
        await send_tg(session,
            f"✅ *ArbBot запущен!*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode}\n"
            f"Площадки: Binance, KuCoin\n"
            f"Монеты и лоты: {lots_str}\n\n"
            f"⚙️ Мин. прибыль: `{config['min_profit_pct']}%`\n"
            f"⚙️ Интервал: `{config['scan_interval']} сек`\n"
            f"⚙️ Лимит: `{config['max_trades_per_min']} сделок/мин`\n"
            f"⚙️ Общий стоп-лосс: `-{config['stop_loss_usdt']} USDT` (при срабатывании — только `/resume` включает обратно)\n\n"
            f"/scan — скан прямо сейчас\n"
            f"/top — топ пар по спреду\n"
            f"/prices — цены на биржах\n"
            f"/exchanges — диагностика: сколько монет реально отдаёт каждая биржа\n"
            f"/coins — список монет с лотами и стоп-лоссами\n"
            f"/stats — статистика\n"
            f"/history — последние сделки\n"
            f"/mode — симуляция ↔ реал\n"
            f"/resume — снять паузу после стоп-лосса\n"
            f"/setprofit 0.15 — мин. прибыль %\n"
            f"/setlot YFI 40 — изменить лот конкретной монеты\n"
        )

    elif cmd == "/scan":
        await send_tg(session, f"🔍 Сканирую 2 биржи, {len(SYMBOLS)} монет...")
        opps, active = await scan_cycle(session)
        if not opps:
            await send_tg(session,
                f"😔 Нет сигналов (порог {config['min_profit_pct']}%).\n\n"
                f"Активных бирж: {len(active)}\n"
                f"{', '.join(active)}\n\n"
                f"Сканов: {stats['scans']}\n"
                f"Напиши /top чтобы увидеть лучшие пары, /exchanges — диагностику."
            )
        else:
            await send_tg(session, f"✅ Найдено {len(opps)} сигналов! Топ-3:")
            for opp in opps[:3]:
                await send_tg(session, format_signal(opp))
                if config["simulation_mode"]:
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
                msg += f"⚠️ {name}: 0 монет (проверь сеть/гео-блок)\n"
            else:
                msg += f"✅ {name}: {len(r)} монет\n"
        await send_tg(session, msg)

    elif cmd == "/top":
        await send_tg(session, "📊 Ищу лучшие пары...")
        all_data, active = await fetch_all(session)
        if len(active) < 2:
            await send_tg(session, "❌ Недостаточно бирж (обе должны быть живы для сравнения).")
            return
        saved = config["min_profit_pct"]
        config["min_profit_pct"] = -999
        opps = find_arbitrage(all_data)
        config["min_profit_pct"] = saved
        if not opps:
            await send_tg(session, "❌ Нет данных.")
            return
        msg = f"📊 *ТОП-15 — {datetime.now().strftime('%H:%M:%S')}*\n"
        msg += f"Бирж: {', '.join(active)}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for i, opp in enumerate(opps[:15], 1):
            icon = "🟢" if opp["net_pct"] >= config["min_profit_pct"] else "🔴"
            msg += (
                f"{icon} *{i}. {opp['symbol']}* "
                f"{opp['buy_ex']}→{opp['sell_ex']}\n"
                f"   Спред: `{opp['gross_pct']}%` | "
                f"Чистая: `{opp['net_pct']}%`\n"
                f"   Купить: `{opp['buy_price']}` "
                f"Продать: `{opp['sell_price']}`\n\n"
            )
        msg += f"_Порог: {config['min_profit_pct']}%_"
        await send_tg(session, msg)

    elif cmd == "/prices":
        await send_tg(session, "📊 Получаю цены...")
        all_data, active = await fetch_all(session)
        msg = f"📊 *ЦЕНЫ — {datetime.now().strftime('%H:%M:%S')}*\n"
        msg += f"Активных бирж: {len(active)} ({', '.join(active)})\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for sym in SYMBOLS:
            ex_data = all_data.get(sym, {})
            msg += f"*{sym}:*\n"
            for ex in ("Binance", "KuCoin"):
                if ex in ex_data:
                    d = ex_data[ex]
                    msg += f"  {ex}: bid `{d['bid']}` / ask `{d['ask']}`\n"
                else:
                    msg += f"  ⚠️ {ex}: нет данных по этой монете\n"
            msg += "\n"
        await send_tg(session, msg)

    elif cmd == "/stats":
        uptime = datetime.now() - stats["start_time"]
        h = int(uptime.total_seconds() // 3600)
        m = int((uptime.total_seconds() % 3600) // 60)
        mode = "Симуляция 🔵" if config["simulation_mode"] else "Реальная 🔴"
        now = datetime.now()
        elapsed = (now - stats["minute_start"]).total_seconds()
        trades_left = config["max_trades_per_min"] - stats["trades_this_minute"]
        await send_tg(session,
            f"📈 *СТАТИСТИКА*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Режим: {mode}\n"
            f"🛑 Стоп-лосс: {'*АКТИВЕН — торговля на паузе*' if trading_paused else 'не сработал'}\n"
            f"Аптайм: {h}ч {m}м\n\n"
            f"🔍 Сканов: {stats['scans']}\n"
            f"🎯 Сигналов: {stats['signals']}\n"
            f"✅ Сделок (симуляция): {stats['trades_sim']}\n"
            f"💰 Прибыль (симуляция): "
            f"{round(stats['profit_sim'], 4)} USDT\n"
            f"❌ Ошибок: {stats['errors']}\n\n"
            f"⏱ Сделок этой минуты: "
            f"{stats['trades_this_minute']}/{config['max_trades_per_min']}\n"
            f"⏱ Осталось слотов: {trades_left}\n\n"
            f"⚙️ Мин. прибыль: {config['min_profit_pct']}%\n"
            f"⚙️ Лоты по монетам: см. /coins\n"
            f"⚙️ Интервал: {config['scan_interval']} сек\n"
            f"⚙️ Лимит: {config['max_trades_per_min']} сделок/мин\n"
            f"⚙️ Стоп-лосс: -{config['stop_loss_usdt']} USDT\n"
            f"⚙️ Монет: {len(SYMBOLS)}\n"
            f"⚙️ Бирж: 2 (Binance/KuCoin)"
        )

    elif cmd == "/history":
        if not trade_history:
            await send_tg(session, "📋 Нет сделок в этой сессии.")
            return
        msg = "📋 *ПОСЛЕДНИЕ СДЕЛКИ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for t in trade_history[-10:][::-1]:
            sign = "+" if t["profit_usdt"] > 0 else ""
            msg += (
                f"#{t['id']} *{t['symbol']}* "
                f"{t['buy_ex']}→{t['sell_ex']}\n"
                f"   {sign}{t['net_pct']}% | "
                f"{sign}{t['profit_usdt']} USDT\n"
                f"   {t['time']}\n\n"
            )
        await send_tg(session, msg)

    elif cmd == "/mode":
        config["simulation_mode"] = not config["simulation_mode"]
        mode = "🔵 СИМУЛЯЦИЯ" if config["simulation_mode"] else "🔴 РЕАЛЬНАЯ"
        warn = (
            "\n\n⚠️ В этом боте реальная торговля НЕ реализована — "
            "переключатель только меняет надпись в сигналах и отключает "
            "накопление симулированного P&L. Ордера бот не отправляет ни "
            "в каком режиме. Если нужна настоящая автоторговля — это "
            "отдельная доработка (подписанные ордера через API-ключи)."
            if not config["simulation_mode"] else ""
        )
        await send_tg(session, f"Режим: {mode}{warn}")

    elif cmd == "/resume":
        if not trading_paused:
            await send_tg(session, "✅ Торговля и так не на паузе — стоп-лосс не срабатывал.")
        else:
            trading_paused = False
            old_reason = pause_reason
            pause_reason = ""
            await send_tg(session,
                f"▶️ *Торговля возобновлена вручную*\n"
                f"Была на паузе из-за: {old_reason}\n"
                f"P&L симуляции НЕ сброшен — если нужно начать с чистого листа, это отдельное действие (можно добавить /resetpnl при необходимости)."
            )

    elif cmd == "/coins":
        msg = "🪙 *МОНЕТЫ И ЛОТЫ*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for coin, cfg in COIN_CONFIG.items():
            msg += f"*{coin}*: лот `{cfg['lot_usdt']} USDT` | стоп-лосс `-{cfg['stop_loss_usdt']} USDT` | вес `{cfg['weight']*100:.0f}%`\n"
        if WATCHLIST:
            msg += f"\n👀 На наблюдении (не торгуются): {', '.join(WATCHLIST)}"
        await send_tg(session, msg)

    elif cmd == "/setprofit":
        if len(parts) < 2:
            await send_tg(session, "Пример: `/setprofit 0.15`")
            return
        try:
            config["min_profit_pct"] = float(parts[1])
            await send_tg(session,
                f"✅ Мин. прибыль: `{config['min_profit_pct']}%`")
        except:
            await send_tg(session, "❌ Пример: `/setprofit 0.15`")

    elif cmd == "/setlot":
        if len(parts) < 3:
            await send_tg(session, "Пример: `/setlot YFI 40`\nМонеты: " + ", ".join(COIN_CONFIG.keys()))
            return
        coin = parts[1].upper()
        if coin not in COIN_CONFIG:
            await send_tg(session, f"❌ Монета `{coin}` не в списке. Доступны: {', '.join(COIN_CONFIG.keys())}")
            return
        try:
            COIN_CONFIG[coin]["lot_usdt"] = float(parts[2])
            await send_tg(session, f"✅ Лот {coin}: `{COIN_CONFIG[coin]['lot_usdt']} USDT`")
        except:
            await send_tg(session, "❌ Пример: `/setlot YFI 40`")

    else:
        await send_tg(session,
            "/start /scan /top /prices /exchanges /coins\n"
            "/stats /history /mode /resume\n"
            "/setprofit 0.15 /setlot YFI 40"
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
                global CHAT_ID
                CHAT_ID = msg["chat"]["id"]
                text = msg.get("text", "")
                if text.startswith("/"):
                    await handle_command(session, text, CHAT_ID)
        await asyncio.sleep(1)


async def scan_loop(session):
    await asyncio.sleep(15)
    while True:
        try:
            opps, active = await scan_cycle(session)
            logger.info(
                f"Scan #{stats['scans']}: "
                f"{len(active)} бирж, {len(opps)} сигналов | "
                f"trades_this_min={stats['trades_this_minute']}"
            )
            for opp in opps[:3]:
                key = f"{opp['symbol']}-{opp['buy_ex']}-{opp['sell_ex']}"
                now = datetime.now().timestamp()
                # Не спамить одним сигналом чаще раз в 2 минуты
                if now - last_signal_time.get(key, 0) > 120:
                    last_signal_time[key] = now
                    if CHAT_ID:
                        await send_tg(session, format_signal(opp))
                    if config["simulation_mode"]:
                        await execute_sim(opp, session)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"Scan error: {e}")
        # 6 секунд интервал = не более 10 сканов в минуту
        await asyncio.sleep(config["scan_interval"])


async def main():
    if not TG_TOKEN:
        logger.error("ARB_BOT_TOKEN не установлен!")
        return
    logger.info(
        f"ArbBot | {len(SYMBOLS)} монет | "
        f"2 биржи (Binance/KuCoin) | порог {config['min_profit_pct']}% | "
        f"лимит {config['max_trades_per_min']}/мин"
    )
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        await asyncio.gather(polling_loop(session), scan_loop(session))


if __name__ == "__main__":
    asyncio.run(main())
