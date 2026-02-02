# bot_handlers.py

from datetime import datetime
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CallbackContext,
    CommandHandler,
    MessageHandler,
    filters,
)
from bingx_api import (
    get_futures_balance,
    get_futures_positions,
    get_positions_dict,
    get_trade_orders,
)
from db import (
    add_trade,
    close_trade,
    get_open_trades,
    get_today_stats,
    get_week_stats,
    get_trade_by_symbol_and_side,
    get_top_trades,
    update_trade_with_exit,
    clean_obsolete_open_trades,
    hard_reset_db,
)
from config import TELEGRAM_CHANNEL_ID
import httpx
import asyncio
import time

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📂 Позиции")],
        [KeyboardButton(text="📊 Обновить сводку")],
        [KeyboardButton(text="🧹 Очистить БД"), KeyboardButton(text="♻️ Пересчитать и обновить")],
    ],
    resize_keyboard=True,
)

BINGX_REF_LINK = "https://iciclebridge.com/invite/LYLIF3/"

_last_positions_state: dict = {}
_trade_messages: dict = {}
_last_summary_message_id: int | None = None
_price_cache: dict = {}
_last_summary_update: float = 0.0


# ----------------- УТИЛИТЫ -----------------


def _pick_usdt_balance(balance_data: dict) -> dict | None:
    data = balance_data.get("data")
    if not data:
        return None

    if isinstance(data, dict) and "balance" in data:
        b = data["balance"]
        return {
            "asset": b.get("asset", "USDT"),
            "balance": b.get("balance", "0"),
            "equity": b.get("equity", "0"),
            "availableMargin": b.get("availableMargin", "0"),
            "unrealizedProfit": b.get("unrealizedProfit", "0"),
            "realisedProfit": b.get("realisedProfit", "0"),
        }

    items = data if isinstance(data, list) else []
    if not items:
        return None
    for item in items:
        if item.get("asset") == "USDT":
            return item
    return items[0]


def _fmt_price(value: str) -> str:
    try:
        v = float(value)
        if v >= 100:
            return f"{v:.2f}"
        if v >= 1:
            return f"{v:.4f}"
        return f"{v:.6f}"
    except Exception:
        return value


def _fmt_margin(value: str) -> str:
    try:
        v = float(value)
        return f"{v:.2f}"
    except Exception:
        return value


def _fmt_liq(value: str) -> str:
    try:
        v = float(value)
        return _fmt_price(str(v))
    except Exception:
        return value[:10]


def _clean_symbol(symbol: str) -> str:
    return symbol.replace("-", "")


def _coin_from_symbol(symbol: str) -> str:
    if "-" in symbol:
        return symbol.split("-")[0]
    if symbol.endswith("USDT"):
        return symbol.replace("USDT", "")
    return symbol


def _get_order_type(symbol: str, side: str) -> str:
    try:
        orders = get_trade_orders(symbol, side)
        if not orders:
            return "LIMIT"
        for order in orders:
            order_type = order.get("type", "LIMIT")
            if order_type.upper() == "MARKET":
                return "MARKET"
        return "LIMIT"
    except Exception as e:
        print(f"⚠️ Error detecting order type: {e}")
        return "LIMIT"


def _format_leverage_with_risk(leverage: int) -> tuple[str, str]:
    """
    Возвращает:
    - текст плеча с эмодзи;
    - риск-пометку с разными эмодзи (🟢/🟠/🚀).
    """
    if leverage <= 25:
        lev_text = f"🟢 x{leverage}"
        risk = "🟢 LOW RISK"
    elif 25 < leverage < 50:
        lev_text = f"🟠 x{leverage}"
        risk = "🟠 MEDIUM RISK"
    else:
        lev_text = f"🚀 x{leverage}"
        risk = "🚀 HIGH RISK"
    return lev_text, risk


# ----------------- ЦЕНА С BINGX -----------------


async def get_live_price(symbol: str) -> float | None:
    """
    Берём цену с BingX для конкретного фьючерсного символа.
    """
    api_symbol = symbol

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            url = f"https://open-api.bingx.com/openApi/swap/v2/quote/price?symbol={api_symbol}"
            resp = await client.get(url)
            data = resp.json()
            if data.get("code") == 0 and "data" in data:
                d = data["data"]
                price_str = d.get("price") or d.get("lastPrice")
                if not price_str:
                    return None
                price = float(price_str)
                if price > 0:
                    _price_cache[api_symbol] = price
                    print(f"✅ LivePrice {api_symbol}: {price}")
                    return price
    except Exception as e:
        print(f"⚠️ Error getting live price for {api_symbol}: {e}")

    return _price_cache.get(api_symbol)


def _calculate_upnl(
    avg_price: float,
    current_price: float,
    amount: float,
    leverage: float,
    side: str,
) -> tuple[float, float]:
    try:
        if current_price <= 0 or avg_price <= 0 or amount <= 0:
            return 0.0, 0.0

        if side.upper() == "LONG":
            price_diff = current_price - avg_price
        else:
            price_diff = avg_price - current_price

        upnl_value = price_diff * amount
        notional = avg_price * amount
        margin_used = notional / leverage if leverage > 0 else notional
        upnl_percent = (upnl_value / margin_used * 100) if margin_used > 0 else 0.0
        return upnl_value, upnl_percent
    except Exception as e:
        print(f"❌ Error calculating uPnL: {e}")
        return 0.0, 0.0


def init_state_from_exchange():
    global _last_positions_state, _trade_messages
    try:
        current_positions = get_positions_dict()
        _last_positions_state = current_positions
        print(f"✅ State initialized with {len(_last_positions_state)} positions")

        open_trades = get_open_trades()
        for t in open_trades:
            key = (t["symbol"], t["side"])
            if key not in _trade_messages:
                _trade_messages[key] = {
                    "open_msg_id": t.get("channel_message_id"),
                    "trade_id": t["id"],
                    "last_upnl_pct": 0.0,
                }
    except Exception as e:
        print(f"⚠️ Error in init_state_from_exchange: {e}")


# ----------------- ФОРМАТЫ СООБЩЕНИЙ -----------------


def format_balance_message(balance_data: dict) -> str:
    if balance_data.get("code") != 0 or "data" not in balance_data:
        code = balance_data.get("code")
        msg = balance_data.get("msg", "Неизвестная ошибка")
        return (
            "⚠️ <b>Не удалось получить баланс BingX</b>\n\n"
            f"<code>Код ошибки: {code}</code>\n"
            f"<code>Сообщение: {msg}</code>"
        )

    b = _pick_usdt_balance(balance_data)
    if b is None:
        return "⚠️ <b>Не удалось найти данные баланса в ответе BingX.</b>"

    asset = b.get("asset", "USDT")
    balance = b.get("balance", "0")
    equity = b.get("equity", "0")
    available = b.get("availableMargin", "0")
    unrealized = b.get("unrealizedProfit", "0")
    realised = b.get("realisedProfit", "0")

    return (
        "<b>💰 Баланс BingX (фьючерсы)</b>\n\n"
        f"<b>Монета:</b> <code>{asset}</code>\n"
        f"<b>Баланс:</b> <code>{balance}</code>\n"
        f"<b>Эквити:</b> <code>{equity}</code>\n"
        f"<b>Доступная маржа:</b> <code>{available}</code>\n"
        f"<b>Нереализ. PnL:</b> <code>{unrealized}</code>\n"
        f"<b>Реализ. PnL:</b> <code>{realised}</code>"
    )


def format_positions_message(positions_data: dict) -> str:
    header = "<b>📂 Открытые позиции BingX</b>\n\n"
    if positions_data.get("code") != 0 or "data" not in positions_data:
        code = positions_data.get("code")
        msg = positions_data.get("msg", "Неизвестная ошибка")
        return header + (
            "⚠️ <b>Не удалось получить позиции</b>\n"
            f"<code>Код ошибки: {code}</code>\n"
            f"<code>Сообщение: {msg}</code>"
        )

    positions = positions_data.get("data", [])
    if not positions:
        return header + (
            "Сейчас нет открытых позиций.\n"
            "Открой сделку на фьючерсах, и здесь появится список."
        )

    blocks = []
    for pos in positions:
        raw_symbol = pos.get("symbol", "UNKNOWN")
        symbol = _clean_symbol(raw_symbol)
        side = pos.get("positionSide", "LONG")

        entry = _fmt_price(str(pos.get("avgPrice") or pos.get("entryPrice", "0")))
        amt = pos.get("positionAmt", "0")
        leverage = int(pos.get("leverage", "0") or 0)
        margin = _fmt_margin(str(pos.get("margin", "0")))
        liq = _fmt_liq(str(pos.get("liquidationPrice", "0")))

        lev_text, risk_label = _format_leverage_with_risk(leverage)

        try:
            upnl = float(pos.get("unrealizedProfit", "0") or 0)
            rpnl = float(pos.get("realisedProfit", "0") or 0)

            avg_price = float(pos.get("avgPrice") or pos.get("entryPrice", "0"))
            amt_f = float(pos.get("positionAmt", "0"))
            lev_f = float(pos.get("leverage", "0") or 1.0)

            notional = avg_price * amt_f
            margin_used = notional / lev_f if lev_f > 0 else notional
            upnl_pct = (upnl / margin_used * 100) if margin_used > 0 else 0.0
        except Exception as e:
            print(f"Error in format_positions_message: {e}")
            upnl = upnl_pct = rpnl = 0.0

        emoji = "🟢" if side.upper() == "LONG" else "🔴"
        coin = _coin_from_symbol(raw_symbol)

        pnl_emoji = ""
        if upnl_pct > 20:
            pnl_emoji = " 🚀"
        elif upnl_pct < -20:
            pnl_emoji = " 🩸"

        blocks.append(
            f"<b>{emoji} {side} {symbol}</b> — <code>{risk_label}</code>\n\n"
            f"<b>Вход (средняя):</b> <code>{entry}$</code>\n"
            f"<b>Объём:</b> <code>{amt} {coin}</code>\n"
            f"<b>Плечо:</b> <code>{lev_text}</code>\n"
            f"<b>Маржа:</b> <code>{margin}$</code>\n"
            f"<b>Ликвидация:</b> <code>{liq}$</code>\n\n"
            f"<b>uPnL (нереализованный):</b> <code>{upnl:+.2f}$</code> (<code>{upnl_pct:+.2f}%</code>){pnl_emoji}\n"
            f"<b>PnL (реализованный):</b> <code>{rpnl:+.2f}$</code>"
        )

    return header + "\n\n".join(blocks)


def trade_ref_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(text="🔗 Торговать на BingX", url=BINGX_REF_LINK)]]
    )


async def format_trade_open_notification(pos: dict) -> str:
    raw_symbol = pos.get("symbol", "UNKNOWN")
    symbol = _clean_symbol(raw_symbol)
    coin = _coin_from_symbol(raw_symbol)
    side = pos.get("positionSide", "LONG")

    entry_raw = str(pos.get("avgPrice") or pos.get("entryPrice", "0"))
    liq_raw = str(pos.get("liquidationPrice", "0"))

    entry = _fmt_price(entry_raw)
    liq = _fmt_liq(liq_raw)
    amt = pos.get("positionAmt", "0")
    leverage = int(pos.get("leverage", "0") or 0)
    margin = _fmt_margin(str(pos.get("margin", "0")))

    lev_text, risk_label = _format_leverage_with_risk(leverage)

    order_type = _get_order_type(raw_symbol, side)
    order_type_emoji = "📊" if order_type == "LIMIT" else "⚡"

    mark_raw = pos.get("markPrice")
    current_price_val = None
    if mark_raw not in (None, "", "0", "0.0", "0.000"):
        try:
            current_price_val = float(mark_raw)
        except Exception:
            current_price_val = None

    if current_price_val is None:
        current_price_val = await get_live_price(raw_symbol)
    if current_price_val is None:
        current_price_val = float(entry_raw)

    current_price = _fmt_price(str(current_price_val))

    try:
        avg_price_f = float(entry_raw)
        amt_f = float(amt)
        lev_f = float(leverage) or 1.0
        upnl_value, upnl_percent = _calculate_upnl(
            avg_price_f, current_price_val, amt_f, lev_f, side
        )
    except Exception as e:
        print(f"Error in format_trade_open_notification: {e}")
        upnl_value = upnl_percent = 0.0

    # Ракета в открытой сделке при uPnL > 50%
    pnl_rocket = " 🚀" if upnl_percent > 50 else ""

    emoji = "🟢" if side.upper() == "LONG" else "🔴"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return (
        f"<blockquote><b>{emoji} {side} #{symbol} {order_type_emoji} {order_type} ⚙️ Perp</b></blockquote>\n\n"
        f"<b>🎛 Риск-профиль:</b> <code>{risk_label}</code>\n\n"
        f"<b>💎 Параметры сделки:</b>\n"
        f"<blockquote>"
        f"💰 <b>Вход:</b> ${entry}\n"
        f"📦 <b>Объём:</b> {amt} {coin}\n"
        f"⚡ <b>Плечо:</b> {lev_text}\n"
        f"💳 <b>Маржа:</b> ${margin}\n"
        f"🔻 <b>Ликвидация:</b> ${liq}"
        f"</blockquote>\n\n"
        f"<b>📊 P&L (Live):</b>\n"
        f"<blockquote>"
        f"📈 <b>uPnL:</b> <code>{upnl_value:+.2f}$</code> (<code>{upnl_percent:+.2f}%</code>){pnl_rocket}\n"
        f"💵 <b>Цена (BingX):</b> <code>{current_price}$</code>"
        f"</blockquote>\n\n"
        f"<b>⏰ Время:</b> {now}"
    )


def format_trade_close_notification(
    trade_db: dict,
    current_price: float,
    pnl: float,
    pnl_percent: float,
    liquidated: bool = False,
) -> str:
    symbol_raw = trade_db.get("symbol", "UNKNOWN")
    symbol = _clean_symbol(symbol_raw)
    coin = _coin_from_symbol(symbol_raw)
    side = trade_db.get("side", "LONG")
    entry = trade_db.get("entry_price", "0")
    amt = trade_db.get("amount", "0")
    leverage = int(trade_db.get("leverage", "0") or 0)
    entry_time = trade_db.get("entry_time", "")

    lev_text, risk_label = _format_leverage_with_risk(leverage)

    try:
        entry_dt = datetime.fromisoformat(entry_time)
        duration = datetime.now() - entry_dt
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes = remainder // 60
        duration_str = f"{hours}ч {minutes}м"
    except Exception:
        duration_str = "N/A"

    emoji = "🟢" if side.upper() == "LONG" else "🔴"
    pnl_emoji = "✅" if pnl >= 0 else "❌"
    pnl_icon = "📈" if pnl >= 0 else "📉"
    pnl_mark = "🟢" if pnl >= 0 else "🔴"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    entry_fmt = _fmt_price(str(entry))
    current_fmt = _fmt_price(str(current_price))

    liq_text = ""
    if liquidated:
        liq_text = "💀 <b>Позиция ликвидирована.</b>\n\n"

    return (
        f"<blockquote><b>{emoji} {side} #{symbol} {pnl_emoji} ЗАКРЫТО {pnl_icon}</b></blockquote>\n\n"
        f"{liq_text}"
        f"<b>🎛 Риск-профиль:</b> <code>{risk_label}</code>\n\n"
        f"<b>🎯 Результаты:</b>\n"
        f"<blockquote>"
        f"📍 <b>Вход:</b> ${entry_fmt}\n"
        f"🎪 <b>Выход:</b> ${current_fmt}\n"
        f"📦 <b>Объём:</b> {amt} {coin}\n"
        f"⚡ <b>Плечо:</b> {lev_text}\n"
        f"💰 <b>Total PnL:</b> {pnl_mark} <b>{pnl:+.2f}$</b> ({pnl_percent:+.2f}%)"
        f"</blockquote>\n\n"
        f"⏱️ <b>Время держания:</b> {duration_str}\n"
        f"⏰ <b>Закрыто:</b> {now}"
    )


def format_pinned_message(
    balance_data: dict, open_trades_db: list, trade_message_links: dict
) -> str:
    b = _pick_usdt_balance(balance_data)
    if b is None:
        balance = equity = available = "N/A"
    else:
        balance = b.get("balance") or "0"
        equity = b.get("equity") or "0"
        available = b.get("availableMargin") or "0"

    unique_keys = {
        (t.get("symbol", "?"), t.get("side", "?")) for t in open_trades_db
    }
    open_trades_count = len(unique_keys)

    today_stats = get_today_stats()
    week_stats = get_week_stats()
    top_trades = get_top_trades(limit=10)

    today_pnl = float(today_stats.get("total_pnl", 0.0))
    today_trades = int(today_stats.get("total_trades", 0))
    today_win_rate = float(today_stats.get("win_rate", 0.0))

    week_pnl = float(week_stats.get("total_pnl", 0.0))
    week_trades = int(week_stats.get("total_trades", 0))
    week_win_rate = float(week_stats.get("win_rate", 0.0))

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    text = (
        "<blockquote><b>📊 BingX Futures Radar 🧬</b></blockquote>\n\n"
        "<b>💰 Account Balance</b>\n"
        "<blockquote>"
        f"💵 Balance:          {balance} USDT\n"
        f"📈 Equity:           {equity} USDT\n"
        f"💳 Available Margin: {available} USDT"
        "</blockquote>\n\n"
        "<b>🟢 Active Positions</b>\n"
        "<blockquote>"
        f"📊 Open Trades: {open_trades_count}"
    )

    if open_trades_db:
        text += "\n\n<b>Positions:</b>\n"
        seen = set()
        for trade in open_trades_db:
            symbol_raw = trade.get("symbol", "?")
            symbol_clean = _clean_symbol(symbol_raw)
            side = trade.get("side", "?")
            key = (symbol_raw, side)
            if key in seen:
                continue
            seen.add(key)

            emoji = "🟢" if side == "LONG" else "🔴"

            if key in trade_message_links:
                msg_id = trade_message_links[key].get("open_msg_id")
                if msg_id:
                    msg_link = (
                        f"https://t.me/c/{str(TELEGRAM_CHANNEL_ID).lstrip('-100')}/{msg_id}"
                    )
                    text += (
                        f"<a href=\"{msg_link}\">{emoji} {side} {symbol_clean}</a>\n"
                    )
                else:
                    text += f"{emoji} {side} {symbol_clean}\n"
            else:
                text += f"{emoji} {side} {symbol_clean}\n"

    text += (
        "</blockquote>\n\n"
        "<b>📈 Today Statistics</b>\n"
        "<blockquote>"
        f"🎯 Trades: {today_trades} | 💰 P&L: {'✅⬆️' if today_pnl >= 0 else '❌⬇️'} {today_pnl:+.2f}$ | 📊 Win Rate: {today_win_rate:.1f}%"
        "</blockquote>\n\n"
        "<b>📊 Week Statistics</b>\n"
        "<blockquote>"
        f"🎯 Trades: {week_trades} | 💰 P&L: {'✅⬆️' if week_pnl >= 0 else '❌⬇️'} {week_pnl:+.2f}$ | 📊 Win Rate: {week_win_rate:.1f}%"
        "</blockquote>\n\n"
    )

    if top_trades:
        text += "<b>🏆 Top Trades (Top 10) 🏆</b>\n<blockquote>"
        for idx, t in enumerate(top_trades, 1):
            sym_raw = t.get("symbol", "?")
            sym = _clean_symbol(sym_raw)
            sside = t.get("side", "?")
            tpnl = float(t.get("pnl", 0.0))
            status_emoji = "✅" if tpnl >= 0 else "❌"
            closed_badge = " 🔒"
            text += (
                f"#{idx} {status_emoji} {sside} {sym}: <b>{tpnl:+.2f}$</b>{closed_badge}\n"
            )
        text += "</blockquote>\n\n"

    text += f"🕐 <b>Last Updated:</b> {now}"
    return text


# ----------------- КОМАНДЫ -----------------


async def start_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    text = (
        "Привет! Я бот для счёта BingX.\n\n"
        "<b>Доступные команды:</b>\n"
        "/balance — баланс фьючерсного аккаунта\n"
        "/trades — открытые позиции\n"
        "/summary — обновить сводку в канале\n"
        "/reset_db — очистить БД от устаревших сделок\n"
        "/hard_reset — полный сброс БД и состояния\n"
        "/refresh_all — пересчитать и обновить все открытые сделки и сводку"
    )
    await context.bot.send_message(
        chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=MAIN_KEYBOARD
    )


async def balance_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    balance_raw = get_futures_balance()
    print("BALANCE RAW:", balance_raw)
    msg = format_balance_message(balance_raw)
    await context.bot.send_message(
        chat_id=chat_id, text=msg, parse_mode="HTML", reply_markup=MAIN_KEYBOARD
    )


async def trades_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    positions_raw = get_futures_positions()
    print("POSITIONS RAW:", positions_raw)
    msg = format_positions_message(positions_raw)
    await context.bot.send_message(
        chat_id=chat_id, text=msg, parse_mode="HTML", reply_markup=MAIN_KEYBOARD
    )


async def summary_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    await update_summary_message(context)
    await context.bot.send_message(
        chat_id=chat_id,
        text="✅ Сводка обновлена!",
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD,
    )


async def reset_db_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    try:
        current_positions = get_positions_dict()
        active_keys = list(current_positions.keys())
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Не удалось получить актуальные позиции BingX:\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD,
        )
        return

    deleted = clean_obsolete_open_trades(active_keys)

    global _last_positions_state, _trade_messages, _last_summary_message_id
    _last_positions_state = current_positions
    _trade_messages = {k: v for k, v in _trade_messages.items() if k in active_keys}
    _last_summary_message_id = None

    await update_summary_message(context)

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🧹 Очистка завершена.\n"
            f"Удалено устаревших открытых сделок: <b>{deleted}</b>.\n"
            f"Оставлены только активные позиции с биржи."
        ),
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD,
    )


async def hard_reset_command(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    try:
        hard_reset_db()
        global _last_positions_state, _trade_messages, _last_summary_message_id
        _last_positions_state = {}
        _trade_messages = {}
        _last_summary_message_id = None

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "🧨 Полный сброс выполнен.\n"
                "БД очищена, состояние обнулено.\n"
                "Перезапусти бота и при необходимости поменяй URL на основной аккаунт BingX."
            ),
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD,
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при полном сбросе:\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD,
        )


async def refresh_all_command(update: Update, context: CallbackContext) -> None:
    """
    Полный рефреш:
    - подтянуть позиции с BingX;
    - пересчитать PnL по открытым сделкам в БД;
    - пересоздать сообщения по открытым сделкам в канале;
    - обновить сводку.
    """
    chat_id = update.effective_chat.id
    global _last_positions_state, _trade_messages

    try:
        positions_dict = get_positions_dict()
        print("REFRESH POSITIONS DICT:", positions_dict)

        for key, meta in list(_trade_messages.items()):
            msg_id = meta.get("open_msg_id")
            if msg_id:
                try:
                    await context.bot.delete_message(
                        chat_id=TELEGRAM_CHANNEL_ID,
                        message_id=msg_id,
                    )
                    print(f"🗑 Refresh: deleted old open message for {key}")
                except Exception as e:
                    print(f"⚠️ Refresh: error deleting message {key}: {e}")

        _trade_messages = {}

        for (symbol, side), pos in positions_dict.items():
            entry_price = pos.get("avgPrice") or pos.get("entryPrice", "0")
            amount = pos.get("positionAmt", "0")
            leverage = pos.get("leverage", "0")
            margin = pos.get("margin", "0")

            create_ms = pos.get("createTime")
            if create_ms:
                entry_time = datetime.fromtimestamp(
                    int(create_ms) / 1000
                ).isoformat()
            else:
                entry_time = datetime.utcnow().isoformat()

            trade_id = add_trade(
                symbol=symbol,
                side=side,
                entry_price=float(entry_price),
                amount=float(amount),
                leverage=int(leverage),
                entry_margin=float(margin),
                entry_time=entry_time,
                channel_message_id=None,
            )

            notification = await format_trade_open_notification(pos)
            try:
                msg = await context.bot.send_message(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    text=notification,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    reply_markup=trade_ref_keyboard(),
                )
                _trade_messages[(symbol, side)] = {
                    "open_msg_id": msg.message_id,
                    "trade_id": trade_id,
                    "last_upnl_pct": 0.0,
                }
                print(f"✅ Refresh: reposted {symbol} {side}")
            except Exception as e:
                print(f"❌ Refresh: error posting {symbol} {side}: {e}")

        _last_positions_state = positions_dict

        await update_summary_message(context)

        await context.bot.send_message(
            chat_id=chat_id,
            text="♻️ Пересчёт и обновление завершены.\nОткрытые сделки и сводка синхронизированы с BingX.",
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD,
        )
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Ошибка при пересчёте:\n<code>{e}</code>",
            parse_mode="HTML",
            reply_markup=MAIN_KEYBOARD,
        )


async def text_message_handler(update: Update, context: CallbackContext) -> None:
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if text == "💰 Баланс":
        await balance_command(update, context)
    elif text == "📂 Позиции":
        await trades_command(update, context)
    elif text == "📊 Обновить сводку":
        await summary_command(update, context)
    elif text == "🧹 Очистить БД":
        await reset_db_command(update, context)
    elif text == "♻️ Пересчитать и обновить":
        await refresh_all_command(update, context)


# ----------------- СВОДКА -----------------


async def update_summary_message(context: CallbackContext) -> None:
    global _last_summary_message_id
    try:
        balance_raw = get_futures_balance()
        open_trades = get_open_trades()
        msg = format_pinned_message(balance_raw, open_trades, _trade_messages)

        if _last_summary_message_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=TELEGRAM_CHANNEL_ID,
                    message_id=_last_summary_message_id,
                    text=msg,
                    parse_mode="HTML",
                )
                print(f"✅ Summary updated: {_last_summary_message_id}")
                return
            except Exception as e:
                err_text = str(e)
                if "message is not modified" in err_text:
                    print("ℹ️ Summary not changed (message is not modified)")
                    return
                print(f"⚠️ Error updating summary message:", repr(e))

        sent_msg = await context.bot.send_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            text=msg,
            parse_mode="HTML",
        )
        await context.bot.pin_chat_message(
            chat_id=TELEGRAM_CHANNEL_ID,
            message_id=sent_msg.message_id,
        )
        _last_summary_message_id = sent_msg.message_id
        print(f"✅ Summary created & pinned: {_last_summary_message_id}")
    except Exception as e:
        print("❌ Error in update_summary_message (outer):", repr(e))


# ----------------- ДЖОБЫ -----------------


async def update_pnl_job(context: CallbackContext) -> None:
    """
    Обновлять сообщение сделки только при реальном движении PnL (порог 0.3%).
    """
    try:
        current_positions = get_positions_dict()
        for (symbol, side), pos in current_positions.items():
            key = (symbol, side)
            if key in _trade_messages:
                msg_id = _trade_messages[key].get("open_msg_id")
                trade_id = _trade_messages[key].get("trade_id")
                if msg_id and trade_id:
                    try:
                        avg_price_f = float(
                            pos.get("avgPrice") or pos.get("entryPrice", "0")
                        )
                        amt_f = float(pos.get("positionAmt", "0"))
                        lev_f = float(pos.get("leverage", "0")) or 1.0

                        mark_raw = pos.get("markPrice")
                        current_price_val = None
                        if mark_raw not in (None, "", "0", "0.0", "0.000"):
                            current_price_val = float(mark_raw)
                        if current_price_val is None:
                            current_price_val = await get_live_price(symbol)
                        if current_price_val is None:
                            current_price_val = avg_price_f

                        upnl_value, upnl_percent = _calculate_upnl(
                            avg_price_f,
                            current_price_val,
                            amt_f,
                            lev_f,
                            side,
                        )

                        update_trade_with_exit(
                            trade_id, float(current_price_val), upnl_value, upnl_percent
                        )

                        last_pct = _trade_messages[key].get("last_upnl_pct", 0.0)
                        if abs(upnl_percent - last_pct) < 0.3:
                            continue

                        _trade_messages[key]["last_upnl_pct"] = upnl_percent
                        updated_notification = await format_trade_open_notification(pos)

                        await context.bot.edit_message_text(
                            chat_id=TELEGRAM_CHANNEL_ID,
                            message_id=msg_id,
                            text=updated_notification,
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                            reply_markup=trade_ref_keyboard(),
                        )
                    except Exception as e:
                        print(f"⚠️ Error updating PnL for {symbol} {side}: {e}")

        global _last_summary_update
        current_time = time.time()
        if current_time - _last_summary_update >= 30:
            _last_summary_update = current_time
            await update_summary_message(context)

    except Exception as e:
        print(f"❌ Error in update_pnl_job: {e}")


async def check_positions_job(context: CallbackContext) -> None:
    global _last_positions_state
    try:
        current_positions = get_positions_dict()
        print("POSITIONS RAW DICT:", current_positions)

        for (symbol, side), pos in current_positions.items():
            key = (symbol, side)
            if key not in _last_positions_state:
                entry_price = pos.get("avgPrice") or pos.get("entryPrice", "0")
                amount = pos.get("positionAmt", "0")
                leverage = pos.get("leverage", "0")
                margin = pos.get("margin", "0")

                create_ms = pos.get("createTime")
                if create_ms:
                    entry_time = datetime.fromtimestamp(
                        int(create_ms) / 1000
                    ).isoformat()
                else:
                    entry_time = datetime.utcnow().isoformat()

                trade_id = add_trade(
                    symbol=symbol,
                    side=side,
                    entry_price=float(entry_price),
                    amount=float(amount),
                    leverage=int(leverage),
                    entry_margin=float(margin),
                    entry_time=entry_time,
                    channel_message_id=None,
                )

                notification = await format_trade_open_notification(pos)
                try:
                    msg = await context.bot.send_message(
                        chat_id=TELEGRAM_CHANNEL_ID,
                        text=notification,
                        parse_mode="HTML",
                        disable_web_page_preview=True,
                        reply_markup=trade_ref_keyboard(),
                    )
                    _trade_messages[key] = {
                        "open_msg_id": msg.message_id,
                        "trade_id": trade_id,
                        "last_upnl_pct": 0.0,
                    }
                    print(f"✅ Trade OPEN posted: {symbol} {side}")
                    await update_summary_message(context)
                except Exception as e:
                    print(f"❌ Error posting to channel: {e}")
            else:
                old_pos = _last_positions_state[(symbol, side)]
                old_amt = float(old_pos.get("positionAmt", "0") or 0)
                new_amt = float(pos.get("positionAmt", "0") or 0)

                if old_amt > 0 and new_amt > old_amt:
                    increase_pct = (new_amt - old_amt) / old_amt * 100
                    if increase_pct >= 10:
                        try:
                            await context.bot.send_message(
                                chat_id=TELEGRAM_CHANNEL_ID,
                                text=(
                                    f"➕ <b>Усиление позиции</b> {side} {_clean_symbol(symbol)}\n"
                                    f"📈 Объём: {old_amt} → {new_amt}"
                                ),
                                parse_mode="HTML",
                            )
                        except Exception as e:
                            print(f"⚠️ Error sending scale-in notification: {e}")

        for (symbol, side) in list(_last_positions_state.keys()):
            if (symbol, side) not in current_positions:
                old_pos = _last_positions_state[(symbol, side)]

                avg_price = float(
                    old_pos.get("avgPrice") or old_pos.get("entryPrice", "0")
                )
                amount = float(old_pos.get("positionAmt", "0"))
                leverage = float(old_pos.get("leverage", "1") or 1.0)
                margin = float(
                    old_pos.get("margin", "0")
                    or old_pos.get("initialMargin", "0")
                    or 0.0
                )
                liq_price = float(
                    old_pos.get("liquidationPrice", "0") or 0.0
                )
                mark_price = float(old_pos.get("markPrice", "0") or 0.0)

                liquidated = False
                if liq_price > 0 and mark_price > 0:
                    diff_pct = abs(mark_price - liq_price) / liq_price * 100
                    if diff_pct < 1.0:
                        liquidated = True

                if liquidated:
                    pnl_value = -margin
                else:
                    pnl_value = float(old_pos.get("realisedProfit", "0") or 0.0)

                notional = avg_price * amount
                margin_used = notional / leverage if leverage > 0 else notional
                pnl_percent = (
                    (pnl_value / margin_used * 100) if margin_used > 0 else 0.0
                )

                last_price = await get_live_price(symbol)
                if last_price is None:
                    last_price = mark_price or avg_price

                close_trade(
                    symbol=symbol,
                    side=side,
                    exit_price=float(last_price),
                    pnl=pnl_value,
                    pnl_percent=pnl_percent,
                )

                trade_db = get_trade_by_symbol_and_side(symbol, side)
                key = (symbol, side)
                open_msg_id = _trade_messages.get(key, {}).get("open_msg_id")

                if trade_db:
                    notification = format_trade_close_notification(
                        trade_db,
                        last_price,
                        pnl_value,
                        pnl_percent,
                        liquidated=liquidated,
                    )
                    try:
                        await context.bot.send_message(
                            chat_id=TELEGRAM_CHANNEL_ID,
                            text=notification,
                            parse_mode="HTML",
                            disable_web_page_preview=True,
                            reply_markup=trade_ref_keyboard(),
                        )
                        print(f"✅ Trade CLOSE posted: {symbol} {side}")
                    except Exception as e:
                        print(f"❌ Error posting CLOSE to channel: {e}")

                if open_msg_id:
                    try:
                        await context.bot.delete_message(
                            chat_id=TELEGRAM_CHANNEL_ID,
                            message_id=open_msg_id,
                        )
                        print(f"🗑 Deleted open trade message: {symbol} {side}")
                    except Exception as e:
                        print(f"⚠️ Error deleting open trade message: {e}")

                if key in _trade_messages:
                    del _trade_messages[key]

                await update_summary_message(context)

        _last_positions_state = current_positions
    except Exception as e:
        print(f"❌ Error in check_positions_job: {e}")


# ----------------- APPLICATION BUILDER -----------------


def build_application(token: str) -> Application:
    print("🔧 build_application() start")
    application = Application.builder().token(token).build()
    print("🔧 Application created:", application)

    init_state_from_exchange()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("trades", trades_command))
    application.add_handler(CommandHandler("summary", summary_command))
    application.add_handler(CommandHandler("reset_db", reset_db_command))
    application.add_handler(CommandHandler("hard_reset", hard_reset_command))
    application.add_handler(CommandHandler("refresh_all", refresh_all_command))

    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler)
    )

    if application.job_queue is None:
        print(
            "⚠️ JobQueue недоступен (application.job_queue is None) — Джобы НЕ будут запущены"
        )
    else:
        print("✅ JobQueue доступен, настраиваем джобы...")
        application.job_queue.run_repeating(
            check_positions_job,
            interval=10,
            first=5,
        )
        application.job_queue.run_repeating(
            update_pnl_job,
            interval=10,
            first=10,
        )
        print("✅ Jobs scheduled successfully")

    print("✅ build_application() done, returning application")
    return application
