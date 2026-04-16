from __future__ import annotations

import asyncio
import html
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, types
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv


load_dotenv()


BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TIMEPAD_TOKEN = os.getenv("TIMEPAD_TOKEN", "").strip()
ADMIN_IDS = [
    int(item.strip())
    for item in os.getenv("ADMIN_IDS", "").split(",")
    if item.strip().isdigit()
]
CITIES = [
    item.strip()
    for item in os.getenv(
        "CITIES", "Москва,Санкт-Петербург,Новосибирск,Екатеринбург"
    ).split(",")
    if item.strip()
]
EVENTS_LIMIT = max(1, min(int(os.getenv("EVENTS_LIMIT", "10")), 20))
HTTP_TIMEOUT = max(3, int(os.getenv("HTTP_TIMEOUT", "10")))
DB_PATH = Path(os.getenv("DB_PATH", "data/eventradar.db"))


CATEGORY_FALLBACK = [
    {"id": None, "name": "Концерты"},
    {"id": None, "name": "Искусство и культура"},
    {"id": None, "name": "Экскурсии и путешествия"},
]


if not BOT_TOKEN:
    raise RuntimeError(
        "Не найден BOT_TOKEN. Укажите токен Telegram-бота в переменной окружения или .env"
    )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
for noisy_logger in ("aiogram", "aiohttp", "asyncio"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)
logger = logging.getLogger("eventradar_bot")


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()

broadcast_mode: dict[int, dict[str, Any]] = {}
CACHED_CATEGORIES: list[dict[str, Any]] = []
GLOBAL_AIO_SESSION: aiohttp.ClientSession | None = None


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn: aiosqlite.Connection | None = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path.as_posix())
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA foreign_keys=ON;")
        await self.conn.commit()
        await self.init_schema()

    async def init_schema(self) -> None:
        assert self.conn is not None
        async with self.lock:
            await self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    city TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_name TEXT NOT NULL,
                    event_date TEXT,
                    city TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_history_user_id ON history(user_id);
                """
            )
            await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def execute(self, query: str, params: tuple[Any, ...] = ()) -> list[aiosqlite.Row]:
        assert self.conn is not None
        async with self.lock:
            cursor = await self.conn.execute(query, params)
            try:
                is_select = query.lstrip().upper().startswith("SELECT")
                rows = await cursor.fetchall() if is_select else []
                await self.conn.commit()
                return rows
            finally:
                await cursor.close()

    async def execute_one(
        self, query: str, params: tuple[Any, ...] = ()
    ) -> aiosqlite.Row | None:
        rows = await self.execute(query, params)
        return rows[0] if rows else None


DB = Database(DB_PATH)


def timepad_headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if TIMEPAD_TOKEN:
        headers["Authorization"] = f"Bearer {TIMEPAD_TOKEN}"
    return headers


async def create_aio_session() -> aiohttp.ClientSession:
    global GLOBAL_AIO_SESSION
    if GLOBAL_AIO_SESSION and not GLOBAL_AIO_SESSION.closed:
        return GLOBAL_AIO_SESSION

    timeout = aiohttp.ClientTimeout(total=HTTP_TIMEOUT)
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    GLOBAL_AIO_SESSION = aiohttp.ClientSession(timeout=timeout, connector=connector)
    return GLOBAL_AIO_SESSION


async def close_aio_session() -> None:
    global GLOBAL_AIO_SESSION
    if GLOBAL_AIO_SESSION and not GLOBAL_AIO_SESSION.closed:
        await GLOBAL_AIO_SESSION.close()
    GLOBAL_AIO_SESSION = None


async def notify_admins(text: str) -> None:
    if not text or not ADMIN_IDS:
        return
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text[:3500])
        except Exception:
            logger.exception("Не удалось отправить уведомление админу %s", admin_id)


async def answer_callback(
    callback: types.CallbackQuery, text: str | None = None, show_alert: bool = False
) -> None:
    try:
        await callback.answer(text=text or "", show_alert=show_alert)
    except Exception:
        pass


async def safe_edit(
    message: types.Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    try:
        await message.edit_text(
            text,
            reply_markup=reply_markup,
            disable_web_page_preview=True,
        )
    except TelegramBadRequest as exc:
        error_text = str(exc).lower()
        if "message is not modified" in error_text:
            return
        if (
            "message can't be edited" in error_text
            or "message to edit not found" in error_text
            or "there is no text in the message" in error_text
        ):
            await message.answer(
                text,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )
            return
        raise


async def safe_send(
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    await bot.send_message(
        chat_id,
        text,
        reply_markup=reply_markup,
        disable_web_page_preview=True,
    )


@dataclass(slots=True)
class EventItem:
    name: str
    date: str
    url: str
    address: str
    categories: list[str]
    location_obj: dict[str, Any]
    city: str


def normalize_datetime(value: str | None) -> str:
    if not value:
        return ""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime(
            "%Y-%m-%d %H:%M"
        )
    except ValueError:
        return value.replace("T", " ")[:16]


def build_map_link(event: EventItem, city: str | None) -> str:
    location_obj = event.location_obj or {}
    lat = location_obj.get("latitude") or location_obj.get("lat")
    lon = location_obj.get("longitude") or location_obj.get("lon") or location_obj.get("lng")
    if lat and lon:
        try:
            return f"https://yandex.ru/maps/?ll={float(lon)}%2C{float(lat)}&z=16"
        except Exception:
            pass
    if event.address and event.address != "Не указано":
        return f"https://yandex.ru/maps/?text={quote_plus(event.address)}"
    fallback = quote_plus(f"{event.name} {city or event.city or ''}".strip())
    return f"https://yandex.ru/maps/?text={fallback}"


def main_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🌆 Выбрать город", callback_data="choose_city")],
        [InlineKeyboardButton(text="🗂 Категории", callback_data="categories")],
    ]
    if user_id in ADMIN_IDS:
        buttons.append([InlineKeyboardButton(text="🛠 Админ панель", callback_data="admin")])
        buttons.append([InlineKeyboardButton(text="/refresh", callback_data="refresh")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def city_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=city, callback_data=f"city_{city}")]
            for city in CITIES
        ]
    )


def back_to_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")]]
    )


def categories_keyboard(categories: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=item["name"], callback_data=f"cat_{index}")]
        for index, item in enumerate(categories)
    ]
    rows.append([InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def fetch_categories_from_api(max_events: int = 200) -> list[dict[str, Any]]:
    session = await create_aio_session()
    headers = timepad_headers()

    try:
        async with session.get(
            "https://api.timepad.ru/v1/dictionary/event_categories",
            headers=headers,
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                values = data.get("values", [])
                categories: list[dict[str, Any]] = []
                if isinstance(values, list):
                    for item in values:
                        if not isinstance(item, dict):
                            continue
                        name = item.get("name")
                        category_id = item.get("id")
                        if name:
                            categories.append({"id": category_id, "name": str(name)})
                if categories:
                    categories.sort(key=lambda x: x["name"].lower())
                    return categories
    except Exception:
        logger.warning("Не удалось получить категории из словаря Timepad")

    categories_map: dict[str, dict[str, Any]] = {}
    skip = 0
    per_page = 100

    while skip < max_events:
        params: dict[str, Any] = {
            "limit": per_page,
            "skip": skip,
            "fields": ["categories"],
            "sort": "+starts_at",
            "starts_at_min": datetime.now(timezone.utc).isoformat(),
            "access_statuses": ["public"],
        }
        try:
            async with session.get(
                "https://api.timepad.ru/v1/events", headers=headers, params=params
            ) as resp:
                if resp.status != 200:
                    break
                data = await resp.json()
                values = data.get("values", [])
                if not values:
                    break

                for item in values:
                    raw_categories = item.get("categories") or []
                    if isinstance(raw_categories, list):
                        for cat in raw_categories:
                            if isinstance(cat, dict):
                                name = cat.get("name")
                                category_id = cat.get("id")
                                if name:
                                    categories_map[name] = {"id": category_id, "name": name}

                skip += len(values)
        except Exception:
            logger.exception("Ошибка при fallback-сборе категорий")
            break

    if categories_map:
        categories = list(categories_map.values())
        categories.sort(key=lambda x: x["name"].lower())
        return categories

    return CATEGORY_FALLBACK.copy()


async def fetch_timepad_events(
    city: str | None,
    limit: int = EVENTS_LIMIT,
    category_id: int | None = None,
    category_name: str | None = None,
) -> list[EventItem]:
    session = await create_aio_session()
    headers = timepad_headers()

    params: dict[str, Any] = {
        "limit": 100,
        "skip": 0,
        "fields": ["location", "categories"],
        "sort": "+starts_at",
        "access_statuses": ["public"],
        "starts_at_min": datetime.now(timezone.utc).isoformat(),
    }

    # Публичный endpoint /v1/events может работать без токена.
    # Если токен пустой, Authorization не отправляется.
    try:
        async with session.get(
            "https://api.timepad.ru/v1/events", headers=headers, params=params
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error("Timepad вернул %s: %s", resp.status, body[:500])
                return []
            data = await resp.json()
    except Exception:
        logger.exception("Ошибка запроса к Timepad API")
        return []

    events: list[EventItem] = []
    category_lower = category_name.lower() if category_name else None

    for item in data.get("values", []):
        location = item.get("location") or {}
        raw_categories = item.get("categories") or []

        categories: list[str] = []
        if isinstance(raw_categories, list):
            for cat in raw_categories:
                if isinstance(cat, dict) and cat.get("name"):
                    categories.append(str(cat["name"]))
                elif isinstance(cat, str):
                    categories.append(cat)

        if category_lower and categories:
            if not any(category_lower in cat.lower() for cat in categories):
                continue

        event_city = str(
            location.get("city")
            or location.get("city_name")
            or item.get("city")
            or city
            or ""
        ).strip()

        events.append(
            EventItem(
                name=str(item.get("name") or "Без названия"),
                date=normalize_datetime(item.get("starts_at")),
                url=str(item.get("url") or ""),
                address=str(location.get("address") or "Не указано"),
                categories=categories,
                location_obj=location,
                city=event_city,
            )
        )

        if len(events) >= limit:
            break

    return events


async def send_events(
    message: types.Message,
    events: list[EventItem],
    city: str | None,
    selected_category: str | None = None,
) -> None:
    if not events:
        await safe_edit(
            message,
            "😕 Событий не найдено.",
            reply_markup=main_menu(message.chat.id),
        )
        return

    header_parts: list[str] = []
    if selected_category:
        header_parts.append(f"Категория: {html.escape(selected_category)}")
    if city:
        header_parts.append(f"Город: {html.escape(city)}")
    header = " | ".join(header_parts)

    text = f"🎫 Подборка событий{(' — ' + header) if header else ''}:\n\n"

    for event in events[:EVENTS_LIMIT]:
        name = html.escape(event.name)
        date = html.escape(event.date)
        shown_type = selected_category or (event.categories[0] if event.categories else None)
        shown_type = html.escape(shown_type) if shown_type else None
        shown_city = html.escape(city or event.city) if (city or event.city) else None

        map_link = html.escape(build_map_link(event, city), quote=True)
        event_url = html.escape(event.url, quote=True)

        text += f"• <b>{name}</b>\n"
        if date:
            text += f"  🕒 {date}\n"
        text += f'  📌 <a href="{map_link}">Адрес</a>\n'
        if event.url:
            text += f'  🌐 <a href="{event_url}">Ссылка</a>\n'
        if shown_type:
            text += f"  🏷 Тип: {shown_type}\n"
        if shown_city:
            text += f"  📍 Город: {shown_city}\n"
        text += "\n"

        try:
            await DB.execute(
                "INSERT INTO history (user_id, event_name, event_date, city) VALUES (?, ?, ?, ?)",
                (message.chat.id, event.name, event.date, city or event.city),
            )
        except Exception:
            logger.exception("Не удалось сохранить событие в историю")

    await safe_edit(message, text, reply_markup=back_to_menu_keyboard())


async def get_user_city(user_id: int) -> str | None:
    row = await DB.execute_one("SELECT city FROM users WHERE id = ?", (user_id,))
    return str(row["city"]) if row and row["city"] else None


async def ensure_user(user_id: int, username: str | None) -> None:
    existing = await DB.execute_one("SELECT id FROM users WHERE id = ?", (user_id,))
    if existing:
        await DB.execute(
            "UPDATE users SET username = COALESCE(?, username) WHERE id = ?",
            (username, user_id),
        )
    else:
        await DB.execute(
            "INSERT INTO users (id, username, city) VALUES (?, ?, NULL)",
            (user_id, username),
        )


@dp.message(Command("start"))
async def start(message: types.Message) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    city = await get_user_city(message.from_user.id)
    if city:
        await message.answer(
            f"👋 Привет! Ваш город: {html.escape(city)}",
            reply_markup=main_menu(message.from_user.id),
        )
        return
    await message.answer("👋 Сначала выберите свой город:", reply_markup=city_keyboard())


@dp.message(Command("menu"))
async def menu_command(message: types.Message) -> None:
    await ensure_user(message.from_user.id, message.from_user.username)
    await message.answer("Выберите действие:", reply_markup=main_menu(message.from_user.id))


@dp.message(Command("refresh"))
async def refresh_command(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа к обновлению кеша.")
        return
    global CACHED_CATEGORIES
    CACHED_CATEGORIES = []
    await message.answer("✅ Кеш категорий очищен.", reply_markup=main_menu(message.from_user.id))


@dp.callback_query(F.data == "refresh")
async def refresh_menu(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    if callback.from_user.id not in ADMIN_IDS:
        await safe_edit(callback.message, "❌ У вас нет доступа.", reply_markup=main_menu(callback.from_user.id))
        return
    global CACHED_CATEGORIES
    CACHED_CATEGORIES = []
    await safe_edit(
        callback.message,
        "✅ Кеш категорий очищен. Выберите действие:",
        reply_markup=main_menu(callback.from_user.id),
    )


@dp.callback_query(F.data.startswith("city_"))
async def set_city(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    city = callback.data.split("_", 1)[1]
    await ensure_user(callback.from_user.id, callback.from_user.username)
    await DB.execute("UPDATE users SET city = ? WHERE id = ?", (city, callback.from_user.id))
    await safe_edit(callback.message, f"🌆 Город установлен: {html.escape(city)}")
    await callback.message.answer(
        "Выберите действие:", reply_markup=main_menu(callback.from_user.id)
    )


@dp.callback_query(F.data == "choose_city")
async def choose_city(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    await safe_edit(callback.message, "🌆 Выберите город:", reply_markup=city_keyboard())


@dp.callback_query(F.data == "categories")
async def show_categories(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    global CACHED_CATEGORIES
    if not CACHED_CATEGORIES:
        CACHED_CATEGORIES = await fetch_categories_from_api()
        if not CACHED_CATEGORIES:
            CACHED_CATEGORIES = CATEGORY_FALLBACK.copy()

    await safe_edit(
        callback.message,
        "Выберите категорию событий:",
        reply_markup=categories_keyboard(CACHED_CATEGORIES),
    )


@dp.callback_query(F.data.startswith("cat_"))
async def category_selected(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    try:
        index = int(callback.data.split("_", 1)[1])
        category_obj = CACHED_CATEGORIES[index]
    except Exception:
        await safe_edit(callback.message, "❌ Неизвестная категория.")
        return

    city = await get_user_city(callback.from_user.id)
    events = await fetch_timepad_events(
        city=city,
        limit=EVENTS_LIMIT,
        category_id=category_obj.get("id"),
        category_name=category_obj.get("name"),
    )
    await send_events(
        callback.message,
        events,
        city=city,
        selected_category=category_obj.get("name"),
    )


@dp.callback_query(F.data == "menu")
async def back_to_menu(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    await safe_edit(
        callback.message,
        "Выберите действие:",
        reply_markup=main_menu(callback.from_user.id),
    )


@dp.callback_query(F.data == "admin")
async def admin_panel(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    if callback.from_user.id not in ADMIN_IDS:
        await safe_edit(callback.message, "❌ У вас нет доступа к админ-панели.")
        return
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Количество пользователей", callback_data="admin_users")],
            [InlineKeyboardButton(text="✉️ Рассылка всем", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="🔙 Главное меню", callback_data="menu")],
        ]
    )
    await safe_edit(callback.message, "🛠 Админ-панель:", reply_markup=keyboard)


@dp.callback_query(F.data == "admin_users")
async def show_users(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    if callback.from_user.id not in ADMIN_IDS:
        await safe_edit(callback.message, "❌ У вас нет доступа.")
        return
    row = await DB.execute_one("SELECT COUNT(*) AS total FROM users")
    total = int(row["total"]) if row else 0
    await safe_edit(
        callback.message,
        f"📊 Всего пользователей: {total}",
        reply_markup=main_menu(callback.from_user.id),
    )


@dp.callback_query(F.data == "admin_broadcast")
async def broadcast_prompt(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    if callback.from_user.id not in ADMIN_IDS:
        await safe_edit(callback.message, "❌ У вас нет доступа.")
        return
    broadcast_mode[callback.from_user.id] = {"text": None}
    await safe_edit(callback.message, "✉️ Отправьте сообщение для рассылки всем пользователям:")


@dp.message(lambda message: message.from_user.id in broadcast_mode)
async def receive_broadcast_text(message: types.Message) -> None:
    if message.from_user.id not in ADMIN_IDS:
        return
    if not message.text:
        await message.answer("❌ Для рассылки нужен обычный текст.")
        return

    broadcast_mode[message.from_user.id]["text"] = message.text
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, отправить", callback_data="confirm_broadcast"),
                InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_broadcast"),
            ]
        ]
    )
    await message.answer(
        f"Вы уверены, что хотите отправить это сообщение всем?\n\n{html.escape(message.text)}",
        reply_markup=keyboard,
    )


@dp.callback_query(F.data == "confirm_broadcast")
async def confirm_broadcast(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    data = broadcast_mode.get(callback.from_user.id)
    if not data or not data.get("text"):
        await safe_edit(callback.message, "❌ Нет текста для рассылки.")
        return

    users = await DB.execute("SELECT id FROM users")
    success = 0
    for row in users:
        try:
            await safe_send(int(row["id"]), f"📢 Админ: {html.escape(data['text'])}")
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("Не удалось отправить рассылку пользователю %s", row["id"])

    broadcast_mode.pop(callback.from_user.id, None)
    await safe_edit(
        callback.message,
        f"✅ Рассылка завершена. Отправлено: {success}",
        reply_markup=main_menu(callback.from_user.id),
    )


@dp.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback: types.CallbackQuery) -> None:
    await answer_callback(callback)
    broadcast_mode.pop(callback.from_user.id, None)
    await safe_edit(
        callback.message,
        "❌ Рассылка отменена.",
        reply_markup=main_menu(callback.from_user.id),
    )


def handle_loop_exception(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
    exc = context.get("exception")
    msg = context.get("message")
    if exc:
        text = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    else:
        text = str(msg)
    logger.error("Unhandled event loop error: %s", text)
    try:
        asyncio.create_task(notify_admins(f"⚠️ Ошибка event loop:\n{text[:3500]}"))
    except Exception:
        logger.exception("Не удалось поставить задачу уведомления админов")


async def main() -> None:
    await DB.connect()
    await create_aio_session()

    loop = asyncio.get_running_loop()
    loop.set_exception_handler(handle_loop_exception)

    def custom_excepthook(exc_type, exc, tb) -> None:
        text = "".join(traceback.format_exception(exc_type, exc, tb))
        logger.error("Uncaught exception: %s", text)
        try:
            asyncio.create_task(notify_admins(f"⚠️ Uncaught exception:\n{text[:3500]}"))
        except Exception:
            logger.exception("Не удалось уведомить админов об uncaught exception")

    sys.excepthook = custom_excepthook

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await DB.close()
        await close_aio_session()
        await bot.session.close()


def run_forever_with_restart() -> None:
    backoff = 5
    while True:
        try:
            asyncio.run(main())
            break
        except KeyboardInterrupt:
            logger.info("Бот остановлен вручную")
            break
        except Exception as exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.error("Бот упал и будет перезапущен: %s", exc)
            try:
                asyncio.run(notify_admins(f"⚠️ Бот упал и перезапускается:\n{tb[:3500]}"))
            except Exception:
                logger.exception("Не удалось уведомить админов о перезапуске")
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)


if __name__ == "__main__":
    run_forever_with_restart()
