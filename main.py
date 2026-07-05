import configparser
import json
import os
import sqlite3
import gc
import sys
import signal
try:
    import resource
except ImportError:
    resource = None
from pydantic import BaseModel
import asyncio
from typing import Union, Any
import logging
from aiogram import Router, Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.exceptions import TelegramRetryAfter
from html import escape
from datetime import datetime, timezone, timedelta
import pytz
from contextlib import contextmanager
from aiohttp import web
from cryptography.fernet import Fernet

try:
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool
    from psycopg2.extras import RealDictCursor
    psycopg2_available = True
except ImportError:
    psycopg2_available = False

config = configparser.ConfigParser()
# Make config loading robust relative to script location
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
config.read(config_path)

# Encryption Key setup
ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY")
if not ENCRYPTION_KEY:
    ENCRYPTION_KEY = config.get("settings", "ENCRYPTION_KEY", fallback="")

if not ENCRYPTION_KEY:
    raise ValueError("ENCRYPTION_KEY is not configured! Please generate a key with generate_key.py and set it in config.ini or environment variables.")

# Validate key format strictly
try:
    Fernet(ENCRYPTION_KEY.encode())
except Exception as e:
    raise ValueError(f"Invalid ENCRYPTION_KEY format: {e}. The key must be a valid 32-byte url-safe base64-encoded key.")

cipher_suite = Fernet(ENCRYPTION_KEY.encode())

MEMORY_GC_THRESHOLD_MB = int(os.getenv("MEMORY_GC_THRESHOLD_MB", "150"))
MEMORY_KILL_THRESHOLD_MB = int(os.getenv("MEMORY_KILL_THRESHOLD_MB", "400"))
CLEANUP_DAYS = int(os.getenv("CLEANUP_DAYS", "30"))
SELF_PING_INTERVAL_SEC = int(os.getenv("SELF_PING_INTERVAL_SEC", "300"))
RACE_CONDITION_RETRY_ATTEMPTS = int(os.getenv("RACE_CONDITION_RETRY_ATTEMPTS", "10"))
RACE_CONDITION_RETRY_DELAY = float(os.getenv("RACE_CONDITION_RETRY_DELAY", "0.1"))


def encrypt_text(text: str) -> str:
    if not text:
        return text
    return cipher_suite.encrypt(text.encode()).decode()


def decrypt_text(encrypted_text: str) -> str:
    if not encrypted_text:
        return encrypted_text
    try:
        return cipher_suite.decrypt(encrypted_text.encode()).decode()
    except Exception as e:
        logging.error(f"Failed to decrypt message: {e}")
        return "[Зашифрованное сообщение - Ошибка дешифрования]"


async def safe_api_call(func, *args, **kwargs):
    """Обертка для безопасного вызова методов API Telegram с защитой от FloodWait"""
    try:
        return await func(*args, **kwargs)
    except TelegramRetryAfter as e:
        logger.warning(f"FloodWait hit: sleeping for {e.retry_after}s before retrying.")
        await asyncio.sleep(e.retry_after)
        return await func(*args, **kwargs)
    except Exception as e:
        logger.error(f"API call failed: {e}")
        raise


async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs):
    """Отправка сообщения с защитой от FloodWait"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except TelegramRetryAfter as e:
            if attempt < max_retries - 1:
                logger.warning(f"FloodWait {e.retry_after}s for chat {chat_id}. Retrying...")
                await asyncio.sleep(e.retry_after)
            else:
                raise
        except Exception as e:
            logger.error(f"Failed to send to {chat_id}: {e}")
            raise


SEND_METHODS = {
    "photo": lambda bot, chat_id, media, **kw: bot.send_photo(chat_id, photo=media, **kw),
    "video": lambda bot, chat_id, media, **kw: bot.send_video(chat_id, video=media, **kw),
    "voice": lambda bot, chat_id, media, **kw: bot.send_voice(chat_id, voice=media, **kw),
    "document": lambda bot, chat_id, media, **kw: bot.send_document(chat_id, document=media, **kw),
    "audio": lambda bot, chat_id, media, **kw: bot.send_audio(chat_id, audio=media, **kw),
    "animation": lambda bot, chat_id, media, **kw: bot.send_animation(chat_id, animation=media, **kw),
}

NO_CAPTION_TYPES = {"video_note", "sticker"}

TOKEN = os.environ.get("BOT_TOKEN")
if not TOKEN:
    TOKEN = config.get("settings", "TOKEN", fallback="YOUR_BOT_TOKEN_HERE")

user_id_env = os.environ.get("USER_ID")
if user_id_env:
    try:
        USER_ID = int(user_id_env)
    except ValueError as e:
        raise ValueError(f"USER_ID environment variable must be a valid integer, got '{user_id_env}'") from e
else:
    try:
        USER_ID = config.getint("settings", "USER_ID")
    except (configparser.NoSectionError, configparser.NoOptionError, ValueError) as e:
        raise ValueError("USER_ID is not configured! Please set USER_ID in config.ini or environment variables.") from e

if USER_ID <= 0:
    raise ValueError(f"USER_ID must be a positive integer, got {USER_ID}")

TIMEZONE_NAME = os.environ.get("TIMEZONE_NAME")
if not TIMEZONE_NAME:
    TIMEZONE_NAME = config.get("settings", "TIMEZONE_NAME", fallback="Europe/Moscow")
timezone_local = pytz.timezone(TIMEZONE_NAME)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_URL = config.get("settings", "DATABASE_URL", fallback="")
PLACEHOLDER = "%s" if DATABASE_URL else "?"

db_pool = None

DELETED_MESSAGE_FORMAT = (
    "🗑 <b>Удалено {media_name} от {user_fullname_escaped}</b> (ID: <code>{user_id}</code>)\n"
    "⏰ <b>Время отправки:</b> {timestamp}\n\n"
    "💬 <b>Содержание:</b>\n"
    "<blockquote>{old_text}</blockquote>"
)

DELETED_MESSAGE_NO_CONTENT_FORMAT = (
    "🗑 <b>Удалено {media_name} от {user_fullname_escaped}</b> (ID: <code>{user_id}</code>)\n"
    "⏰ <b>Время отправки:</b> {timestamp}"
)

EDITED_MESSAGE_FORMAT = (
    "📝 <b>Изменено {media_name} от {user_fullname_escaped}</b> (ID: <code>{user_id}</code>)\n"
    "⏰ <b>Время отправки:</b> {timestamp}\n\n"
    "<b>Было:</b>\n"
    "<blockquote>{old_text}</blockquote>\n\n"
    "<b>Стало:</b>\n"
    "<blockquote>{new_text}</blockquote>"
)


router = Router(name=__name__)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, force=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def get_db_cursor(conn):
    if DATABASE_URL:
        return conn.cursor(cursor_factory=RealDictCursor)
    else:
        conn.row_factory = sqlite3.Row
        return conn.cursor()


@contextmanager
def db_session():
    if DATABASE_URL:
        if not psycopg2_available:
            raise RuntimeError("psycopg2 is not installed but DATABASE_URL is set.")
        if not db_pool:
            raise RuntimeError("Database connection pool is not initialized.")
        conn = db_pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            db_pool.putconn(conn)
    else:
        db_path = "messages.db"
        if os.path.basename(BASE_DIR) == "test":
            db_path = os.path.join(os.path.dirname(BASE_DIR), "messages.db")
        conn = sqlite3.connect(db_path)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


# Whitelist of allowed column names to prevent SQL Injection
ALLOWED_COLUMNS = {
    "user_id", "message_id", "message_text", "timestamp", "media_type", "file_id",
    "connection_id", "key", "value", "notify_updates", "notify_startup", "delete_reply", "forward_info"
}


def update_format(sql, parameters: dict) -> tuple[str, list]:
    validated = []
    for key, value in parameters.items():
        if key not in ALLOWED_COLUMNS:
            raise ValueError(f"Unsanitized database column name: {key}")
        validated.append((key, value))
    
    placeholder = config.PLACEHOLDER if ('config' in globals() and hasattr(config, 'PLACEHOLDER')) else PLACEHOLDER
    set_clause = ", ".join([f"{key} = {placeholder}" for key, _ in validated])
    sql += f" {set_clause}"
    return sql, [value for _, value in validated]


class MessageRecord(BaseModel):
    user_id: int
    message_id: int
    message_text: Union[str, None] = None
    timestamp: str
    media_type: str = "text"
    file_id: Union[str, None] = None
    forward_info: Union[str, None] = None


class MessageStore:
    storage_name = "messages"

    @staticmethod
    async def create_db():
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS messages
                                      (id SERIAL PRIMARY KEY,
                                       user_id BIGINT,
                                       message_id BIGINT,
                                       message_text TEXT,
                                       timestamp TEXT,
                                       media_type TEXT DEFAULT 'text',
                                       file_id TEXT,
                                       forward_info TEXT)''')
                else:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS messages
                                      (id INTEGER PRIMARY KEY,
                                       user_id INTEGER,
                                       message_id INTEGER,
                                       message_text TEXT,
                                       timestamp TEXT,
                                       media_type TEXT DEFAULT 'text',
                                       file_id TEXT,
                                       forward_info TEXT)''')
                    try:
                        cursor.execute("ALTER TABLE messages ADD COLUMN media_type TEXT DEFAULT 'text'")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE messages ADD COLUMN file_id TEXT")
                    except Exception:
                        pass
                try:
                    cursor.execute("ALTER TABLE messages ADD COLUMN forward_info TEXT")
                except Exception:
                    pass
        await asyncio.to_thread(_sync)

    @staticmethod
    async def add(user_id: int, message_id: int, message_text: Union[str, None], timestamp: str, media_type: str = "text", file_id: Union[str, None] = None, forward_info: Union[str, None] = None):
        encrypted_text = encrypt_text(message_text) if message_text else None
        def _sync():
            with db_session() as con:
                cursor = con.cursor()
                cursor.execute(
                    f"INSERT INTO messages (user_id, message_id, message_text, timestamp, media_type, file_id, forward_info) VALUES ({PLACEHOLDER}, {PLACEHOLDER}, {PLACEHOLDER}, {PLACEHOLDER}, {PLACEHOLDER}, {PLACEHOLDER}, {PLACEHOLDER})",
                    [user_id, message_id, encrypted_text, timestamp, media_type, file_id, forward_info],
                )
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get(user_id: int, message_id: int) -> Union[MessageRecord, None]:
        def _sync():
            with db_session() as con:
                cursor = get_db_cursor(con)
                sql = f"SELECT * FROM messages WHERE user_id = {PLACEHOLDER} AND message_id = {PLACEHOLDER}"
                cursor.execute(sql, [user_id, message_id])
                return cursor.fetchone()
        response = await asyncio.to_thread(_sync)
        if response is not None:
            response = dict(response)
            if response.get("message_text"):
                response["message_text"] = decrypt_text(response["message_text"])
            response = MessageRecord(**response)
        return response

    @staticmethod
    async def update(user_id: int, message_id: int, **kwargs):
        if "message_text" in kwargs and kwargs["message_text"]:
            kwargs["message_text"] = encrypt_text(kwargs["message_text"])
        def _sync():
            with db_session() as con:
                cursor = con.cursor()
                sql = f"UPDATE messages SET"
                sql, parameters = update_format(sql, kwargs)
                parameters.extend([user_id, message_id])
                cursor.execute(sql + f" WHERE user_id = {PLACEHOLDER} AND message_id = {PLACEHOLDER}", parameters)
        await asyncio.to_thread(_sync)

    @staticmethod
    async def delete(user_id: int, message_id: int):
        def _sync():
            with db_session() as con:
                cursor = con.cursor()
                sql = f"DELETE FROM messages WHERE user_id = {PLACEHOLDER} AND message_id = {PLACEHOLDER}"
                cursor.execute(sql, [user_id, message_id])
        await asyncio.to_thread(_sync)

    @staticmethod
    async def delete_old_messages(cutoff_timestamp: str):
        def _sync():
            with db_session() as con:
                cursor = get_db_cursor(con)
                cursor.execute(
                    f"SELECT file_id FROM messages WHERE timestamp < {PLACEHOLDER} AND file_id IS NOT NULL",
                    [cutoff_timestamp]
                )
                rows = cursor.fetchall()
                file_ids = [row['file_id'] for row in rows]
                
                cursor = con.cursor()
                sql = f"DELETE FROM messages WHERE timestamp < {PLACEHOLDER}"
                cursor.execute(sql, [cutoff_timestamp])
                return file_ids
                
        file_ids = await asyncio.to_thread(_sync)
        if file_ids:
            downloads_dir = os.path.join(BASE_DIR, "downloads")
            for fid in file_ids:
                local_path = os.path.join(downloads_dir, fid)
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                        logger.info(f"Cleanup: deleted local file {local_path}")
                except Exception as e:
                    logger.warning(f"Cleanup: failed to delete local file {local_path}: {e}")


class ConnectionsDB:
    storage_name = "connections"

    @staticmethod
    async def create_table():
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS connections
                                      (connection_id TEXT PRIMARY KEY, user_id BIGINT)''')
                else:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS connections
                                      (connection_id TEXT PRIMARY KEY, user_id INTEGER)''')
        await asyncio.to_thread(_sync)

    @staticmethod
    async def add_or_update(connection_id: str, user_id: int):
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute(
                        f"INSERT INTO connections (connection_id, user_id) VALUES (%s, %s) ON CONFLICT (connection_id) DO UPDATE SET user_id = EXCLUDED.user_id",
                        [connection_id, user_id]
                    )
                else:
                    cursor.execute(
                        f"INSERT OR REPLACE INTO connections (connection_id, user_id) VALUES (?, ?)",
                        [connection_id, user_id]
                    )
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get_user_id(connection_id: str) -> Union[int, None]:
        def _sync():
            with db_session() as conn:
                cursor = get_db_cursor(conn)
                cursor.execute(
                    f"SELECT user_id FROM connections WHERE connection_id = {PLACEHOLDER}",
                    [connection_id]
                )
                row = cursor.fetchone()
                return row['user_id'] if row else None
        return await asyncio.to_thread(_sync)

    @staticmethod
    async def delete(connection_id: str):
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    f"DELETE FROM connections WHERE connection_id = {PLACEHOLDER}",
                    [connection_id]
                )
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get_all_user_ids() -> list[int]:
        def _sync():
            with db_session() as conn:
                cursor = get_db_cursor(conn)
                cursor.execute("SELECT DISTINCT user_id FROM connections")
                rows = cursor.fetchall()
                return [row['user_id'] for row in rows]
        return await asyncio.to_thread(_sync)


class SystemStateDB:
    storage_name = "system_state"

    @staticmethod
    async def create_table():
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS system_state
                                      (key TEXT PRIMARY KEY, value TEXT)''')
                else:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS system_state
                                      (key TEXT PRIMARY KEY, value TEXT)''')
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get_value(key: str) -> Union[str, None]:
        def _sync():
            with db_session() as conn:
                cursor = get_db_cursor(conn)
                cursor.execute(
                    f"SELECT value FROM system_state WHERE key = {PLACEHOLDER}",
                    [key]
                )
                row = cursor.fetchone()
                return row['value'] if row else None
        return await asyncio.to_thread(_sync)

    @staticmethod
    async def set_value(key: str, value: str):
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute(
                        f"INSERT INTO system_state (key, value) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        [key, value]
                    )
                else:
                    cursor.execute(
                        f"INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)",
                        [key, value]
                    )
        await asyncio.to_thread(_sync)


class UserSettingsDB:
    storage_name = "user_settings"

    @staticmethod
    async def create_table():
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS user_settings
                                      (user_id BIGINT PRIMARY KEY,
                                       notify_updates INTEGER DEFAULT 1,
                                       notify_startup INTEGER DEFAULT 1,
                                       delete_reply INTEGER DEFAULT 1)''')
                else:
                    cursor.execute('''CREATE TABLE IF NOT EXISTS user_settings
                                      (user_id INTEGER PRIMARY KEY,
                                       notify_updates INTEGER DEFAULT 1,
                                       notify_startup INTEGER DEFAULT 1,
                                       delete_reply INTEGER DEFAULT 1)''')
                try:
                    cursor.execute("ALTER TABLE user_settings ADD COLUMN delete_reply INTEGER DEFAULT 1")
                except Exception:
                    pass
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get_settings(user_id: int) -> dict:
        def _sync():
            with db_session() as conn:
                cursor = get_db_cursor(conn)
                cursor.execute(f"SELECT * FROM user_settings WHERE user_id = {PLACEHOLDER}", [user_id])
                row = cursor.fetchone()
                if row:
                    res = dict(row)
                    if "delete_reply" not in res:
                        res["delete_reply"] = 1
                    return res
                else:
                    cursor = conn.cursor()
                    if DATABASE_URL:
                        cursor.execute(
                            "INSERT INTO user_settings (user_id, notify_updates, notify_startup, delete_reply) "
                            "VALUES (%s, 1, 1, 1) ON CONFLICT (user_id) DO NOTHING",
                            [user_id]
                        )
                    else:
                        cursor.execute(
                            "INSERT OR IGNORE INTO user_settings (user_id, notify_updates, notify_startup, delete_reply) "
                            "VALUES (?, 1, 1, 1)",
                            [user_id]
                        )
                    return {"user_id": user_id, "notify_updates": 1, "notify_startup": 1, "delete_reply": 1}
        return await asyncio.to_thread(_sync)

    @staticmethod
    async def set_setting(user_id: int, key: str, value: int):
        if key not in ALLOWED_COLUMNS:
            raise ValueError(f"Unsanitized database column: {key}")
        def _sync():
            with db_session() as conn:
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute(
                        f"INSERT INTO user_settings (user_id, {key}) VALUES (%s, %s) "
                        f"ON CONFLICT (user_id) DO UPDATE SET {key} = EXCLUDED.{key}",
                        [user_id, value]
                    )
                else:
                    cursor.execute(
                        f"INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)",
                        [user_id]
                    )
                    cursor.execute(
                        f"UPDATE user_settings SET {key} = ? WHERE user_id = ?",
                        [value, user_id]
                    )
        await asyncio.to_thread(_sync)

    @staticmethod
    async def get_all_user_ids() -> list[int]:
        def _sync():
            with db_session() as conn:
                cursor = get_db_cursor(conn)
                cursor.execute("SELECT user_id FROM user_settings")
                rows = cursor.fetchall()
                return [row['user_id'] for row in rows]
        return await asyncio.to_thread(_sync)


async def cleanup_old_messages():
    while True:
        now_local = datetime.now(timezone_local)
        next_run = now_local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        sleep_seconds = (next_run - now_local).total_seconds()
        await asyncio.sleep(sleep_seconds)
        cutoff_datetime = datetime.now(timezone.utc) - timedelta(days=CLEANUP_DAYS)
        cutoff_timestamp_iso = cutoff_datetime.isoformat()
        await MessageStore.delete_old_messages(cutoff_timestamp_iso)


BOT_VERSION = "1.9.3"
CHANGELOG_TEXT = (
    f"📢 <b>Обновление бота (v{BOT_VERSION}):</b>\n\n"
    "• <b>Логирование пересланных сообщений (Forward Info):</b> Бот теперь запоминает и детально отображает информацию об исходном отправителе пересланного сообщения (включая имя, юзернейм и ID) при его изменении или удалении."
)


async def monitor_memory():
    while True:
        await asyncio.sleep(300)  # Check every 5 minutes
        if not resource:
            continue
        try:
            usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform == "darwin":  # macOS (bytes)
                usage_mb = usage_kb / 1024.0 / 1024.0
            else:  # Linux (kilobytes)
                usage_mb = usage_kb / 1024.0
            
            logger.info(f"Current memory usage: {usage_mb:.2f} MB")
            
            # If memory exceeds threshold, run GC
            if usage_mb > MEMORY_GC_THRESHOLD_MB:
                collected = gc.collect()
                logger.info(f"Memory cleanup: current usage is {usage_mb:.2f} MB. gc.collect() cleared {collected} objects.")
                
            # If memory exceeds kill threshold, restart the bot gracefully
            if usage_mb > MEMORY_KILL_THRESHOLD_MB:
                logger.critical(f"Memory usage critical ({usage_mb:.2f} MB). Triggering graceful shutdown.")
                # Send SIGTERM to self. The Dispatcher polling loop catches this signal and terminates gracefully.
                os.kill(os.getpid(), signal.SIGTERM)
        except Exception as e:
            logger.error(f"Error during memory monitoring: {e}")


async def update_last_active():
    while True:
        try:
            now_iso = datetime.now(timezone.utc).isoformat()
            await SystemStateDB.set_value("last_active_timestamp", now_iso)
        except Exception as e:
            logger.warning(f"Failed to update last active timestamp: {e}")
        await asyncio.sleep(60)


async def check_and_broadcast_changelog(bot: Bot):
    # 1. Determine startup state (is_restart)
    last_active_str = await SystemStateDB.get_value("last_active_timestamp")
    is_restart = False
    if last_active_str:
        try:
            last_active_dt = datetime.fromisoformat(last_active_str)
            now_utc = datetime.now(timezone.utc)
            if now_utc - last_active_dt < timedelta(minutes=15):
                is_restart = True
        except Exception:
            pass

    # 2. Send startup notification to admin
    admin_settings = await UserSettingsDB.get_settings(USER_ID)
    if admin_settings.get("notify_startup", 1) == 1:
        try:
            status_text = "перезапущен" if is_restart else "запущен"
            await safe_send(bot, USER_ID, f"🤖 Бот успешно {status_text}!")
        except Exception as e:
            logger.warning(f"Failed to send startup notification to admin: {e}")

    # 3. If it was a cold start (downtime > 15 mins), notify regular users
    if not is_restart:
        logger.info("Cold start detected. Notifying users about bot recovery...")
        user_ids = await UserSettingsDB.get_all_user_ids()
            
        if user_ids:
            for uid in user_ids:
                if uid == USER_ID:
                    continue  # Admin already got the admin notification
                user_settings = await UserSettingsDB.get_settings(uid)
                if user_settings.get("notify_startup", 1) == 1:
                    try:
                        await safe_send(bot, uid, "🤖 Бот снова в сети и готов к работе после технического перерыва!")
                        await asyncio.sleep(0.05)
                    except Exception as e:
                        logger.warning(f"Failed to send recovery notification to {uid}: {e}")

    # 4. Check and broadcast changelog (Disabled by user request)
    # try:
    #     last_broadcasted = await SystemStateDB.get_value("last_broadcasted_version")
    #     if last_broadcasted != BOT_VERSION:
    #         logger.info(f"Broadcasting changelog for version {BOT_VERSION}...")
    #         
    #         user_ids = await UserSettingsDB.get_all_user_ids()
    #             
    #         if user_ids:
    #             success_count = 0
    #             for uid in user_ids:
    #                 user_settings = await UserSettingsDB.get_settings(uid)
    #                 if user_settings.get("notify_updates", 1) == 1:
    #                     try:
    #                         await bot.send_message(uid, CHANGELOG_TEXT, parse_mode="html")
    #                         success_count += 1
    #                         await asyncio.sleep(0.05)
    #                     except Exception as e:
    #                         logger.warning(f"Failed to send changelog to {uid}: {e}")
    #             logger.info(f"Changelog broadcasted to {success_count}/{len(user_ids)} users.")
    #         
    #         # Save that we have broadcasted this version
    #         await SystemStateDB.set_value("last_broadcasted_version", BOT_VERSION)
    # except Exception as e:
    #     logger.error(f"Error during changelog check/broadcast: {e}")
    pass


MEDIA_NAMES = {
    "text": "сообщение",
    "photo": "фото",
    "video": "видео",
    "voice": "голосовое сообщение (ГС)",
    "video_note": "видео-сообщение (кружок)",
    "document": "документ",
    "audio": "аудио",
    "sticker": "стикер",
    "animation": "анимация (GIF)"
}


async def _send_media_with_fallback(
    bot: Bot,
    recipient_id: int,
    media_type: str,
    media_val: Union[types.FSInputFile, str],
    msg: str,
    original_file_id: Union[str, None] = None
):
    try:
        if media_type in NO_CAPTION_TYPES:
            await safe_send(bot, recipient_id, msg, parse_mode='html')
            if media_type == "video_note":
                await bot.send_video_note(recipient_id, video_note=media_val)
            elif media_type == "sticker":
                await bot.send_sticker(recipient_id, sticker=media_val)
        else:
            send_method = SEND_METHODS.get(media_type)
            if send_method:
                await send_method(bot, recipient_id, media_val, caption=msg, parse_mode='html')
            else:
                await safe_send(bot, recipient_id, msg, parse_mode='html')
    except Exception as e:
        logger.error(f"Failed to send media with caption: {e}")
        # Fallback - отправляем отдельно текст, затем медиа (используя original_file_id как резерв)
        await safe_send(bot, recipient_id, msg, parse_mode='html')
        fallback_val = original_file_id if original_file_id else media_val
        if media_type in NO_CAPTION_TYPES:
            try:
                if media_type == "video_note":
                    await bot.send_video_note(recipient_id, video_note=fallback_val)
                elif media_type == "sticker":
                    await bot.send_sticker(recipient_id, sticker=fallback_val)
            except Exception as e2:
                logger.error(f"Fallback no-caption media sending failed: {e2}")
        else:
            send_method = SEND_METHODS.get(media_type)
            if send_method:
                try:
                    await send_method(bot, recipient_id, fallback_val)
                except Exception as e2:
                    logger.error(f"Fallback media sending failed: {e2}")


async def send_msg(
    message_old: str,
    message_new: Union[str, None],
    user_fullname: str,
    user_id: int,
    timestamp: str,
    recipient_id: int,
    bot: Bot,
    username: Union[str, None] = None,
    media_type: str = "text",
    file_id: Union[str, None] = None,
    forward_info: Union[str, None] = None
):
    if username:
        user_display = f"{user_fullname} (@{username})"
    else:
        user_display = user_fullname
    user_fullname_escaped = escape(user_display)
    
    media_name = MEDIA_NAMES.get(media_type, "сообщение")
    
    if message_old:
        old_text_escaped = escape(message_old)
    else:
        old_text_escaped = "<i>(без описания/текста)</i>"
            
    # Resolve local path if downloaded
    local_path = None
    if file_id:
        downloads_dir = os.path.join(BASE_DIR, "downloads")
        path_candidate = os.path.join(downloads_dir, file_id)
        if os.path.exists(path_candidate):
            local_path = path_candidate

    media_val = types.FSInputFile(local_path) if local_path else file_id

    if message_new is None:
        if media_type != "text" and not message_old:
            msg = DELETED_MESSAGE_NO_CONTENT_FORMAT.format(
                media_name=media_name,
                user_fullname_escaped=user_fullname_escaped,
                user_id=user_id,
                timestamp=timestamp
            )
        else:
            msg = DELETED_MESSAGE_FORMAT.format(
                media_name=media_name,
                user_fullname_escaped=user_fullname_escaped,
                user_id=user_id,
                timestamp=timestamp,
                old_text=old_text_escaped
            )
    else:
        new_text_escaped = escape(message_new) if message_new else "<i>(без описания/текста)</i>"
        msg = EDITED_MESSAGE_FORMAT.format(
            media_name=media_name,
            user_fullname_escaped=user_fullname_escaped,
            user_id=user_id,
            timestamp=timestamp,
            old_text=old_text_escaped,
            new_text=new_text_escaped
        )

    forward_data = json.loads(forward_info) if forward_info else None
    if forward_data:
        sender_name = forward_data['sender_name']
        username = forward_data.get('username')
        sender_id = forward_data.get('sender_id')
        
        display = escape(sender_name)
        if username:
            display += f" (@{escape(username)})"
        if sender_id:
            display += f" (ID: <code>{sender_id}</code>)"
            
        msg += f"\n📤 <b>Переслано от:</b> {display}"

    if media_type != "text" and file_id:
        await _send_media_with_fallback(bot, recipient_id, media_type, media_val, msg, original_file_id=file_id)
    else:
        await safe_send(bot, recipient_id, msg, parse_mode='html')


@router.business_connection()
async def business_connection_handler(con: types.BusinessConnection, bot: Bot):
    if con.is_enabled:
        await ConnectionsDB.add_or_update(con.id, con.user_chat_id)
        logger.info(f"Business connection established: ID={con.id}, User Chat ID={con.user_chat_id}")
        try:
            await bot.send_message(
                chat_id=con.user_chat_id,
                text="🔌 <b>Бот успешно подключен!</b>\n\nТеперь измененные и удаленные сообщения в ваших чатах будут дублироваться сюда.",
                parse_mode="html"
            )
        except Exception as e:
            logger.warning(f"Failed to send connection notification to user {con.user_chat_id}: {e}")
    else:
        await ConnectionsDB.delete(con.id)
        logger.info(f"Business connection removed: ID={con.id}, User Chat ID={con.user_chat_id}")
        try:
            await bot.send_message(
                chat_id=con.user_chat_id,
                text="🔌 <b>Бот отключен от вашего аккаунта.</b>\n\nУведомления больше приходить не будут.",
                parse_mode="html"
            )
        except Exception as e:
            logger.warning(f"Failed to send disconnection notification to user {con.user_chat_id}: {e}")


@router.message(Command(commands=["broadcast", "sendall"]))
async def broadcast_command(message: types.Message, bot: Bot):
    if message.from_user.id != USER_ID:
        return

    # Check if this is a reply to a message
    reply = message.reply_to_message

    # Extract text if not a reply
    broadcast_text = None
    if not reply:
        args = message.text.split(maxsplit=1)
        if len(args) < 2:
            await message.answer(
                "📢 <b>Рассылка объявлений:</b>\n\n"
                "1. Отправьте команду <code>/broadcast Текст</code> (для обычного текста).\n"
                "2. Или сделайте <b>Ответ (Reply)</b> на любое сообщение (картинку, видео, кружок, текст) с текстом <code>/broadcast</code>.",
                parse_mode="html"
            )
            return
        broadcast_text = args[1]

    # Fetch all unique user IDs from user_settings table
    user_ids = await UserSettingsDB.get_all_user_ids()

    if not user_ids:
        await message.answer("В базе данных нет зарегистрированных пользователей для рассылки.")
        return

    status_msg = await message.answer(f"Начинаю рассылку для {len(user_ids)} пользователей...")

    success_count = 0
    fail_count = 0

    for uid in user_ids:
        try:
            if reply:
                # Copy the original message exactly as it is (media, formatting, etc.)
                await bot.copy_message(
                    chat_id=uid,
                    from_chat_id=message.chat.id,
                    message_id=reply.message_id
                )
            else:
                await safe_send(bot, uid, broadcast_text)
            success_count += 1
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as e:
            logger.warning(f"FloodWait hit during broadcast: sleeping for {e.retry_after}s")
            await asyncio.sleep(e.retry_after)
            try:
                if reply:
                    await bot.copy_message(
                        chat_id=uid,
                        from_chat_id=message.chat.id,
                        message_id=reply.message_id
                    )
                else:
                    await safe_send(bot, uid, broadcast_text)
                success_count += 1
                await asyncio.sleep(0.05)
            except Exception as retry_err:
                logger.warning(f"Failed to send broadcast to {uid} after retry: {retry_err}")
                fail_count += 1
        except Exception as e:
            logger.warning(f"Failed to send broadcast to {uid}: {e}")
            fail_count += 1

    await status_msg.edit_text(
        f"📢 <b>Рассылка завершена!</b>\n\n"
        f"✅ Успешно отправлено: <code>{success_count}</code>\n"
        f"❌ Ошибок (заблокировали бота): <code>{fail_count}</code>",
        parse_mode="html"
    )


def get_settings_keyboard(user_id: int, settings: dict) -> types.InlineKeyboardMarkup:
    buttons = []
    
    # 1. Updates notification toggle
    updates_val = settings.get("notify_updates", 1)
    updates_icon = "🔔 Вкл" if updates_val == 1 else "🔕 Выкл"
    buttons.append([
        types.InlineKeyboardButton(
            text=f"Оповещения об обновлениях: {updates_icon}",
            callback_data="toggle_notify_updates"
        )
    ])
    
    # 2. Startup/restart notification toggle (for all users)
    startup_val = settings.get("notify_startup", 1)
    startup_icon = "🔔 Вкл" if startup_val == 1 else "🔕 Выкл"
    label = "Запуск/перезапуск бота" if user_id == USER_ID else "Оповещения о работе бота"
    buttons.append([
        types.InlineKeyboardButton(
            text=f"{label}: {startup_icon}",
            callback_data="toggle_notify_startup"
        )
    ])
    
    # 3. Delete dot-reply toggle
    delete_reply_val = settings.get("delete_reply", 1)
    delete_reply_icon = "🗑 Удалять" if delete_reply_val == 1 else "💾 Оставлять"
    buttons.append([
        types.InlineKeyboardButton(
            text=f"Удаление точки на реплай: {delete_reply_icon}",
            callback_data="toggle_delete_reply"
        )
    ])
        
    return types.InlineKeyboardMarkup(inline_keyboard=buttons)


@router.message(Command(commands=["start"]))
async def start_command(message: types.Message):
    kb = [
        [types.KeyboardButton(text="🪞 Создать зеркало"), types.KeyboardButton(text="🛡️ Безопасность и хостинг")],
        [types.KeyboardButton(text="⚙️ Настройки")]
    ]
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=kb,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    start_text = (
        "Free spy — бесплатная опенсурс замена Dialog spy bot. На стадии тестирования.\n\n"
        "<b>Возможности бота:</b>\n"
        "• Моментально пришлет уведомление, если собеседник изменит или удалит сообщение (включая текст, медиа, кружки и ГС).\n"
        "• Сохраняет одноразовые (самоудаляющиеся) фото и видео — достаточно ответить на них в чате точкой или эмодзи.\n"
        "• Все данные шифруются (AES-128). Доступ к вашим перепискам есть только у вас.\n"
        "• Настройка оповещений об обновлениях бота и статусе его работы.\n\n"
        "Исходный код проекта доступен на <a href='https://github.com/Claxy-mod/Free-spy'>GitHub</a>."
    )
    # Register user in settings DB on start
    await UserSettingsDB.get_settings(message.from_user.id)
    await message.answer(
        start_text,
        parse_mode='html',
        reply_markup=keyboard,
        disable_web_page_preview=True
    )


@router.message(F.text == "⚙️ Настройки")
async def settings_menu_handler(message: types.Message):
    settings = await UserSettingsDB.get_settings(message.from_user.id)
    keyboard = get_settings_keyboard(message.from_user.id, settings)
    await message.answer("⚙️ <b>Настройки оповещений</b>", parse_mode="html", reply_markup=keyboard)


@router.callback_query(F.data.startswith("toggle_"))
async def toggle_notification_callback(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    action = callback_query.data
    
    settings = await UserSettingsDB.get_settings(user_id)
    
    if action == "toggle_notify_updates":
        current_val = settings.get("notify_updates", 1)
        new_val = 0 if current_val == 1 else 1
        await UserSettingsDB.set_setting(user_id, "notify_updates", new_val)
        settings["notify_updates"] = new_val
    elif action == "toggle_notify_startup":
        current_val = settings.get("notify_startup", 1)
        new_val = 0 if current_val == 1 else 1
        await UserSettingsDB.set_setting(user_id, "notify_startup", new_val)
        settings["notify_startup"] = new_val
    elif action == "toggle_delete_reply":
        current_val = settings.get("delete_reply", 1)
        new_val = 0 if current_val == 1 else 1
        await UserSettingsDB.set_setting(user_id, "delete_reply", new_val)
        settings["delete_reply"] = new_val
        
    # Edit message to update keyboard
    keyboard = get_settings_keyboard(user_id, settings)
    try:
        await callback_query.message.edit_reply_markup(reply_markup=keyboard)
    except Exception:
        pass
    await callback_query.answer("Настройки обновлены!")


@router.message(F.text == "🛡️ Безопасность и хостинг")
async def security_info_handler(message: types.Message):
    info_text = (
        "🛡️ <b>Вопросы безопасности и конфиденциальности</b>\n\n"
        "Мы заботимся о вашей приватности и предлагаем максимальную прозрачность:\n\n"
        "1. <b>Сквозное шифрование данных:</b>\n"
        "Все перехваченные сообщения шифруются на лету алгоритмом AES-128 (Fernet) перед сохранением в базу данных. "
        "Ключ дешифрования хранится исключительно в переменных окружения работающего сервера. "
        "Ни хостинг базы данных (Supabase), ни разработчики не могут прочесть ваши сообщения.\n\n"
        "2. <b>Изоляция пользователей:</b>\n"
        "Ваши сообщения доставляются строго на ваш аккаунт Telegram. "
        "Перехват чужих переписок другими пользователями физически невозможен.\n\n"
        "3. <b>Не доверяете нашему серверу?</b>\n"
        "Если вы не хотите доверять свои переписки нашему публичному серверу, вы можете развернуть "
        "<b>собственную независимую копию бота абсолютно бесплатно</b> за 5 минут!\n\n"
        "Код проекта полностью открыт. Вы можете проверить каждую строчку кода, "
        "настроить свой сервер и свою базу данных, чтобы иметь 100% контроля над данными.\n\n"
        "💻 <b>Исходный код и инструкция по запуску:</b>\n"
        "https://github.com/Claxy-mod/Free-spy"
    )
    await message.answer(info_text, parse_mode="html", disable_web_page_preview=True)


@router.message(F.text == "🪞 Создать зеркало")
async def create_mirror_handler(message: types.Message):
    mirror_text = (
        "🪞 <b>Создание личного зеркала бота</b>\n\n"
        "Вы можете развернуть собственную независимую копию бота на бесплатных серверах Render. "
        "Вся переписка будет храниться <b>только в вашей личной базе данных</b> и аппаратно шифроваться. "
        "Если при установке вы укажете свой USER_ID, бот будет работать <b>исключительно у вас</b> (другие люди не смогут им воспользоваться).\n\n"
        "<b>Шаги для запуска (займет 2 минуты):</b>\n"
        "1. Перейдите в @BotFather и создайте нового бота (команда <code>/newbot</code>).\n"
        "2. Скопируйте полученный токен.\n"
        "3. Нажмите на кнопку ниже, чтобы открыть страницу автоматического создания сервера.\n"
        "4. Зарегистрируйтесь на Render (если нет аккаунта) и вставьте токен в поле <code>BOT_TOKEN</code>.\n"
        f"5. В поле <code>USER_ID</code> укажите ваш ID: <code>{message.from_user.id}</code>\n\n"
        "✨ <b>Всё остальное</b> (PostgreSQL, генерация ключей, запуск кода) <b>настроится полностью автоматически!</b>"
    )
    
    inline_kb = types.InlineKeyboardMarkup(
        inline_keyboard=[
            [types.InlineKeyboardButton(text="🚀 Развернуть на Render (Бесплатно)", url="https://render.com/deploy?repo=https://github.com/Claxy-mod/Free-spy")]
        ]
    )
    
    await message.answer(mirror_text, parse_mode="html", reply_markup=inline_kb)


# Startup missed updates buffering system
IS_BOOTING = True
MISSED_UPDATES_BUFFER = []
PENDING_STARTUP_MEDIA = {}
LAST_STARTUP_UPDATE_TIME = 0.0


def safe_escape(text: Union[str, None]) -> str:
    if not text:
        return "<i>(без описания/текста)</i>"
    return escape(str(text))


async def process_startup_digest(bot: Bot):
    global IS_BOOTING
    # Wait at least 3 seconds, and then wait until no updates have been received for 2 seconds
    await asyncio.sleep(3)
    while True:
        now = asyncio.get_event_loop().time()
        silence_duration = now - LAST_STARTUP_UPDATE_TIME
        if silence_duration >= 2.0:
            break
        await asyncio.sleep(0.5)
    IS_BOOTING = False
    logger.info(f"Startup buffering complete. Processing {len(MISSED_UPDATES_BUFFER)} missed updates...")
    
    # Group updates by recipient_id
    grouped = {}
    for item in MISSED_UPDATES_BUFFER:
        r_id = item["recipient_id"]
        if r_id not in grouped:
            grouped[r_id] = []
        grouped[r_id].append(item)
        
    for r_id, items in grouped.items():
        text_lines = []
        media_items = []
        
        for item in items:
            user_display = item["user_fullname"]
            if item.get("username"):
                user_display += f" (@{item['username']})"
            user_display = escape(user_display)
            user_id = item["user_id"]
            timestamp = item["timestamp"]
            media_type = item["media_type"]
            media_name = MEDIA_NAMES.get(media_type, "сообщение")
            
            forward_data = json.loads(item["forward_info"]) if item.get("forward_info") else None
            if forward_data:
                sender_name = forward_data['sender_name']
                username = forward_data.get('username')
                sender_id = forward_data.get('sender_id')
                
                display = escape(sender_name)
                if username:
                    display += f" (@{escape(username)})"
                if sender_id:
                    display += f" (ID: <code>{sender_id}</code>)"
                forward_part = f"\n📤 <b>Переслано от:</b> {display}"
            else:
                forward_part = ""
            
            if item["type"] == "delete":
                if media_type == "text":
                    text_lines.append(
                        f"🗑 <b>Удалено сообщение</b> от {user_display} (ID: <code>{user_id}</code>) в {timestamp}:{forward_part}\n"
                        f"<blockquote>{safe_escape(item['old_text'])}</blockquote>"
                    )
                else:
                    caption_part = ""
                    if item.get("old_text") and item["old_text"] != "<i>(без описания/текста)</i>":
                        caption_part = f"\n<blockquote>Описание: {escape(item['old_text'])}</blockquote>"
                    text_lines.append(
                        f"🗑 <b>Удалено {media_name}</b> от {user_display} (ID: <code>{user_id}</code>) в {timestamp}{forward_part}{caption_part}"
                    )
                    if item.get("file_id"):
                        media_items.append(item)
            elif item["type"] == "edit":
                if media_type == "text":
                    text_lines.append(
                        f"📝 <b>Изменено сообщение</b> от {user_display} (ID: <code>{user_id}</code>) в {timestamp}:{forward_part}\n"
                        f"<b>Было:</b> <blockquote>{safe_escape(item['old_text'])}</blockquote>\n"
                        f"<b>Стало:</b> <blockquote>{safe_escape(item['new_text'])}</blockquote>"
                    )
                else:
                    text_lines.append(
                        f"📝 <b>Изменено описание {media_name}</b> от {user_display} (ID: <code>{user_id}</code>) в {timestamp}:{forward_part}\n"
                        f"<b>Было:</b> <blockquote>{safe_escape(item['old_text'])}</blockquote>\n"
                        f"<b>Стало:</b> <blockquote>{safe_escape(item['new_text'])}</blockquote>"
                    )
                    if item.get("file_id"):
                        media_items.append(item)
                        
        if text_lines:
            header = "📋 <b>Отчет о сообщениях, пропущенных за время отсутствия сети:</b>\n\n"
            current_msg = header
            for line in text_lines:
                if len(current_msg) + len(line) + 2 > 4000:
                    try:
                        await bot.send_message(r_id, current_msg, parse_mode="html")
                    except Exception as e:
                        logger.error(f"Failed to send digest part to {r_id}: {e}")
                    current_msg = line + "\n\n"
                else:
                    current_msg += line + "\n\n"
            if current_msg:
                try:
                    await bot.send_message(r_id, current_msg, parse_mode="html")
                except Exception as e:
                    logger.error(f"Failed to send digest to {r_id}: {e}")
                    
        if media_items:
            PENDING_STARTUP_MEDIA[r_id] = media_items
            kb = [
                [
                    types.InlineKeyboardButton(text="📥 Да, прислать", callback_data="startup_media_yes"),
                    types.InlineKeyboardButton(text="❌ Нет, не нужно", callback_data="startup_media_no")
                ]
            ]
            keyboard = types.InlineKeyboardMarkup(inline_keyboard=kb)
            try:
                await bot.send_message(
                    r_id,
                    f"📷 За время отсутствия сети были удалены/изменены медиафайлы ({len(media_items)} шт.). Отправить их вам?",
                    reply_markup=keyboard
                )
            except Exception as e:
                logger.error(f"Failed to send startup media prompt to {r_id}: {e}")
                
    # Clear the buffer
    MISSED_UPDATES_BUFFER.clear()


@router.callback_query(F.data.startswith("startup_media_"))
async def startup_media_callback(callback_query: types.CallbackQuery, bot: Bot):
    user_id = callback_query.from_user.id
    action = callback_query.data
    
    if action == "startup_media_yes":
        media_items = PENDING_STARTUP_MEDIA.get(user_id, [])
        if not media_items:
            await callback_query.message.edit_text("Медиафайлы не найдены или уже были выгружены.")
            return
            
        await callback_query.message.edit_text(f"Начинаю отправку медиафайлов ({len(media_items)} шт.)...")
        
        for item in media_items:
            media_type = item["media_type"]
            file_id = item["file_id"]
            
            user_display = item["user_fullname"]
            if item.get("username"):
                user_display += f" (@{item['username']})"
            user_display_escaped = escape(user_display)
            timestamp = item["timestamp"]
            media_name = MEDIA_NAMES.get(media_type, "сообщение")
            
            if item["type"] == "delete":
                caption = f"💾 <b>Удаленное медиа ({media_name}) от {user_display_escaped}</b> в {timestamp}"
                if item.get("old_text") and item["old_text"] != "<i>(без описания/текста)</i>":
                    caption += f"\n\n💬 <b>Содержание:</b>\n<blockquote>{safe_escape(item['old_text'])}</blockquote>"
            else:
                caption = f"💾 <b>Измененное медиа ({media_name}) от {user_display_escaped}</b> в {timestamp}"
                if item.get("old_text") and item["old_text"] != "<i>(без описания/текста)</i>":
                    caption += f"\n\n<b>Было:</b>\n<blockquote>{safe_escape(item['old_text'])}</blockquote>"
                if item.get("new_text") and item["new_text"] != "<i>(без описания/текста)</i>":
                    caption += f"\n\n<b>Стало:</b>\n<blockquote>{safe_escape(item['new_text'])}</blockquote>"
                
            local_path = None
            downloads_dir = os.path.join(BASE_DIR, "downloads")
            path_candidate = os.path.join(downloads_dir, file_id)
            if os.path.exists(path_candidate):
                local_path = path_candidate
                
            media_val = types.FSInputFile(local_path) if local_path else file_id
            
            try:
                if media_type in NO_CAPTION_TYPES:
                    await safe_send(bot, user_id, caption, parse_mode='html')
                    if media_type == "video_note":
                        await bot.send_video_note(user_id, video_note=media_val)
                    elif media_type == "sticker":
                        await bot.send_sticker(user_id, sticker=media_val)
                else:
                    send_method = SEND_METHODS.get(media_type)
                    if send_method:
                        await send_method(bot, user_id, media_val, caption=caption, parse_mode='html')
                    else:
                        await safe_send(bot, user_id, caption, parse_mode='html')
                
                if local_path:
                    try:
                        os.remove(local_path)
                    except Exception:
                        pass
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.error(f"Failed to send media {file_id}: {e}")
                
        await callback_query.message.edit_text("Отправка медиафайлов завершена!")
        PENDING_STARTUP_MEDIA.pop(user_id, None)
        
    elif action == "startup_media_no":
        media_items = PENDING_STARTUP_MEDIA.get(user_id, [])
        for item in media_items:
            file_id = item.get("file_id")
            if file_id:
                downloads_dir = os.path.join(BASE_DIR, "downloads")
                local_path = os.path.join(downloads_dir, file_id)
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                except Exception:
                    pass
        PENDING_STARTUP_MEDIA.pop(user_id, None)
        await callback_query.message.edit_text("Медиафайлы удалены из буфера.")


@router.edited_business_message()
async def edited_business_message(message: types.Message):
    if message.from_user and message.from_user.id == message.chat.id:
        recipient_id = await ConnectionsDB.get_user_id(message.business_connection_id)
        if not recipient_id:
            return
        user_msg = await MessageStore.get(user_id=message.from_user.id, message_id=message.message_id)
        if not user_msg:
            # Retry to handle concurrent insert/edit race condition
            for _ in range(RACE_CONDITION_RETRY_ATTEMPTS):
                await asyncio.sleep(RACE_CONDITION_RETRY_DELAY)
                user_msg = await MessageStore.get(user_id=message.from_user.id, message_id=message.message_id)
                if user_msg:
                    break
        if user_msg:
            message_timestamp = datetime.fromisoformat(user_msg.timestamp).astimezone(timezone_local)
            timestamp_formatted = message_timestamp.strftime('%d/%m/%y %H:%M')
            if IS_BOOTING:
                MISSED_UPDATES_BUFFER.append({
                    "type": "edit",
                    "recipient_id": recipient_id,
                    "user_fullname": message.from_user.full_name,
                    "user_id": message.chat.id,
                    "username": message.from_user.username,
                    "timestamp": timestamp_formatted,
                    "media_type": user_msg.media_type,
                    "file_id": user_msg.file_id,
                    "old_text": user_msg.message_text,
                    "new_text": message.text or message.caption or "",
                    "forward_info": user_msg.forward_info
                })
            else:
                await send_msg(
                    message_old=user_msg.message_text,
                    message_new=message.text or message.caption or "",
                    user_fullname=message.from_user.full_name,
                    user_id=message.chat.id,
                    timestamp=timestamp_formatted,
                    recipient_id=recipient_id,
                    username=message.from_user.username,
                    media_type=user_msg.media_type,
                    file_id=user_msg.file_id,
                    bot=message.bot,
                    forward_info=user_msg.forward_info
                )
            await MessageStore.update(user_id=message.from_user.id, message_id=message.message_id, message_text=message.text or message.caption)


@router.deleted_business_messages()
async def deleted_business_messages(event: types.BusinessMessagesDeleted, bot: Bot):
    logger.info(f"Received deleted business messages event: Chat={event.chat.id}, IDs={event.message_ids}")
    user_id = event.chat.id
    user_fullname = event.chat.full_name
    username = event.chat.username
    recipient_id = await ConnectionsDB.get_user_id(event.business_connection_id)
    if not recipient_id:
        return
    for msg_id in event.message_ids:
        try:
            user_msg = await MessageStore.get(user_id=user_id, message_id=msg_id)
            if not user_msg:
                # Retry to handle concurrent insert/delete race condition
                for _ in range(RACE_CONDITION_RETRY_ATTEMPTS):
                    await asyncio.sleep(RACE_CONDITION_RETRY_DELAY)
                    user_msg = await MessageStore.get(user_id=user_id, message_id=msg_id)
                    if user_msg:
                        break
            if user_msg:
                message_timestamp = datetime.fromisoformat(user_msg.timestamp).astimezone(timezone_local)
                timestamp_formatted = message_timestamp.strftime('%d/%m/%y %H:%M')
                if IS_BOOTING:
                    MISSED_UPDATES_BUFFER.append({
                        "type": "delete",
                        "recipient_id": recipient_id,
                        "user_fullname": user_fullname,
                        "user_id": user_id,
                        "username": username,
                        "timestamp": timestamp_formatted,
                        "media_type": user_msg.media_type,
                        "file_id": user_msg.file_id,
                        "old_text": user_msg.message_text,
                        "forward_info": user_msg.forward_info
                    })
                else:
                    await send_msg(
                        message_old=user_msg.message_text,
                        message_new=None,
                        user_fullname=user_fullname,
                        user_id=user_id,
                        timestamp=timestamp_formatted,
                        recipient_id=recipient_id,
                        username=username,
                        media_type=user_msg.media_type,
                        file_id=user_msg.file_id,
                        bot=bot,
                        forward_info=user_msg.forward_info
                    )
                    if user_msg.file_id:
                        downloads_dir = os.path.join(BASE_DIR, "downloads")
                        local_path = os.path.join(downloads_dir, user_msg.file_id)
                        try:
                            if os.path.exists(local_path):
                                os.remove(local_path)
                                logger.info(f"Deleted local file after deletion: {local_path}")
                        except Exception as e:
                            logger.warning(f"Failed to delete local file {local_path}: {e}")
                await MessageStore.delete(user_id=user_id, message_id=msg_id)
        except Exception as e:
            logger.error(f"Error processing deleted message {msg_id} in business chat: {e}")


async def download_media(bot: Bot, file_id: str) -> Union[str, None]:
    try:
        file_info = await bot.get_file(file_id)
        downloads_dir = os.path.join(BASE_DIR, "downloads")
        os.makedirs(downloads_dir, exist_ok=True)
        local_path = os.path.join(downloads_dir, file_id)
        if os.path.exists(local_path):
            return local_path
        await bot.download_file(file_info.file_path, local_path)
        logger.info(f"Successfully downloaded media: {local_path}")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download media {file_id}: {e}")
        return None


@router.business_message()
async def business_message(message: types.Message):
    logger.info(f"Received business message: MsgID={message.message_id}, Chat={message.chat.id}, From={message.from_user.id if message.from_user else None}, ChatType={message.chat.type}")
    
    # Check if this is an outgoing message from the business user (i.e. reply to save media)
    is_outgoing = message.from_user and message.from_user.id != message.chat.id
    if is_outgoing:
        if message.reply_to_message:
            replied_msg = message.reply_to_message
            media_type = "text"
            file_id = None
            if replied_msg.photo:
                media_type = "photo"
                file_id = replied_msg.photo[-1].file_id
            elif replied_msg.video:
                media_type = "video"
                file_id = replied_msg.video.file_id
            elif replied_msg.voice:
                media_type = "voice"
                file_id = replied_msg.voice.file_id
            elif replied_msg.video_note:
                media_type = "video_note"
                file_id = replied_msg.video_note.file_id
            elif replied_msg.document:
                media_type = "document"
                file_id = replied_msg.document.file_id
            elif replied_msg.audio:
                media_type = "audio"
                file_id = replied_msg.audio.file_id
            elif replied_msg.sticker:
                media_type = "sticker"
                file_id = replied_msg.sticker.file_id
            elif replied_msg.animation:
                media_type = "animation"
                file_id = replied_msg.animation.file_id

            if file_id and replied_msg.has_protected_content:
                recipient_id = await ConnectionsDB.get_user_id(message.business_connection_id)
                if recipient_id:
                    logger.info(f"Replied-to message details: {replied_msg.model_dump()}")
                    logger.info(f"User replied to save media {file_id}. Downloading...")
                    local_path = await download_media(message.bot, file_id)
                    if local_path:
                        media_name = MEDIA_NAMES.get(media_type, "медиа")
                        caption = f"💾 <b>Сохранено {media_name}</b>"
                        if replied_msg.caption and replied_msg.caption.strip():
                            caption += f"\n\n💬 <b>Подпись:</b>\n<blockquote>{escape(replied_msg.caption)}</blockquote>"
                        media_val = types.FSInputFile(local_path)
                        try:
                            if media_type == "photo":
                                await message.bot.send_photo(recipient_id, photo=media_val, caption=caption, parse_mode='html')
                            elif media_type == "video":
                                await message.bot.send_video(recipient_id, video=media_val, caption=caption, parse_mode='html')
                            elif media_type == "voice":
                                await message.bot.send_voice(recipient_id, voice=media_val, caption=caption, parse_mode='html')
                            elif media_type == "video_note":
                                await message.bot.send_message(recipient_id, caption, parse_mode='html')
                                await message.bot.send_video_note(recipient_id, video_note=media_val)
                            elif media_type == "document":
                                await message.bot.send_document(recipient_id, document=media_val, caption=caption, parse_mode='html')
                            elif media_type == "audio":
                                await message.bot.send_audio(recipient_id, audio=media_val, caption=caption, parse_mode='html')
                            elif media_type == "sticker":
                                await message.bot.send_message(recipient_id, caption, parse_mode='html')
                                await message.bot.send_sticker(recipient_id, sticker=media_val)
                            elif media_type == "animation":
                                await message.bot.send_animation(recipient_id, animation=media_val, caption=caption, parse_mode='html')
                            logger.info("Successfully sent saved media to user.")
                        except Exception as e:
                            logger.error(f"Failed to send saved media: {e}")
                        finally:
                            try:
                                os.remove(local_path)
                            except Exception:
                                pass
                settings = await UserSettingsDB.get_settings(recipient_id or USER_ID)
                should_delete = settings.get("delete_reply", 1) == 1

                if message.text == "." and should_delete:
                    try:
                        await message.bot.delete_business_messages(
                            business_connection_id=message.business_connection_id,
                            message_ids=[message.message_id]
                        )
                        logger.info(f"Successfully deleted reply message {message.message_id} from chat.")
                    except Exception as del_err:
                        logger.error(f"Failed to delete reply message {message.message_id}: {del_err}")
                else:
                    logger.info(f"Reply message '{message.text}' was NOT deleted (should_delete={should_delete}).")
        return

    if message.from_user and message.from_user.id == message.chat.id:
        user_id = message.from_user.id
        
        media_type = "text"
        file_id = None
        message_text = message.text or message.caption or ""
        
        if message.photo:
            media_type = "photo"
            file_id = message.photo[-1].file_id
        elif message.video:
            media_type = "video"
            file_id = message.video.file_id
        elif message.voice:
            media_type = "voice"
            file_id = message.voice.file_id
        elif message.video_note:
            media_type = "video_note"
            file_id = message.video_note.file_id
        elif message.document:
            media_type = "document"
            file_id = message.document.file_id
        elif message.audio:
            media_type = "audio"
            file_id = message.audio.file_id
        elif message.sticker:
            media_type = "sticker"
            file_id = message.sticker.file_id
        elif message.animation:
            media_type = "animation"
            file_id = message.animation.file_id
            
        if file_id and media_type != "sticker":
            asyncio.create_task(download_media(message.bot, file_id))

        message_datetime_utc = message.date.replace(tzinfo=timezone.utc)
        timestamp_iso = message_datetime_utc.isoformat()
        
        forward_info = None
        if message.forward_origin:
            origin = message.forward_origin
            sender_id = None
            username = None
            if origin.type == "user":
                name = origin.sender_user.full_name
                sender_id = origin.sender_user.id
                username = origin.sender_user.username
            elif origin.type == "hidden_user":
                name = origin.sender_user_name
            elif origin.type == "chat":
                name = origin.sender_chat.title
                sender_id = origin.sender_chat.id
                username = origin.sender_chat.username
            elif origin.type == "channel":
                name = f"{origin.chat.title} (канал)"
                sender_id = origin.chat.id
                username = origin.chat.username
            else:
                name = "Неизвестный источник"
                
            forward_info = {
                "sender_name": name,
                "sender_id": sender_id,
                "username": username,
                "date": origin.date.isoformat() if hasattr(origin, "date") else None
            }
            
        await MessageStore.add(
            user_id=user_id,
            message_id=message.message_id,
            message_text=message_text,
            timestamp=timestamp_iso,
            media_type=media_type,
            file_id=file_id,
            forward_info=json.dumps(forward_info) if forward_info else None
        )


async def handle(request):
    return web.Response(text="Bot is running!")


# Web server globals
web_runner = None
background_tasks = set()


async def start_web_server():
    global web_runner
    app = web.Application()
    app.router.add_get("/", handle)
    web_runner = web.AppRunner(app)
    await web_runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(web_runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server started on port {port}")


async def stop_web_server():
    global web_runner
    if web_runner:
        logger.info("Stopping web server gracefully...")
        await web_runner.cleanup()


async def self_ping():
    """Фоновая задача для предотвращения засыпания сервера на Render (авто-пинг)"""
    import aiohttp
    
    external_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not external_url:
        return
        
    while True:
        try:
            # Ping to keep Render instance awake
            await asyncio.sleep(SELF_PING_INTERVAL_SEC)
            async with aiohttp.ClientSession() as session:
                async with session.get(external_url) as response:
                    logger.info(f"Self-ping to {external_url}: {response.status}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Self-ping error: {e}")


async def on_startup(bot: Bot):
    global LAST_STARTUP_UPDATE_TIME
    LAST_STARTUP_UPDATE_TIME = asyncio.get_event_loop().time()
    
    logger.info("Running database migrations/initialization...")
    # Initialize DB tables asynchronously
    await MessageStore.create_db()
    await ConnectionsDB.create_table()
    await SystemStateDB.create_table()
    await UserSettingsDB.create_table()

    logger.info("Starting background tasks...")
    # Startup digest task
    task_digest = asyncio.create_task(process_startup_digest(bot))
    background_tasks.add(task_digest)
    task_digest.add_done_callback(background_tasks.discard)
    # Startup notification and changelog
    task_changelog = asyncio.create_task(check_and_broadcast_changelog(bot))
    background_tasks.add(task_changelog)
    task_changelog.add_done_callback(background_tasks.discard)

    # Active timestamp update
    task_active = asyncio.create_task(update_last_active())
    background_tasks.add(task_active)
    task_active.add_done_callback(background_tasks.discard)

    # Memory monitor
    task_memory = asyncio.create_task(monitor_memory())
    background_tasks.add(task_memory)
    task_memory.add_done_callback(background_tasks.discard)
    
    # Message cleanup (30-day cutoff)
    task_cleanup = asyncio.create_task(cleanup_old_messages())
    background_tasks.add(task_cleanup)
    task_cleanup.add_done_callback(background_tasks.discard)

    # Self-ping for Render
    task_ping = asyncio.create_task(self_ping())
    background_tasks.add(task_ping)
    task_ping.add_done_callback(background_tasks.discard)

async def on_shutdown(bot: Bot):
    logger.info("Shutdown initiated. Canceling background tasks...")
    for task in background_tasks:
        task.cancel()
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    
    await stop_web_server()

    global db_pool
    if db_pool:
        logger.info("Closing PostgreSQL connection pool...")
        db_pool.closeall()
        db_pool = None

    logger.info("Graceful shutdown complete.")


from aiogram.types import Update

async def raw_update_middleware(handler, event: Update, data):
    global LAST_STARTUP_UPDATE_TIME
    if IS_BOOTING:
        LAST_STARTUP_UPDATE_TIME = asyncio.get_event_loop().time()
        
    logger.info(f"⚡️ RAW UPDATE RECEIVED: ID={event.update_id}, Type={event.event_type}")
    # If it is a business message, let's print if it has photo or video
    if event.business_message:
        msg = event.business_message
        logger.info(f"   ↳ Business Message: ID={msg.message_id}, Chat={msg.chat.id}, Photo={bool(msg.photo)}, Video={bool(msg.video)}, Voice={bool(msg.voice)}")
    elif event.business_connection:
        conn = event.business_connection
        logger.info(f"   ↳ Business Connection: ID={conn.id}, UserChatID={conn.user_chat_id}, Enabled={conn.is_enabled}")
    elif event.deleted_business_messages:
        del_ev = event.deleted_business_messages
        logger.info(f"   ↳ Deleted Business Messages: Chat={del_ev.chat.id}, IDs={del_ev.message_ids}")
    return await handler(event, data)


async def main() -> None:
    # Initialize connection pool if using PostgreSQL
    global db_pool
    if DATABASE_URL:
        if not psycopg2_available:
            raise RuntimeError("psycopg2 is not installed but DATABASE_URL is set.")
        logger.info("Initializing PostgreSQL connection pool...")
        db_pool = ThreadedConnectionPool(1, 20, dsn=DATABASE_URL)

    # Start web server for Render keep-alive
    await start_web_server()

    bot = Bot(token=TOKEN)
    dp = Dispatcher()
    dp.update.outer_middleware(raw_update_middleware)
    dp.include_router(router)
    
    # Register lifecycle hooks
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
