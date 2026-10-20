import asyncio
import io
import json
import logging
import logging.handlers
import os
import shutil
import time
from datetime import datetime
from typing import Optional

from dropbox import DropRoomManager, load_drop_users, RoomTarget
import nio
from nio import (
    AsyncClient,
    MatrixRoom,
    RoomMessageText,
    RoomMessageFile,
    RoomMessageImage,
)
import redis.asyncio as redis

try:
    from smbclient import open_file, register_session, mkdir as smb_mkdir, stat as smb_stat
    from smbprotocol.exceptions import SMBException

    SMB_AVAILABLE = True
except ImportError:
    SMB_AVAILABLE = False

from config import load_config, load_folders, save_folders, ALLOWED_MIMETYPES

# Конфиги
config = load_config()

DOWNLOAD_DIR = "/app/tmp_downloads"
SYNC_TOKEN_FILE = "/app/data/sync_token"
STATE_TTL_SECONDS = 3600
REDIS_PREFIX = "pending"
EVENT_MAX_AGE_SECONDS = 60
TARGET_FOLDERS: dict[str, str] = {}

drop_manager: Optional["DropRoomManager"] = None
drop_locks: dict[str, asyncio.Lock] = {}
folders_cache: Optional["FoldersCache"] = None


def _parse_smb_path(path: str) -> tuple[str, str, str]:
    """Разбирает smb путь на (server, share, relative_path)"""
    if path.startswith("smb://"):
        path = "smb:/" + path[6:]
    if not path.startswith("smb:/"):
        raise ValueError(f"Неверный формат smb пути: {path}")
    rest = path[5:]
    parts = rest.strip("/").split("/", 2)
    if len(parts) < 2:
        raise ValueError(f"Неверный формат smb пути: {path}")
    return parts[0], parts[1], parts[2] if len(parts) > 2 else ""


#############################################################
# Логирование
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("matrix_bot")
    logger.setLevel(getattr(logging, config.logging.level, logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        os.makedirs(os.path.dirname(config.logging.log_file) or ".", exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            config.logging.log_file,
            maxBytes=config.logging.max_bytes,
            backupCount=config.logging.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as e:
        logger.warning(f"Не удалось создать файл логов: {e}")

    logging.getLogger("nio").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("smbprotocol").setLevel(logging.WARNING)

    return logger


logger = setup_logging()


# Кэш папок в FSM
class FoldersCache:
    CACHE_KEY = "bot:folders"
    CACHE_TTL = 30 * 24 * 3600

    def __init__(self, redis_client):
        self.redis = redis_client

    async def load(self) -> Optional[dict[str, str]]:
        try:
            raw = await self.redis.get(self.CACHE_KEY)
            if not raw:
                return None
            data = json.loads(raw)
            if not isinstance(data, dict):
                logger.warning("Кэш folders в Redis имеет неверный формат")
                return None
            logger.info(f"Список папок загружен из Redis-кэша ({len(data)} папок)")
            return data
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"Ошибка загрузки кэша folders из Redis: {e}")
            return None

    async def save(self, folders: dict[str, str]) -> bool:
        try:
            await self.redis.set(self.CACHE_KEY, json.dumps(folders, ensure_ascii=False), ex=self.CACHE_TTL)
            logger.info(f"Список папок сохранён в Redis-кэш ({len(folders)} папок)")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша folders в Redis: {e}")
            return False

    async def clear(self) -> bool:
        try:
            await self.redis.delete(self.CACHE_KEY)
            logger.info("Кэш folders очищен")
            return True
        except Exception as e:
            logger.error(f"Ошибка очистки кэша folders: {e}")
            return False


# FSM на Redis
class StateManager:
    def __init__(self):
        redis_url = f"redis://{config.db.host}:{config.db.port}"
        if config.db.password:
            redis_url += f"?password={config.db.password}"
        self.redis = redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Redis подключён: {config.db.host}:{config.db.port}")

    async def close(self):
        await self.redis.aclose()

    async def cleanup_legacy_keys(self):
        try:
            keys_to_delete = []
            async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:*"):
                key_type = await self.redis.type(key)
                if key_type == b"string" and ":queue:" in key:
                    keys_to_delete.append(key)
                elif key_type not in (b"string", b"list"):
                    keys_to_delete.append(key)
            if keys_to_delete:
                await self.redis.delete(*keys_to_delete)
                logger.info(f"Очищено устаревших ключей Redis: {len(keys_to_delete)}")
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших ключей: {e}")

    def _queue_key(self, room_id: str, user_id: str) -> str:
        return f"{REDIS_PREFIX}:queue:{room_id}:{user_id}"

    def _admin_state_key(self, room_id: str, user_id: str) -> str:
        return f"{REDIS_PREFIX}:admin_state:{room_id}:{user_id}"

    def _file_key(self, event_id: str) -> str:
        return f"{REDIS_PREFIX}:file:{event_id}"

    async def add_file_to_queue(self, room_id: str, user_id: str, event_id: str, temp_path: str, filename: str):
        file_data = {"event_id": event_id, "temp_path": temp_path, "filename": filename, "created_at": time.time()}
        await self.redis.set(self._file_key(event_id), json.dumps(file_data), ex=STATE_TTL_SECONDS)
        await self.redis.rpush(self._queue_key(room_id, user_id), event_id)
        await self.redis.expire(self._queue_key(room_id, user_id), STATE_TTL_SECONDS)
        logger.debug(f"Файл добавлен в очередь: event_id={event_id}, user={user_id}")

    async def get_next_file(self, room_id: str, user_id: str) -> Optional[dict]:
        queue_key = self._queue_key(room_id, user_id)
        event_id = await self.redis.lindex(queue_key, 0)
        if not event_id:
            return None
        file_data_raw = await self.redis.get(self._file_key(event_id))
        if not file_data_raw:
            await self.redis.lpop(queue_key)
            return None
        return json.loads(file_data_raw)

    async def remove_file_from_queue(self, room_id: str, user_id: str) -> Optional[dict]:
        queue_key = self._queue_key(room_id, user_id)
        event_id = await self.redis.lpop(queue_key)
        if not event_id:
            return None
        file_data_raw = await self.redis.get(self._file_key(event_id))
        await self.redis.delete(self._file_key(event_id))
        return json.loads(file_data_raw) if file_data_raw else None

    async def get_queue_length(self, room_id: str, user_id: str) -> int:
        return await self.redis.llen(self._queue_key(room_id, user_id))

    async def clear_queue(self, room_id: str, user_id: str) -> int:
        queue_key = self._queue_key(room_id, user_id)
        event_ids = await self.redis.lrange(queue_key, 0, -1)
        deleted_count = 0
        for event_id in event_ids:
            file_key = self._file_key(event_id)
            file_data_raw = await self.redis.get(file_key)
            if file_data_raw:
                file_data = json.loads(file_data_raw)
                temp_path = file_data.get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                        deleted_count += 1
                    except OSError:
                        pass
            await self.redis.delete(file_key)
        await self.redis.delete(queue_key)
        await self.redis.delete(self._admin_state_key(room_id, user_id))
        return deleted_count

    async def set_awaiting_folders(self, room_id: str, user_id: str):
        data = {"state": "awaiting_folders", "created_at": time.time()}
        await self.redis.set(self._admin_state_key(room_id, user_id), json.dumps(data), ex=STATE_TTL_SECONDS)

    async def is_awaiting_folders(self, room_id: str, user_id: str) -> bool:
        raw = await self.redis.get(self._admin_state_key(room_id, user_id))
        if raw:
            try:
                return json.loads(raw).get("state") == "awaiting_folders"
            except json.JSONDecodeError:
                return False
        return False

    async def clear_awaiting_folders(self, room_id: str, user_id: str):
        await self.redis.delete(self._admin_state_key(room_id, user_id))

    async def set_awaiting_drop_users(self, room_id: str, user_id: str):
        data = {"state": "awaiting_drop_users", "created_at": time.time()}
        await self.redis.set(self._admin_state_key(room_id, user_id), json.dumps(data), ex=STATE_TTL_SECONDS)

    async def is_awaiting_drop_users(self, room_id: str, user_id: str) -> bool:
        raw = await self.redis.get(self._admin_state_key(room_id, user_id))
        if raw:
            try:
                return json.loads(raw).get("state") == "awaiting_drop_users"
            except json.JSONDecodeError:
                return False
        return False

    async def clear_awaiting_drop_users(self, room_id: str, user_id: str):
        await self.redis.delete(self._admin_state_key(room_id, user_id))

    async def iter_all_files(self):
        async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:file:*"):
            raw = await self.redis.get(key)
            if raw:
                yield key, json.loads(raw)


# smb-клиент с поддержкой разных серверов
class SMBClient:
    def __init__(self):
        self._registered_servers = set()

    async def register_server(self, server: str, username: str, password: str, port: int = 445) -> bool:
        if not SMB_AVAILABLE:
            logger.error("smbprotocol не установлен.")
            return False
        if server in self._registered_servers:
            return True
        try:
            await asyncio.to_thread(register_session, server, username, password, port=port)
            self._registered_servers.add(server)
            logger.info(f"smb-сессия зарегистрирована для сервера: {server}")
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации smb-сессии для {server}: {e}")
            return False

    def _unc_path(self, server: str, share: str, relative_path: str) -> str:
        relative_path = relative_path.strip("/").replace("/", os.sep)
        return f"\\\\{server}\\{share}\\{relative_path}" if relative_path else f"\\\\{server}\\{share}"

    async def ensure_dir(self, server: str, share: str, relative_path: str) -> bool:
        if not await self.register_server(server, config.smb.username, config.smb.password, config.smb.port):
            return False
        parts = relative_path.strip("/").split("/") if relative_path.strip("/") else []
        current = ""
        try:
            for part in parts:
                current = f"{current}/{part}" if current else part
                unc = self._unc_path(server, share, current)
                try:
                    await asyncio.to_thread(smb_stat, unc)
                except Exception:
                    try:
                        await asyncio.to_thread(smb_mkdir, unc)
                        logger.info(f"Создана smb-директория: {unc}")
                    except Exception as e:
                        logger.error(f"Не удалось создать smb-директорию {unc}: {e}")
                        return False
            return True
        except Exception as e:
            logger.error(f"Ошибка ensure_dir: {e}")
            return False

    async def upload_file(self, server: str, share: str, relative_path: str, local_path: str) -> bool:
        if not await self.register_server(server, config.smb.username, config.smb.password, config.smb.port):
            return False
        unc = self._unc_path(server, share, relative_path)
        try:
            with open(local_path, "rb") as src:
                data = src.read()

            def _write():
                with open_file(unc, mode="wb") as dst:
                    dst.write(data)

            await asyncio.to_thread(_write)
            logger.info(f"Файл загружен на smb: {local_path} -> {unc}")
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки на smb: {e}")
            self._registered_servers.discard(server)
            return False

    async def delete_old_files(self, server: str, share: str, relative_path: str, max_age_days: int) -> int:
        if not await self.register_server(server, config.smb.username, config.smb.password, config.smb.port):
            return 0
        unc = self._unc_path(server, share, relative_path)
        deleted_count = 0
        cutoff_time = time.time() - (max_age_days * 24 * 3600)
        try:
            from smbclient import listdir, remove
            entries = await asyncio.to_thread(listdir, unc)
            for entry in entries:
                entry_unc = f"{unc}\\{entry}"
                try:
                    stat_info = await asyncio.to_thread(smb_stat, entry_unc)
                    if hasattr(stat_info, 'st_mode') and not (stat_info.st_mode & 0o170000 == 0o040000):
                        mtime = getattr(stat_info, 'st_mtime', 0)
                        if mtime < cutoff_time:
                            await asyncio.to_thread(remove, entry_unc)
                            deleted_count += 1
                            logger.info(f"[cleanup] Удалён старый файл: {entry_unc}")
                except Exception:
                    pass
            return deleted_count
        except Exception as e:
            logger.error(f"Ошибка удаления старых файлов в {unc}: {e}")
            return 0


smb_client = SMBClient()
client = AsyncClient(config.mtrx_bot.mtrx_url)
state = StateManager()


def is_admin(user_id: str) -> bool:
    return user_id in config.mtrx_bot.admin_users


def is_event_too_old(event) -> bool:
    ts = event.source.get("origin_server_ts", 0) / 1000
    return (time.time() - ts) > EVENT_MAX_AGE_SECONDS


async def send_message(room_id: str, text: str):
    try:
        await client.room_send(room_id, "m.room.message", {"msgtype": "m.text", "body": text})
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения в {room_id}: {e}")


async def send_file(room_id: str, filename: str, content: bytes, mimetype: str = "text/plain"):
    try:
        buffer = io.BytesIO(content)
        upload_response = await client.upload(buffer, filename=filename, content_type=mimetype, filesize=len(content))
        if isinstance(upload_response, nio.UploadError):
            logger.error(f"Ошибка загрузки файла: {upload_response.message}")
            return False

        content_uri = str(upload_response[0].content_uri) if isinstance(upload_response, tuple) else str(
            upload_response.content_uri)

        await client.room_send(room_id, "m.room.message", {
            "msgtype": "m.file", "body": filename, "url": content_uri,
            "info": {"mimetype": mimetype, "size": len(content)},
        })
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки файла: {e}", exc_info=True)
        return False


def is_allowed_room(room_id: str) -> bool:
    allowed_rooms = [rid for rid in [config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv] if rid]
    is_allowed = room_id in allowed_rooms
    if not is_allowed:
        logger.warning(f"Комната {room_id} не в списке разрешённых: {allowed_rooms}")
    return is_allowed


def build_menu_text(filename: str, is_personal: bool = False) -> str:
    text = f"Получен файл: {filename}\n\nКуда его сохранить? Ответьте цифрой:\n"
    for key, folder_path in TARGET_FOLDERS.items():
        clean_path = folder_path.replace("\\", "/").rstrip("/")
        display_name = clean_path.split("/")[-1]
        text += f"{key}. {display_name}\n"

    if is_personal:
        text += "Л. Личная папка (сохранить в ваш персональный Dropbox)\n"

    text += "\nИли отправьте /cancel для отмены."
    return text


def is_smb_path(path: str) -> bool:
    return path.startswith("smb:/") or path.startswith("smb://")


async def handle_file_event(room: MatrixRoom, event):
    if is_event_too_old(event) or event.sender == client.user_id:
        return

    if await state.is_awaiting_folders(room.room_id, event.sender) or await state.is_awaiting_drop_users(room.room_id,
                                                                                                         event.sender):
        return

    logger.info(f"Получено событие файла: room={room.room_id}, sender={event.sender}, type={type(event).__name__}")

    target = None
    if drop_manager is not None:
        target = await drop_manager.resolve(room.room_id, event.sender)
        if target is None:
            logger.debug(f"Комната {room.room_id} не распознана, игнорируем.")
            return

    content = event.source.get("content", {})
    mimetype = content.get("info", {}).get("mimetype", "")

    if mimetype not in ALLOWED_MIMETYPES:
        await send_message(room.room_id, f"Формат {mimetype} не поддерживается.")
        return

    filename = content.get("body", "unknown_file")
    mxc_url = content.get("url")
    if not mxc_url:
        return

    try:
        response = await client.download(mxc_url)
        if isinstance(response, nio.DownloadError):
            raise Exception(response.message)
    except Exception as e:
        logger.error(f"Ошибка скачивания {mxc_url}: {e}")
        await send_message(room.room_id, "Ошибка при скачивании файла с сервера.")
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ")[:80]
    temp_path = os.path.join(DOWNLOAD_DIR, f"{event.event_id}_{safe_name}")

    try:
        with open(temp_path, "wb") as f:
            f.write(response.body)
    except OSError as e:
        logger.error(f"Ошибка записи временного файла {temp_path}: {e}")
        await send_message(room.room_id, "Ошибка при сохранении файла на диск.")
        return

    await state.add_file_to_queue(room.room_id, event.sender, event.event_id, temp_path, filename)
    queue_length = await state.get_queue_length(room.room_id, event.sender)

    if queue_length == 1:
        is_personal = (target is not None and target.kind == "drop")
        await send_message(room.room_id, build_menu_text(filename, is_personal))
    else:
        await send_message(room.room_id,
                           f"Файл {filename} добавлен в очередь. В очереди: {queue_length} файл(ов). Сначала обработайте предыдущие файлы.")


async def on_room_message(room: MatrixRoom, event):
    global TARGET_FOLDERS, folders_cache

    if is_event_too_old(event) or event.sender == client.user_id or not isinstance(event, RoomMessageText):
        return

    target = None
    if drop_manager is not None:
        target = await drop_manager.resolve(room.room_id, event.sender)
        if target is None:
            return
    else:
        if not is_allowed_room(room.room_id):
            return
        target = RoomTarget(kind="general", room_id=room.room_id)

    text = event.body.strip()
    logger.debug(f"Текстовое сообщение от {event.sender}: {text}")

    # --- АДМИНСКИЕ И ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ ---

    if text.lower() == "/drop" and drop_manager is not None:
        room_id, error = await drop_manager.get_or_create(event.sender)
        if error:
            await send_message(room.room_id, error)
            return
        await drop_manager._rename_room(room_id)
        link = f"https://matrix.to/#/{room_id}"
        await send_message(room.room_id,
                           f"Создана персональная комната **Dropbox** и отправлено приглашение.\n\nЧтобы войти, откройте раздел **'Приглашения' (Invites)** и нажмите 'Принять'.\nСсылка: {link}")
        return

    if text.lower() == "/drop_status" and drop_manager is not None:
        room_id = await drop_manager.room_of(event.sender)
        msg = f"Персональная комната привязана: https://matrix.to/#/{room_id}" if room_id else "Персональная комната ещё не создана. Отправьте /drop."
        await send_message(room.room_id, msg)
        return

    if text.lower() == "/drop_users" and drop_manager is not None:
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        if not os.path.exists(config.dropbox.users_file):
            await send_message(room.room_id, "Файл drop_users.json не найден.")
            return
        try:
            with open(config.dropbox.users_file, "rb") as f:
                content = f.read()
            if await send_file(room.room_id, "drop_users.json", content, "application/json"):
                await state.set_awaiting_drop_users(room.room_id, event.sender)
                await send_message(room.room_id,
                                   "Отправлен файл drop_users.json. Отредактируйте его и отправьте обратно. Для отмены: /cancel.")
        except OSError as e:
            logger.error(f"Ошибка чтения drop_users.json: {e}")
        return

    if text.lower() == "/list":
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        folders_content = json.dumps(TARGET_FOLDERS, indent=2, ensure_ascii=False)
        if await send_file(room.room_id, "folders.json", folders_content.encode("utf-8"), "application/json"):
            await state.set_awaiting_folders(room.room_id, event.sender)
            await send_message(room.room_id,
                               "Отправлен файл folders.json. Отредактируйте и отправьте обратно. Для отмены: /cancel.")
        return

    if text.lower().startswith("/drop_create ") and drop_manager is not None:
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await send_message(room.room_id, "Использование: /drop_create @user:domain")
            return
        target_user = parts[1].strip()
        room_id, error = await drop_manager.get_or_create(target_user)
        if error:
            await send_message(room.room_id, f"Ошибка: {error}")
        else:
            await drop_manager._rename_room(room_id)
            await send_message(room.room_id, f"Комната для {target_user} готова: https://matrix.to/#/{room_id}")
        return

    if text.lower().startswith("/drop_reset ") and drop_manager is not None:
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await send_message(room.room_id, "Использование: /drop_reset @user:domain")
            return
        target_user = parts[1].strip()
        old = await drop_manager.unbind(target_user)
        await send_message(room.room_id, "Привязка снята." if old else "Привязка не найдена.")
        return

    if text.lower() == "/drop_reload" and drop_manager is not None:
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        drop_manager.users = load_drop_users(config.dropbox.users_file)
        await send_message(room.room_id, f"Whitelist обновлён. Пользователей: {len(drop_manager.users)}")
        return

    if text.lower() == "/reload":
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            return
        new_folders = load_folders(config.folders_file)
        if new_folders is None:
            await send_message(room.room_id, "Ошибка чтения файла folders.json.")
            return
        TARGET_FOLDERS = new_folders
        await folders_cache.save(new_folders)
        await send_message(room.room_id, f"Список папок перезагружен. Папок: {len(new_folders)}.")
        return

    if text.lower() == "/cancel":
        deleted_count = await state.clear_queue(room.room_id, event.sender)
        msg = f"Операция отменена. Удалено файлов: {deleted_count}." if deleted_count > 0 else "У вас нет активных операций для отмены."
        await send_message(room.room_id, msg)
        return

    # --- ОБРАБОТКА ВЫБОРА ПАПКИ ---
    file_data = await state.get_next_file(room.room_id, event.sender)
    if not file_data:
        return

    choice = text.strip().upper()
    folder_path = None

    if choice == "Л" and target is not None and target.kind == "drop":
        folder_path = target.folder
    elif choice in TARGET_FOLDERS:
        folder_path = TARGET_FOLDERS[choice]
    else:
        queue_length = await state.get_queue_length(room.room_id, event.sender)
        await send_message(room.room_id,
                           f"Неверный выбор. Ответьте цифрой или 'Л' (для личной папки). В очереди: {queue_length} файл(ов).")
        return

    src_path = file_data["temp_path"]
    final_filename = file_data["filename"]

    if is_smb_path(folder_path):
        try:
            server, share, rel_path = _parse_smb_path(folder_path)
        except ValueError as e:
            logger.error(f"Ошибка парсинга пути {folder_path}: {e}")
            await send_message(room.room_id, "Ошибка в конфигурации папки. Сообщите администратору.")
            return

        remote_full_path = f"{rel_path}/{final_filename}" if rel_path else final_filename
        success = await smb_client.upload_file(server, share, remote_full_path, src_path)

        if success:
            try:
                os.remove(src_path)
            except OSError:
                pass
            await state.remove_file_from_queue(room.room_id, event.sender)
            clean_smb_path = f"{server}/{share}/{remote_full_path}".replace("//", "/")
            await send_message(room.room_id, f"Файл `{final_filename}` успешно загружен:\n`{clean_smb_path}`")

            next_file = await state.get_next_file(room.room_id, event.sender)
            if next_file:
                is_personal = (target is not None and target.kind == "drop")
                await send_message(room.room_id,
                                   f"Следующий файл: {next_file['filename']}. Выберите папку или /cancel.")
                await send_message(room.room_id, build_menu_text(next_file["filename"], is_personal))
        else:
            await send_message(room.room_id, "Ошибка загрузки файла на smb-шару.")
    else:
        os.makedirs(folder_path, exist_ok=True)
        dst_path = os.path.join(folder_path, final_filename)
        if os.path.exists(dst_path):
            name, ext = os.path.splitext(final_filename)
            dst_path = os.path.join(folder_path, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")

        try:
            shutil.move(src_path, dst_path)
            await state.remove_file_from_queue(room.room_id, event.sender)
            await send_message(room.room_id, f"Файл `{final_filename}` успешно сохранён:\n`{dst_path}`")

            next_file = await state.get_next_file(room.room_id, event.sender)
            if next_file:
                is_personal = (target is not None and target.kind == "drop")
                await send_message(room.room_id,
                                   f"Следующий файл: {next_file['filename']}. Выберите папку или /cancel.")
                await send_message(room.room_id, build_menu_text(next_file["filename"], is_personal))
        except OSError as e:
            logger.error(f"Ошибка перемещения файла: {e}")
            await send_message(room.room_id, f"Ошибка при сохранении: {e}")


async def handle_folders_file(room: MatrixRoom, event):
    global TARGET_FOLDERS, folders_cache
    if is_event_too_old(event) or not is_admin(event.sender) or not await state.is_awaiting_folders(room.room_id,
                                                                                                    event.sender):
        return

    mxc_url = event.source.get("content", {}).get("url")
    if not mxc_url:
        return

    try:
        response = await client.download(mxc_url)
        if isinstance(response, nio.DownloadError):
            raise Exception(response.message)

        new_folders = json.loads(response.body.decode("utf-8"))
        if not isinstance(new_folders, dict):
            raise ValueError("Ожидается объект JSON")

        save_folders(config.folders_file, new_folders)
        await folders_cache.save(new_folders)
        TARGET_FOLDERS = new_folders
        logger.info(f"Список папок обновлён: {new_folders}")

        created = []
        for key, path in new_folders.items():
            if is_smb_path(path):
                server, share, rel_path = _parse_smb_path(path)
                if await smb_client.ensure_dir(server, share, rel_path):
                    created.append(f"{server}/{share}/{rel_path}")
            else:
                if not os.path.exists(path):
                    os.makedirs(path, exist_ok=True)
                    created.append(path)

        await state.clear_awaiting_folders(room.room_id, event.sender)
        msg = "Список папок обновлён." + (f"\nСозданы папки: {', '.join(created)}" if created else "")
        await send_message(room.room_id, msg)
    except Exception as e:
        logger.error(f"Ошибка обработки folders.json: {e}")
        await send_message(room.room_id, f"Ошибка в формате файла: {e}")


async def handle_drop_users_file(room: MatrixRoom, event):
    global drop_manager
    if is_event_too_old(event) or not is_admin(event.sender) or not await state.is_awaiting_drop_users(room.room_id,
                                                                                                       event.sender):
        return

    mxc_url = event.source.get("content", {}).get("url")
    if not mxc_url:
        return

    try:
        response = await client.download(mxc_url)
        if isinstance(response, nio.DownloadError):
            raise Exception(response.message)

        with open(config.dropbox.users_file, "wb") as f:
            f.write(response.body)

        if drop_manager is not None:
            drop_manager.users = load_drop_users(config.dropbox.users_file)
            await send_message(room.room_id,
                               f"Список пользователей обновлён. Пользователей: {len(drop_manager.users)}.")

        await state.clear_awaiting_drop_users(room.room_id, event.sender)
    except Exception as e:
        logger.error(f"Ошибка обработки drop_users.json: {e}")
        await send_message(room.room_id, f"Ошибка при сохранении файла: {e}")


# Фоновые задачи
async def orphan_cleanup_loop():
    while True:
        try:
            await asyncio.sleep(600)
            if not os.path.exists(DOWNLOAD_DIR):
                continue
            active_paths = {data.get("temp_path") async for _, data in state.iter_all_files() if data.get("temp_path")}
            for entry in os.scandir(DOWNLOAD_DIR):
                if entry.is_file() and entry.path not in active_paths:
                    try:
                        os.remove(entry.path)
                        logger.info(f"[cleanup] удалён orphan-файл: {entry.path}")
                    except OSError as e:
                        logger.error(f"[cleanup] ошибка удаления {entry.path}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[cleanup] ошибка: {e}", exc_info=True)


async def sync_token_saver():
    while True:
        try:
            await asyncio.sleep(10)
            token = getattr(client, "sync_token", None)
            if token:
                os.makedirs(os.path.dirname(SYNC_TOKEN_FILE) or ".", exist_ok=True)
                with open(SYNC_TOKEN_FILE, "w") as f:
                    f.write(token)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка сохранения sync_token: {e}")


async def old_files_cleanup_loop():
    while True:
        try:
            await asyncio.sleep(3600)
            if drop_manager is None or not SMB_AVAILABLE:
                continue
            for user_id, drop_user in drop_manager.users.items():
                server, share, rel_path = _parse_smb_path(drop_user.smb_path)
                deleted = await smb_client.delete_old_files(server, share, rel_path, 14)
                if deleted > 0:
                    logger.info(f"[cleanup] Удалено {deleted} старых файлов из {drop_user.smb_path}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка очистки старых файлов: {e}", exc_info=True)


async def invite_checker_loop():
    while True:
        try:
            await asyncio.sleep(5)
            for room_id, room in list(client.invited_rooms.items()):
                owner = await drop_manager.owner_of(room_id) if drop_manager else None
                if owner:
                    await client.join(room_id)
                    continue

                inviter = None
                if hasattr(room, 'invite_state') and room.invite_state:
                    for ev in room.invite_state:
                        if ev.type == "m.room.member" and ev.state_key == client.user_id and ev.content.get(
                                "membership") == "invite":
                            inviter = ev.sender
                            break

                if drop_manager is not None and inviter in drop_manager.users:
                    result = await client.join(room_id)
                    if not isinstance(result, nio.JoinError):
                        await drop_manager.bind(inviter, room_id)
                        await drop_manager._rename_room(room_id)
                        logger.info(f"Принято приглашение в {room_id} от {inviter}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка проверки приглашений: {e}", exc_info=True)


# Точка входа
async def main():
    global TARGET_FOLDERS, folders_cache, drop_manager

    folders_cache = FoldersCache(state.redis)
    await state.cleanup_legacy_keys()

    TARGET_FOLDERS = await folders_cache.load()
    if TARGET_FOLDERS is None:
        TARGET_FOLDERS = load_folders(config.folders_file)
        if TARGET_FOLDERS:
            await folders_cache.save(TARGET_FOLDERS)
        else:
            logger.warning("Список папок пуст.")

    if config.dropbox.enabled:
        drop_users = load_drop_users(config.dropbox.users_file)
        general_rooms = {rid for rid in [config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv] if rid}
        drop_manager = DropRoomManager(client, state.redis, drop_users, config.dropbox.bindings_file,
                                       excluded_rooms=general_rooms)
        await drop_manager.restore()
        logger.info(f"Персональный дроп включён. Пользователей: {len(drop_users)}")

    if os.path.exists(SYNC_TOKEN_FILE):
        try:
            with open(SYNC_TOKEN_FILE) as f:
                token = f.read().strip()
                if token:
                    client.sync_token = token
        except Exception as e:
            logger.warning(f"Не удалось загрузить sync_token: {e}")

    if config.mtrx_bot.mtrx_token:
        client.access_token = config.mtrx_bot.mtrx_token
        client.user_id = config.mtrx_bot.mtrx_user_id
        client.device_id = config.mtrx_bot.mtrx_dev_id
        whoami = await client.whoami()
        if isinstance(whoami, nio.ErrorResponse):
            logger.error(f"Токен невалиден: {whoami.message}")
            return
        logger.info(f"Бот {client.user_id} авторизован по токену")
    else:
        login_resp = await client.login(config.mtrx_bot.mtrx_user_id, config.mtrx_bot.user_pass, device_name="FileBot",
                                        device_id=config.mtrx_bot.mtrx_dev_id or None)
        if isinstance(login_resp, nio.LoginError):
            logger.error(f"Ошибка входа: {login_resp.message}")
            return
        logger.info(f"Бот {client.user_id} авторизован по паролю")

    client.add_event_callback(on_room_message, RoomMessageText)
    client.add_event_callback(handle_file_event, RoomMessageFile)
    client.add_event_callback(handle_file_event, RoomMessageImage)
    client.add_event_callback(handle_folders_file, RoomMessageFile)
    client.add_event_callback(handle_drop_users_file, RoomMessageFile)

    logger.info("Callback'и зарегистрированы. Запускаю sync_forever...")

    cleanup_task = asyncio.create_task(orphan_cleanup_loop())
    token_saver_task = asyncio.create_task(sync_token_saver())
    invite_task = asyncio.create_task(invite_checker_loop())
    old_files_task = asyncio.create_task(old_files_cleanup_loop())

    try:
        await client.sync_forever(timeout=30000, full_state=False)
    except asyncio.CancelledError:
        logger.info("sync_forever отменён")
    except Exception as e:
        logger.error(f"Критическая ошибка в sync_forever: {e}", exc_info=True)
    finally:
        token = getattr(client, "sync_token", None)
        if token:
            try:
                os.makedirs(os.path.dirname(SYNC_TOKEN_FILE) or ".", exist_ok=True)
                with open(SYNC_TOKEN_FILE, "w") as f:
                    f.write(token)
            except Exception as e:
                logger.error(f"Ошибка сохранения sync_token: {e}")

        for task in [cleanup_task, token_saver_task, invite_task, old_files_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await state.close()
        await client.close()
        logger.info("Бот остановлен, ресурсы освобождены")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен.")