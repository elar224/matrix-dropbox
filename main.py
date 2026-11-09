import asyncio
import errno
import hashlib
import io
import json
import logging
import logging.handlers
import os
import shutil
import stat as stat_lib
import time
from datetime import datetime
from functools import wraps
from typing import Optional

import nio
from nio import AsyncClient, MatrixRoom, RoomMessageText, RoomMessageFile, RoomMessageImage
import redis.asyncio as redis

try:
    from smbclient import (
        open_file,
        register_session,
        mkdir as smb_mkdir,
        stat as smb_stat,
        listdir as smb_listdir,
        remove as smb_remove,
    )
    SMB_AVAILABLE = True
except ImportError:
    SMB_AVAILABLE = False

from config import (
    ConfigError,
    load_config,
    load_folders,
    save_folders,
    validate_folders,
    ALLOWED_MIMETYPES,
)
from drop_rooms import (
    DropRoomManager,
    RoomTarget,
    load_drop_users,
    parse_drop_users,
    save_drop_users_atomic,
)

try:
    config = load_config()
except ConfigError as e:
    raise SystemExit(f"Ошибка конфигурации: {e}")

#########################################################################
DOWNLOAD_DIR = "/app/tmp_downloads"
STATE_TTL_SECONDS = 3600
REDIS_PREFIX = "pending"
EVENT_MAX_AGE_SECONDS = 60
SMB_UPLOAD_TIMEOUT = 120.0
ORPHAN_MIN_AGE_SECONDS = 900
MAX_FILE_BYTES = config.dropbox.max_file_mb * 1024 * 1024

TARGET_FOLDERS: dict[str, str] = {}
drop_manager: Optional[DropRoomManager] = None
folders_cache: Optional["FoldersCache"] = None

_user_locks: dict[tuple[str, str], asyncio.Lock] = {}
_background: set[asyncio.Task] = set()


####################################################
#Логирование

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
    for name in ("nio", "httpx", "httpcore", "smbprotocol"):
        logging.getLogger(name).setLevel(logging.WARNING)
    return logger


logger = setup_logging()


####################################################################
#Утилиты

def _parse_smb_path(path: str) -> tuple[str, str, str]:
    ###smb:/server/share/rel/path -> (server, share, rel/path)
    if path.startswith("smb://"):
        path = "smb:/" + path[6:]
    if not path.startswith("smb:/"):
        raise ValueError(f"Неверный формат smb пути: {path}")
    parts = path[5:].strip("/").split("/", 2)
    if len(parts) < 2:
        raise ValueError(f"Неверный формат smb пути: {path}")
    return parts[0], parts[1], parts[2] if len(parts) > 2 else ""


def is_smb_path(path: str) -> bool:
    return path.startswith("smb:/")


def sanitize_filename(name: str) -> str:
    ###Оставляет только безопасное имя файла (без путей и запрещённых символов).
    name = (name or "").replace("\\", "/").split("/")[-1]
    name = "".join(c for c in name if c not in '<>:"|?*' and ord(c) >= 32)
    name = name.strip(" .")
    if len(name) > 150:
        root, ext = os.path.splitext(name)
        if len(ext) > 20:
            root, ext = name, ""
        name = root[:150 - len(ext)] + ext
    return name or "unnamed_file"


def strip_reply_fallback(body: str) -> str:
    ###Убирает цитату ('> ...') из ответа в Element, чтобы выбор цифры работал.
    lines = body.split("\n")
    i = 0
    while i < len(lines) and lines[i].startswith(">"):
        i += 1
    if i:
        while i < len(lines) and not lines[i].strip():
            i += 1
    return "\n".join(lines[i:]).strip()


def get_lock(room_id: str, user_id: str) -> asyncio.Lock:
    return _user_locks.setdefault((room_id, user_id), asyncio.Lock())


def spawn(coro, name: str):
    async def _guard():
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Ошибка в фоновой задаче {name}")

    task = asyncio.create_task(_guard())
    _background.add(task)
    task.add_done_callback(_background.discard)


def safe(handler):
    @wraps(handler)
    async def wrapper(room, event):
        try:
            await handler(room, event)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Ошибка в обработчике {handler.__name__}")
    return wrapper


########################
#Кэш папок

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
            data = validate_folders(json.loads(raw))
            logger.info(f"Список папок загружен из Redis-кэша ({len(data)} папок)")
            return data
        except Exception as e:
            logger.error(f"Ошибка загрузки кэша folders из Redis: {e}")
            return None

    async def save(self, folders: dict[str, str]) -> bool:
        try:
            await self.redis.set(
                self.CACHE_KEY, json.dumps(folders, ensure_ascii=False), ex=self.CACHE_TTL
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша folders в Redis: {e}")
            return False


#######################################################################################
#FSM на Redis

class StateManager:
    def __init__(self):
        self.redis = redis.Redis(
            host=config.db.host,
            port=config.db.port,
            password=config.db.password or None,
            decode_responses=True,
        )
        logger.info(f"Redis: {config.db.host}:{config.db.port}")

    async def close(self):
        await self.redis.aclose()

    async def cleanup_legacy_keys(self):
        ###удаляет только ключи старого формата (decode_responses=True -> type() возвращает str).
        try:
            keys_to_delete = []
            async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:*"):
                key_type = await self.redis.type(key)
                if key_type == "string" and ":queue:" in key:
                    keys_to_delete.append(key)
                elif key_type not in ("string", "list"):
                    keys_to_delete.append(key)
            if keys_to_delete:
                await self.redis.delete(*keys_to_delete)
                logger.info(f"Очищено устаревших ключей Redis: {len(keys_to_delete)}")
        except Exception as e:
            logger.error(f"Ошибка очистки устаревших ключей: {e}")

    def _queue_key(self, room_id, user_id):
        return f"{REDIS_PREFIX}:queue:{room_id}:{user_id}"

    def _admin_state_key(self, room_id, user_id):
        return f"{REDIS_PREFIX}:admin_state:{room_id}:{user_id}"

    def _file_key(self, event_id):
        return f"{REDIS_PREFIX}:file:{event_id}"

    async def mark_seen(self, event_id: str) -> bool:
        #True, если событие видим впервые (защита от повторной обработки после рестарта).
        return bool(await self.redis.set(f"{REDIS_PREFIX}:seen:{event_id}", "1", nx=True, ex=300))

    async def add_file_to_queue(self, room_id, user_id, event_id, temp_path, filename) -> int:
        #Возвращает позицию файла в очереди (атомарно, из RPUSH).
        file_data = {
            "event_id": event_id,
            "temp_path": temp_path,
            "filename": filename,
            "created_at": time.time(),
        }
        await self.redis.set(self._file_key(event_id), json.dumps(file_data), ex=STATE_TTL_SECONDS)
        position = await self.redis.rpush(self._queue_key(room_id, user_id), event_id)
        await self.redis.expire(self._queue_key(room_id, user_id), STATE_TTL_SECONDS)
        return position

    async def get_next_file(self, room_id, user_id) -> Optional[dict]:
        queue_key = self._queue_key(room_id, user_id)
        while True:
            event_id = await self.redis.lindex(queue_key, 0)
            if not event_id:
                return None
            raw = await self.redis.get(self._file_key(event_id))
            if raw:
                return json.loads(raw)
            await self.redis.lpop(queue_key)  # просроченная запись - пропускаем

    async def remove_file_from_queue(self, room_id, user_id) -> Optional[dict]:
        event_id = await self.redis.lpop(self._queue_key(room_id, user_id))
        if not event_id:
            return None
        raw = await self.redis.get(self._file_key(event_id))
        await self.redis.delete(self._file_key(event_id))
        return json.loads(raw) if raw else None

    async def get_queue_length(self, room_id, user_id) -> int:
        #cчитает только живые записи, мёртвые убирает.
        queue_key = self._queue_key(room_id, user_id)
        alive = 0
        for event_id in await self.redis.lrange(queue_key, 0, -1):
            if await self.redis.exists(self._file_key(event_id)):
                alive += 1
            else:
                await self.redis.lrem(queue_key, 0, event_id)
        return alive

    async def clear_queue(self, room_id, user_id) -> tuple[int, bool]:
        ##Возвращает (удалено файлов, было ли состояние ожидания админа)
        queue_key = self._queue_key(room_id, user_id)
        deleted = 0
        for event_id in await self.redis.lrange(queue_key, 0, -1):
            file_key = self._file_key(event_id)
            raw = await self.redis.get(file_key)
            if raw:
                temp_path = json.loads(raw).get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                        deleted += 1
                    except OSError:
                        pass
            await self.redis.delete(file_key)
        await self.redis.delete(queue_key)
        had_state = bool(await self.redis.delete(self._admin_state_key(room_id, user_id)))
        return deleted, had_state

    async def _set_admin_state(self, room_id, user_id, name):
        await self.redis.set(
            self._admin_state_key(room_id, user_id),
            json.dumps({"state": name, "created_at": time.time()}),
            ex=STATE_TTL_SECONDS,
        )

    async def _is_admin_state(self, room_id, user_id, name) -> bool:
        raw = await self.redis.get(self._admin_state_key(room_id, user_id))
        if not raw:
            return False
        try:
            return json.loads(raw).get("state") == name
        except json.JSONDecodeError:
            return False

    async def set_awaiting_folders(self, room_id, user_id):
        await self._set_admin_state(room_id, user_id, "awaiting_folders")

    async def is_awaiting_folders(self, room_id, user_id) -> bool:
        return await self._is_admin_state(room_id, user_id, "awaiting_folders")

    async def set_awaiting_drop_users(self, room_id, user_id):
        await self._set_admin_state(room_id, user_id, "awaiting_drop_users")

    async def is_awaiting_drop_users(self, room_id, user_id) -> bool:
        return await self._is_admin_state(room_id, user_id, "awaiting_drop_users")

    async def clear_admin_state(self, room_id, user_id):
        await self.redis.delete(self._admin_state_key(room_id, user_id))

    async def iter_all_files(self):
        async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:file:*"):
            raw = await self.redis.get(key)
            if raw:
                yield key, json.loads(raw)


###################################################
#smb

class SMBClient:
    def __init__(self):
        self._registered: set[str] = set()

    async def register_server(self, server: str) -> bool:
        if not SMB_AVAILABLE:
            logger.error("smbprotocol не установлен.")
            return False
        if server in self._registered:
            return True
        try:
            await asyncio.to_thread(
                register_session, server,
                username=config.smb.username, password=config.smb.password, port=config.smb.port,
            )
            self._registered.add(server)
            logger.info(f"smb-сессия зарегистрирована: {server}")
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации smb-сессии для {server}: {e}")
            return False

    @staticmethod
    def _unc_path(server: str, share: str, relative_path: str) -> str:
        relative = relative_path.strip("/").replace("/", "\\")
        base = f"\\\\{server}\\{share}"
        return f"{base}\\{relative}" if relative else base

    async def _exists(self, unc: str) -> bool:
        try:
            await asyncio.to_thread(smb_stat, unc)
            return True
        except OSError as e:
            if e.errno == errno.ENOENT:
                return False
            raise

    async def ensure_dir(self, server: str, share: str, relative_path: str) -> bool:
        if not await self.register_server(server):
            return False
        current = ""
        for part in [p for p in relative_path.strip("/").split("/") if p]:
            current = f"{current}/{part}" if current else part
            unc = self._unc_path(server, share, current)
            try:
                if await self._exists(unc):
                    continue
                await asyncio.to_thread(smb_mkdir, unc)
                logger.info(f"Создана smb-директория: {unc}")
            except Exception as e:
                logger.error(f"Не удалось создать/проверить smb-директорию {unc}: {e}")
                self._registered.discard(server)
                return False
        return True

    async def upload_file(self, server: str, share: str, relative_path: str, local_path: str) -> Optional[str]:
        ###возвращает итоговый относительный путь (с учётом переименования при коллизии) или None.
        if not await self.register_server(server):
            return None
        rel_dir, _, name = relative_path.rpartition("/")
        if rel_dir and not await self.ensure_dir(server, share, rel_dir):
            return None

        try:
            final_rel = relative_path
            unc = self._unc_path(server, share, final_rel)
            if await self._exists(unc):
                root, ext = os.path.splitext(name)
                name = f"{root}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
                final_rel = f"{rel_dir}/{name}" if rel_dir else name
                unc = self._unc_path(server, share, final_rel)

            def _write():
                with open(local_path, "rb") as src, open_file(unc, mode="wb") as dst:
                    shutil.copyfileobj(src, dst, 1024 * 1024)

            await asyncio.wait_for(asyncio.to_thread(_write), timeout=SMB_UPLOAD_TIMEOUT)
            logger.info(f"Файл загружен на smb: {local_path} -> {unc}")
            return final_rel
        except asyncio.TimeoutError:
            logger.error(f"ТАЙМАУТ загрузки на smb ({SMB_UPLOAD_TIMEOUT:.0f} сек). Проверьте права доступа.")
        except Exception as e:
            logger.error(f"Ошибка загрузки на smb: {e}")
        self._registered.discard(server)
        return None

    async def delete_old_files(self, server: str, share: str, relative_path: str, max_age_days: int) -> int:
        if not await self.register_server(server):
            return 0
        unc = self._unc_path(server, share, relative_path)
        cutoff = time.time() - max_age_days * 24 * 3600
        deleted = 0
        try:
            for entry in await asyncio.to_thread(smb_listdir, unc):
                entry_unc = f"{unc}\\{entry}"
                try:
                    st = await asyncio.to_thread(smb_stat, entry_unc)
                    if stat_lib.S_ISDIR(st.st_mode):
                        continue
                    if st.st_mtime < cutoff:
                        await asyncio.to_thread(smb_remove, entry_unc)
                        deleted += 1
                        logger.info(f"[cleanup] Удалён старый файл: {entry_unc}")
                except FileNotFoundError:
                    continue
                except Exception as e:
                    logger.error(f"[cleanup] {entry_unc}: {e}")
        except Exception as e:
            logger.error(f"Ошибка удаления старых файлов в {unc}: {e}")
        return deleted


smb_client = SMBClient()
client = AsyncClient(
    config.mtrx_bot.mtrx_url,
    user=config.mtrx_bot.mtrx_user_id,
    device_id=config.mtrx_bot.mtrx_dev_id or "",
)
state = StateManager()


#№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№№
# Matrix helpers

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


async def send_file(room_id: str, filename: str, content: bytes, mimetype: str = "text/plain") -> bool:
    try:
        upload_response = await client.upload(
            io.BytesIO(content), content_type=mimetype, filename=filename, filesize=len(content)
        )
        if isinstance(upload_response, tuple):
            upload_response = upload_response[0]
        if isinstance(upload_response, nio.UploadError):
            logger.error(f"Ошибка загрузки файла: {upload_response.message}")
            return False
        await client.room_send(room_id, "m.room.message", {
            "msgtype": "m.file",
            "body": filename,
            "url": str(upload_response.content_uri),
            "info": {"mimetype": mimetype, "size": len(content)},
        })
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки файла: {e}", exc_info=True)
        return False


async def download_media(mxc_url: str) -> Optional[bytes]:
    try:
        response = await client.download(mxc=mxc_url)
    except Exception as e:
        logger.error(f"Ошибка скачивания {mxc_url}: {e}")
        return None
    if isinstance(response, nio.DownloadError):
        logger.error(f"DownloadError: {response.message}")
        return None
    return response.body


def is_allowed_room(room_id: str) -> bool:
    allowed = [r for r in (config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv) if r]
    if room_id not in allowed:
        logger.debug(f"Комната {room_id} не в списке разрешённых")
        return False
    return True


async def resolve_target(room_id: str, sender: str) -> Optional[RoomTarget]:
    if drop_manager is not None:
        return await drop_manager.resolve(room_id, sender)
    if not is_allowed_room(room_id):
        return None
    return RoomTarget(kind="general", room_id=room_id)


def build_menu_text(filename: str, is_personal: bool = False) -> str:
    text = f"Получен файл: {filename}\n\nКуда его сохранить? Ответьте цифрой:\n"
    for key, folder_path in TARGET_FOLDERS.items():
        display_name = folder_path.replace("\\", "/").rstrip("/").split("/")[-1]
        text += f"{key}. {display_name}\n"
    if is_personal:
        text += "*. Личная папка (сохранить в ваш персональный Dropbox)\n"
    text += "\nИли отправьте !cancel для отмены."
    return text


async def deny_unless_admin(room_id: str, sender: str) -> bool:
    if is_admin(sender):
        return False
    await send_message(room_id, "У вас нет прав для выполнения этой команды.")
    logger.warning(f"Попытка выполнить админ-команду не-админом: {sender}")
    return True


def _write_bytes(path: str, data: bytes):
    with open(path, "wb") as f:
        f.write(data)


###########################################
# приём файлов

async def handle_file_event(room: MatrixRoom, event):
    ##cкачивает файл, кладёт в очередь и показывает меню
    target = await resolve_target(room.room_id, event.sender)
    logger.info(
        f"Файл от {event.sender} в {room.room_id}: "
        f"target={target.kind if target else None}"
    )
    if target is None or target.kind not in ("general", "drop"):
        logger.warning(
            f"Файл от {event.sender} в {room.room_id} проигнорирован: комната не распознана "
            f"(для личной комнаты проверьте !drop_status и drop_users.json)"
        )
        return

    content = event.source.get("content", {})
    info = content.get("info", {}) or {}
    mimetype = info.get("mimetype", "")
    if mimetype not in ALLOWED_MIMETYPES:
        logger.warning(f"Файл отклонён (тип): {mimetype!r}")
        await send_message(room.room_id, f"Формат {mimetype or 'неизвестный'} не поддерживается.")
        return

    size = info.get("size")
    if isinstance(size, int) and size > MAX_FILE_BYTES:
        await send_message(room.room_id, f"Файл слишком большой (максимум {config.dropbox.max_file_mb} МБ).")
        return

    mxc_url = content.get("url")
    if not mxc_url:
        logger.warning("Файл без URL, пропускаю")
        return

    filename = sanitize_filename(content.get("body", "unknown_file"))
    logger.info(f"Файл принят в работу: {filename} от {event.sender}")

    body = await download_media(mxc_url)
    if body is None:
        await send_message(room.room_id, "Ошибка при скачивании файла с сервера.")
        return
    if len(body) > MAX_FILE_BYTES:
        await send_message(room.room_id, f"Файл слишком большой (максимум {config.dropbox.max_file_mb} МБ).")
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    id_hash = hashlib.sha1(event.event_id.encode()).hexdigest()[:16]
    temp_path = os.path.join(DOWNLOAD_DIR, f"{id_hash}_{filename}")
    try:
        await asyncio.to_thread(_write_bytes, temp_path, body)
    except OSError as e:
        logger.error(f"Ошибка записи временного файла {temp_path}: {e}")
        await send_message(room.room_id, "Ошибка при сохранении файла на диск.")
        return

    position = await state.add_file_to_queue(room.room_id, event.sender, event.event_id, temp_path, filename)
    if position > 1:
        position = await state.get_queue_length(room.room_id, event.sender)  # учитываем просроченные записи

    if position == 1:
        logger.info(f"Показываю меню папок для {filename} ({event.sender}, kind={target.kind})")
        await send_message(room.room_id, build_menu_text(filename, is_personal=(target.kind == "drop")))
    else:
        await send_message(
            room.room_id,
            f"Файл {filename} добавлен в очередь. В очереди: {position} файл(ов). "
            f"Сначала обработайте предыдущие файлы.",
        )


async def handle_folders_file(room: MatrixRoom, event):
    ###Админ вернул отредактированный folders.json
    global TARGET_FOLDERS

    content = event.source.get("content", {})
    mxc_url = content.get("url")
    if not mxc_url:
        return
    body = await download_media(mxc_url)
    if body is None:
        await send_message(room.room_id, "Ошибка при скачивании файла.")
        return

    try:
        new_folders = validate_folders(json.loads(body.decode("utf-8")))
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as e:
        logger.error(f"Ошибка парсинга folders.json: {e}")
        await send_message(room.room_id, f"Ошибка в формате файла: {e}\nИсправьте и отправьте снова или !cancel.")
        return
    if not new_folders:
        await send_message(room.room_id, "Список папок пуст - не применяю. Исправьте файл или отправьте !cancel.")
        return

    try:
        await asyncio.to_thread(save_folders, config.folders_file, new_folders)
        await folders_cache.save(new_folders)
        TARGET_FOLDERS = new_folders
        logger.info(f"Список папок обновлён: {new_folders}")
    except Exception as e:
        logger.error(f"Ошибка сохранения folders: {e}")
        await send_message(room.room_id, f"Ошибка сохранения: {e}")
        return

    created, failed = [], []
    for path in new_folders.values():
        if is_smb_path(path):
            try:
                server, share, rel_path = _parse_smb_path(path)
            except ValueError as e:
                failed.append(f"{path} ({e})")
                continue
            if await smb_client.ensure_dir(server, share, rel_path):
                created.append(f"{server}/{share}/{rel_path}")
            else:
                failed.append(path)
        elif not os.path.exists(path):
            try:
                await asyncio.to_thread(os.makedirs, path, exist_ok=True)
                created.append(path)
            except OSError as e:
                logger.error(f"Не удалось создать папку {path}: {e}")
                failed.append(path)

    await state.clear_admin_state(room.room_id, event.sender)
    msg = "Список папок обновлён."
    if created:
        msg += f"\nПроверены/созданы папки: {', '.join(created)}"
    if failed:
        msg += f"\nНе удалось создать: {', '.join(failed)}"
    await send_message(room.room_id, msg)


async def handle_drop_users_file(room: MatrixRoom, event):
    ###Админ вернул отредактированный drop_users.json. Сначала валидация, потом атомарная запись
    content = event.source.get("content", {})
    mxc_url = content.get("url")
    if not mxc_url:
        return
    body = await download_media(mxc_url)
    if body is None:
        await send_message(room.room_id, "Ошибка при скачивании файла.")
        return

    try:
        users = parse_drop_users(body.decode("utf-8"))
    except UnicodeDecodeError:
        users = {}
    if not users:
        await send_message(
            room.room_id,
            "Не удалось разобрать файл или в нём нет ни одного валидного пользователя. "
            "Текущий список не изменён. Исправьте файл или отправьте !cancel.",
        )
        return

    try:
        await asyncio.to_thread(save_drop_users_atomic, config.dropbox.users_file, body)
    except OSError as e:
        logger.error(f"Ошибка сохранения drop_users.json: {e}")
        await send_message(room.room_id, "Ошибка при сохранении файла.")
        return

    if drop_manager is not None:
        drop_manager.users = users
        await send_message(room.room_id, f"Список пользователей обновлён. Пользователей: {len(users)}.")
        logger.info(f"Список пользователей обновлён админом {event.sender}: {len(users)}")
    else:
        await send_message(room.room_id, "Файл сохранён, но персональный дроп отключён в конфигурации.")
    await state.clear_admin_state(room.room_id, event.sender)


@safe
async def on_file_message(room: MatrixRoom, event):
    ###Единая точка входа для файлов: либо ответ админа на !list / !drop_users, либо обычная загрузка
    if is_event_too_old(event) or event.sender == client.user_id:
        return
    if not await state.mark_seen(event.event_id):
        return

    if await state.is_awaiting_folders(room.room_id, event.sender):
        if is_admin(event.sender):
            await handle_folders_file(room, event)
        return
    if await state.is_awaiting_drop_users(room.room_id, event.sender):
        if is_admin(event.sender):
            await handle_drop_users_file(room, event)
        return

    spawn(handle_file_event(room, event), "handle_file_event")


_warned_encrypted: set[str] = set()


@safe
async def on_encrypted_event(room: MatrixRoom, event):
    ###Бот не умеет E2EE: сообщения в зашифрованной комнате до него не доходят. Хотя бы пишем это в лог
    if room.room_id in _warned_encrypted:
        return
    _warned_encrypted.add(room.room_id)
    logger.error(
        f"Комната {room.room_id} ЗАШИФРОВАНА (событие от {event.sender}). "
        f"Бот не может читать сообщения и файлы в ней. Создайте комнату без шифрования."
    )


##############################################################
# команды

async def handle_command(room: MatrixRoom, event, text: str) -> bool:
    ###True, если текст был распознанной командой

    global TARGET_FOLDERS
    cmd = text.lower()
    sender, rid = event.sender, room.room_id
    dm = drop_manager

    if cmd == "!drop" and dm is not None:
        room_id, error, created = await dm.get_or_create(sender)
        if error:
            await send_message(rid, error)
            return True
        link = f"https://matrix.to/#/{room_id}"
        if created:
            await send_message(
                rid,
                "Создана персональная комната и отправлено приглашение.\n"
                "Откройте раздел «Приглашения» (Invites) в клиенте Matrix и нажмите «Принять».\n"
                f"Прямая ссылка: {link}",
            )
        else:
            await send_message(rid, f"Ваша персональная комната уже существует: {link}")
        return True

    if cmd == "!drop_status" and dm is not None:
        room_id = await dm.room_of(sender)
        if room_id:
            await send_message(rid, f"Персональная комната привязана: https://matrix.to/#/{room_id}")
        else:
            await send_message(rid, "Персональная комната ещё не создана. Отправьте !drop.")
        return True

    if cmd == "!drop_users" and dm is not None:
        if await deny_unless_admin(rid, sender):
            return True
        if not os.path.exists(config.dropbox.users_file):
            await send_message(rid, "Файл drop_users.json не найден.")
            return True
        try:
            with open(config.dropbox.users_file, "rb") as f:
                data = f.read()
        except OSError as e:
            logger.error(f"Ошибка чтения drop_users.json: {e}")
            await send_message(rid, "Ошибка при чтении файла.")
            return True
        if await send_file(rid, "drop_users.json", data, "application/json"):
            await state.set_awaiting_drop_users(rid, sender)
            await send_message(
                rid,
                "Отправлен drop_users.json. Отредактируйте и отправьте обратно. "
                "Форматы: JSON или TSV (user_id<TAB>путь). Для отмены - !cancel.",
            )
        return True

    if cmd == "!list":
        if await deny_unless_admin(rid, sender):
            return True
        data = json.dumps(TARGET_FOLDERS, indent=2, ensure_ascii=False).encode("utf-8")
        if await send_file(rid, "folders.json", data, "application/json"):
            await state.set_awaiting_folders(rid, sender)
            await send_message(
                rid,
                "Отправлен folders.json. Отредактируйте и отправьте обратно. "
                'Формат: {"1": "smb://server/share/path", "2": "/local/path"}. '
                "Ключ '*' зарезервирован. Для отмены - !cancel.",
            )
        return True

    if cmd.startswith("!drop_create") and dm is not None:
        if await deny_unless_admin(rid, sender):
            return True
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await send_message(rid, "Использование: !drop_create @user:domain")
            return True
        target_user = parts[1].strip()
        if target_user not in dm.users:
            await send_message(rid, f"Пользователь {target_user} не найден в списке. Сначала добавьте его через !drop_users.")
            return True
        room_id, error, _ = await dm.get_or_create(target_user)
        if error:
            await send_message(rid, f"Ошибка: {error}")
        else:
            await send_message(rid, f"Комната для {target_user} готова: https://matrix.to/#/{room_id}")
        return True

    if cmd.startswith("!drop_reset") and dm is not None:
        if await deny_unless_admin(rid, sender):
            return True
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await send_message(rid, "Использование: !drop_reset @user:domain")
            return True
        target_user = parts[1].strip()
        if await dm.unbind(target_user):
            await send_message(rid, f"Привязка для {target_user} снята.")
        else:
            await send_message(rid, f"Привязка для {target_user} не найдена.")
        return True

    if cmd == "!drop_reload" and dm is not None:
        if await deny_unless_admin(rid, sender):
            return True
        users = load_drop_users(config.dropbox.users_file)
        if not users:
            await send_message(rid, "Файл не найден, повреждён или пуст - список не изменён.")
        else:
            dm.users = users
            await send_message(rid, f"Whitelist обновлён. Пользователей: {len(users)}")
        return True

    if cmd == "!reload":
        if await deny_unless_admin(rid, sender):
            return True
        new_folders = load_folders(config.folders_file)
        if not new_folders:
            await send_message(rid, "Файл folders.json не найден, повреждён или пуст - список не изменён.")
            return True
        TARGET_FOLDERS = new_folders
        await folders_cache.save(new_folders)
        await send_message(rid, f"Список папок перезагружен из файла. Папок: {len(new_folders)}.")
        logger.info(f"Список папок перезагружен админом {sender}")
        return True

    if cmd == "!cancel":
        deleted, had_state = await state.clear_queue(rid, sender)
        if deleted:
            await send_message(rid, f"Операция отменена. Удалено файлов из вашей очереди: {deleted}.")
        elif had_state:
            await send_message(rid, "Ожидание файла отменено.")
        else:
            await send_message(rid, "У вас нет активных операций для отмены.")
        return True

    return False


##############################################################
# выбор папки

def _save_local(src: str, folder: str, filename: str) -> str:
    os.makedirs(folder, exist_ok=True)
    dst = os.path.join(folder, filename)
    if os.path.exists(dst):
        name, ext = os.path.splitext(filename)
        dst = os.path.join(folder, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
    shutil.move(src, dst)
    return dst


async def offer_next_file(room_id: str, sender: str, is_personal: bool):
    next_file = await state.get_next_file(room_id, sender)
    if next_file:
        await send_message(
            room_id,
            "Следующий файл в очереди.\n\n" + build_menu_text(next_file["filename"], is_personal=is_personal),
        )


async def process_choice(room_id: str, sender: str, choice: str, target: RoomTarget):
    async with get_lock(room_id, sender):
        file_data = await state.get_next_file(room_id, sender)
        if not file_data:
            return

        is_personal = target.kind == "drop"
        if choice == "*" and is_personal:
            folder_path = target.folder
        elif choice in TARGET_FOLDERS:
            folder_path = TARGET_FOLDERS[choice]
        else:
            queue_length = await state.get_queue_length(room_id, sender)
            await send_message(
                room_id,
                "Неверный выбор. Ответьте цифрой из списка"
                + (" или '*' (личная папка)" if is_personal else "")
                + f". В очереди: {queue_length} файл(ов). Или отправьте !cancel.",
            )
            return

        src_path = file_data["temp_path"]
        filename = file_data["filename"]

        if not os.path.exists(src_path):
            await state.remove_file_from_queue(room_id, sender)
            await send_message(room_id, f"Временный файл {filename} утерян. Отправьте его заново.")
            await offer_next_file(room_id, sender, is_personal)
            return

        if is_smb_path(folder_path):
            try:
                server, share, rel_path = _parse_smb_path(folder_path)
            except ValueError as e:
                logger.error(f"Ошибка парсинга пути {folder_path}: {e}")
                await send_message(room_id, "⚠️ Папка настроена некорректно. Сообщите администратору.")
                return
            remote = f"{rel_path}/{filename}" if rel_path else filename
            final_rel = await smb_client.upload_file(server, share, remote, src_path)
            if not final_rel:
                await send_message(room_id, "Ошибка загрузки файла на smb-шару. Файл остался в очереди, можно повторить выбор.")
                return
            try:
                os.remove(src_path)
            except OSError:
                pass
            shown = f"{server}/{share}/{final_rel}"
        else:
            try:
                shown = await asyncio.to_thread(_save_local, src_path, folder_path, filename)
            except OSError as e:
                logger.error(f"Ошибка сохранения файла: {e}")
                await send_message(room_id, f"Ошибка при сохранении: {e}")
                return

        await state.remove_file_from_queue(room_id, sender)
        logger.info(f"Файл сохранён: {shown}")
        await send_message(room_id, f"Файл {filename} успешно сохранён:\n{shown}")
        await offer_next_file(room_id, sender, is_personal)


@safe
async def on_room_message(room: MatrixRoom, event):
    if is_event_too_old(event) or event.sender == client.user_id:
        return
    target = await resolve_target(room.room_id, event.sender)
    if target is None:
        return
    if not await state.mark_seen(event.event_id):
        return

    text = strip_reply_fallback(event.body)
    if not text:
        return

    if await handle_command(room, event, text):
        return

    # Выбор папки
    if target.kind not in ("general", "drop"):
        return
    if not await state.get_next_file(room.room_id, event.sender):
        return
    spawn(process_choice(room.room_id, event.sender, text, target), "process_choice")


##############################################################
# фоновые задачи

async def orphan_cleanup_loop():
    while True:
        try:
            await asyncio.sleep(600)
            if not os.path.exists(DOWNLOAD_DIR):
                continue
            active = set()
            async for _, data in state.iter_all_files():
                if data.get("temp_path"):
                    active.add(data["temp_path"])
            now = time.time()
            for entry in os.scandir(DOWNLOAD_DIR):
                if not entry.is_file() or entry.path in active:
                    continue
                if now - entry.stat().st_mtime < ORPHAN_MIN_AGE_SECONDS:
                    continue  # возможно, ещё скачивается/ставится в очередь
                try:
                    os.remove(entry.path)
                    logger.info(f"[cleanup] удалён orphan-файл: {entry.path}")
                except OSError as e:
                    logger.error(f"[cleanup] ошибка удаления {entry.path}: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[cleanup] ошибка: {e}", exc_info=True)


async def old_files_cleanup_loop():
    ###Удаляет старые файлы ТОЛЬКО в личных папках MATRIX_DROPBOX
    while True:
        try:
            await asyncio.sleep(3600)
            if drop_manager is None or not SMB_AVAILABLE:
                continue
            for user_id, drop_user in list(drop_manager.users.items()):
                try:
                    server, share, rel_path = _parse_smb_path(drop_user.smb_path)
                except ValueError as e:
                    logger.error(f"Ошибка парсинга пути {drop_user.smb_path}: {e}")
                    continue
                if not rel_path.endswith("MATRIX_DROPBOX"):
                    continue  # страховка: чистим только личные приёмники
                deleted = await smb_client.delete_old_files(server, share, rel_path, config.dropbox.retention_days)
                if deleted:
                    logger.info(f"[cleanup] Удалено {deleted} старых файлов из {server}/{share}/{rel_path}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка очистки старых файлов: {e}", exc_info=True)


def _inviter_of(room) -> Optional[str]:
    inviter = getattr(room, "inviter", None)
    if inviter:
        return inviter
    for ev in getattr(room, "invite_state", None) or []:
        if getattr(ev, "type", None) == "m.room.member" and getattr(ev, "state_key", None) == client.user_id:
            return getattr(ev, "sender", None)
    return None


async def invite_checker_loop():
    ###Принимает приглашения в персональные комнаты от пользователей из whitelist
    ignored: set[str] = set()  # чтобы не спамить логами каждые 5 секунд
    while True:
        try:
            await asyncio.sleep(5)
            if drop_manager is None:
                continue
            invited = dict(client.invited_rooms)
            ignored &= set(invited)

            for room_id, room in invited.items():
                if room_id in ignored:
                    continue

                owner = await drop_manager.owner_of(room_id)
                inviter = _inviter_of(room)

                if owner:
                    logger.info(f"Принимаю приглашение в известную персональную комнату {room_id}")
                    result = await client.join(room_id)
                    if isinstance(result, nio.JoinError):
                        logger.error(f"Не удалось войти в {room_id}: {result.message}")
                        ignored.add(room_id)
                    continue

                if not inviter or inviter not in drop_manager.users:
                    logger.warning(f"Приглашение в {room_id} от {inviter} проигнорировано (нет в whitelist)")
                    ignored.add(room_id)
                    continue

                current = await drop_manager.room_of(inviter)
                if current and current != room_id and current in client.rooms:
                    logger.warning(f"{inviter} уже имеет комнату {current}; приглашение в {room_id} проигнорировано")
                    ignored.add(room_id)
                    continue

                result = await client.join(room_id)
                if isinstance(result, nio.JoinError):
                    logger.error(f"Не удалось принять приглашение в {room_id}: {result.message}")
                    ignored.add(room_id)
                    continue

                if current and current != room_id:
                    await drop_manager.unbind(inviter)  # старая привязка протухла
                if await drop_manager.bind(inviter, room_id) == room_id:
                    await drop_manager.rename_room(room_id, inviter)
                    logger.info(f"Вошёл в {room_id}, привязал к {inviter}")
                else:
                    logger.error(f"Не удалось привязать {room_id} к {inviter}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка проверки приглашений: {e}", exc_info=True)


##############################################################
# точка входа

async def main():
    global TARGET_FOLDERS, folders_cache, drop_manager

    folders_cache = FoldersCache(state.redis)
    await state.cleanup_legacy_keys()

    # Приоритет: Redis -> файл. Пустой кэш считаем отсутствующим.
    cached = await folders_cache.load()
    if cached:
        TARGET_FOLDERS = cached
    else:
        from_file = load_folders(config.folders_file)
        if from_file is None:
            logger.error("folders.json повреждён. Папки не загружены, исправьте файл и выполните !reload.")
            TARGET_FOLDERS = {}
        else:
            TARGET_FOLDERS = from_file
            if from_file:
                await folders_cache.save(from_file)
    if not TARGET_FOLDERS:
        logger.warning("Список папок пуст. Используйте команду !list для настройки.")

    if config.dropbox.enabled:
        drop_users = load_drop_users(config.dropbox.users_file) or {}
        general_rooms = {r for r in (config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv) if r}
        drop_manager = DropRoomManager(
            client,
            state.redis,
            drop_users,
            config.dropbox.bindings_file,
            excluded_rooms=general_rooms,
            room_name_template=config.dropbox.room_name_template,
            retention_days=config.dropbox.retention_days,
        )
        await drop_manager.restore()
        logger.info(f"Персональный дроп включён. Пользователей в whitelist: {len(drop_users)}")

    if config.mtrx_bot.mtrx_token:
        client.restore_login(
            config.mtrx_bot.mtrx_user_id,
            config.mtrx_bot.mtrx_dev_id,
            config.mtrx_bot.mtrx_token,
        )
        try:
            whoami = await client.whoami()
            if isinstance(whoami, nio.ErrorResponse):
                logger.error(f"Токен невалиден: {whoami.message}")
                return
            logger.info(f"Бот {config.mtrx_bot.mtrx_user_id} авторизован по токену")
        except Exception as e:
            logger.error(f"Ошибка проверки токена: {e}")
            return
    else:
        try:
            login_resp = await client.login(config.mtrx_bot.user_pass, device_name="FileBot")
            if isinstance(login_resp, nio.LoginError):
                logger.error(f"Ошибка входа: {login_resp.message}")
                return
            logger.info(f"Бот {config.mtrx_bot.mtrx_user_id} авторизован по паролю")
        except Exception as e:
            logger.error(f"Ошибка логина: {e}")
            return

    # sync_token намеренно НЕ сохраняем: после рестарта нужен полный первый sync,
    # чтобы client.rooms содержал все комнаты (на этом строится DropRoomManager).
    # От повторной обработки старых событий защищают фильтр по возрасту и state.mark_seen().
    client.add_event_callback(on_room_message, RoomMessageText)
    client.add_event_callback(on_file_message, RoomMessageFile)
    client.add_event_callback(on_file_message, RoomMessageImage)
    client.add_event_callback(on_encrypted_event, nio.MegolmEvent)

    logger.info("Callback'и зарегистрированы. Запускаю sync_forever...")

    tasks = [
        asyncio.create_task(orphan_cleanup_loop()),
        asyncio.create_task(invite_checker_loop()),
        asyncio.create_task(old_files_cleanup_loop()),
    ]

    try:
        await client.sync_forever(timeout=30000, full_state=False)
    except asyncio.CancelledError:
        logger.info("sync_forever отменён")
    except Exception as e:
        logger.error(f"Критическая ошибка в sync_forever: {e}", exc_info=True)
    finally:
        for task in tasks + list(_background):
            task.cancel()
        await asyncio.gather(*tasks, *list(_background), return_exceptions=True)
        await state.close()
        await client.close()
        logger.info("Бот остановлен, ресурсы освобождены")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Бот остановлен.")