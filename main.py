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

# ============================================================
# Конфигурация
# ============================================================
config = load_config()

DOWNLOAD_DIR = "/app/tmp_downloads"
SYNC_TOKEN_FILE = "/app/data/sync_token"
STATE_TTL_SECONDS = 3600
REDIS_PREFIX = "pending"
EVENT_MAX_AGE_SECONDS = 60

TARGET_FOLDERS: dict[str, str] = {}


# ============================================================
# Логирование
# ============================================================
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


# ============================================================
# Машина состояний на Redis
# ============================================================
class StateManager:
    def __init__(self):
        redis_url = f"redis://{config.db.host}:{config.db.port}"
        if config.db.password:
            redis_url += f"?password={config.db.password}"
        self.redis = redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Redis подключён: {config.db.host}:{config.db.port}")

    async def close(self):
        await self.redis.aclose()

    def _key(self, room_id: str, user_id: str) -> str:
        return f"{REDIS_PREFIX}:{room_id}:{user_id}"

    async def set_pending_file(self, room_id: str, user_id: str, temp_path: str, filename: str):
        data = {
            "state": "pending_file",
            "temp_path": temp_path,
            "filename": filename,
            "created_at": time.time(),
        }
        await self.redis.set(self._key(room_id, user_id), json.dumps(data), ex=STATE_TTL_SECONDS)

    async def set_awaiting_folders(self, room_id: str, user_id: str):
        data = {"state": "awaiting_folders", "created_at": time.time()}
        await self.redis.set(self._key(room_id, user_id), json.dumps(data), ex=STATE_TTL_SECONDS)

    async def get_pending(self, room_id: str, user_id: str) -> Optional[dict]:
        raw = await self.redis.get(self._key(room_id, user_id))
        return json.loads(raw) if raw else None

    async def delete_pending(self, room_id: str, user_id: str) -> bool:
        return bool(await self.redis.delete(self._key(room_id, user_id)))

    async def iter_all_pending(self):
        async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:*"):
            raw = await self.redis.get(key)
            if raw:
                yield key, json.loads(raw)


# ============================================================
# SMB-клиент
# ============================================================
class SMBClient:
    """Работа с SMB-шарой через smbprotocol.

    Пути в folders.json имеют формат: smb:/subfolder/path
    (относительно корня шары).
    """

    def __init__(self):
        self._registered = False

    async def register(self) -> bool:
        """Регистрирует сессию SMB (один раз)."""
        if not SMB_AVAILABLE:
            logger.error("smbprotocol не установлен. SMB недоступен.")
            return False
        if not config.smb.enabled:
            logger.info("SMB отключён в конфигурации.")
            return False
        if self._registered:
            return True

        try:
            # register_session сохраняет учётные данные глобально
            await asyncio.to_thread(
                register_session,
                config.smb.server_ip,
                config.smb.username,
                config.smb.password,
                port=config.smb.port,
            )
            self._registered = True
            logger.info(f"SMB-сессия зарегистрирована: {config.smb.server_ip}/{config.smb.share_name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка регистрации SMB-сессии: {e}")
            return False

    def _unc_path(self, relative_path: str) -> str:
        """Формирует UNC-путь: \\<server>\<share>\<path>"""
        # Убираем префикс smb:/ если есть
        if relative_path.startswith("smb:/"):
            relative_path = relative_path[5:]
        relative_path = relative_path.strip("/")
        # smbprotocol ожидает путь с обратными слешами
        if relative_path:
            return f"\\\\{config.smb.server_ip}\\{config.smb.share_name}\\{relative_path.replace('/', os.sep)}"
        return f"\\\\{config.smb.server_ip}\\{config.smb.share_name}"

    async def ensure_dir(self, relative_path: str) -> bool:
        """Создаёт директорию рекурсивно на SMB-шаре."""
        if not await self.register():
            return False

        parts = relative_path.strip("/").split("/") if relative_path.strip("/") else []
        current = ""
        try:
            for part in parts:
                current = f"{current}/{part}" if current else part
                unc = self._unc_path(current)
                try:
                    await asyncio.to_thread(smb_stat, unc)
                except Exception:
                    # Директории нет — создаём
                    try:
                        await asyncio.to_thread(smb_mkdir, unc)
                        logger.info(f"Создана SMB-директория: {unc}")
                    except Exception as e:
                        logger.error(f"Не удалось создать SMB-директорию {unc}: {e}")
                        return False
            return True
        except Exception as e:
            logger.error(f"Ошибка ensure_dir: {e}")
            return False

    async def upload_file(self, local_path: str, remote_relative_path: str) -> bool:
        """Загружает локальный файл на SMB-шару."""
        if not await self.register():
            return False

        unc = self._unc_path(remote_relative_path)
        try:
            with open(local_path, "rb") as src:
                data = src.read()

            def _write():
                with open_file(unc, mode="wb") as dst:
                    dst.write(data)

            await asyncio.to_thread(_write)
            logger.info(f"Файл загружен на SMB: {local_path} -> {unc}")
            return True
        except Exception as e:
            logger.error(f"Ошибка загрузки на SMB: {e}")
            # При ошибке сбрасываем сессию, чтобы пересоздать при следующей попытке
            self._registered = False
            return False


smb_client = SMBClient()

# ============================================================
# Клиент Matrix и вспомогательные функции
# ============================================================
client = AsyncClient(config.mtrx_bot.mtrx_url)
state = StateManager()


def is_admin(user_id: str) -> bool:
    return user_id in config.mtrx_bot.admin_users


def is_event_too_old(event) -> bool:
    ts = event.source.get("origin_server_ts", 0) / 1000
    return (time.time() - ts) > EVENT_MAX_AGE_SECONDS


async def send_message(room_id: str, text: str):
    try:
        await client.room_send(room_id, "m.room.message", {
            "msgtype": "m.text",
            "body": text,
        })
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения в {room_id}: {e}")


async def send_file(room_id: str, filename: str, content: bytes, mimetype: str = "text/plain"):
    """Отправляет файл в комнату."""
    try:
        buffer = io.BytesIO(content)
        upload_response = await client.upload(
            buffer,
            filename=filename,
            content_type=mimetype,
            filesize=len(content),
        )
        if isinstance(upload_response, nio.UploadError):
            logger.error(f"Ошибка загрузки файла: {upload_response.message}")
            return False

        if isinstance(upload_response, tuple):
            content_uri = str(upload_response[0].content_uri)
        elif hasattr(upload_response, 'content_uri'):
            content_uri = str(upload_response.content_uri)
        else:
            logger.error(f"Неизвестный тип upload_response: {type(upload_response)}")
            return False

        await client.room_send(room_id, "m.room.message", {
            "msgtype": "m.file",
            "body": filename,
            "url": content_uri,
            "info": {"mimetype": mimetype, "size": len(content)},
        })
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки файла: {e}", exc_info=True)
        return False


def is_allowed_room(room_id: str) -> bool:
    allowed_rooms = [
        rid for rid in [config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv]
        if rid
    ]
    is_allowed = room_id in allowed_rooms
    if not is_allowed:
        logger.warning(f"Комната {room_id} не в списке разрешённых: {allowed_rooms}")
    return is_allowed


def build_menu_text(filename: str) -> str:
    global TARGET_FOLDERS
    text = f"Получен файл: {filename}\n\nКуда его сохранить? Ответьте цифрой:\n"
    for key, folder_path in TARGET_FOLDERS.items():
        if folder_path.startswith("smb:/"):
            display = f"[SMB] {folder_path[5:]}"
        else:
            display = os.path.basename(folder_path)
        text += f"{key}. {display}\n"
    text += "\nИли отправьте /cancel для отмены."
    return text


def is_smb_path(path: str) -> bool:
    return path.startswith("smb:/")


def get_remote_path(path: str) -> str:
    return path[5:] if path.startswith("smb:/") else path


# ============================================================
# Обработчики событий
# ============================================================
async def handle_file_event(room: MatrixRoom, event):
    """Обработчик входящих файлов и изображений."""
    if is_event_too_old(event):
        logger.debug(f"Игнорирую старое событие файла от {event.sender}")
        return

    if event.sender == client.user_id:
        return

    pending = await state.get_pending(room.room_id, event.sender)
    if pending and pending.get("state") == "awaiting_folders":
        logger.debug(f"Файл от {event.sender} пропущен (состояние awaiting_folders)")
        return

    logger.info(
        f"Получено событие файла: room={room.room_id}, "
        f"sender={event.sender}, type={type(event).__name__}"
    )

    if not is_allowed_room(room.room_id):
        return

    content = event.source.get("content", {})
    mimetype = content.get("info", {}).get("mimetype", "")

    if mimetype not in ALLOWED_MIMETYPES:
        logger.warning(f"Файл отклонён (неподдерживаемый тип): {mimetype}")
        await send_message(room.room_id, f"Формат {mimetype} не поддерживается.")
        return

    filename = content.get("body", "unknown_file")
    mxc_url = content.get("url")
    if not mxc_url:
        logger.warning("Файл без URL, пропускаю")
        return

    logger.info(f"Файл принят в работу: {filename} от {event.sender}")

    existing = await state.get_pending(room.room_id, event.sender)
    if existing:
        old_path = existing.get("temp_path")
        if old_path and os.path.exists(old_path):
            try:
                os.remove(old_path)
                logger.info(f"Удалён старый временный файл: {old_path}")
            except OSError as e:
                logger.error(f"Не удалось удалить старый файл {old_path}: {e}")
        await send_message(
            room.room_id,
            "У вас уже есть незавершённый запрос. Предыдущий файл удалён. Обрабатываю новый.",
        )

    try:
        response = await client.download(mxc_url)
    except Exception as e:
        logger.error(f"Ошибка скачивания {mxc_url}: {e}")
        await send_message(room.room_id, "Ошибка при скачивании файла с сервера.")
        return

    if isinstance(response, nio.DownloadError):
        logger.error(f"DownloadError: {response.message}")
        await send_message(room.room_id, "Ошибка при скачивании файла с сервера.")
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    safe_name = "".join(c for c in filename if c.isalnum() or c in "._- ")[:80]
    temp_path = os.path.join(DOWNLOAD_DIR, f"{event.event_id}_{safe_name}")

    try:
        with open(temp_path, "wb") as f:
            f.write(response.body)
        logger.info(f"Файл сохранён во временную папку: {temp_path}")
    except OSError as e:
        logger.error(f"Ошибка записи временного файла {temp_path}: {e}")
        await send_message(room.room_id, "Ошибка при сохранении файла на диск.")
        return

    await state.set_pending_file(room.room_id, event.sender, temp_path, filename)
    await send_message(room.room_id, build_menu_text(filename))


async def on_room_message(room: MatrixRoom, event):
    """Обработчик текстовых сообщений."""
    if is_event_too_old(event):
        return

    if event.sender == client.user_id:
        return

    if not is_allowed_room(room.room_id):
        return

    if not isinstance(event, RoomMessageText):
        return

    text = event.body.strip()
    logger.debug(f"Текстовое сообщение от {event.sender}: {text}")

    # Команда /list для админов
    if text.lower() == "/list":
        if not is_admin(event.sender):
            await send_message(room.room_id, "У вас нет прав для выполнения этой команды.")
            logger.warning(f"Попытка выполнения /list не-админом: {event.sender}")
            return

        global TARGET_FOLDERS
        folders_content = json.dumps(TARGET_FOLDERS, indent=2, ensure_ascii=False)
        success = await send_file(
            room.room_id,
            "folders.json",
            folders_content.encode("utf-8"),
            "application/json",
        )
        if success:
            await state.set_awaiting_folders(room.room_id, event.sender)
            await send_message(
                room.room_id,
                "Отправлен файл folders.json с текущим списком папок. "
                "Отредактируйте его и отправьте обратно. "
                "Формат: {\"1\": \"/local/path\", \"2\": \"smb:/remote/path\"}. "
                "Для отмены отправьте /cancel.",
            )
            logger.info(f"Файл folders.json отправлен админу {event.sender}")
        return

    # Команда отмены
    if text.lower() == "/cancel":
        pending = await state.get_pending(room.room_id, event.sender)
        if pending:
            if pending.get("state") == "pending_file":
                temp_path = pending.get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError as e:
                        logger.error(f"Не удалось удалить {temp_path}: {e}")
            await state.delete_pending(room.room_id, event.sender)
            await send_message(room.room_id, "Операция отменена.")
            logger.info(f"Операция отменена пользователем {event.sender}")
        return

    # Обработка состояния
    pending = await state.get_pending(room.room_id, event.sender)
    if not pending:
        return

    state_type = pending.get("state")

    if state_type == "awaiting_folders":
        await send_message(
            room.room_id,
            "Ожидаю файл folders.json. Отправьте его или /cancel для отмены.",
        )
        return

    if state_type == "pending_file":
        choice = text
        if choice not in TARGET_FOLDERS:
            await send_message(
                room.room_id,
                "Неверный выбор. Пожалуйста, ответьте цифрой из списка или отправьте /cancel.",
            )
            return

        src_path = pending["temp_path"]
        folder_path = TARGET_FOLDERS[choice]
        final_filename = pending["filename"]

        if is_smb_path(folder_path):
            # Загрузка по SMB
            remote_subpath = get_remote_path(folder_path)
            remote_full_path = f"{remote_subpath}/{final_filename}" if remote_subpath else final_filename
            success = await smb_client.upload_file(src_path, remote_full_path)
            if success:
                try:
                    os.remove(src_path)
                except OSError:
                    pass
                await send_message(
                    room.room_id,
                    f"Файл загружен на SMB-шару: {config.smb.server_ip}/{config.smb.share_name}/{remote_full_path}",
                )
                logger.info(f"Файл загружен на SMB: {remote_full_path}")
            else:
                await send_message(room.room_id, "Ошибка загрузки файла на SMB-шару.")
                return
        else:
            # Локальное сохранение
            os.makedirs(folder_path, exist_ok=True)
            dst_path = os.path.join(folder_path, final_filename)

            if os.path.exists(dst_path):
                name, ext = os.path.splitext(final_filename)
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dst_path = os.path.join(folder_path, f"{name}_{ts}{ext}")

            try:
                shutil.move(src_path, dst_path)
                logger.info(f"Файл перемещён: {src_path} -> {dst_path}")
                await send_message(
                    room.room_id,
                    f"Файл сохранён в папку {os.path.basename(folder_path)}:\n{dst_path}",
                )
            except OSError as e:
                logger.error(f"Ошибка перемещения файла: {e}")
                await send_message(room.room_id, f"Ошибка при сохранении: {e}")
                return

        await state.delete_pending(room.room_id, event.sender)


async def handle_folders_file(room: MatrixRoom, event):
    """Обработчик файла folders.json от админа."""
    if is_event_too_old(event):
        return

    if not is_admin(event.sender):
        return

    pending = await state.get_pending(room.room_id, event.sender)
    if not pending or pending.get("state") != "awaiting_folders":
        return

    content = event.source.get("content", {})
    mxc_url = content.get("url")
    if not mxc_url:
        return

    try:
        response = await client.download(mxc_url)
    except Exception as e:
        logger.error(f"Ошибка скачивания folders.json: {e}")
        await send_message(room.room_id, "Ошибка при скачивании файла.")
        return

    if isinstance(response, nio.DownloadError):
        logger.error(f"DownloadError: {response.message}")
        await send_message(room.room_id, "Ошибка при скачивании файла.")
        return

    try:
        new_folders = json.loads(response.body.decode("utf-8"))
        if not isinstance(new_folders, dict):
            raise ValueError("Ожидается объект JSON")
        for key, value in new_folders.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise ValueError(f"Неверный формат: ключ {key}, значение {value}")
    except (json.JSONDecodeError, ValueError, UnicodeDecodeError) as e:
        logger.error(f"Ошибка парсинга folders.json: {e}")
        await send_message(room.room_id, f"Ошибка в формате файла: {e}")
        return

    global TARGET_FOLDERS
    try:
        save_folders(config.folders_file, new_folders)
        TARGET_FOLDERS = new_folders
        logger.info(f"Список папок обновлён: {new_folders}")
    except Exception as e:
        logger.error(f"Ошибка сохранения folders.json: {e}")
        await send_message(room.room_id, f"Ошибка сохранения файла: {e}")
        return

    # Создаём недостающие папки
    created = []
    for key, path in new_folders.items():
        if is_smb_path(path):
            remote_path = get_remote_path(path)
            success = await smb_client.ensure_dir(remote_path)
            if success:
                created.append(f"[SMB] {remote_path}")
        else:
            if not os.path.exists(path):
                try:
                    os.makedirs(path, exist_ok=True)
                    created.append(path)
                    logger.info(f"Создана папка: {path}")
                except OSError as e:
                    logger.error(f"Не удалось создать папку {path}: {e}")

    await state.delete_pending(room.room_id, event.sender)

    msg = "Список папок обновлён."
    if created:
        msg += f"\nСозданы папки: {', '.join(created)}"
    await send_message(room.room_id, msg)


# ============================================================
# Фоновые задачи
# ============================================================
async def orphan_cleanup_loop():
    while True:
        try:
            await asyncio.sleep(600)
            if not os.path.exists(DOWNLOAD_DIR):
                continue

            active_paths = set()
            async for _, data in state.iter_all_pending():
                temp_path = data.get("temp_path")
                if temp_path:
                    active_paths.add(temp_path)

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


async def invite_checker_loop():
    while True:
        try:
            await asyncio.sleep(5)
            for room_id in list(client.invited_rooms.keys()):
                logger.info(f"Принимаю приглашение в комнату {room_id}")
                try:
                    result = await client.join(room_id)
                    if isinstance(result, nio.JoinError):
                        logger.error(f"Не удалось принять приглашение: {result.message}")
                    else:
                        logger.info(f"Успешно вступил в комнату {room_id}")
                except Exception as e:
                    logger.error(f"Ошибка приёма приглашения: {e}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Ошибка проверки приглашений: {e}", exc_info=True)


# ============================================================
# Точка входа
# ============================================================
async def main():
    global TARGET_FOLDERS

    TARGET_FOLDERS = load_folders(config.folders_file)
    if not TARGET_FOLDERS:
        logger.warning("Список папок пуст. Используйте команду /list для настройки.")
    else:
        logger.info(f"Загружено папок: {len(TARGET_FOLDERS)}")

    if os.path.exists(SYNC_TOKEN_FILE):
        try:
            with open(SYNC_TOKEN_FILE) as f:
                token = f.read().strip()
                if token:
                    client.sync_token = token
                    logger.info(f"Загружен sync_token: {token[:20]}...")
        except Exception as e:
            logger.warning(f"Не удалось загрузить sync_token: {e}")

    if config.mtrx_bot.mtrx_token:
        client.access_token = config.mtrx_bot.mtrx_token
        client.user_id = config.mtrx_bot.mtrx_user_id
        client.device_id = config.mtrx_bot.mtrx_dev_id

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
            login_resp = await client.login(
                config.mtrx_bot.mtrx_user_id,
                config.mtrx_bot.user_pass,
                device_name="FileBot",
                device_id=config.mtrx_bot.mtrx_dev_id or None,
            )
            if isinstance(login_resp, nio.LoginError):
                logger.error(f"Ошибка входа: {login_resp.message}")
                return
            logger.info(f"Бот {config.mtrx_bot.mtrx_user_id} авторизован по паролю")
        except Exception as e:
            logger.error(f"Ошибка логина: {e}")
            return

    # Регистрируем callback'и
    client.add_event_callback(on_room_message, RoomMessageText)
    client.add_event_callback(handle_file_event, RoomMessageFile)
    client.add_event_callback(handle_file_event, RoomMessageImage)
    client.add_event_callback(handle_folders_file, RoomMessageFile)

    logger.info("Callback'и зарегистрированы. Запускаю sync_forever...")

    cleanup_task = asyncio.create_task(orphan_cleanup_loop())
    token_saver_task = asyncio.create_task(sync_token_saver())
    invite_task = asyncio.create_task(invite_checker_loop())

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
                logger.info("sync_token сохранён")
            except Exception as e:
                logger.error(f"Ошибка сохранения sync_token: {e}")

        for task in [cleanup_task, token_saver_task, invite_task]:
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