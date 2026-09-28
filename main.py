import asyncio
import json
import os
import shutil
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import nio
from nio import (
    AsyncClient,
    MatrixRoom,
    RoomMessageText,
    RoomMessageFile,
    RoomMessageImage)
import redis.asyncio as redis
from config import load_config, ALLOWED_MIMETYPES, TARGET_FOLDERS


#######################################################
config = load_config()

DOWNLOAD_DIR = "/tmp/matrix_downloads"
STATE_TTL_SECONDS = 3600  # 1 час

# Redis-ключи
REDIS_PREFIX = "pending"

################Logging
def setup_logging() -> logging.Logger:
    #########Настраивает логирование: консоль + файл с ротацией
    logger = logging.getLogger("matrix_bot")
    logger.setLevel(getattr(logging, config.logging.level, logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Файловый обработчик с ротацией
    try:
        file_handler = RotatingFileHandler(
            config.logging.log_file,
            maxBytes=config.logging.max_bytes,
            backupCount=config.logging.backup_count,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as e:
        logger.warning(f"Не удалось создать файл логов: {e}")

    # Уменьшаем шум от matrix-nio и httpx
    logging.getLogger("nio").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    return logger


logger = setup_logging()

class StateManager:
    ############## FSM на Redis

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

    async def set_pending(
            self, room_id: str, user_id: str, temp_path: str, filename: str
    ) -> None:
        data = {
            "temp_path": temp_path,
            "filename": filename,
            "created_at": time.time(),
        }
        await self.redis.set(
            self._key(room_id, user_id),
            json.dumps(data),

            ex=STATE_TTL_SECONDS,
        )
        logger.debug(f"Состояние сохранено: room={room_id}, user={user_id}, file={filename}")
    async def get_pending(self, room_id: str, user_id: str) -> dict | None:
        raw = await self.redis.get(self._key(room_id, user_id))
        if raw is None:
            return None
        return json.loads(raw)

    async def delete_pending(self, room_id: str, user_id: str) -> bool:
        return bool(await self.redis.delete(self._key(room_id, user_id)))

    async def iter_all_pending(self):
        async for key in self.redis.scan_iter(match=f"{REDIS_PREFIX}:*"):
            raw = await self.redis.get(key)
            if raw:
                yield key, json.loads(raw)


############# Инициализация клиента Matrix
client = AsyncClient(config.mtrx_bot.mtrx_url)
state = StateManager()


async def send_message(room_id: str, text: str):
    try:
        await client.room_send(room_id, "m.room.message", {
            "msgtype": "m.text",
            "body": text,
        })
    except Exception as e:
        logger.error(f"Ошибка отправки сообщения в {room_id}: {e}")


def is_allowed_room(room_id: str) -> bool:
    """Проверяет, разрешено ли боту работать в этой комнате"""
    # Фильтруем пустые значения, если вдруг что-то не задано в .env
    allowed_rooms = [
        rid for rid in [config.mtrx_bot.mtrx_room_id, config.mtrx_bot.mtrx_room_id_priv]
        if rid
    ]

    is_allowed = room_id in allowed_rooms
    if not is_allowed:
        logger.warning(f"⛔ Комната {room_id} НЕ в списке разрешенных: {allowed_rooms}")
    return is_allowed


def build_menu_text(filename: str) -> str:
    text = f"Получен файл: **{filename}**\n\nКуда его сохранить? Ответьте цифрой:\n"
    for key, folder_path in TARGET_FOLDERS.items():
        folder_name = os.path.basename(folder_path)
        text += f"{key}. {folder_name}\n"
    text += "\nИли отправьте `/cancel` для отмены."
    return text


async def handle_file_event(room: MatrixRoom, event):
    """Общий обработчик входящих файлов и изображений"""
    # 🔥 ЛОГ №1: Мы вообще получили это событие?
    logger.info(f"📥 ПОЛУЧЕНО СОБЫТИЕ ФАЙЛА! Room: {room.room_id}, Sender: {event.sender}, Type: {type(event).__name__}")

    if not is_allowed_room(room.room_id):
        return  # Лог уже выведется внутри is_allowed_room

    content = event.source.get("content", {})
    mimetype = content.get("info", {}).get("mimetype", "")

    # 🔥 ЛОГ №2: Какой MIME-тип видит бот?
    logger.info(f"🔍 Проверка MIME-типа: '{mimetype}'")

    if mimetype not in ALLOWED_MIMETYPES:
        logger.warning(f"Файл отклонён (неподдерживаемый тип): {mimetype}. Разрешенные: {ALLOWED_MIMETYPES}")
        await send_message(room.room_id, f"⚠️ Формат `{mimetype}` не поддерживается.")
        return

    filename = content.get("body", "unknown_file")
    mxc_url = content.get("url")
    if not mxc_url:
        logger.warning("Файл без URL, пропускаю")
        return

    logger.info(f"✅ Файл принят в работу: {filename} от {event.sender}")
    # Проверяем, нет ли у пользователя уже активного состояния
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

    # Скачиваем файл
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

    await state.set_pending(room.room_id, event.sender, temp_path, filename)
    await send_message(room.room_id, build_menu_text(filename))

async def on_room_message(room: MatrixRoom, event):
    #######Обработчик текстовых сообщений
    if event.sender == client.user_id:
        return

    if not is_allowed_room(room.room_id):
        return

    if not isinstance(event, RoomMessageText):
        return

    text = event.body.strip()
    logger.debug(f"Текстовое сообщение от {event.sender}: {text}")

    # Команда отмены
    if text.lower() == "/cancel":
        pending = await state.get_pending(room.room_id, event.sender)
        if pending:
            if os.path.exists(pending["temp_path"]):
                try:
                    os.remove(pending["temp_path"])
                except OSError as e:
                    logger.error(f"Не удалось удалить {pending['temp_path']}: {e}")
            await state.delete_pending(room.room_id, event.sender)
            await send_message(room.room_id, "Операция отменена.")
            logger.info(f"Операция отменена пользователем {event.sender}")
        return

    # Обработка выбора папки
    pending = await state.get_pending(room.room_id, event.sender)
    if not pending:
        return

    choice = text
    if choice not in TARGET_FOLDERS:
        await send_message(
            room.room_id,
            "Неверный выбор. Пожалуйста, ответьте цифрой из списка или отправьте `/cancel`.",
        )
        return

    src_path = pending["temp_path"]
    dst_dir = TARGET_FOLDERS[choice]
    os.makedirs(dst_dir, exist_ok=True)

    final_filename = pending["filename"]
    dst_path = os.path.join(dst_dir, final_filename)

    # Защита от перезаписи
    if os.path.exists(dst_path):
        name, ext = os.path.splitext(final_filename)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst_path = os.path.join(dst_dir, f"{name}_{ts}{ext}")

    try:
        shutil.move(src_path, dst_path)
        logger.info(f"Файл перемещён: {src_path} -> {dst_path}")
        await send_message(
            room.room_id,
            f"✅ Файл сохранён в папку `{os.path.basename(dst_dir)}`:\n`{dst_path}`",
        )
    except OSError as e:
        logger.error(f"Ошибка перемещения файла: {e}")
        await send_message(room.room_id, f"❌ Ошибка при сохранении: {e}")
        return

    await state.delete_pending(room.room_id, event.sender)



async def orphan_cleanup_loop():
    ########Фоновая задача для очистки orphan-файлов
    while True:
        try:
            await asyncio.sleep(600)
            if not os.path.exists(DOWNLOAD_DIR):
                continue

            active_paths = set()
            async for _, data in state.iter_all_pending():
                active_paths.add(data.get("temp_path"))

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


async def main():
    # Авторизация
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


    client.add_event_callback(on_room_message, RoomMessageText)
    client.add_event_callback(handle_file_event, RoomMessageFile)
    client.add_event_callback(handle_file_event, RoomMessageImage)


    logger.info("Callback'и зарегистрированы. Запускаю sync_forever...")

    cleanup_task = asyncio.create_task(orphan_cleanup_loop())

    try:
        await client.sync_forever(timeout=30000, full_state=False)
    except asyncio.CancelledError:
        logger.info("sync_forever отменён")
    except Exception as e:
        logger.error(f"Критическая ошибка в sync_forever: {e}", exc_info=True)
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
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