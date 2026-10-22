import json
import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv


class ConfigError(RuntimeError):
    pass


@dataclass
class MtrxBot:
    mtrx_token: str
    mtrx_url: str
    user_pass: str
    mtrx_room_id: str
    mtrx_room_id_priv: str
    mtrx_user_id: str
    mtrx_dev_id: str
    admin_users: list[str] = field(default_factory=list)


@dataclass
class RedisConfig:
    host: str
    port: int
    password: str


@dataclass
class LoggingConfig:
    level: str
    log_file: str
    max_bytes: int
    backup_count: int


@dataclass
class SMBConfig:
    username: str
    password: str
    port: int = 445

@dataclass
class DropboxConfig:
    enabled: bool
    users_file: str
    bindings_file: str
    room_name_template: str
    max_file_mb: int
    retention_days: int

@dataclass
class Config:
    db: RedisConfig
    mtrx_bot: MtrxBot
    logging: LoggingConfig
    smb: SMBConfig
    folders_file: str
    dropbox: DropboxConfig


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Не задана обязательная переменная окружения: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"Переменная {name} должна быть целым числом, получено: {raw!r}")


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("true", "1", "yes")


def load_config(path: str | None = None) -> Config:
    if path:
        load_dotenv(path)
    else:
        load_dotenv()

    token = os.getenv("TOKEN_MATRIX", "").strip()
    password = os.getenv("USER_PASS", "").strip()
    if not token and not password:
        raise ConfigError("Нужна одна из переменных: TOKEN_MATRIX или USER_PASS")

    admin_users = [u.strip() for u in os.getenv("ADMIN_USERS", "").split(",") if u.strip()]

    return Config(
        db=RedisConfig(
            host=os.getenv("DB_HOST", "localhost"),
            port=_int("DB_PORT", 6379),
            password=os.getenv("DB_PASSWORD", ""),
        ),
        mtrx_bot=MtrxBot(
            mtrx_token=token,
            mtrx_url=_required("MATRIX_URL"),
            user_pass=password,
            mtrx_user_id=_required("MATRIX_USER_ID"),
            mtrx_room_id=os.getenv("ROOM_ID", "").strip(),
            mtrx_room_id_priv=os.getenv("ROOM_ID_PRIV", "").strip(),
            mtrx_dev_id=os.getenv("MATRIX_DEVICE_ID", "").strip(),
            admin_users=admin_users,
        ),
        smb=SMBConfig(
            username=os.getenv("SMB_USERNAME", ""),
            password=os.getenv("SMB_PASSWORD", ""),
            port=_int("SMB_PORT", 445),
        ),
        logging=LoggingConfig(
            level=os.getenv("LOG_LEVEL", "INFO").upper(),
            log_file=os.getenv("LOG_FILE", "/app/logs/bot.log"),
            max_bytes=_int("LOG_MAX_BYTES", 10 * 1024 * 1024),
            backup_count=_int("LOG_BACKUP_COUNT", 5),
        ),
        folders_file=os.getenv("FOLDERS_FILE", "/app/config/folders.json"),
        dropbox=DropboxConfig(
            enabled=_bool("DROPBOX_ENABLED", False),
            users_file=os.getenv("DROP_USERS_FILE", "/app/config/drop_users.json"),
            bindings_file=os.getenv("DROP_BINDINGS_FILE", "/app/config/personal_rooms.json"),
            room_name_template=os.getenv("DROP_ROOM_NAME", "Личный Dropbox — {display_name}"),
            max_file_mb=_int("DROP_MAX_FILE_MB", 50),
            retention_days=_int("DROP_RETENTION_DAYS", 14),
        ),
    )


############################################################################################
ALLOWED_DOCUMENT_MIMETYPES = [
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/msword",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
]
ALLOWED_IMAGE_MIMETYPES = [
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "image/heic",
    "image/heif",
]
ALLOWED_MIMETYPES = ALLOWED_DOCUMENT_MIMETYPES + ALLOWED_IMAGE_MIMETYPES


def validate_folders(data) -> dict[str, str]:
    """Проверяет структуру списка папок. Бросает ValueError при ошибке."""
    if not isinstance(data, dict):
        raise ValueError("Ожидается JSON-объект {\"1\": \"путь\", ...}")
    result: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError(f"Ключ и значение должны быть строками: {key!r}: {value!r}")
        if not key or key != key.strip():
            raise ValueError(f"Недопустимый ключ: {key!r}")
        if key == "*":
            raise ValueError("Ключ '*' зарезервирован для личной папки")
        if key.startswith("!"):
            raise ValueError(f"Ключ не может начинаться с '!': {key!r}")
        if not value.strip():
            raise ValueError(f"Пустой путь для ключа {key!r}")
        result[key] = value.strip()
    return result


def load_folders(folders_file: str) -> Optional[dict[str, str]]:
    #  {} - файла нет (первый запуск) или он пустой.
    # None - файл есть, но повреждён/невалиден (вызывающий код не должен затирать текущий список).

    if not os.path.exists(folders_file):
        return {}
    try:
        with open(folders_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return validate_folders(data)
    except (json.JSONDecodeError, OSError, ValueError) as e:
        print(f"Ошибка загрузки {folders_file}: {e}")
        return None


def save_folders(folders_file: str, folders: dict[str, str]) -> None:
    os.makedirs(os.path.dirname(folders_file) or ".", exist_ok=True)
    tmp = folders_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(folders, f, indent=2, ensure_ascii=False)
    os.replace(tmp, folders_file)
