import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
import json

load_dotenv()

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
    enabled: bool
    server_ip: str
    username: str
    password: str
    share_name: str
    port: int = 445

@dataclass
class Config:
    db: RedisConfig
    mtrx_bot: MtrxBot
    logging: LoggingConfig
    smb: SMBConfig
    folders_file: str


# Создаем экземпляр класса Config и наполняем его данными из переменных окружения
def load_config(path: str | None=None) -> Config:

    if path:
        load_dotenv(path)
    else:
        load_dotenv()
    folders_file = os.getenv("FOLDERS_FILE", "/app/config/folders.json")
    admin_users_raw = os.getenv("ADMIN_USERS", "")
    admin_users = [u.strip() for u in admin_users_raw.split(",") if u.strip()]
    return Config(
                db=RedisConfig(
                host=os.getenv("DB_HOST", ""),
                port=int(os.getenv("DB_PORT", "")),
                password=str(os.getenv("DB_PASSWORD", ""))
                ),
                mtrx_bot=MtrxBot(
                    mtrx_token=os.getenv("TOKEN_MATRIX"),
                    mtrx_url=os.getenv('MATRIX_URL'),
                    user_pass=os.getenv('USER_PASS'),
                    mtrx_user_id=os.getenv('MATRIX_USER_ID'),
                    mtrx_room_id=os.getenv('ROOM_ID'),
                    mtrx_room_id_priv=os.getenv('ROOM_ID_PRIV'),
                    mtrx_dev_id=os.getenv("MATRIX_DEVICE_ID"),
                    admin_users=admin_users,
                ),
                smb=SMBConfig(
                    enabled=os.getenv("SMB_ENABLED", "false").lower() in ("true", "1", "yes"),
                    server_ip=os.getenv("SMB_SERVER_IP", ""),
                    username=os.getenv("SMB_USERNAME", ""),
                    password=os.getenv("SMB_PASSWORD", ""),
                    share_name=os.getenv("SMB_SHARE_NAME", ""),
                    port=int(os.getenv("SMB_PORT", "445")),
                ),
                logging=LoggingConfig(
                    level=os.getenv("LOG_LEVEL", "INFO").upper(),
                    log_file=os.getenv("LOG_FILE", "bot.log"),
                    max_bytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),  # 10 MB
                    backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5"))
                        ),
                    folders_file=folders_file,)
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
    "image/heic",
    "image/heif",
]
ALLOWED_MIMETYPES = ALLOWED_DOCUMENT_MIMETYPES + ALLOWED_IMAGE_MIMETYPES

def load_folders(folders_file: str) -> dict[str, str]:

    if not os.path.exists(folders_file):
        return {}
    try:
        with open(folders_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Валидация
        result = {}
        for key, value in data.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            result[key] = value
        return result
    except (json.JSONDecodeError, OSError) as e:
        print(f"Ошибка загрузки folders.json: {e}")
        return {}


def save_folders(folders_file: str, folders: dict[str, str]) -> None:
    os.makedirs(os.path.dirname(folders_file) or ".", exist_ok=True)
    with open(folders_file, "w", encoding="utf-8") as f:
        json.dump(folders, f, indent=2, ensure_ascii=False)
