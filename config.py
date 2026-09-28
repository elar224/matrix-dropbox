import os
from dataclasses import dataclass
from dotenv import load_dotenv

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
class Config:
    db: RedisConfig
    mtrx_bot: MtrxBot
    logging: LoggingConfig


# Создаем экземпляр класса Config и наполняем его данными из переменных окружения
def load_config(path: str | None=None) -> Config:

    if path:
        load_dotenv(path)
    else:
        load_dotenv()
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
                    mtrx_dev_id=os.getenv("MATRIX_DEVICE_ID")
                ),
                logging=LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO").upper(),
                log_file=os.getenv("LOG_FILE", "bot.log"),
                max_bytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),  # 10 MB
                backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5"))
                    ))
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

# Список папок: ключ — цифра ответа, значение — путь
TARGET_FOLDERS = {
    "1": "/home/mkuznetcova/PycharmProjects/Matrix echo bot /matrix_downloads/1022",
    "2": "/home/mkuznetcova/PycharmProjects/Matrix echo bot /matrix_downloads/1088",
    "3": "/home/mkuznetcova/PycharmProjects/Matrix echo bot /matrix_downloads/Все подряд",
}