import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("matrix_bot")

@dataclass
class DropUser:
    user_id: str
    display_name: str
    smb_path: str

@dataclass
class RoomTarget:
    room_id: str
    owner: str
    folder: str

def load_drop_users(path: str) -> dict[str, DropUser]:
    """Загружает whitelist пользователей для персонального дропа"""
    if not os.path.exists(path):
        logger.warning(f"Файл пользователей не найден: {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Ошибка чтения drop_users.json: {e}")
        return {}

    if not isinstance(raw, dict):
        logger.error("Неизвестный формат drop_users.json")
        return {}

    result = {}
    for user_id, data in raw.items():
        if not user_id.startswith("@"):
            continue
        localpart = user_id[1:].split(":")[0]
        if isinstance(data, str):
            # Простой формат: путь как строка
            path = data.replace("smb://", "smb:/")
            display_name = os.path.basename(path.rstrip("/"))
            result[user_id] = DropUser(user_id, display_name, path)
        elif isinstance(data, dict):
            # Расширенный формат: {display_name, path}
            path = data.get("path", "").replace("smb://", "smb:/")
            display_name = data.get("display_name", os.path.basename(path.rstrip("/")))
            result[user_id] = DropUser(user_id, display_name, path)

    return result

# Атомарная привязка в двух направлениях
_BIND_LUA = """
local existing_room = redis.call('HGET', KEYS[1], ARGV[1])
if existing_room then
    return existing_room
end
local existing_owner = redis.call('HGET', KEYS[2], ARGV[2])
if existing_owner and existing_owner ~= ARGV[1] then
    return false
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return ARGV[2]
"""

class DropRoomManager:
    """Управление персональными комнатами и их привязкой к пользователям"""
    HASH_ROOMS = "drop:rooms"
    HASH_OWNERS = "drop:owners"
    LOCK_PREFIX = "drop:lock:"

    def __init__(
        self,
        client,
        redis_client,
        users: dict[str, DropUser],
        bindings_file: str,
        *,
        name_template: str = "Личный Dropbox — {display_name}",
    ):
        self.client = client
        self.redis = redis_client
        self.users = users
        self.bindings_file = bindings_file
        self.name_template = name_template

    async def owner_of(self, room_id: str) -> Optional[str]:
        """Кому принадлежит комната"""
        return await self.redis.hget(self.HASH_OWNERS, room_id)

    async def room_of(self, user_id: str) -> Optional[str]:
        """Комната пользователя"""
        return await self.redis.hget(self.HASH_ROOMS, user_id)

    def members_of(self, room_id: str) -> set[str]:
        """Участники комнаты из кэша синхронизации"""
        room = self.client.rooms.get(room_id)
        if room and room.users:
            return set(room.users.keys())
        return set()

    async def _find_existing_dm(self, user_id: str) -> Optional[str]:
        """Ищет уже существующий диалог 1:1 с пользователем"""
        bot_id = self.client.user_id
        for room_id, room in self.client.rooms.items():
            members = set(room.users.keys()) if room.users else set()
            if len(members) == 2 and user_id in members and bot_id in members:
                return room_id
        return None

    async def persist(self):
        """Сохраняет реестр привязок в JSON"""
        pairs = await self.redis.hgetall(self.HASH_ROOMS)
        tmp = self.bindings_file + ".tmp"
        os.makedirs(os.path.dirname(self.bindings_file) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.bindings_file)
        logger.info(f"Реестр персональных комнат сохранён: {len(pairs)} записей")

    async def restore(self):
        """Восстанавливает Redis из файла если Redis пуст"""
        if await self.redis.exists(self.HASH_ROOMS):
            logger.info("Реестр персональных комнат уже в Redis")
            return

        if not os.path.exists(self.bindings_file):
            return

        try:
            with open(self.bindings_file, "r", encoding="utf-8") as f:
                pairs = json.load(f)
            if not isinstance(pairs, dict):
                return
            for user_id, room_id in pairs.items():
                if user_id not in self.users or not isinstance(room_id, str):
                    continue
                await self.redis.hsetnx(self.HASH_ROOMS, user_id, room_id)
                await self.redis.hsetnx(self.HASH_OWNERS, room_id, user_id)
            logger.info(f"Реестр персональных комнат восстановлен из файла: {len(pairs)} записей")
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Ошибка восстановления реестра: {e}")

    async def bind(self, user_id: str, room_id: str) -> Optional[str]:
        """Привязывает существующую комнату к пользователю"""
        if user_id not in self.users:
            return None

        result = await self.redis.eval(
            _BIND_LUA, 2, self.HASH_ROOMS, self.HASH_OWNERS, user_id, room_id
        )
        if not result:
            logger.warning(f"Комната {room_id} уже принадлежит другому пользователю")
            return None

        await self.persist()
        logger.info(f"Персональная комната привязана: {user_id} -> {room_id}")
        return result

    async def get_or_create(self, user_id: str) -> tuple[Optional[str], Optional[str]]:
        """Возвращает (room_id, error). Создаёт комнату только если её нет."""
        if user_id not in self.users:
            return None, "У вас нет доступа к персональному дропу. Обратитесь к администратору."

        # 1. Уже есть привязка
        existing = await self.room_of(user_id)
        if existing and existing in self.client.rooms:
            return existing, None

        # 2. Блокировка от параллельных /drop
        lock_key = f"{self.LOCK_PREFIX}{user_id}"
        token = uuid.uuid4().hex
        acquired = await self.redis.set(lock_key, token, nx=True, ex=30)
        if not acquired:
            return None, "Комната уже создаётся. Повторите запрос через несколько секунд."

        try:
            # Повторная проверка
            existing = await self.room_of(user_id)
            if existing:
                return existing, None

            # 3. Ищем существующий личный диалог
            found = await self._find_existing_dm(user_id)
            if found:
                bound = await self.bind(user_id, found)
                if bound:
                    return bound, None

            # 4. Создаём новую приватную комнату
            drop_user = self.users[user_id]
            room_name = self.name_template.format(
                display_name=drop_user.display_name,
                localpart=user_id[1:].split(":")[0]
            )

            power_levels = {
                "users": {self.client.user_id: 100, user_id: 0},
                "users_default": 0,
                "events_default": 0,
                "state_default": 50,
                "invite": 50,
                "kick": 100,
                "ban": 100,
                "redact": 100,
                "events": {
                    "m.room.name": 50,
                    "m.room.topic": 50,
                    "m.room.power_levels": 100,
                    "m.room.join_rules": 50,
                    "m.room.history_visibility": 100,
                    "m.room.encryption": 100,
                },
            }

            resp = await self.client.room_create(
                visibility="private",
                preset="private_chat",
                room_name=room_name,
                topic=(
                    "Персональный приёмник файлов. "
                    f"Файлы автоматически сохраняются в {drop_user.smb_path}."
                ),
                invite=[user_id],
                is_direct=True,
                creation_content={"m.federate": False},
                power_level_content_override=power_levels,
                initial_state=[
                    {
                        "type": "m.room.history_visibility",
                        "state_key": "",
                        "content": {"history_visibility": "shared"},
                    },
                    {
                        "type": "m.room.join_rules",
                        "state_key": "",
                        "content": {"join_rule": "invite"},
                    },
                ],
            )

            new_room = getattr(resp, "room_id", None)
            if not new_room:
                message = getattr(resp, "message", str(resp))
                logger.error(f"Не удалось создать персональную комнату: {message}")
                return None, "Не удалось создать личную комнату. Сообщите администратору."

            bound = await self.bind(user_id, new_room)
            if not bound:
                return None, "Не удалось закрепить комнату за вами. Сообщите администратору."

            # Приветствие
            await self.client.room_send(bound, "m.room.message", {
                "msgtype": "m.text",
                "body": (
                    "Это ваш персональный Dropbox.\n"
                    "Отправляйте сюда изображения и документы - они автоматически "
                    f"попадут в вашу папку {drop_user.smb_path}.\n"
                    "При отправке файла появится меню выбора папки."
                ),
            })

            logger.info(f"Создана персональная комната: {user_id} -> {bound}")
            return bound, None
        finally:
            current = await self.redis.get(lock_key)
            if current == token:
                await self.redis.delete(lock_key)

    async def unbind(self, user_id: str) -> Optional[str]:
        """Снимает привязку"""
        room_id = await self.room_of(user_id)
        if not room_id:
            return None
        await self.redis.hdel(self.HASH_ROOMS, user_id)
        await self.redis.hdel(self.HASH_OWNERS, room_id)
        await self.persist()
        logger.info(f"Привязка снята: {user_id}")
        return room_id

    async def resolve(self, room_id: str, sender: str) -> Optional[RoomTarget]:
        """Определяет, является ли комната персональной и принадлежит ли она отправителю"""
        owner = await self.owner_of(room_id)
        if not owner:
            return None

        if sender != owner:
            logger.warning(f"Невладелец {sender} пытается отправить файл в персональную комнату {room_id}")
            return None

        if owner not in self.users:
            logger.warning(f"Пользователь {owner} больше нет в whitelist, комната {room_id} заблокирована")
            return None

        return RoomTarget(room_id=room_id, owner=owner, folder=self.users[owner].smb_path)