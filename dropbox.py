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
    smb_path: str  # Полный путь, например: smb:/srve1.silar.local/technology/Кузнецова/MATRIX_DROPBOX


@dataclass
class RoomTarget:
    kind: str  # "general" или "drop"
    room_id: str
    owner: Optional[str] = None
    folder: Optional[str] = None


def _normalize_smb_path(path: str) -> str:
     #Нормализует smb:// в smb:/ и добавляет MATRIX_DROPBOX
    if path.startswith("smb://"):
        path = "smb:/" + path[6:]
    if not path.endswith("MATRIX_DROPBOX"):
        path = os.path.join(path, "MATRIX_DROPBOX")
    return path


def load_drop_users(path: str) -> dict[str, DropUser]:
     #Загружает whitelist. Поддерживает JSON и TSV. Добавляет MATRIX_DROPBOX.
    if not os.path.exists(path):
        logger.warning(f"Файл пользователей не найден: {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as e:
        logger.error(f"Ошибка чтения {path}: {e}")
        return {}

    result = {}

    # Пробуем прочитать как JSON
    try:
        raw = json.loads(content)
        if isinstance(raw, dict):
            for user_id, data in raw.items():
                if not user_id.startswith("@"):
                    continue
                localpart = user_id[1:].split(":")[0]
                if isinstance(data, str):
                    result[user_id] = DropUser(user_id, localpart, _normalize_smb_path(data))
                elif isinstance(data, dict):
                    full_path = data.get("path", "")
                    if full_path:
                        result[user_id] = DropUser(
                            user_id=user_id,
                            display_name=data.get("display_name", localpart),
                            smb_path=_normalize_smb_path(full_path),
                        )
            logger.info(f"Загружено пользователей из JSON: {len(result)}")
            return result
    except json.JSONDecodeError:
        pass  # Переходим к TSV

    # Читаем как TSV (user_id\tpath)
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            logger.warning(f"Пропущена строка (недостаточно колонок): {line}")
            continue

        user_id = parts[0].strip()
        path = parts[1].strip()

        if not user_id.startswith("@"):
            logger.warning(f"Пропущена строка (неверный user_id): {line}")
            continue

        localpart = user_id[1:].split(":")[0]
        result[user_id] = DropUser(user_id, localpart, _normalize_smb_path(path))

    logger.info(f"Загружено пользователей из TSV: {len(result)}")
    return result


# Атомарная привязка в двух направлениях
_BIND_LUA =  """
local existing_room = redis.call('HGET', KEYS[1], ARGV[1])
if existing_room then return existing_room end
local existing_owner = redis.call('HGET', KEYS[2], ARGV[2])
if existing_owner and existing_owner ~= ARGV[1] then return false end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return ARGV[2]
"""


class DropRoomManager:
     #Управление персональными комнатами и их привязкой к пользователям
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
        excluded_rooms: Optional[set[str]] = None,
    ):
        self.client = client
        self.redis = redis_client
        self.users = users
        self.bindings_file = bindings_file
        self.excluded_rooms = excluded_rooms or set()

    async def owner_of(self, room_id: str) -> Optional[str]:
         #Кому принадлежит комната
        return await self.redis.hget(self.HASH_OWNERS, room_id)

    async def room_of(self, user_id: str) -> Optional[str]:
         #Комната пользователя
        return await self.redis.hget(self.HASH_ROOMS, user_id)

    def members_of(self, room_id: str) -> set[str]:
         #Участники комнаты из кэша синхронизации
        room = self.client.rooms.get(room_id)
        return set(room.users.keys()) if room and room.users else set()

    async def _find_existing_dm(self, user_id: str) -> Optional[str]:
         #Ищет уже существующий диалог 1:1 с пользователем
        bot_id = self.client.user_id
        for room_id, room in self.client.rooms.items():
            if room_id in self.excluded_rooms:
                continue
            members = set(room.users.keys()) if room.users else set()
            if len(members) == 2 and user_id in members and bot_id in members:
                return room_id
        return None

    async def _rename_room(self, room_id: str, user_id: str) -> None:
         #Переименовывает комнату в 'Личный Dropbox — {display_name}'
        try:
            import asyncio
            await asyncio.sleep(0.5)

            display_name = "Пользователь"
            if user_id in self.users:
                display_name = self.users[user_id].display_name

            room_name = f"Личный Dropbox — {display_name}"

            await self.client.room_put_state(
                room_id,
                "m.room.name",
                {"name": room_name},
            )
            logger.info(f"Комната {room_id} переименована в '{room_name}'")
        except Exception as e:
            logger.warning(f"Не удалось переименовать комнату {room_id}: {e}")

    async def persist(self):
         #Сохраняет реестр привязок в JSON
        pairs = await self.redis.hgetall(self.HASH_ROOMS)
        tmp = self.bindings_file + ".tmp"
        os.makedirs(os.path.dirname(self.bindings_file) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.bindings_file)

    async def restore(self):
         #Восстанавливает Redis из файла если Redis пуст
        if await self.redis.exists(self.HASH_ROOMS):
            return
        if not os.path.exists(self.bindings_file):
            return
        try:
            with open(self.bindings_file, "r", encoding="utf-8") as f:
                pairs = json.load(f)
            if isinstance(pairs, dict):
                for user_id, room_id in pairs.items():
                    if isinstance(room_id, str):
                        await self.redis.hsetnx(self.HASH_ROOMS, user_id, room_id)
                        await self.redis.hsetnx(self.HASH_OWNERS, room_id, user_id)
        except Exception as e:
            logger.error(f"Ошибка восстановления реестра: {e}")

    async def bind(self, user_id: str, room_id: str) -> Optional[str]:
         #Привязывает существующую комнату к пользователю
        if room_id in self.excluded_rooms:
            return None
        result = await self.redis.eval(
            _BIND_LUA, 2, self.HASH_ROOMS, self.HASH_OWNERS, user_id, room_id
        )
        if result:
            await self.persist()
            logger.info(f"Персональная комната привязана: {user_id} -> {room_id}")
            return result
        return None

    async def get_or_create(self, user_id: str) -> tuple[Optional[str], Optional[str]]:
        # Возвращает (room_id, error). Создаёт комнату только если её нет.
        if user_id not in self.users:
            return None, "У вас нет доступа к персональному дропу."

        # Уже есть привязка
        existing = await self.room_of(user_id)
        if existing and existing in self.client.rooms:
            room = self.client.rooms[existing]
            members = set(room.users.keys()) if room.users else set()
            if user_id not in members:
                try:
                    await self.client.room_invite(existing, user_id)
                except Exception as e:
                    logger.error(f"Ошибка приглашения {user_id} в {existing}: {e}")
            await self._rename_room(existing, user_id)
            return existing, None

        # Блокировка от параллельных запросов
        lock_key = f"{self.LOCK_PREFIX}{user_id}"
        token = uuid.uuid4().hex
        if not await self.redis.set(lock_key, token, nx=True, ex=30):
            return None, "Комната уже создаётся. Повторите запрос через несколько секунд."

        try:
            # Повторная проверка
            existing = await self.room_of(user_id)
            if existing and existing in self.client.rooms:
                await self._rename_room(existing, user_id)
                return existing, None

            # Ищем существующий личный диалог 1:1
            found = await self._find_existing_dm(user_id)
            if found:
                room = self.client.rooms[found]
                members = set(room.users.keys()) if room.users else set()
                if user_id not in members:
                    try:
                        await self.client.room_invite(found, user_id)
                    except Exception as e:
                        logger.error(f"Ошибка приглашения {user_id} в {found}: {e}")

                bound = await self.bind(user_id, found)
                if bound:
                    await self._rename_room(bound, user_id)
                    return bound, None

            # 4. Создаём новую приватную комнату
            drop_user = self.users[user_id]
            room_name = f"Личный Dropbox — {drop_user.display_name}"

            power_levels = {
                "users": {self.client.user_id: 100, user_id: 0},
                "users_default": 0, "events_default": 0, "state_default": 50,
                "invite": 50, "kick": 100, "ban": 100, "redact": 100,
                "events": {
                    "m.room.name": 50, "m.room.topic": 50, "m.room.power_levels": 100,
                    "m.room.join_rules": 50, "m.room.history_visibility": 100, "m.room.encryption": 100,
                },
            }

            resp = await self.client.room_create(
                visibility="private",
                preset="private_chat",
                room_name=room_name,
                topic=f"Персональный приёмник файлов. Сохраняется в: {drop_user.smb_path}",
                invite=[user_id],
                is_direct=True,
                creation_content={"m.federate": False},
                power_level_content_override=power_levels,
                initial_state=[
                    {"type": "m.room.history_visibility", "state_key": "", "content": {"history_visibility": "shared"}},
                    {"type": "m.room.join_rules", "state_key": "", "content": {"join_rule": "invite"}},
                ],
            )

            new_room = getattr(resp, "room_id", None)
            if not new_room:
                message = getattr(resp, "message", str(resp))
                logger.error(f"Не удалось создать персональную комнату: {message}")
                return None, "Не удалось создать комнату. Сообщите администратору."

            bound = await self.bind(user_id, new_room)
            if not bound:
                return None, "Не удалось закрепить комнату."

            await self._rename_room(bound, user_id)

            await self.client.room_send(bound, "m.room.message", {
                "msgtype": "m.text",
                "body": (
                    "Это ваш персональный Dropbox.\n"
                    "Отправляйте сюда файлы. Вы сможете выбрать: сохранить их в вашу личную папку (*) или в одну из общих."
                ),
            })

            logger.info(f"Создана персональная комната: {user_id} -> {bound}")
            return bound, None
        finally:
            if await self.redis.get(lock_key) == token:
                await self.redis.delete(lock_key)

    async def unbind(self, user_id: str) -> Optional[str]:
         #Снимает привязку
        room_id = await self.room_of(user_id)
        if not room_id:
            return None
        await self.redis.hdel(self.HASH_ROOMS, user_id)
        await self.redis.hdel(self.HASH_OWNERS, room_id)
        await self.persist()
        logger.info(f"Привязка снята: {user_id}")
        return room_id

    async def resolve(self, room_id: str, sender: str) -> Optional[RoomTarget]:
        # Определяет, является ли комната персональной и принадлежит ли она отправителю
        if room_id in self.excluded_rooms:
            return RoomTarget(kind="general", room_id=room_id)

        owner = await self.owner_of(room_id)
        if owner:
            if sender != owner or owner not in self.users:
                return None
            return RoomTarget(kind="drop", room_id=room_id, owner=owner, folder=self.users[owner].smb_path)

        # Если комната не зарегистрирована, но отправитель в whitelist, разрешаем команды
        if sender in self.users:
            return RoomTarget(kind="general", room_id=room_id)

        return None