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
    server: str  # smb сервер
    share: str  # smb шара
    relative_path: str  # Путь внутри шары (включая MATRIX_DROPBOX)


@dataclass
class RoomTarget:
    kind: str
    room_id: str
    owner: Optional[str] = None
    folder: Optional[str] = None  # Полный путь в формате smb://server/share/path
    server: Optional[str] = None
    share: Optional[str] = None
    relative_path: Optional[str] = None


def _parse_smb_path(path: str) -> tuple[str, str, str]:
    #Разбирает smb путь на (server, share, relative_path)
    # Нормализуем smb:// в smb:/
    if path.startswith("smb://"):
        path = "smb:/" + path[6:]

    if not path.startswith("smb:/"):
        raise ValueError(f"Неверный формат smb пути: {path}")

    # Убирает префикс smb:/
    rest = path[5:]
    parts = rest.strip("/").split("/", 2)

    if len(parts) < 2:
        raise ValueError(f"Неверный формат smb пути: {path}")

    server = parts[0]
    share = parts[1]
    relative_path = parts[2] if len(parts) > 2 else ""

    return server, share, relative_path


def load_drop_users(path: str, path_template: str) -> dict[str, DropUser]:
    #Загрузка whitelist пользователей для персонального дропа. Поддерживает форматы: JSON и TSV. Автоматически добавляет MATRIX_DROPBOX к пути.

    if not os.path.exists(path):
        logger.warning(f"Файл пользователей не найден: {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as e:
        logger.error(f"Ошибка чтения drop_users.json: {e}")
        return {}

    result = {}

    # Пробуем JSON
    try:
        raw = json.loads(content)

        # Простой список MXID
        if isinstance(raw, list):
            for user_id in raw:
                if not isinstance(user_id, str) or not user_id.startswith("@"):
                    continue
                localpart = user_id[1:].split(":")[0]
                full_path = path_template.format(localpart=localpart, user_id=user_id)
                server, share, rel_path = _parse_smb_path(full_path)
                # Добавляем MATRIX_DROPBOX
                rel_path = os.path.join(rel_path, "MATRIX_DROPBOX") if rel_path else "MATRIX_DROPBOX"
                result[user_id] = DropUser(
                    user_id=user_id,
                    display_name=localpart,
                    server=server,
                    share=share,
                    relative_path=rel_path,
                )
            return result

        # Словарь MXID -> {display_name, path} или MXID -> path
        if isinstance(raw, dict):
            for user_id, data in raw.items():
                if not user_id.startswith("@"):
                    continue
                localpart = user_id[1:].split(":")[0]

                if isinstance(data, str):
                    full_path = data if data.startswith("smb:") else path_template.format(localpart=localpart)
                    server, share, rel_path = _parse_smb_path(full_path)
                    rel_path = os.path.join(rel_path, "MATRIX_DROPBOX") if rel_path else "MATRIX_DROPBOX"
                    result[user_id] = DropUser(user_id, localpart, server, share, rel_path)
                elif isinstance(data, dict):
                    full_path = data.get("path") or path_template.format(localpart=localpart, user_id=user_id)
                    server, share, rel_path = _parse_smb_path(full_path)
                    rel_path = os.path.join(rel_path, "MATRIX_DROPBOX") if rel_path else "MATRIX_DROPBOX"
                    result[user_id] = DropUser(
                        user_id=user_id,
                        display_name=data.get("display_name", localpart),
                        server=server,
                        share=share,
                        relative_path=rel_path,
                    )
            return result

        logger.error("Неизвестный формат drop_users.json")
        return {}

    except json.JSONDecodeError:
        #  TSV
        pass

    # TSV: user_id\tpath
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) < 2:
            continue

        user_id = parts[0].strip()
        path = parts[1].strip()

        if not user_id.startswith("@"):
            continue

        localpart = user_id[1:].split(":")[0]

        try:
            server, share, rel_path = _parse_smb_path(path)
            rel_path = os.path.join(rel_path, "MATRIX_DROPBOX") if rel_path else "MATRIX_DROPBOX"
            result[user_id] = DropUser(user_id, localpart, server, share, rel_path)
        except ValueError as e:
            logger.warning(f"Пропущена строка в drop_users.json: {e}")

    return result


def save_drop_users(path: str, users: dict[str, DropUser]) -> None:
    #Пересохраняет whitelist пользователей в JSON формате
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    data = {}
    for user_id, user in users.items():
        # Восстанавливаем полный путь
        full_path = f"smb://{user.server}/{user.share}/{user.relative_path}"
        data[user_id] = {
            "display_name": user.display_name,
            "path": full_path,
        }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


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
            name_template: str = "Личный Dropbox — {display_name}",
            excluded_rooms: Optional[set[str]] = None,
    ):
        self.client = client
        self.redis = redis_client
        self.users = users
        self.bindings_file = bindings_file
        self.name_template = name_template
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
        if room and room.users:
            return set(room.users.keys())
        return set()

    async def _find_existing_dm(self, user_id: str) -> Optional[str]:
        #Поиск уже существующего приватного диалога с пользователем
        bot_id = self.client.user_id
        for room_id, room in self.client.rooms.items():
            if room_id in self.excluded_rooms:
                continue
            members = set(room.users.keys()) if room.users else set()
            if len(members) == 2 and user_id in members and bot_id in members:
                return room_id
        return None

    async def persist(self):
        #Cохранеybt реестрf привязок в JSON
        pairs = await self.redis.hgetall(self.HASH_ROOMS)
        tmp = self.bindings_file + ".tmp"
        os.makedirs(os.path.dirname(self.bindings_file) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.bindings_file)
        logger.info(f"Реестр персональных комнат сохранён: {len(pairs)} записей")

    async def restore(self):
        #Восстанавливает Redis из файла если Redis пуст
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
                if not isinstance(room_id, str):
                    continue
                await self.redis.hsetnx(self.HASH_ROOMS, user_id, room_id)
                await self.redis.hsetnx(self.HASH_OWNERS, room_id, user_id)
            logger.info(f"Реестр персональных комнат восстановлен из файла: {len(pairs)} записей")
        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Ошибка восстановления реестра: {e}")

    async def bind(self, user_id: str, room_id: str) -> Optional[str]:
        #Привязка существующей комнаты к пользователю
        if room_id in self.excluded_rooms:
            logger.warning(f"Попытка привязать служебную комнату как персональную: {room_id}")
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
        #Возвращает (room_id, error). Создаёт комнату только если её неn
        if user_id not in self.users:
            return None, "У вас нет доступа к персональному дропу. Обратитесь к администратору."

        # Уже есть привязка
        existing = await self.room_of(user_id)
        if existing and existing in self.client.rooms:
            return existing, None

        # Блокировка от параллельных /drop
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

            # Ищем существующий личный диалог
            found = await self._find_existing_dm(user_id)
            if found:
                bound = await self.bind(user_id, found)
                if bound:
                    return bound, None

            # Создаём новую приватную комнату
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
                    f"Файлы автоматически сохраняются в {drop_user.server}/{drop_user.share}/{drop_user.relative_path}."
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
                    f"попадут в вашу папку {drop_user.server}/{drop_user.share}/{drop_user.relative_path}.\n"
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
        #Снятие привязки
        room_id = await self.room_of(user_id)
        if not room_id:
            return None
        await self.redis.hdel(self.HASH_ROOMS, user_id)
        await self.redis.hdel(self.HASH_OWNERS, room_id)
        await self.persist()
        logger.info(f"Привязка снята: {user_id}")
        return room_id

    async def resolve(self, room_id: str, sender: str) -> Optional[RoomTarget]:
        #Определяет, является ли комната персональной и принадлежит ли она отправителю
        if room_id in self.excluded_rooms:
            return RoomTarget(kind="general", room_id=room_id)

        owner = await self.owner_of(room_id)
        if not owner:
            return None

        if sender != owner:
            logger.warning(f"Невладелец {sender} пытается отправить файл в персональную комнату {room_id}")
            return None

        if owner not in self.users:
            logger.warning(f"Пользователь {owner} больше нет в whitelist, комната {room_id} заблокирована")
            return None

        user = self.users[owner]
        # Восстанавливаем полный путь
        full_path = f"smb://{user.server}/{user.share}/{user.relative_path}"
        return RoomTarget(
            kind="drop",
            room_id=room_id,
            owner=owner,
            folder=full_path,
            server=user.server,
            share=user.share,
            relative_path=user.relative_path,
        )