import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import Optional

import nio
from nio import RoomPreset, RoomVisibility

logger = logging.getLogger("matrix_bot")


@dataclass
class DropUser:
    user_id: str
    display_name: str
    smb_path: str  # например: smb:/srve1.silar.local/technology/Кузнецова/MATRIX_DROPBOX


@dataclass
class RoomTarget:
    kind: str  # "general" | "drop" | "other" (неприявязанная комната вайтлист-пользователя: только команды)
    room_id: str
    owner: Optional[str] = None
    folder: Optional[str] = None


def _normalize_smb_path(path: str) -> str:
    """smb://x -> smb:/x и гарантирует ровно один суффикс /MATRIX_DROPBOX."""
    path = path.strip().replace("\\", "/")
    if path.startswith("smb://"):
        path = "smb:/" + path[6:]
    path = path.rstrip("/")
    if not path.endswith("/MATRIX_DROPBOX"):
        path += "/MATRIX_DROPBOX"
    return path


def _make_user(user_id, data) -> Optional[DropUser]:
    if not isinstance(user_id, str) or not user_id.startswith("@"):
        logger.warning(f"Пропущен пользователь (неверный user_id): {user_id!r}")
        return None
    localpart = user_id[1:].split(":")[0]
    if isinstance(data, str):
        path, display = data, localpart
    elif isinstance(data, dict):
        path = data.get("path", "")
        display = data.get("display_name") or localpart
    else:
        logger.warning(f"Пропущен {user_id}: неверный формат значения")
        return None
    if not isinstance(path, str) or not path.strip():
        logger.warning(f"Пропущен {user_id}: пустой путь")
        return None
    path = _normalize_smb_path(path)
    if not path.startswith("smb:/"):
        logger.warning(f"Пропущен {user_id}: путь не начинается с smb:/ ({path})")
        return None
    return DropUser(user_id, str(display), path)


def parse_drop_users(content: str) -> dict[str, DropUser]:
    """Разбирает JSON или TSV. Невалидные записи пропускаются; пустой результат = ошибка на стороне вызывающего."""
    result: dict[str, DropUser] = {}

    try:
        raw = json.loads(content)
    except json.JSONDecodeError:
        raw = None
        is_json = False
    else:
        is_json = True

    if is_json:
        if not isinstance(raw, dict):
            return {}
        for user_id, data in raw.items():
            user = _make_user(user_id, data)
            if user:
                result[user_id] = user
        return result

    # TSV: user_id<TAB>path
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            logger.warning(f"Пропущена строка (недостаточно колонок): {line}")
            continue
        user = _make_user(parts[0].strip(), parts[1].strip())
        if user:
            result[user.user_id] = user
    return result


def load_drop_users(path: str) -> Optional[dict[str, DropUser]]:
    """None - файл не найден или не читается. Иначе словарь (возможно пустой)."""
    if not os.path.exists(path):
        logger.warning(f"Файл пользователей не найден: {path}")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except (OSError, UnicodeDecodeError) as e:
        logger.error(f"Ошибка чтения {path}: {e}")
        return None
    users = parse_drop_users(content)
    logger.info(f"Загружено пользователей: {len(users)}")
    return users


def save_drop_users_atomic(path: str, data: bytes) -> None:
    """Атомарная запись с резервной копией .bak."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if os.path.exists(path):
        shutil.copy2(path, path + ".bak")
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


# Атомарная привязка в двух направлениях.
# Возвращает room_id при успехе (в т.ч. если уже привязана именно эта комната),
# nil - если у пользователя другая комната или комната занята другим.
_BIND_LUA = """
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur then
  if cur == ARGV[2] then return cur end
  return false
end
local owner = redis.call('HGET', KEYS[2], ARGV[2])
if owner and owner ~= ARGV[1] then return false end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
return ARGV[2]
"""


class DropRoomManager:
    """Управление персональными комнатами и их привязкой к пользователям."""
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
        room_name_template: str = "Личный Dropbox — {display_name}",
        retention_days: int = 14,
    ):
        self.client = client
        self.redis = redis_client
        self.users = users
        self.bindings_file = bindings_file
        self.excluded_rooms = excluded_rooms or set()
        self.room_name_template = room_name_template
        self.retention_days = retention_days

    async def owner_of(self, room_id: str) -> Optional[str]:
        return await self.redis.hget(self.HASH_OWNERS, room_id)

    async def room_of(self, user_id: str) -> Optional[str]:
        return await self.redis.hget(self.HASH_ROOMS, user_id)

    def _room_name(self, user_id: str) -> str:
        display = self.users[user_id].display_name if user_id in self.users else "Пользователь"
        try:
            return self.room_name_template.format(display_name=display)
        except (KeyError, IndexError, ValueError):
            return f"Личный Dropbox — {display}"

    async def rename_room(self, room_id: str, user_id: str) -> None:
        try:
            name = self._room_name(user_id)
            room = self.client.rooms.get(room_id)
            if room is not None and getattr(room, "name", None) == name:
                return  # уже так называется - не плодим state-события
            resp = await self.client.room_put_state(room_id, "m.room.name", {"name": name})
            if isinstance(resp, nio.RoomPutStateError):
                logger.warning(f"Не удалось переименовать {room_id}: {resp.message}")
            else:
                logger.info(f"Комната {room_id} переименована в '{name}'")
        except Exception as e:
            logger.warning(f"Не удалось переименовать комнату {room_id}: {e}")

    async def _ensure_member(self, room_id: str, user_id: str) -> None:
        room = self.client.rooms.get(room_id)
        if room is None:
            return
        present = set(room.users.keys()) if room.users else set()
        present |= set(getattr(room, "invited_users", {}) or {})
        if user_id in present:
            return
        try:
            resp = await self.client.room_invite(room_id, user_id)
            if isinstance(resp, nio.RoomInviteError):
                logger.error(f"Ошибка приглашения {user_id} в {room_id}: {resp.message}")
        except Exception as e:
            logger.error(f"Ошибка приглашения {user_id} в {room_id}: {e}")

    async def persist(self):
        pairs = await self.redis.hgetall(self.HASH_ROOMS)
        tmp = self.bindings_file + ".tmp"
        os.makedirs(os.path.dirname(self.bindings_file) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pairs, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.bindings_file)

    async def restore(self):
        """Восстанавливает Redis из файла, если Redis пуст."""
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
        """Возвращает room_id только если пользователь теперь привязан именно к ЭТОЙ комнате."""
        if room_id in self.excluded_rooms:
            return None
        result = await self.redis.eval(
            _BIND_LUA, 2, self.HASH_ROOMS, self.HASH_OWNERS, user_id, room_id
        )
        if result == room_id:
            await self.persist()
            logger.info(f"Персональная комната привязана: {user_id} -> {room_id}")
            return result
        return None

    async def unbind(self, user_id: str) -> Optional[str]:
        room_id = await self.room_of(user_id)
        if not room_id:
            return None
        await self.redis.hdel(self.HASH_ROOMS, user_id)
        await self.redis.hdel(self.HASH_OWNERS, room_id)
        await self.persist()
        logger.info(f"Привязка снята: {user_id} (было {room_id})")
        return room_id

    async def _find_existing_dm(self, user_id: str) -> Optional[str]:
        """Ищет существующий диалог 1:1 бота и пользователя (не занятый и не общий)."""
        bot_id = self.client.user_id
        for room_id, room in self.client.rooms.items():
            if room_id in self.excluded_rooms:
                continue
            members = set(room.users.keys()) if room.users else set()
            if len(members) == 2 and user_id in members and bot_id in members:
                owner = await self.owner_of(room_id)
                if owner and owner != user_id:
                    continue
                return room_id
        return None

    async def get_or_create(self, user_id: str) -> tuple[Optional[str], Optional[str], bool]:
        """Возвращает (room_id, error, created)."""
        if user_id not in self.users:
            return None, "У вас нет доступа к персональному дропу.", False

        existing = await self.room_of(user_id)
        if existing:
            if existing in self.client.rooms:
                await self._ensure_member(existing, user_id)
                await self.rename_room(existing, user_id)
                return existing, None, False
            logger.warning(f"Привязка {user_id} -> {existing} устарела (бот не в комнате), снимаю")
            await self.unbind(user_id)

        lock_key = f"{self.LOCK_PREFIX}{user_id}"
        token = uuid.uuid4().hex
        if not await self.redis.set(lock_key, token, nx=True, ex=30):
            return None, "Комната уже создаётся. Повторите запрос через несколько секунд.", False

        try:
            existing = await self.room_of(user_id)
            if existing and existing in self.client.rooms:
                return existing, None, False

            found = await self._find_existing_dm(user_id)
            if found:
                await self._ensure_member(found, user_id)
                if await self.bind(user_id, found) == found:
                    await self.rename_room(found, user_id)
                    return found, None, False

            power_levels = {
                "users": {self.client.user_id: 100, user_id: 0},
                "users_default": 0, "events_default": 0, "state_default": 50,
                "invite": 50, "kick": 100, "ban": 100, "redact": 100,
                "events": {
                    "m.room.name": 50, "m.room.topic": 50, "m.room.power_levels": 100,
                    "m.room.join_rules": 50, "m.room.history_visibility": 100,
                    "m.room.encryption": 100,
                },
            }

            # Имена параметров и enum'ы - по сигнатуре nio.AsyncClient.room_create
            resp = await self.client.room_create(
                visibility=RoomVisibility.private,
                name=self._room_name(user_id),
                topic="Персональный приёмник файлов.",
                federate=False,
                is_direct=True,
                preset=RoomPreset.private_chat,
                invite=[user_id],
                initial_state=[
                    {"type": "m.room.history_visibility", "state_key": "",
                     "content": {"history_visibility": "shared"}},
                    {"type": "m.room.join_rules", "state_key": "",
                     "content": {"join_rule": "invite"}},
                ],
                power_level_override=power_levels,
            )

            new_room = getattr(resp, "room_id", None)
            if not new_room:
                message = getattr(resp, "message", str(resp))
                logger.error(f"Не удалось создать персональную комнату: {message}")
                return None, "Не удалось создать комнату. Сообщите администратору.", False

            if await self.bind(user_id, new_room) != new_room:
                logger.error(f"Не удалось закрепить {new_room} за {user_id}")
                return None, "Не удалось закрепить комнату.", False

            await self.client.room_send(new_room, "m.room.message", {
                "msgtype": "m.text",
                "body": (
                    "Это ваш персональный Dropbox.\n"
                    "Отправляйте сюда файлы и выбирайте: личная папка (*) или одна из общих.\n"
                    f"Внимание: файлы в личной папке автоматически удаляются через {self.retention_days} дн."
                ),
            })

            logger.info(f"Создана персональная комната: {user_id} -> {new_room}")
            return new_room, None, True
        finally:
            if await self.redis.get(lock_key) == token:
                await self.redis.delete(lock_key)

    async def resolve(self, room_id: str, sender: str) -> Optional[RoomTarget]:
        """
        general - одна из двух общих комнат
        drop    - персональная комната отправителя
        other   - неприявязанная комната, где пишет вайтлист-пользователь (только команды, без загрузки файлов)
        """
        if room_id in self.excluded_rooms:
            return RoomTarget(kind="general", room_id=room_id)

        owner = await self.owner_of(room_id)
        if owner:
            if sender != owner or owner not in self.users:
                return None
            return RoomTarget(kind="drop", room_id=room_id, owner=owner, folder=self.users[owner].smb_path)

        if sender in self.users:
            return RoomTarget(kind="other", room_id=room_id)

        return None