import os
from typing import Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConfigurationError
from pymongo.server_api import ServerApi


_client: Optional[MongoClient] = None
_users_collection: Optional[Collection] = None

def _get_users_collection() -> Collection:
    """Return the MongoDB collection used to store user data."""
    global _client, _users_collection

    if _users_collection is not None:
        return _users_collection

    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise ConfigurationError("MONGODB_URI environment variable is not set")

    db_name = os.getenv("MONGODB_DB_NAME", "slashgather")

    _client = MongoClient(mongo_uri, server_api=ServerApi("1"))
    _users_collection = _client[db_name]["users"]
    return _users_collection


def _ensure_user_document(user_id: int) -> None:
    """Create a default user document if one does not already exist."""
    users = _get_users_collection()
    default_doc = {
        "balance": float(os.getenv("DEFAULT_BALANCE", 100.0)),
        "last_gather_time": 0.0,
        "total_forage_count": 0,
        "items": {},
        "ripeness_stats": {},
    }

    users.update_one(
        {"_id": int(user_id)},
        {"$setOnInsert": default_doc},
        upsert=True,
    )


def init_database() -> None:
    """Initialise MongoDB indexes and verify connectivity."""
    users = _get_users_collection()

    # Ensure common indexes exist
    users.create_index("last_gather_time")
    users.create_index("total_forage_count")

    # Trigger a ping to verify connectivity
    users.database.client.admin.command("ping")


def get_user_balance(user_id: int) -> float:
    users = _get_users_collection()
    _ensure_user_document(user_id)

    doc = users.find_one({"_id": int(user_id)}, {"balance": 1})
    return float(doc.get("balance", 100.0)) if doc else 100.0


def update_user_balance(user_id: int, new_balance: float) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"balance": float(new_balance)}},
        upsert=True,
    )


def increment_forage_count(user_id: int) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"total_forage_count": 1}},
        upsert=True,
    )


def get_forage_count(user_id: int) -> int:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"total_forage_count": 1})
    return int(doc.get("total_forage_count", 0)) if doc else 0


def add_user_item(user_id: int, item_name: str) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {f"items.{item_name}": 1}},
        upsert=True,
    )


def get_user_items(user_id: int) -> Dict[str, int]:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"items": 1})
    items: Dict[str, int] = doc.get("items", {}) if doc else {}
    return dict(sorted(items.items(), key=lambda item: item[1], reverse=True))


def add_ripeness_stat(user_id: int, ripeness_name: str) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {f"ripeness_stats.{ripeness_name}": 1}},
        upsert=True,
    )


def get_user_ripeness_stats(user_id: int) -> Dict[str, int]:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"ripeness_stats": 1})
    stats: Dict[str, int] = doc.get("ripeness_stats", {}) if doc else {}
    return dict(sorted(stats.items(), key=lambda item: item[1], reverse=True))


def get_user_last_gather_time(user_id: int) -> float:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"last_gather_time": 1})
    return float(doc.get("last_gather_time", 0.0)) if doc else 0.0


def update_user_last_gather_time(user_id: int, timestamp: float) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"last_gather_time": float(timestamp)}},
        upsert=True,
    )


