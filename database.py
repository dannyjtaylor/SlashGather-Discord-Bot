import os
import time
from typing import Dict, Optional

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConfigurationError
from pymongo.server_api import ServerApi


_client: Optional[MongoClient] = None
_users_collection: Optional[Collection] = None


def _get_environment() -> str:
    """Determine the active environment for the application."""
    env_value = os.getenv("ENVIRONMENT")
    if env_value:
        return env_value

    prod_value = os.getenv("ENVIRONMENT_PROD")
    if prod_value:
        return prod_value

    return "development"


def _get_default_balance() -> float:
    """Return the default balance based on environment-specific variables."""
    environment = _get_environment().lower()
    if environment == "production":
        fallback = 100.0
        balance_var = "DEFAULT_BALANCE_PROD"
    else:
        fallback = 10000.0
        balance_var = "DEFAULT_BALANCE"

    try:
        return float(os.getenv(balance_var, fallback))
    except (TypeError, ValueError):
        return float(fallback)


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
        "balance": _get_default_balance(),
        "last_gather_time": 0.0,
        "last_harvest_time": 0.0,
        "last_mine_time": 0.0,
        "total_forage_count": 0,
        "items": {},
        "ripeness_stats": {},
        "gather_stats": {
            "total_items": 0,
            "categories": {},
            "items": {}
        },
        "basket_upgrades": {
            "basket": 0,
            "shoes": 0,
            "gloves": 0,
            "soil": 0
        },
        "crypto_holdings": {
            "RTC": 0.0,
            "TER": 0.0,
            "CNY": 0.0
        },
        "gardeners": [],
        "notification_channel_id": None
    }
    users.update_one(
        {"_id": int(user_id)},
        {"$setOnInsert": default_doc},
        upsert=True,
    )


def increment_gather_stats(userid: int, category: str, item: str) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(userid)},
        {
            "$inc": {
                "gather_stats.total_items": 1,
                f"gather_stats.categories.{category}": 1,
                f"gather_stats.items.{item}": 1,
            }
        },
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
    if not doc:
        return _get_default_balance()
    try:
        return float(doc.get("balance", _get_default_balance()))
    except (TypeError, ValueError):
        return _get_default_balance()


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


def _get_item_count(item):
    """Helper function to get the count (value) from an item tuple."""
    return item[1]

def get_user_items(user_id: int) -> Dict[str, int]:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"items": 1})
    items: Dict[str, int] = doc.get("items", {}) if doc else {}
    return dict(sorted(items.items(), key=_get_item_count, reverse=True))


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
    return dict(sorted(stats.items(), key=_get_item_count, reverse=True))


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

def get_user_last_harvest_time(user_id: int) -> float:
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"last_harvest_time": 1})
    return float(doc.get("last_harvest_time", 0.0)) if doc else 0.0


def update_user_last_harvest_time(user_id: int, timestamp: float) -> None:
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"last_harvest_time": float(timestamp)}},
        upsert=True,
    )


def _get_balance_value(user_tuple):
    """Helper function to get the balance (second element) from a user tuple."""
    return user_tuple[1]

def get_all_users_balance() -> list[tuple[int, float]]:
    """Get all users with their balances, sorted by balance descending."""
    users = _get_users_collection()
    cursor = users.find({}, {"_id": 1, "balance": 1})
    results = []
    for doc in cursor:
        user_id = doc.get("_id")
        balance = float(doc.get("balance", _get_default_balance()))
        results.append((user_id, balance))
    # Sort by balance descending
    results.sort(key=_get_balance_value, reverse=True)
    return results


def _get_total_items_value(user_tuple):
    """Helper function to get the total_items (second element) from a user tuple."""
    return user_tuple[1]

def get_user_total_items(user_id: int) -> int:
    """Get a user's total items gathered."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"gather_stats.total_items": 1})
    if not doc:
        return 0
    gather_stats = doc.get("gather_stats", {})
    return int(gather_stats.get("total_items", 0))

def get_all_users_total_items() -> list[tuple[int, int]]:
    """Get all users with their total items gathered, sorted by total_items descending."""
    users = _get_users_collection()
    cursor = users.find({}, {"_id": 1, "gather_stats.total_items": 1})
    results = []
    for doc in cursor:
        user_id = doc.get("_id")
        total_items = int(doc.get("gather_stats", {}).get("total_items", 0))
        results.append((user_id, total_items))
    # Sort by total_items descending
    results.sort(key=_get_total_items_value, reverse=True)
    return results


def get_user_basket_upgrades(user_id: int) -> Dict[str, int]:
    """Get user's basket upgrade levels. Returns dict with keys: basket, shoes, gloves, soil."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"basket_upgrades": 1})
    if not doc:
        return {"basket": 0, "shoes": 0, "gloves": 0, "soil": 0}
    upgrades = doc.get("basket_upgrades", {})
    return {
        "basket": upgrades.get("basket", 0),
        "shoes": upgrades.get("shoes", 0),
        "gloves": upgrades.get("gloves", 0),
        "soil": upgrades.get("soil", 0)
    }


def set_user_basket_upgrade(user_id: int, upgrade_type: str, tier: int) -> None:
    """Set user's basket upgrade tier. upgrade_type: 'basket', 'shoes', 'gloves', or 'soil'."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {f"basket_upgrades.{upgrade_type}": int(tier)}},
        upsert=True,
    )


# Cryptocurrency functions
def get_user_crypto_holdings(user_id: int) -> Dict[str, float]:
    """Get user's cryptocurrency holdings. Returns dict with keys: RTC, TER, CNY."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"crypto_holdings": 1})
    if not doc:
        return {"RTC": 0.0, "TER": 0.0, "CNY": 0.0}
    holdings = doc.get("crypto_holdings", {})
    return {
        "RTC": float(holdings.get("RTC", 0.0)),
        "TER": float(holdings.get("TER", 0.0)),
        "CNY": float(holdings.get("CNY", 0.0))
    }


def update_user_crypto_holdings(user_id: int, coin: str, amount: float) -> None:
    """Update user's cryptocurrency holdings. Adds amount to existing holdings."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {f"crypto_holdings.{coin}": float(amount)}},
        upsert=True,
    )


def get_user_last_mine_time(user_id: int) -> float:
    """Get user's last mine time."""
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"last_mine_time": 1})
    return float(doc.get("last_mine_time", 0.0)) if doc else 0.0


def update_user_last_mine_time(user_id: int, timestamp: float) -> None:
    """Update user's last mine time."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"last_mine_time": float(timestamp)}},
        upsert=True,
    )


def get_crypto_prices() -> Dict[str, float]:
    """Get current cryptocurrency prices from database."""
    users = _get_users_collection()
    doc = users.find_one({"_id": 0}, {"crypto_prices": 1})  # Use _id=0 for global data
    if not doc:
        # Initialize with default prices
        default_prices = {"RTC": 100.0, "TER": 100.0, "CNY": 100.0}
        users.update_one(
            {"_id": 0},
            {"$set": {"crypto_prices": default_prices}},
            upsert=True,
        )
        return default_prices
    prices = doc.get("crypto_prices", {})
    return {
        "RTC": float(prices.get("RTC", 100.0)),
        "TER": float(prices.get("TER", 100.0)),
        "CNY": float(prices.get("CNY", 100.0))
    }


def update_crypto_prices(prices: Dict[str, float]) -> None:
    """Update cryptocurrency prices in database."""
    users = _get_users_collection()
    users.update_one(
        {"_id": 0},
        {"$set": {"crypto_prices": prices}},
        upsert=True,
    )


# Gardener functions
def get_user_gardeners(user_id: int) -> list[Dict]:
    """Get user's gardeners. Returns list of gardener dicts."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"gardeners": 1})
    if not doc:
        return []
    gardeners = doc.get("gardeners", [])
    return gardeners if isinstance(gardeners, list) else []


def add_gardener(user_id: int, gardener_id: int, price: float) -> bool:
    """Add a gardener to user's collection and deduct money. Returns True if successful."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    # Check if user has enough balance
    current_balance = get_user_balance(user_id)
    if current_balance < price:
        return False
    
    # Check if gardener slot is already taken
    existing_gardeners = get_user_gardeners(user_id)
    if any(g.get("id") == gardener_id for g in existing_gardeners):
        return False
    
    # Check if user has reached max gardeners (5)
    if len(existing_gardeners) >= 5:
        return False
    
    # Deduct money
    new_balance = current_balance - price
    update_user_balance(user_id, new_balance)
    
    # Add gardener
    new_gardener = {
        "id": int(gardener_id),
        "times_gathered": 0,
        "total_money_earned": 0.0,
        "hired_at": time.time()
    }
    
    users.update_one(
        {"_id": int(user_id)},
        {"$push": {"gardeners": new_gardener}},
        upsert=True,
    )
    
    return True


def update_gardener_stats(user_id: int, gardener_id: int, money_earned: float) -> None:
    """Update gardener stats after a successful gather."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id), "gardeners.id": int(gardener_id)},
        {
            "$inc": {
                "gardeners.$.times_gathered": 1,
                "gardeners.$.total_money_earned": float(money_earned)
            }
        }
    )


def get_all_users_with_gardeners() -> list[tuple[int, list[Dict]]]:
    """Get all users who have gardeners. Returns list of (user_id, gardeners) tuples."""
    users = _get_users_collection()
    cursor = users.find({"gardeners": {"$exists": True, "$ne": []}}, {"_id": 1, "gardeners": 1})
    results = []
    for doc in cursor:
        user_id = doc.get("_id")
        gardeners = doc.get("gardeners", [])
        if gardeners and isinstance(gardeners, list):
            results.append((user_id, gardeners))
    return results


def set_user_notification_channel(user_id: int, channel_id: int) -> None:
    """Store user's preferred notification channel for gardener updates."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"notification_channel_id": int(channel_id)}},
        upsert=True,
    )


def get_user_notification_channel(user_id: int) -> Optional[int]:
    """Get user's preferred notification channel ID. Returns None if not set."""
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"notification_channel_id": 1})
    if not doc:
        return None
    channel_id = doc.get("notification_channel_id")
    return int(channel_id) if channel_id is not None else None


# Stock holdings functions
def get_user_stock_holdings(user_id: int) -> Dict[str, int]:
    """Get user's stock holdings. Returns dict with stock symbols as keys and share counts as values."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"stock_holdings": 1})
    if not doc:
        return {}
    holdings = doc.get("stock_holdings", {})
    # Convert all values to int
    return {symbol: int(amount) for symbol, amount in holdings.items() if amount > 0}


def update_user_stock_holdings(user_id: int, symbol: str, amount: int) -> None:
    """Update user's stock holdings. Adds amount to existing holdings (can be negative to sell)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {f"stock_holdings.{symbol}": int(amount)}},
        upsert=True,
    )


# Event functions
def _get_events_collection() -> Collection:
    """Return the MongoDB collection used to store events."""
    global _client
    if _client is None:
        _get_users_collection()  # Initialize client if needed
    
    db_name = os.getenv("MONGODB_DB_NAME", "slashgather")
    return _client[db_name]["events"]


def get_active_events() -> list[Dict]:
    """Get all currently active events. Returns list of event dicts."""
    events = _get_events_collection()
    current_time = time.time()
    cursor = events.find({"end_time": {"$gt": current_time}})
    results = []
    for doc in cursor:
        results.append({
            "event_id": doc.get("event_id"),
            "event_type": doc.get("event_type"),  # "hourly" or "daily"
            "event_name": doc.get("event_name"),
            "start_time": float(doc.get("start_time", 0)),
            "end_time": float(doc.get("end_time", 0)),
            "effects": doc.get("effects", {})
        })
    return results


def get_active_event_by_type(event_type: str) -> Optional[Dict]:
    """Get active event of specific type (hourly or daily). Returns None if none active."""
    events = _get_events_collection()
    current_time = time.time()
    doc = events.find_one({
        "event_type": event_type,
        "end_time": {"$gt": current_time}
    })
    if not doc:
        return None
    return {
        "event_id": doc.get("event_id"),
        "event_type": doc.get("event_type"),
        "event_name": doc.get("event_name"),
        "start_time": float(doc.get("start_time", 0)),
        "end_time": float(doc.get("end_time", 0)),
        "effects": doc.get("effects", {})
    }


def set_active_event(event_id: str, event_type: str, event_name: str, start_time: float, end_time: float, effects: Dict) -> None:
    """Store an active event. Replaces any existing event of the same type."""
    events = _get_events_collection()
    # Remove any existing event of the same type
    events.delete_many({"event_type": event_type})
    # Insert new event
    events.insert_one({
        "event_id": event_id,
        "event_type": event_type,
        "event_name": event_name,
        "start_time": float(start_time),
        "end_time": float(end_time),
        "effects": effects
    })


def clear_event(event_id: str) -> None:
    """Remove an event by its ID."""
    events = _get_events_collection()
    events.delete_one({"event_id": event_id})


def clear_expired_events() -> None:
    """Remove all expired events."""
    events = _get_events_collection()
    current_time = time.time()
    events.delete_many({"end_time": {"$lte": current_time}})