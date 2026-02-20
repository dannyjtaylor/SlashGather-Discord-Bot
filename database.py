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

    _client = MongoClient(
        mongo_uri,
        server_api=ServerApi("1"),
        maxPoolSize=10,  # Maximum connections in the pool
        minPoolSize=2,   # Keep minimum connections ready for faster response
        maxIdleTimeMS=45000,  # Close idle connections after 45 seconds
        connectTimeoutMS=5000,  # Fail fast if can't connect (5 seconds)
        serverSelectionTimeoutMS=5000,  # Fast server selection timeout
        socketTimeoutMS=20000,  # Socket timeout for operations (20 seconds)
        retryWrites=True,  # Automatically retry write operations
        retryReads=True  # Automatically retry read operations
    )
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
        "last_roulette_elimination_time": 0.0,
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
        "harvest_upgrades": {
            "car": 0,
            "chain": 0,
            "fertilizer": 0,
            "cooldown": 0
        },
        "crypto_holdings": {
            "RTC": 0.0,
            "TER": 0.0,
            "CNY": 0.0
        },
        "gardeners": [],
        "gpus": [],
        "notification_channel_id": None,
        "tree_rings": 0,
        "bloom_count": 0,
        "bloom_cycle_plants": 0,
        "last_water_time": 0.0,
        "consecutive_water_days": 0,
        "water_count": 0,
        "achievements": {
            "gatherer": 0,
            "coinflip_total": 0,
            "coinflip_win_streak": 0,
            "harvesting": 0,
            "planter": 0,
            "water_streak": 0,
            "blooming": 0,
            "russian_roulette": 0,
            "hidden_achievements_discovered": 0,
            "hidden_achievements": {
                "john_rockefeller": False,
                "beating_the_odds": False,
                "beneficiary": False,
                "leap_year": False,
                "ceo": False,
                "blockchain": False,
                "almost_got_it": False,
                "maxed_out": False,
                "social_butterfly": False,
                "high_reroller": False
            },
            "areas_unlocked": 0
        },
        "coinflip_count": 0,
        "coinflip_win_streak": 0,
        "gather_command_count": 0,
        "harvest_command_count": 0,
        "invite_stats": {
            "invites_created": 0,
            "total_joins": 0,
            "rewards_earned": 0.0,
            "invite_codes": [],
            "claimed_rewards": []
        },
        "hoe_enchantment": None,
        "tractor_enchantment": None,
        "russian_games_played": 0,
        "unlocked_areas": {
            "grove": False,
            "marsh": False,
            "bog": False,
            "mire": False
        },
        "shop_inventory": {},
        "daily_shop_purchases_count": 0,
        "daily_shop_last_date_est": ""
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


def perform_gather_update(user_id: int, balance_increment: float, item_name: str, 
                          ripeness_name: str, category: str, apply_cooldown: bool = True,
                          increment_command_count: bool = False) -> bool:
    """
    Perform all gather-related database updates in a single MongoDB operation.
    This batches: balance update, item addition, ripeness stat, gather stats, cooldown,
    and optionally the gather command count.
    
    Args:
        user_id: User ID
        balance_increment: Amount to add to balance (can be negative)
        item_name: Name of the item gathered
        ripeness_name: Name of the ripeness level
        category: Item category (Fruit, Vegetable, Flower, etc.)
        apply_cooldown: If True, update last_gather_time to current time
        increment_command_count: If True, also increment gather_command_count
    """
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    # Get current total_items and bloom_cycle_plants for Tree Ring and sync logic
    doc = users.find_one(
        {"_id": int(user_id)},
        {"gather_stats.total_items": 1, "bloom_cycle_plants": 1}
    )
    current_total = 0
    current_bloom_cycle = 0
    if doc:
        if doc.get("gather_stats"):
            current_total = int(doc.get("gather_stats", {}).get("total_items", 0))
        current_bloom_cycle = int(doc.get("bloom_cycle_plants", 0))
    
    # Check if this gather will cross a 200-plant milestone
    new_total = current_total + 1
    should_award_tree_ring = (new_total % 200 == 0) and new_total > 0
    
    new_bloom_cycle = current_bloom_cycle + 1
    
    # Build the update operation with all increments and sets
    update_ops = {
        "$inc": {
            "balance": float(balance_increment),
            f"items.{item_name}": 1,
            f"ripeness_stats.{ripeness_name}": 1,
            "gather_stats.total_items": 1,
            f"gather_stats.categories.{category}": 1,
            f"gather_stats.items.{item_name}": 1,
            "total_forage_count": 1,  # Keep in sync with gather_stats.total_items for backwards compatibility
        },
        "$set": {
            "bloom_cycle_plants": new_bloom_cycle,
        }
    }
    
    # Award Tree Ring if milestone reached
    if should_award_tree_ring:
        update_ops["$inc"]["tree_rings"] = 1
    
    # Optionally include gather command count in the same write
    if increment_command_count:
        update_ops["$inc"]["gather_command_count"] = 1
    
    # Add cooldown update if requested
    if apply_cooldown:
        if "$set" not in update_ops:
            update_ops["$set"] = {}
        update_ops["$set"]["last_gather_time"] = float(time.time())
    
    users.update_one(
        {"_id": int(user_id)},
        update_ops,
        upsert=True,
    )
    
    # Return whether a Tree Ring was awarded
    return should_award_tree_ring

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


def increment_total_items_only(user_id: int) -> None:
    """Increment only gather_stats.total_items (for harvests to update userstats, but not gatherer achievements).
    Also increments bloom_cycle_plants to track plants gathered this bloom cycle."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    users.update_one(
        {"_id": int(user_id)},
        {
            "$inc": {"gather_stats.total_items": 1, "total_forage_count": 1, "bloom_cycle_plants": 1},
        },
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


def get_user_last_roulette_elimination_time(user_id: int) -> float:
    """Get user's last Russian Roulette elimination time."""
    users = _get_users_collection()
    doc = users.find_one({"_id": int(user_id)}, {"last_roulette_elimination_time": 1})
    return float(doc.get("last_roulette_elimination_time", 0.0)) if doc else 0.0


def update_user_last_roulette_elimination_time(user_id: int, timestamp: float) -> None:
    """Update user's last Russian Roulette elimination time (sets 30-minute cooldown)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"last_roulette_elimination_time": float(timestamp)}},
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

def get_user_bloom_cycle_plants(user_id: int) -> int:
    """Get user's plants gathered in the current bloom cycle (resets on bloom)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"bloom_cycle_plants": 1})
    if not doc:
        return 0
    return int(doc.get("bloom_cycle_plants", 0))


def deduct_user_bloom_cycle_plants(user_id: int, amount: int) -> bool:
    """Deduct plants from user's bloom cycle. Returns True if they had enough and deduction succeeded."""
    if amount <= 0:
        return True
    users = _get_users_collection()
    result = users.update_one(
        {"_id": int(user_id), "bloom_cycle_plants": {"$gte": amount}},
        {"$inc": {"bloom_cycle_plants": -amount}},
    )
    return result.modified_count == 1


def add_user_bloom_cycle_plants(user_id: int, amount: int) -> None:
    """Add plants to user's bloom cycle (e.g. winnings)."""
    if amount <= 0:
        return
    users = _get_users_collection()
    _ensure_user_document(user_id)
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"bloom_cycle_plants": amount}},
        upsert=True,
    )


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


def _get_rank_sort_value(rank_tuple: tuple[int, str]) -> int:
    """Helper function to get the sort value for a rank. Higher ranks have higher values."""
    rank = rank_tuple[1]
    # Rank hierarchy from highest to lowest
    rank_hierarchy = {
        "REDWOOD": 18,
        "FIR III": 17, "FIR II": 16, "FIR I": 15,
        "OAK III": 14, "OAK II": 13, "OAK I": 12,
        "MAPLE III": 11, "MAPLE II": 10, "MAPLE I": 9,
        "BIRCH III": 8, "BIRCH II": 7, "BIRCH I": 6,
        "CEDAR III": 5, "CEDAR II": 4, "CEDAR I": 3,
        "PINE III": 2, "PINE II": 1, "PINE I": 0
    }
    return rank_hierarchy.get(rank, 0)


def get_all_users_ranks() -> list[tuple[int, str]]:
    """Get all users with their bloom ranks, sorted by rank descending (REDWOOD -> PINE I)."""
    users = _get_users_collection()
    cursor = users.find({}, {"_id": 1, "bloom_count": 1})
    results = []
    for doc in cursor:
        user_id = doc.get("_id")
        bloom_count = int(doc.get("bloom_count", 0))
        rank = get_bloom_rank(user_id)
        results.append((user_id, rank))
    # Sort by rank descending (highest rank first)
    results.sort(key=_get_rank_sort_value, reverse=True)
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


def get_user_harvest_upgrades(user_id: int) -> Dict[str, int]:
    """Get user's harvest upgrade levels. Returns dict with keys: car, chain, fertilizer, cooldown."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"harvest_upgrades": 1})
    if not doc:
        return {"car": 0, "chain": 0, "fertilizer": 0, "cooldown": 0}
    upgrades = doc.get("harvest_upgrades", {})
    return {
        "car": upgrades.get("car", 0),
        "chain": upgrades.get("chain", 0),
        "fertilizer": upgrades.get("fertilizer", 0),
        "cooldown": upgrades.get("cooldown", 0)
    }


def set_user_harvest_upgrade(user_id: int, upgrade_type: str, tier: int) -> None:
    """Set user's harvest upgrade tier. upgrade_type: 'car', 'chain', 'fertilizer', or 'cooldown'."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {f"harvest_upgrades.{upgrade_type}": int(tier)}},
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


def get_user_last_water_time(user_id: int) -> float:
    """Get user's last water time."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"last_water_time": 1})
    return float(doc.get("last_water_time", 0.0)) if doc else 0.0


def update_user_last_water_time(user_id: int, timestamp: float) -> None:
    """Update user's last water time."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"last_water_time": float(timestamp)}},
        upsert=True,
    )


def get_user_consecutive_water_days(user_id: int) -> int:
    """Get user's consecutive water days."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"consecutive_water_days": 1})
    return int(doc.get("consecutive_water_days", 0)) if doc else 0


def set_user_consecutive_water_days(user_id: int, days: int) -> None:
    """Set user's consecutive water days."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"consecutive_water_days": int(days)}},
        upsert=True,
    )


def get_user_water_count(user_id: int) -> int:
    """Get user's water count (total times watered)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"water_count": 1})
    return int(doc.get("water_count", 0)) if doc else 0


def increment_user_water_count(user_id: int) -> None:
    """Increment user's water count by 1."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"water_count": 1}},
        upsert=True,
    )


def get_water_multiplier(user_id: int) -> float:
    """Calculate money multiplier based on water count. Formula: 1.0 + (water_count * 0.01) - 1% per water (additive, similar to Tree Rings)."""
    water_count = get_user_water_count(user_id)
    return 1.0 + (water_count * 0.01)


def get_daily_bonus_multiplier(user_id: int) -> float:
    """Calculate daily streak bonus multiplier based on consecutive water days. Formula: 1.0 + (consecutive_days * 0.02) - 2% per consecutive day."""
    consecutive_days = get_user_consecutive_water_days(user_id)
    return 1.0 + (consecutive_days * 0.02)


def get_crypto_prices() -> Dict[str, float]:
    """Get current cryptocurrency prices from database."""
    users = _get_users_collection()
    doc = users.find_one({"_id": 0}, {"crypto_prices": 1})  # Use _id=0 for global data
    if not doc:
        # Initialize with default prices
        default_prices = {"RTC": 90000.0, "TER": 3100.0, "CNY": 855.0}
        users.update_one(
            {"_id": 0},
            {"$set": {"crypto_prices": default_prices}},
            upsert=True,
        )
        return default_prices
    prices = doc.get("crypto_prices", {})
    return {
        "RTC": float(prices.get("RTC", 90000.0)),
        "TER": float(prices.get("TER", 3100.0)),
        "CNY": float(prices.get("CNY", 855.0))
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
        "plants_gathered": 0,
        "total_money_earned": 0.0,
        "hired_at": time.time(),
        "has_tool": False,
    }
    
    users.update_one(
        {"_id": int(user_id)},
        {"$push": {"gardeners": new_gardener}},
        upsert=True,
    )
    
    return True


def update_gardener_stats(user_id: int, gardener_id: int, money_earned: float, plants_count: int = 1) -> None:
    """Update gardener stats after a successful gather or harvest.
    
    Args:
        user_id: User ID
        gardener_id: Gardener ID
        money_earned: Total money earned from this action
        plants_count: Number of plants gathered (1 for single gather, multiple for harvest)
    """
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id), "gardeners.id": int(gardener_id)},
        {
            "$inc": {
                "gardeners.$.times_gathered": 1,
                "gardeners.$.plants_gathered": int(plants_count),
                "gardeners.$.total_money_earned": float(money_earned)
            }
        }
    )


def set_gardener_has_tool(user_id: int, gardener_id: int, tool_price: float) -> bool:
    """Give a gardener their tool (deduct balance and set has_tool). Returns True if successful."""
    users = _get_users_collection()
    current_balance = get_user_balance(user_id)
    if current_balance < tool_price:
        return False
    existing = get_user_gardeners(user_id)
    if not any(g.get("id") == gardener_id for g in existing):
        return False
    if any(g.get("id") == gardener_id and g.get("has_tool") for g in existing):
        return False  # already has tool
    new_balance = current_balance - tool_price
    update_user_balance(user_id, new_balance)
    users.update_one(
        {"_id": int(user_id), "gardeners.id": int(gardener_id)},
        {"$set": {"gardeners.$.has_tool": True}},
    )
    return True


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


def get_all_users_with_gpus() -> list[tuple[int, list[str]]]:
    """Get all users who have GPUs. Returns list of (user_id, gpus) tuples."""
    users = _get_users_collection()
    cursor = users.find({"gpus": {"$exists": True, "$ne": []}}, {"_id": 1, "gpus": 1})
    results = []
    for doc in cursor:
        user_id = doc.get("_id")
        gpus = doc.get("gpus", [])
        if gpus and isinstance(gpus, list):
            results.append((user_id, gpus))
    return results


# GPU functions
def get_user_gpus(user_id: int) -> list[str]:
    """Get user's GPUs. Returns list of GPU names (allows duplicates for stacking)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"gpus": 1})
    if not doc:
        return []
    gpus = doc.get("gpus", [])
    return gpus if isinstance(gpus, list) else []


def add_gpu(user_id: int, gpu_name: str, price: float) -> bool:
    """Add a GPU to user's collection and deduct money. Returns True if successful. Users can only have 1 of each GPU."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    # Check if user already has this GPU
    existing_gpus = get_user_gpus(user_id)
    if gpu_name in existing_gpus:
        return False
    
    # Check if user has enough balance
    current_balance = get_user_balance(user_id)
    if current_balance < price:
        return False
    
    # Deduct money
    new_balance = current_balance - price
    update_user_balance(user_id, new_balance)
    
    # Add GPU (only one of each type allowed)
    users.update_one(
        {"_id": int(user_id)},
        {"$push": {"gpus": str(gpu_name)}},
        upsert=True,
    )
    
    return True


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


# Bloom system functions
def get_user_tree_rings(user_id: int) -> int:
    """Get user's Tree Rings."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"tree_rings": 1})
    if not doc:
        return 0
    return int(doc.get("tree_rings", 0))


def increment_tree_rings(user_id: int, amount: int) -> None:
    """Add Tree Rings to user."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"tree_rings": int(amount)}},
        upsert=True,
    )


def get_bloom_multiplier(user_id: int) -> float:
    """Calculate money multiplier based on Tree Rings. Formula: 1.0 + (tree_rings * 0.005)"""
    tree_rings = get_user_tree_rings(user_id)
    return 1.0 + (tree_rings * 0.005)


# ---------------------------------------------------------------------------
# Daily shop (Tree Rings currency)
# ---------------------------------------------------------------------------

def get_user_shop_inventory(user_id: int) -> Dict[str, int]:
    """Get user's shop item counts (item_id -> count)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"shop_inventory": 1})
    if not doc:
        return {}
    return dict(doc.get("shop_inventory", {}))


def has_shop_item(user_id: int, item_id: str) -> bool:
    """Return True if user owns at least one of the given shop item."""
    inv = get_user_shop_inventory(user_id)
    return inv.get(item_id, 0) >= 1


def get_user_daily_shop_purchases(user_id: int) -> tuple:
    """Return (purchases_count_today: int, last_date_est: str). Count resets when date changes."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one(
        {"_id": int(user_id)},
        {"daily_shop_purchases_count": 1, "daily_shop_last_date_est": 1}
    )
    if not doc:
        return 0, ""
    return (
        int(doc.get("daily_shop_purchases_count", 0)),
        str(doc.get("daily_shop_last_date_est", ""))
    )


def purchase_daily_shop_item(user_id: int, item_id: str, cost: int, date_est: str) -> bool:
    """
    Deduct cost tree rings, add item to shop_inventory (max 1 per item), increment daily purchases, set date.
    Caller must ensure user has enough tree rings and has not exceeded 3 purchases today.
    Returns True on success. Returns False if user already owns this item (one per item only).
    """
    if has_shop_item(user_id, item_id):
        return False  # Can only own one of each daily shop item
    users = _get_users_collection()
    _ensure_user_document(user_id)
    # Atomic: deduct tree_rings and update inventory + daily stats
    doc = users.find_one({"_id": int(user_id)}, {"tree_rings": 1, "shop_inventory": 1, "daily_shop_purchases_count": 1, "daily_shop_last_date_est": 1})
    if not doc:
        return False
    tree_rings = int(doc.get("tree_rings", 0))
    if tree_rings < cost:
        return False
    inv = dict(doc.get("shop_inventory", {}))
    inv[item_id] = 1  # Only one of each item ever
    purchase_count = int(doc.get("daily_shop_purchases_count", 0))
    last_date = str(doc.get("daily_shop_last_date_est", ""))
    if last_date != date_est:
        purchase_count = 0
    purchase_count += 1
    users.update_one(
        {"_id": int(user_id)},
        {
            "$inc": {"tree_rings": -cost},
            "$set": {
                "shop_inventory": inv,
                "daily_shop_purchases_count": purchase_count,
                "daily_shop_last_date_est": date_est,
            }
        }
    )
    return True


def get_roulette_elimination_cooldown_seconds(user_id: int) -> int:
    """Return cooldown in seconds (300 if Gambler's Revolver, else 1800)."""
    if has_shop_item(user_id, "gamblers_revolver"):
        return 300  # 5 minutes
    return 60 * 30  # 30 minutes


def get_user_ids_with_shop_item(item_id: str) -> list:
    """Return list of user _ids that have at least one of the given shop item."""
    users = _get_users_collection()
    cursor = users.find(
        {f"shop_inventory.{item_id}": {"$gte": 1}},
        {"_id": 1}
    )
    return [doc["_id"] for doc in cursor]


def get_bloom_rank(user_id: int) -> str:
    """Get user's current Bloom Rank based on bloom_count."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"bloom_count": 1})
    if not doc:
        return "PINE I"
    
    bloom_count = int(doc.get("bloom_count", 0))
    
    # Bloom Rank progression
    if bloom_count >= 18:
        return "REDWOOD"
    elif bloom_count == 17:
        return "FIR III"
    elif bloom_count == 16:
        return "FIR II"
    elif bloom_count == 15:
        return "FIR I"
    elif bloom_count == 14:
        return "OAK III"
    elif bloom_count == 13:
        return "OAK II"
    elif bloom_count == 12:
        return "OAK I"
    elif bloom_count == 11:
        return "MAPLE III"
    elif bloom_count == 10:
        return "MAPLE II"
    elif bloom_count == 9:
        return "MAPLE I"
    elif bloom_count == 8:
        return "BIRCH III"
    elif bloom_count == 7:
        return "BIRCH II"
    elif bloom_count == 6:
        return "BIRCH I"
    elif bloom_count == 5:
        return "CEDAR III"
    elif bloom_count == 4:
        return "CEDAR II"
    elif bloom_count == 3:
        return "CEDAR I"
    elif bloom_count == 2:
        return "PINE III"
    elif bloom_count == 1:
        return "PINE II"
    else:  # bloom_count == 0
        return "PINE I"


def get_user_bloom_count(user_id: int) -> int:
    """Get user's bloom count."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"bloom_count": 1})
    if not doc:
        return 0
    return int(doc.get("bloom_count", 0))


def perform_bloom(user_id: int) -> None:
    """Reset user's progress while keeping lifetime plants (gather_stats.total_items, total_forage_count),
    Tree Rings, achievements, and incrementing bloom_count.
    Resets bloom_cycle_plants to 0 so PLANTER rank restarts at I.
    Note: All achievements persist through bloom, including planter achievements."""
    users = _get_users_collection()
    default_balance = _get_default_balance()

    # Preserve the lifetime plants gathered count before resetting
    doc = users.find_one({"_id": int(user_id)}, {"gather_stats.total_items": 1, "total_forage_count": 1})
    preserved_total_items = 0
    preserved_forage_count = 0
    if doc:
        preserved_total_items = int(doc.get("gather_stats", {}).get("total_items", 0))
        preserved_forage_count = int(doc.get("total_forage_count", 0))

    users.update_one(
        {"_id": int(user_id)},
        {
            "$set": {
                "balance": float(default_balance),
                "basket_upgrades": {
                    "basket": 0,
                    "shoes": 0,
                    "gloves": 0,
                    "soil": 0
                },
                "harvest_upgrades": {
                    "car": 0,
                    "chain": 0,
                    "fertilizer": 0,
                    "cooldown": 0
                },
                "gardeners": [],
                "gpus": [],
                "items": {},
                "ripeness_stats": {},
                "gather_stats": {
                    "total_items": preserved_total_items,
                    "categories": {},
                    "items": {}
                },
                "total_forage_count": preserved_forage_count,
                "bloom_cycle_plants": 0,
                "stock_holdings": {},
                "crypto_holdings": {
                    "RTC": 0.0,
                    "TER": 0.0,
                    "CNY": 0.0
                },
                "unlocked_areas": {
                    "grove": False,
                    "marsh": False,
                    "bog": False,
                    "mire": False
                },
                "last_roulette_elimination_time": 0.0
                # All achievements persist through bloom (gatherer, planter, harvesting, coinflip, water_streak, hidden)
            },
            "$inc": {
                "bloom_count": 1
            }
        },
        upsert=True,
    )


# Event functions
_events_cache: Optional[list[Dict]] = None
_events_cache_time: float = 0.0
_EVENTS_CACHE_TTL: float = 30.0  # 30 seconds cache TTL


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


def get_active_events_cached() -> list[Dict]:
    """
    Get all currently active events with caching (30 second TTL).
    Returns list of event dicts.
    """
    global _events_cache, _events_cache_time
    
    current_time = time.time()
    
    # Check if cache is valid
    if _events_cache is not None and (current_time - _events_cache_time) < _EVENTS_CACHE_TTL:
        # Filter out expired events from cache
        filtered_cache = [e for e in _events_cache if e.get("end_time", 0) > current_time]
        if len(filtered_cache) == len(_events_cache):
            # No events expired, return cache as-is
            return _events_cache
        # Some events expired, update cache
        _events_cache = filtered_cache
        _events_cache_time = current_time
        return _events_cache
    
    # Cache miss or expired, fetch from database
    _events_cache = get_active_events()
    _events_cache_time = current_time
    return _events_cache


def _clear_events_cache() -> None:
    """Clear the events cache. Called when events are modified."""
    global _events_cache, _events_cache_time
    _events_cache = None
    _events_cache_time = 0.0


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
    # Clear cache when events are modified
    _clear_events_cache()


def clear_event(event_id: str) -> None:
    """Remove an event by its ID."""
    events = _get_events_collection()
    events.delete_one({"event_id": event_id})
    # Clear cache when events are modified
    _clear_events_cache()


def clear_expired_events() -> None:
    """Remove all expired events."""
    events = _get_events_collection()
    current_time = time.time()
    events.delete_many({"end_time": {"$lte": current_time}})
    # Clear cache when events are modified
    _clear_events_cache()


def get_user_gather_data(user_id: int) -> Dict:
    """
    Fetch all user data needed for gather operations in a single query.
    Returns dict with: balance, basket_upgrades, last_gather_time
    """
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    doc = users.find_one(
        {"_id": int(user_id)},
        {"balance": 1, "basket_upgrades": 1, "last_gather_time": 1}
    )
    
    if not doc:
        return {
            "balance": _get_default_balance(),
            "basket_upgrades": {"basket": 0, "shoes": 0, "gloves": 0, "soil": 0},
            "last_gather_time": 0.0
        }
    
    upgrades = doc.get("basket_upgrades", {})
    return {
        "balance": float(doc.get("balance", _get_default_balance())),
        "basket_upgrades": {
            "basket": upgrades.get("basket", 0),
            "shoes": upgrades.get("shoes", 0),
            "gloves": upgrades.get("gloves", 0),
            "soil": upgrades.get("soil", 0)
        },
        "last_gather_time": float(doc.get("last_gather_time", 0.0))
    }


def reset_user_cooldowns(user_id: int) -> None:
    """Reset all cooldowns for a user (gather, harvest, mine, Russian Roulette elimination, water)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {
            "last_gather_time": 0.0,
            "last_harvest_time": 0.0,
            "last_mine_time": 0.0,
            "last_roulette_elimination_time": 0.0,
            "last_water_time": 0.0,
            "consecutive_water_days": 0
        }},
        upsert=True,
    )


def wipe_user_money(user_id: int) -> None:
    """Reset user's money to default balance, stock holdings, and crypto holdings, keeping all upgrades."""
    users = _get_users_collection()
    default_balance = _get_default_balance()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {
            "balance": float(default_balance),
            "stock_holdings": {},
            "crypto_holdings": {
                "RTC": 0.0,
                "TER": 0.0,
                "CNY": 0.0
            }
        }},
        upsert=True,
    )


def wipe_user_plants(user_id: int) -> None:
    """Reset user's collected plants (items, gather_stats, ripeness_stats), planter achievement, and cooldowns."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {
            "items": {},
            "ripeness_stats": {},
            "gather_stats": {
                "total_items": 0,
                "categories": {},
                "items": {}
            },
            "total_forage_count": 0,
            "bloom_cycle_plants": 0,
            # Reset planter achievement since it's based on total_items
            "achievements.planter": 0,
            # Reset all cooldowns
            "last_gather_time": 0.0,
            "last_harvest_time": 0.0,
            "last_mine_time": 0.0,
            "last_roulette_elimination_time": 0.0,
            "last_water_time": 0.0
        }},
        upsert=True,
    )


def wipe_user_crypto(user_id: int) -> None:
    """Reset user's crypto holdings to 0, keeping everything else."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {
            "crypto_holdings": {
                "RTC": 0.0,
                "TER": 0.0,
                "CNY": 0.0
            }
        }},
        upsert=True,
    )


def wipe_user_all(user_id: int) -> None:
    """Reset user's money and all upgrades (basket, shoes, gloves, soil, harvest upgrades, gardeners, GPUs, plants, stocks, crypto).
    Also resets all achievement-related stats and cooldowns."""
    users = _get_users_collection()
    default_balance = _get_default_balance()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {
            "balance": float(default_balance),
            "basket_upgrades": {
                "basket": 0,
                "shoes": 0,
                "gloves": 0,
                "soil": 0
            },
            "harvest_upgrades": {
                "car": 0,
                "chain": 0,
                "fertilizer": 0,
                "cooldown": 0
            },
            "gardeners": [],
            "gpus": [],
            "items": {},
            "ripeness_stats": {},
            "gather_stats": {
                "total_items": 0,
                "categories": {},
                "items": {}
            },
            "total_forage_count": 0,
            "bloom_cycle_plants": 0,
            "stock_holdings": {},
            "crypto_holdings": {
                "RTC": 0.0,
                "TER": 0.0,
                "CNY": 0.0
            },
            "bloom_count": 0,
            # Reset all achievement-related stats
            "achievements": {
                "gatherer": 0,
                "coinflip_total": 0,
                "coinflip_win_streak": 0,
                "harvesting": 0,
                "planter": 0,
                "water_streak": 0,
                "blooming": 0,
                "russian_roulette": 0,
                "hidden_achievements_discovered": 0,
                "hidden_achievements": {
                    "john_rockefeller": False,
                    "beating_the_odds": False,
                    "beneficiary": False,
                    "leap_year": False,
                    "ceo": False,
                    "blockchain": False,
                    "almost_got_it": False,
                    "maxed_out": False,
                    "social_butterfly": False,
                    "high_reroller": False
                },
                "areas_unlocked": 0
            },
            "coinflip_count": 0,
            "coinflip_win_streak": 0,
            "gather_command_count": 0,
            "harvest_command_count": 0,
            "consecutive_water_days": 0,
            "water_count": 0,
            "russian_games_played": 0,
            # Reset attunements
            "hoe_enchantment": None,
            "tractor_enchantment": None,
            # Reset invite rewards (claimed rewards reset on wipe)
            "invite_stats": {
                "invites_created": 0,
                "total_joins": 0,
                "rewards_earned": 0.0,
                "invite_codes": [],
                "claimed_rewards": []
            },
            # Reset all cooldowns
            "last_gather_time": 0.0,
            "last_harvest_time": 0.0,
            "last_mine_time": 0.0,
            "last_roulette_elimination_time": 0.0,
            "last_water_time": 0.0,
            # Reset attunements
            "hoe_enchantment": None,
            "tractor_enchantment": None,
            # Reset unlocked areas
            "unlocked_areas": {
                "grove": False,
                "marsh": False,
                "bog": False,
                "mire": False
            },
            # Reset daily shop
            "shop_inventory": {},
            "daily_shop_purchases_count": 0,
            "daily_shop_last_date_est": ""
        }},
        upsert=True,
    )


def add_shop_item_to_user(user_id: int, item_id: str, amount: int = 1) -> None:
    """Add a daily shop item to a user's inventory (e.g. for admin giveaway). Does not deduct tree rings. Caps at 1 per item."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"shop_inventory": 1})
    inv = dict(doc.get("shop_inventory", {})) if doc else {}
    current = inv.get(item_id, 0)
    inv[item_id] = min(current + int(amount), 1)
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"shop_inventory": inv}},
        upsert=True,
    )


# Achievement functions
def get_user_achievement_level(user_id: int, achievement_name: str) -> int:
    """Get user's achievement level for a specific achievement. Returns 0 if not set."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"achievements": 1})
    if not doc:
        return 0
    achievements = doc.get("achievements", {})
    return int(achievements.get(achievement_name, 0))


def set_user_achievement_level(user_id: int, achievement_name: str, level: int) -> None:
    """Set user's achievement level for a specific achievement."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {f"achievements.{achievement_name}": int(level)}},
        upsert=True,
    )


def get_user_hidden_achievements_count(user_id: int) -> int:
    """Get user's count of hidden achievements discovered."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"achievements.hidden_achievements_discovered": 1})
    if not doc:
        return 0
    achievements = doc.get("achievements", {})
    return int(achievements.get("hidden_achievements_discovered", 0))


def increment_hidden_achievements_count(user_id: int) -> None:
    """Increment user's hidden achievements discovered count by 1."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"achievements.hidden_achievements_discovered": 1}},
        upsert=True,
    )


def has_hidden_achievement(user_id: int, achievement_name: str) -> bool:
    """Check if user has a specific hidden achievement."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"achievements.hidden_achievements": 1})
    if not doc:
        return False
    achievements = doc.get("achievements", {})
    hidden_achievements = achievements.get("hidden_achievements", {})
    return bool(hidden_achievements.get(achievement_name, False))


def unlock_hidden_achievement(user_id: int, achievement_name: str) -> bool:
    """Unlock a hidden achievement for a user. Returns True if newly unlocked, False if already had it."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    # Check if already unlocked
    if has_hidden_achievement(user_id, achievement_name):
        return False
    
    # Unlock the achievement and increment count
    users.update_one(
        {"_id": int(user_id)},
        {
            "$set": {f"achievements.hidden_achievements.{achievement_name}": True},
            "$inc": {"achievements.hidden_achievements_discovered": 1}
        },
        upsert=True,
    )
    return True


def get_user_coinflip_count(user_id: int) -> int:
    """Get user's total coinflip count."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"coinflip_count": 1})
    if not doc:
        return 0
    return int(doc.get("coinflip_count", 0))


def increment_user_coinflip_count(user_id: int) -> None:
    """Increment user's coinflip count by 1."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"coinflip_count": 1}},
        upsert=True,
    )


def get_user_coinflip_win_streak(user_id: int) -> int:
    """Get user's current coinflip win streak."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"coinflip_win_streak": 1})
    if not doc:
        return 0
    return int(doc.get("coinflip_win_streak", 0))


def set_user_coinflip_win_streak(user_id: int, streak: int) -> None:
    """Set user's coinflip win streak."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"coinflip_win_streak": int(streak)}},
        upsert=True,
    )


def get_user_gather_command_count(user_id: int) -> int:
    """Get user's total /gather command count (not including gardeners)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"gather_command_count": 1})
    if not doc:
        return 0
    return int(doc.get("gather_command_count", 0))


def increment_user_gather_command_count(user_id: int) -> None:
    """Increment user's /gather command count by 1."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"gather_command_count": 1}},
        upsert=True,
    )


def get_user_harvest_command_count(user_id: int) -> int:
    """Get user's total /harvest command count (not including gardeners or auto-harvest)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"harvest_command_count": 1})
    if not doc:
        return 0
    return int(doc.get("harvest_command_count", 0))


def increment_user_harvest_command_count(user_id: int) -> None:
    """Increment user's /harvest command count by 1."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"harvest_command_count": 1}},
        upsert=True,
    )


def get_user_russian_games_played(user_id: int) -> int:
    """Get user's total Russian Roulette games played (died or cashed out)."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"russian_games_played": 1})
    if not doc:
        return 0
    return int(doc.get("russian_games_played", 0))


def increment_user_russian_games_played(user_id: int) -> int:
    """Increment user's Russian Roulette games played by 1. Returns the new count."""
    users = _get_users_collection()
    result = users.find_one_and_update(
        {"_id": int(user_id)},
        {"$inc": {"russian_games_played": 1}},
        upsert=True,
        return_document=True,
        projection={"russian_games_played": 1}
    )
    return int(result.get("russian_games_played", 1)) if result else 1


# Invite tracking functions
def get_user_invite_stats(user_id: int) -> Dict:
    """Get user's invite statistics. Returns dict with invites_created, total_joins, rewards_earned, claimed_rewards."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"invite_stats": 1})
    if not doc or "invite_stats" not in doc:
        return {
            "invites_created": 0,
            "total_joins": 0,
            "rewards_earned": 0.0,
            "claimed_rewards": []
        }
    stats = doc.get("invite_stats", {})
    return {
        "invites_created": int(stats.get("invites_created", 0)),
        "total_joins": int(stats.get("total_joins", 0)),
        "rewards_earned": float(stats.get("rewards_earned", 0.0)),
        "claimed_rewards": list(stats.get("claimed_rewards", []))
    }


def increment_invite_joins(inviter_id: int, reward_amount: float) -> None:
    """Increment invite joins count and add reward for the inviter."""
    users = _get_users_collection()
    _ensure_user_document(inviter_id)
    users.update_one(
        {"_id": int(inviter_id)},
        {
            "$inc": {
                "invite_stats.total_joins": 1,
                "invite_stats.rewards_earned": float(reward_amount),
                "balance": float(reward_amount)
            }
        },
        upsert=True,
    )


def track_invite_created(user_id: int, invite_code: str) -> None:
    """Track that a user created an invite. Stores invite code for reference."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    users.update_one(
        {"_id": int(user_id)},
        {
            "$inc": {"invite_stats.invites_created": 1},
            "$addToSet": {"invite_stats.invite_codes": str(invite_code)}
        },
        upsert=True,
    )


def claim_invite_reward(user_id: int, reward_tier: int) -> bool:
    """Mark an invite reward tier as claimed. Returns True if newly claimed, False if already claimed."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    
    # Check if already claimed
    doc = users.find_one({"_id": int(user_id)}, {"invite_stats.claimed_rewards": 1})
    claimed = []
    if doc and "invite_stats" in doc:
        claimed = doc["invite_stats"].get("claimed_rewards", [])
    
    if reward_tier in claimed:
        return False
    
    users.update_one(
        {"_id": int(user_id)},
        {"$addToSet": {"invite_stats.claimed_rewards": int(reward_tier)}},
        upsert=True,
    )
    return True


def get_user_claimed_invite_rewards(user_id: int) -> list:
    """Get list of claimed invite reward tiers."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"invite_stats.claimed_rewards": 1})
    if not doc or "invite_stats" not in doc:
        return []
    return list(doc["invite_stats"].get("claimed_rewards", []))


def has_secret_gardener(user_id: int) -> bool:
    """Check if user has the secret gardener unlocked (invite reward tier 10)."""
    claimed = get_user_claimed_invite_rewards(user_id)
    return 10 in claimed


def has_secret_gardener_harvest(user_id: int) -> bool:
    """Check if user has the secret gardener auto-harvest unlocked (invite reward tier 12)."""
    claimed = get_user_claimed_invite_rewards(user_id)
    return 12 in claimed


def get_invite_cooldown_reductions(user_id: int) -> Dict:
    """Get permanent cooldown reductions from invite rewards.
    Returns dict with gather_reduction, harvest_reduction, mine_reduction, water_double."""
    claimed = get_user_claimed_invite_rewards(user_id)
    return {
        "gather_reduction": 10 if 13 in claimed else 0,      # 10 seconds
        "harvest_reduction": 300 if 14 in claimed else 0,     # 5 minutes (300 seconds)
        "mine_reduction": 1200 if 15 in claimed else 0,       # 20 minutes (1200 seconds)
        "water_double": 19 in claimed                         # Can water twice a day
    }


def get_all_users_with_secret_gardener() -> list[int]:
    """Get all user IDs who have the secret gardener unlocked (invite reward tier 10)."""
    users = _get_users_collection()
    cursor = users.find(
        {"invite_stats.claimed_rewards": 10},
        {"_id": 1}
    )
    return [doc["_id"] for doc in cursor]


def has_user_joined_before(user_id: int) -> bool:
    """Check if a user has already joined the server before (to prevent duplicate invite rewards)."""
    users = _get_users_collection()
    # Use _id = -1 as a special document to track all users who have joined
    doc = users.find_one({"_id": -1}, {"joined_users": 1})
    if not doc:
        return False
    joined_users = doc.get("joined_users", [])
    return int(user_id) in joined_users


def mark_user_as_joined(user_id: int) -> None:
    """Mark a user as having joined the server (prevents duplicate invite rewards)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": -1},
        {"$addToSet": {"joined_users": int(user_id)}},
        upsert=True,
    )


def increment_invite_joins_new_user(inviter_id: int, new_user_id: int, reward_amount: float) -> bool:
    """
    Increment invite joins count and add reward for the inviter, but only if the new user hasn't joined before.
    Returns True if reward was awarded, False if user already joined before.
    """
    # Check if this is a new user
    if has_user_joined_before(new_user_id):
        return False
    
    # Mark user as joined
    mark_user_as_joined(new_user_id)
    
    # Award reward to inviter
    increment_invite_joins(inviter_id, reward_amount)
    
    return True


# Attunement functions
def get_user_hoe_attunement(user_id: int) -> Optional[Dict]:
    """Get user's hoe attunement. Returns dict with attunement data or None."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"hoe_enchantment": 1})
    if not doc:
        return None
    return doc.get("hoe_enchantment", None)


def set_user_hoe_attunement(user_id: int, attunement: Optional[Dict]) -> None:
    """Set user's hoe attunement. Pass None to clear."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"hoe_enchantment": attunement}},
        upsert=True,
    )


def get_user_tractor_attunement(user_id: int) -> Optional[Dict]:
    """Get user's tractor attunement. Returns dict with attunement data or None."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"tractor_enchantment": 1})
    if not doc:
        return None
    return doc.get("tractor_enchantment", None)


def set_user_tractor_attunement(user_id: int, attunement: Optional[Dict]) -> None:
    """Set user's tractor attunement. Pass None to clear."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"tractor_enchantment": attunement}},
        upsert=True,
    )


def increment_invite_joins_count_only(inviter_id: int, new_user_id: int) -> bool:
    """
    Increment invite joins count for the inviter (no money reward), but only if the new user hasn't joined before.
    Returns True if count was incremented, False if user already joined before.
    """
    # Check if this is a new user
    if has_user_joined_before(new_user_id):
        return False
    
    # Mark user as joined
    mark_user_as_joined(new_user_id)
    
    # Increment invite join count only (no money reward)
    users = _get_users_collection()
    _ensure_user_document(inviter_id)
    users.update_one(
        {"_id": int(inviter_id)},
        {"$inc": {"invite_stats.total_joins": 1}},
        upsert=True,
    )
    
    return True


# Attunement functions (second block kept for compatibility)
def get_user_hoe_attunement(user_id: int) -> Optional[Dict]:
    """Get user's hoe attunement. Returns dict with attunement data or None."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"hoe_enchantment": 1})
    if not doc:
        return None
    return doc.get("hoe_enchantment", None)


def set_user_hoe_attunement(user_id: int, attunement: Optional[Dict]) -> None:
    """Set user's hoe attunement. Pass None to clear."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"hoe_enchantment": attunement}},
        upsert=True,
    )


def get_user_tractor_attunement(user_id: int) -> Optional[Dict]:
    """Get user's tractor attunement. Returns dict with attunement data or None."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"tractor_enchantment": 1})
    if not doc:
        return None
    return doc.get("tractor_enchantment", None)


def set_user_tractor_attunement(user_id: int, attunement: Optional[Dict]) -> None:
    """Set user's tractor attunement. Pass None to clear."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"tractor_enchantment": attunement}},
        upsert=True,
    )


# BLOOMING rank auto-unlock: CEDAR+ = grove, BIRCH+ = marsh, MAPLE+ = bog, OAK+ = mire (bloom_count 3,6,9,12)
def _merge_bloom_auto_unlock(areas: Dict, bloom_count: int) -> Dict[str, bool]:
    """Merge raw unlocked_areas with BLOOMING rank auto-unlocks."""
    return {
        "grove": bool(areas.get("grove", False)) or bloom_count >= 3,
        "marsh": bool(areas.get("marsh", False)) or bloom_count >= 6,
        "bog": bool(areas.get("bog", False)) or bloom_count >= 9,
        "mire": bool(areas.get("mire", False)) or bloom_count >= 12,
    }


# Area unlock functions
def get_user_unlocked_areas(user_id: int) -> Dict[str, bool]:
    """Get user's unlocked areas. Returns dict with area names as keys and unlock status as values.
    CEDAR+ auto-unlocks grove, BIRCH+ marsh, MAPLE+ bog, OAK+ mire."""
    users = _get_users_collection()
    _ensure_user_document(user_id)
    doc = users.find_one({"_id": int(user_id)}, {"unlocked_areas": 1, "bloom_count": 1})
    if not doc:
        return {"grove": False, "marsh": False, "bog": False, "mire": False}
    areas = doc.get("unlocked_areas", {})
    bloom_count = int(doc.get("bloom_count", 0))
    return _merge_bloom_auto_unlock(areas, bloom_count)


def unlock_user_area(user_id: int, area_name: str) -> None:
    """Unlock a gathering area for a user."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {f"unlocked_areas.{area_name}": True}},
        upsert=True,
    )


def reset_user_areas(user_id: int) -> None:
    """Reset all unlocked areas for a user (used on bloom)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$set": {"unlocked_areas": {
            "grove": False,
            "marsh": False,
            "bog": False,
            "mire": False
        }}},
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Atomic balance operations (for /imbue safety)
# ---------------------------------------------------------------------------

def atomic_deduct_balance(user_id: int, cost: float) -> tuple:
    """Atomically deduct *cost* from the user's balance **only** if they can
    afford it.  Uses ``findOneAndUpdate`` so the check-and-deduct is a single
    database round-trip – no race window between reading and writing.

    Returns ``(success, new_balance)``.  On failure *new_balance* is the
    current (unchanged) balance.
    """
    users = _get_users_collection()
    _ensure_user_document(user_id)

    normalized_cost = round(float(cost), 2)
    result = users.find_one_and_update(
        {"_id": int(user_id), "balance": {"$gte": normalized_cost - 0.001}},
        {"$inc": {"balance": -normalized_cost}},
        return_document=True,
        projection={"balance": 1},
    )
    if result is None:
        # Could not afford – return current balance unchanged
        doc = users.find_one({"_id": int(user_id)}, {"balance": 1})
        bal = float(doc.get("balance", 0)) if doc else 0.0
        return False, round(bal, 2)
    return True, round(float(result.get("balance", 0)), 2)


def refund_balance(user_id: int, amount: float) -> None:
    """Add *amount* back to the user's balance (used when an operation that
    already deducted money fails afterwards)."""
    users = _get_users_collection()
    users.update_one(
        {"_id": int(user_id)},
        {"$inc": {"balance": round(float(amount), 2)}},
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Full-data single-query fetchers (gather / harvest optimisation)
# ---------------------------------------------------------------------------

def get_user_gather_full_data(user_id: int) -> Dict:
    """Fetch **all** fields required for a complete ``/gather`` operation in
    one database round-trip.  This replaces 10+ individual queries."""
    users = _get_users_collection()
    _ensure_user_document(user_id)

    doc = users.find_one(
        {"_id": int(user_id)},
        {
            "balance": 1,
            "basket_upgrades": 1,
            "last_gather_time": 1,
            "last_roulette_elimination_time": 1,
            "hoe_enchantment": 1,
            "tree_rings": 1,
            "water_count": 1,
            "bloom_count": 1,
            "bloom_cycle_plants": 1,
            "achievements": 1,
            "consecutive_water_days": 1,
            "invite_stats.claimed_rewards": 1,
            "gather_stats.total_items": 1,
            "total_forage_count": 1,
            "harvest_upgrades": 1,
            "unlocked_areas": 1,
            "gather_command_count": 1,
            "shop_inventory": 1,
        },
    )

    if not doc:
        return {
            "balance": _get_default_balance(),
            "basket_upgrades": {"basket": 0, "shoes": 0, "gloves": 0, "soil": 0},
            "last_gather_time": 0.0,
            "last_roulette_elimination_time": 0.0,
            "hoe_enchantment": None,
            "tree_rings": 0,
            "water_count": 0,
            "bloom_count": 0,
            "bloom_cycle_plants": 0,
            "achievements": {},
            "consecutive_water_days": 0,
            "invite_claimed_rewards": [],
            "gather_stats_total_items": 0,
            "total_forage_count": 0,
            "harvest_upgrades": {"car": 0, "chain": 0, "fertilizer": 0, "cooldown": 0},
            "unlocked_areas": {},
            "gather_command_count": 0,
            "shop_inventory": {},
        }

    upgrades = doc.get("basket_upgrades", {})
    h_upgrades = doc.get("harvest_upgrades", {})
    achievements = doc.get("achievements", {})
    invite_stats = doc.get("invite_stats", {})

    return {
        "balance": float(doc.get("balance", _get_default_balance())),
        "basket_upgrades": {
            "basket": upgrades.get("basket", 0),
            "shoes": upgrades.get("shoes", 0),
            "gloves": upgrades.get("gloves", 0),
            "soil": upgrades.get("soil", 0),
        },
        "last_gather_time": float(doc.get("last_gather_time", 0.0)),
        "last_roulette_elimination_time": float(doc.get("last_roulette_elimination_time", 0.0)),
        "hoe_enchantment": doc.get("hoe_enchantment", None),
        "tree_rings": int(doc.get("tree_rings", 0)),
        "water_count": int(doc.get("water_count", 0)),
        "bloom_count": int(doc.get("bloom_count", 0)),
        "bloom_cycle_plants": int(doc.get("bloom_cycle_plants", 0)),
        "achievements": achievements,
        "consecutive_water_days": int(doc.get("consecutive_water_days", 0)),
        "invite_claimed_rewards": list(invite_stats.get("claimed_rewards", [])),
        "gather_stats_total_items": int(doc.get("gather_stats", {}).get("total_items", 0)),
        "total_forage_count": int(doc.get("total_forage_count", 0)),
        "harvest_upgrades": {
            "car": h_upgrades.get("car", 0),
            "chain": h_upgrades.get("chain", 0),
            "fertilizer": h_upgrades.get("fertilizer", 0),
            "cooldown": h_upgrades.get("cooldown", 0),
        },
        "unlocked_areas": _merge_bloom_auto_unlock(doc.get("unlocked_areas", {}), int(doc.get("bloom_count", 0))),
        "gather_command_count": int(doc.get("gather_command_count", 0)),
        "shop_inventory": dict(doc.get("shop_inventory", {})),
    }


def get_user_harvest_full_data(user_id: int) -> Dict:
    """Fetch **all** fields required for a complete ``/harvest`` operation in
    one database round-trip.  This replaces 15+ individual queries."""
    users = _get_users_collection()
    _ensure_user_document(user_id)

    doc = users.find_one(
        {"_id": int(user_id)},
        {
            "balance": 1,
            "basket_upgrades": 1,
            "harvest_upgrades": 1,
            "last_harvest_time": 1,
            "last_roulette_elimination_time": 1,
            "tractor_enchantment": 1,
            "tree_rings": 1,
            "water_count": 1,
            "bloom_count": 1,
            "bloom_cycle_plants": 1,
            "achievements": 1,
            "consecutive_water_days": 1,
            "invite_stats.claimed_rewards": 1,
            "gather_stats.total_items": 1,
            "total_forage_count": 1,
            "unlocked_areas": 1,
            "harvest_command_count": 1,
            "shop_inventory": 1,
        },
    )

    if not doc:
        return {
            "balance": _get_default_balance(),
            "basket_upgrades": {"basket": 0, "shoes": 0, "gloves": 0, "soil": 0},
            "harvest_upgrades": {"car": 0, "chain": 0, "fertilizer": 0, "cooldown": 0},
            "last_harvest_time": 0.0,
            "last_roulette_elimination_time": 0.0,
            "tractor_enchantment": None,
            "tree_rings": 0,
            "water_count": 0,
            "bloom_count": 0,
            "bloom_cycle_plants": 0,
            "achievements": {},
            "consecutive_water_days": 0,
            "invite_claimed_rewards": [],
            "gather_stats_total_items": 0,
            "total_forage_count": 0,
            "unlocked_areas": {},
            "harvest_command_count": 0,
            "shop_inventory": {},
        }

    basket_ups = doc.get("basket_upgrades", {})
    harvest_ups = doc.get("harvest_upgrades", {})
    achievements = doc.get("achievements", {})
    invite_stats = doc.get("invite_stats", {})

    return {
        "balance": float(doc.get("balance", _get_default_balance())),
        "basket_upgrades": {
            "basket": basket_ups.get("basket", 0),
            "shoes": basket_ups.get("shoes", 0),
            "gloves": basket_ups.get("gloves", 0),
            "soil": basket_ups.get("soil", 0),
        },
        "harvest_upgrades": {
            "car": harvest_ups.get("car", 0),
            "chain": harvest_ups.get("chain", 0),
            "fertilizer": harvest_ups.get("fertilizer", 0),
            "cooldown": harvest_ups.get("cooldown", 0),
        },
        "last_harvest_time": float(doc.get("last_harvest_time", 0.0)),
        "last_roulette_elimination_time": float(doc.get("last_roulette_elimination_time", 0.0)),
        "tractor_enchantment": doc.get("tractor_enchantment", None),
        "tree_rings": int(doc.get("tree_rings", 0)),
        "water_count": int(doc.get("water_count", 0)),
        "bloom_count": int(doc.get("bloom_count", 0)),
        "bloom_cycle_plants": int(doc.get("bloom_cycle_plants", 0)),
        "achievements": achievements,
        "consecutive_water_days": int(doc.get("consecutive_water_days", 0)),
        "invite_claimed_rewards": list(invite_stats.get("claimed_rewards", [])),
        "gather_stats_total_items": int(doc.get("gather_stats", {}).get("total_items", 0)),
        "total_forage_count": int(doc.get("total_forage_count", 0)),
        "unlocked_areas": _merge_bloom_auto_unlock(doc.get("unlocked_areas", {}), int(doc.get("bloom_count", 0))),
        "harvest_command_count": int(doc.get("harvest_command_count", 0)),
        "shop_inventory": dict(doc.get("shop_inventory", {})),
    }


def perform_harvest_batch_update(
    user_id: int,
    items_inc: Dict[str, int],
    ripeness_inc: Dict[str, int],
    balance_increment: float,
    num_items: int,
    pre_total_items: int,
    pre_bloom_cycle: int,
    set_cooldown: bool = False,
    increment_command_count: bool = False,
) -> int:
    """Perform **all** harvest-related writes in a single MongoDB operation.

    Replaces N×3 individual writes (``add_user_item``, ``add_ripeness_stat``,
    ``increment_total_items_only`` per item) with **one** ``update_one``.

    When *set_cooldown* is True the harvest cooldown is set in the same write.
    When *increment_command_count* is True the harvest_command_count is
    incremented in the same write.

    Returns the number of Tree Rings awarded by this harvest.
    """
    users = _get_users_collection()
    _ensure_user_document(user_id)

    new_total = pre_total_items + num_items
    tree_rings_to_award = 0
    for milestone in range(200, new_total + 1, 200):
        if pre_total_items < milestone <= new_total:
            tree_rings_to_award += 1

    new_bloom_cycle = pre_bloom_cycle + num_items

    inc_ops: Dict[str, float | int] = {
        "balance": float(balance_increment),
        "gather_stats.total_items": num_items,
        "total_forage_count": num_items,
    }
    if tree_rings_to_award > 0:
        inc_ops["tree_rings"] = tree_rings_to_award
    if increment_command_count:
        inc_ops["harvest_command_count"] = 1

    for item_name, count in items_inc.items():
        inc_ops[f"items.{item_name}"] = count

    for ripeness_name, count in ripeness_inc.items():
        inc_ops[f"ripeness_stats.{ripeness_name}"] = count

    set_ops: Dict[str, object] = {"bloom_cycle_plants": new_bloom_cycle}
    if set_cooldown:
        set_ops["last_harvest_time"] = float(time.time())

    users.update_one(
        {"_id": int(user_id)},
        {
            "$inc": inc_ops,
            "$set": set_ops,
        },
        upsert=True,
    )

    return tree_rings_to_award


# ---------------------------------------------------------------------------
# Steal: revert victim / apply to stealer (for gather and harvest)
# ---------------------------------------------------------------------------

def steal_revert_gather(
    victim_id: int,
    value: float,
    item_name: str,
    ripeness_name: str,
    category: str,
) -> None:
    """Revert a gather from the victim (subtract balance, 1 plant, stats, bloom_cycle)."""
    users = _get_users_collection()
    doc = users.find_one(
        {"_id": int(victim_id)},
        {"bloom_cycle_plants": 1},
    )
    current_bloom = int(doc.get("bloom_cycle_plants", 0)) if doc else 0
    new_bloom = max(0, current_bloom - 1)

    inc_ops = {
        "balance": -float(value),
        f"items.{item_name}": -1,
        f"ripeness_stats.{ripeness_name}": -1,
        "gather_stats.total_items": -1,
        f"gather_stats.categories.{category}": -1,
        f"gather_stats.items.{item_name}": -1,
        "total_forage_count": -1,
    }
    users.update_one(
        {"_id": int(victim_id)},
        {"$inc": inc_ops, "$set": {"bloom_cycle_plants": new_bloom}},
        upsert=True,
    )


def steal_apply_gather(
    stealer_id: int,
    value: float,
    item_name: str,
    ripeness_name: str,
    category: str,
) -> bool:
    """Apply a stolen gather to the stealer. Returns True if tree ring was awarded."""
    return perform_gather_update(
        stealer_id,
        balance_increment=value,
        item_name=item_name,
        ripeness_name=ripeness_name,
        category=category,
        apply_cooldown=False,
        increment_command_count=False,
    )


def steal_revert_harvest(
    victim_id: int,
    items_inc: Dict[str, int],
    ripeness_inc: Dict[str, int],
    balance_increment: float,
    num_items: int,
) -> None:
    """Revert a harvest from the victim (subtract balance, items, stats, bloom_cycle)."""
    users = _get_users_collection()
    doc = users.find_one(
        {"_id": int(victim_id)},
        {"bloom_cycle_plants": 1},
    )
    current_bloom = int(doc.get("bloom_cycle_plants", 0)) if doc else 0
    new_bloom = max(0, current_bloom - num_items)

    inc_ops: Dict[str, float | int] = {
        "balance": -float(balance_increment),
        "gather_stats.total_items": -num_items,
        "total_forage_count": -num_items,
    }
    for item_name, count in items_inc.items():
        inc_ops[f"items.{item_name}"] = -count
    for ripeness_name, count in ripeness_inc.items():
        inc_ops[f"ripeness_stats.{ripeness_name}"] = -count

    users.update_one(
        {"_id": int(victim_id)},
        {"$inc": inc_ops, "$set": {"bloom_cycle_plants": new_bloom}},
        upsert=True,
    )


def steal_apply_harvest(
    stealer_id: int,
    items_inc: Dict[str, int],
    ripeness_inc: Dict[str, int],
    balance_increment: float,
    num_items: int,
) -> int:
    """Apply a stolen harvest to the stealer. Returns number of tree rings awarded."""
    users = _get_users_collection()
    _ensure_user_document(stealer_id)
    doc = users.find_one(
        {"_id": int(stealer_id)},
        {"gather_stats.total_items": 1, "bloom_cycle_plants": 1},
    )
    pre_total = 0
    pre_bloom = 0
    if doc:
        pre_total = int(doc.get("gather_stats", {}).get("total_items", 0))
        pre_bloom = int(doc.get("bloom_cycle_plants", 0))
    return perform_harvest_batch_update(
        stealer_id,
        items_inc=items_inc,
        ripeness_inc=ripeness_inc,
        balance_increment=balance_increment,
        num_items=num_items,
        pre_total_items=pre_total,
        pre_bloom_cycle=pre_bloom,
        set_cooldown=False,
        increment_command_count=False,
    )