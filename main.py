import discord
from discord import app_commands
from discord.ext import commands
import logging
from dotenv import load_dotenv
import os
import random
import time
import asyncio
import uuid
import threading
import datetime
import aiohttp
import warnings
import yfinance as yf

# Suppress Pandas4Warning from yfinance library (deprecated Timestamp.utcnow)
warnings.filterwarnings("ignore", category=FutureWarning, message=".*Timestamp.utcnow.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*Timestamp.utcnow.*")

# Load environment variables FIRST
load_dotenv(override=True)

def _resolve_environment() -> str:
    """Determine which environment the bot is running in."""
    # Explicit ENVIRONMENT takes precedence so local development can override.
    env_value = os.getenv('ENVIRONMENT')
    if env_value:
        return env_value

    # Cloud Run / production deployments can provide ENVIRONMENT_PROD.
    prod_value = os.getenv('ENVIRONMENT_PROD')
    if prod_value:
        return prod_value

    return 'development'


def _redact_mongo_uri(uri: str | None) -> str:
    """Return a redacted Mongo URI suitable for logging."""
    if not uri:
        return "None"
    try:
        # Split credentials from the rest, e.g. mongodb+srv://user:pass@host/db
        scheme, remainder = uri.split("://", 1)
        if "@" in remainder:
            _, host_part = remainder.split("@", 1)
            return f"{scheme}://<redacted>@{host_part}"
        return uri
    except ValueError:
        return "<invalid-uri>"

environment = _resolve_environment()
is_production = environment.lower() == 'production'

# Database helpers (MongoDB only)
from database import (
    init_database,
    get_user_balance,
    update_user_balance,
    get_user_last_gather_time,
    update_user_last_gather_time,
    get_user_last_harvest_time,
    update_user_last_harvest_time,
    get_user_last_roulette_elimination_time,
    update_user_last_roulette_elimination_time,
    increment_forage_count,
    get_forage_count,
    increment_total_items_only,
    increment_gather_stats,
    add_user_item,
    add_ripeness_stat,
    get_all_users_balance,
    get_all_users_total_items,
    get_all_users_ranks,
    get_user_total_items,
    get_user_items,
    get_user_basket_upgrades,
    set_user_basket_upgrade,
    get_user_harvest_upgrades,
    set_user_harvest_upgrade,
    get_user_crypto_holdings,
    update_user_crypto_holdings,
    get_user_last_mine_time,
    update_user_last_mine_time,
    get_crypto_prices,
    update_crypto_prices,
    get_user_gardeners,
    add_gardener,
    update_gardener_stats,
    set_gardener_has_tool,
    get_all_users_with_gardeners,
    get_user_gpus,
    add_gpu,
    get_all_users_with_gpus,
    set_user_notification_channel,
    get_user_notification_channel,
    get_user_stock_holdings,
    update_user_stock_holdings,
    get_active_events,
    get_active_events_cached,
    get_active_event_by_type,
    set_active_event,
    clear_event,
    clear_expired_events,
    get_user_gather_data,
    perform_gather_update,
    get_user_tree_rings,
    increment_tree_rings,
    get_bloom_multiplier,
    get_bloom_rank,
    get_user_bloom_count,
    perform_bloom,
    get_user_last_water_time,
    update_user_last_water_time,
    get_user_consecutive_water_days,
    set_user_consecutive_water_days,
    get_user_water_count,
    increment_user_water_count,
    get_water_multiplier,
    get_daily_bonus_multiplier,
    reset_user_cooldowns,
    wipe_user_money,
    wipe_user_plants,
    wipe_user_crypto,
    wipe_user_all,
    _get_users_collection,
    get_user_achievement_level,
    set_user_achievement_level,
    get_user_hidden_achievements_count,
    increment_hidden_achievements_count,
    has_hidden_achievement,
    unlock_hidden_achievement,
    get_user_coinflip_count,
    increment_user_coinflip_count,
    get_user_coinflip_win_streak,
    set_user_coinflip_win_streak,
    get_user_gather_command_count,
    increment_user_gather_command_count,
    get_user_harvest_command_count,
    increment_user_harvest_command_count,
)

try:
    init_database()
    print("Connected to MongoDB successfully")
except Exception as error:
    logging.exception("Failed to initialise MongoDB connection: %s", error)
    raise

# Load the correct token based on environment
token_env_key = 'DISCORD_TOKEN' if is_production else 'DISCORD_DEV_TOKEN'
token = os.getenv(token_env_key)

# Configure logging properly
# Set up file handler for all logs
file_handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
file_handler.setLevel(logging.DEBUG)  # Log everything to file

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)  # Set root logger to INFO
root_logger.addHandler(file_handler)

# Set discord.py logger to INFO to reduce terminal clutter (DEBUG is too verbose)
discord_logger = logging.getLogger('discord')
discord_logger.setLevel(logging.INFO)

# Set discord gateway/HTTP loggers to WARNING to reduce noise even more
logging.getLogger('discord.gateway').setLevel(logging.WARNING)
logging.getLogger('discord.http').setLevel(logging.WARNING)

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='/', intents=intents)

# Helper function to safely handle interaction responses and prevent "interaction failed" messages
async def safe_interaction_response(interaction: discord.Interaction, 
                                   response_func, 
                                   *args, 
                                   error_message="❌ An error occurred. Please try again.",
                                   **kwargs):
    """
    Safely send an interaction response, catching all errors to prevent "interaction failed" message.
    
    Args:
        interaction: The Discord interaction object
        response_func: The function to call (e.g., interaction.response.send_message)
        *args, **kwargs: Arguments to pass to response_func
        error_message: Message to send if response_func fails
    """
    try:
        return await response_func(*args, **kwargs)
    except discord.errors.InteractionResponded:
        # Already responded, try followup instead if applicable
        if hasattr(interaction, 'followup'):
            try:
                if 'send_message' in str(response_func) or 'send' in str(response_func):
                    # Extract message content from args/kwargs
                    content = args[0] if args else kwargs.get('content', error_message)
                    ephemeral = kwargs.get('ephemeral', True)
                    return await interaction.followup.send(content, ephemeral=ephemeral)
                elif 'edit_message' in str(response_func) or 'edit' in str(response_func):
                    message_id = interaction.message.id if hasattr(interaction, 'message') else None
                    if message_id:
                        return await interaction.followup.edit_message(message_id, *args[1:], **kwargs)
            except:
                pass
    except discord.errors.NotFound:
        # Interaction expired
        print(f"Interaction expired for user {interaction.user.id if hasattr(interaction, 'user') else 'unknown'}")
    except Exception as e:
        # Try to send error message
        try:
            if hasattr(interaction, 'response') and not interaction.response.is_done():
                await interaction.response.send_message(error_message, ephemeral=True)
            elif hasattr(interaction, 'followup'):
                await interaction.followup.send(error_message, ephemeral=True)
        except:
            pass
        print(f"Error in interaction response: {e}")
    return None

# Helper function to safely defer an interaction
async def safe_defer(interaction: discord.Interaction, ephemeral: bool = False):
    """Safely defer an interaction, handling all possible errors."""
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral)
        return True
    except discord.errors.InteractionResponded:
        # Already responded, that's fine
        return True
    except discord.errors.NotFound:
        # Interaction expired
        print(f"Interaction expired for user {interaction.user.id if hasattr(interaction, 'user') else 'unknown'}")
        return False
    except Exception as e:
        print(f"Error deferring interaction: {e}")
        return False

# Helper function to send achievement notification
async def send_achievement_notification(interaction: discord.Interaction, achievement_name: str, level: int):
    """Send an ephemeral achievement notification to the user."""
    if achievement_name not in ACHIEVEMENTS:
        return
    
    achievement_data = ACHIEVEMENTS[achievement_name]
    levels = achievement_data["levels"]
    
    if level < len(levels):
        level_data = levels[level]
        achievement_display_name = level_data["name"]
        achievement_description = level_data["description"]
        
        embed = discord.Embed(
            title="🏆 Achievement Unlocked!",
            description=f"**{achievement_display_name}**\n{achievement_description}",
            color=discord.Color.gold()
        )
        
        # Send ephemeral message directly using followup to ensure it's only visible to the user
        try:
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            print(f"Error sending achievement notification: {e}")

# Helper function to send hidden achievement notification
async def send_hidden_achievement_notification(interaction: discord.Interaction, achievement_key: str):
    """Send a hidden achievement notification embed to the user."""
    if achievement_key not in HIDDEN_ACHIEVEMENTS:
        return
    
    achievement_data = HIDDEN_ACHIEVEMENTS[achievement_key]
    achievement_name = achievement_data["name"]
    achievement_description = achievement_data["description"]
    
    embed = discord.Embed(
        title="🏆 Hidden Achievement Unlocked!",
        description=f"**{achievement_name}**\n{achievement_description}",
        color=discord.Color.gold()
    )
    
    # Send ephemeral message directly using followup to ensure it's only visible to the user
    try:
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error sending hidden achievement notification: {e}")

# Helper function to send hidden achievement notification (for non-interaction contexts like background tasks)
async def send_hidden_achievement_notification_async(channel, user_mention: str, achievement_key: str):
    """Send a hidden achievement notification embed to a channel."""
    if achievement_key not in HIDDEN_ACHIEVEMENTS:
        return
    
    achievement_data = HIDDEN_ACHIEVEMENTS[achievement_key]
    achievement_name = achievement_data["name"]
    achievement_description = achievement_data["description"]
    
    embed = discord.Embed(
        title="🏆 Hidden Achievement Unlocked!",
        description=f"{user_mention}\n\n**{achievement_name}**\n{achievement_description}",
        color=discord.Color.gold()
    )
    
    try:
        await channel.send(embed=embed)
    except Exception as e:
        print(f"Error sending hidden achievement notification: {e}")

GATHER_COOLDOWN = 60 #(seconds)
HARVEST_COOLDOWN = 60 * 30 #(30 minutes)
MINE_COOLDOWN = 60 * 60 #(1 hour)
ROULETTE_ELIMINATION_COOLDOWN = 60 * 30 #(30 minutes)

# Event check intervals (for testing - adjust these values to change how often events are checked)
# In production, these should be: HOURLY_EVENT_INTERVAL = 3600, DAILY_EVENT_INTERVAL = 86400
HOURLY_EVENT_INTERVAL = 3600 # Seconds between hourly event checks (default: 3600 = 1 hour)
DAILY_EVENT_INTERVAL = 86400 # Seconds between daily event checks (default: 86400 = 24 hours)

# Gardener prices
GARDENER_PRICES = [1000, 10000, 50000, 100000, 250000]

# Gardener gather chances (by gardener ID: 1-5)
GARDENER_CHANCES = {
    1: 0.05,   # 5%
    2: 0.08,   # 8%
    3: 0.10,   # 10%
    4: 0.15,   # 15%
    5: 0.20    # 20%
}

# Gardener tools: name, cost (hardcoded), chance for auto gather to become harvest (stacked)
GARDENER_TOOLS = {
    1: {"name": "Sickle", "cost": 2000, "chance": 0.005},      # 0.5%
    2: {"name": "Scythe", "cost": 20000, "chance": 0.01},       # 1%
    3: {"name": "Reaper", "cost": 100000, "chance": 0.02},     # 2%
    4: {"name": "Tractor", "cost": 200000, "chance": 0.03},    # 3%
    5: {"name": "Combine", "cost": 500000, "chance": 0.05},    # 5%
}

# GPU shop definitions
GPU_SHOP = [
    {"name": "NATIVIDIA RooTX 3050", "percent_increase": 30, "seconds_increase": 3, "price": 600},
    {"name": "NATIVIDIA RooTX 2060", "percent_increase": 40, "seconds_increase": 4, "price": 1500},
    {"name": "Plantel Barc B580", "percent_increase": 60, "seconds_increase": 5, "price": 4000},
    {"name": "NATIVIDIA RooTX 3070", "percent_increase": 90, "seconds_increase": 8, "price": 10000},
    {"name": "RayMD RX 5700XT", "percent_increase": 180, "seconds_increase": 12, "price": 50000},
    {"name": "NATIVIDIA GrowTX 1080-Ti", "percent_increase": 300, "seconds_increase": 20, "price": 110000},
    {"name": "RayMD RX 9060-XT", "percent_increase": 540, "seconds_increase": 30, "price": 200000},
    {"name": "NATIVIDIA RooTX 4070 Ti Super", "percent_increase": 900, "seconds_increase": 45, "price": 400000},
    {"name": "NATIVIDIA RooTX 4090", "percent_increase": 1500, "seconds_increase": 60, "price": 1000000},
    {"name": "NATIVIDIA RooTX 5090", "percent_increase": 5000, "seconds_increase": 100, "price": 2000000},
]

# BASKET UPGRADE PATHS
UPGRADE_PRICES = [500, 1500, 4000, 10000, 25000, 60000, 150000, 350000, 700000, 1000000]

BASKET_UPGRADES = [
    {"name": "String Basket", "multiplier": 1.1},
    {"name": "Wooden Basket", "multiplier": 1.5},
    {"name": "Stone Basket", "multiplier": 2.0},
    {"name": "Iron Basket", "multiplier": 3.0},
    {"name": "Gold Basket", "multiplier": 5.0},
    {"name": "Diamond Basket", "multiplier": 7.5},
    {"name": "Netherite Basket", "multiplier": 12.0},
    {"name": "Void Basket", "multiplier": 20.0},
    {"name": "Spectral Basket", "multiplier": 30.0},
    {"name": "Luminite Basket", "multiplier": 55.0},
]

SHOES_UPGRADES = [
    {"name": "Sandals", "reduction": 1},
    {"name": "Tennis Shoes", "reduction": 2},
    {"name": "Nike Air Forces", "reduction": 4},
    {"name": "Diamond Boots", "reduction": 6},
    {"name": "Herme's Boots", "reduction": 8},
    {"name": "Lightning Boots", "reduction": 10},
    {"name": "Frostpark Boots", "reduction": 15},
    {"name": "Lava Waders", "reduction": 20},
    {"name": "Terraspark Boots", "reduction": 25},
    {"name": "Solar Boots", "reduction": 30},
]

GLOVES_UPGRADES = [
    {"name": "Paper Gloves", "chain_chance": 0.01},
    {"name": "Oven Mitts", "chain_chance": 0.05},
    {"name": "Latex Gloves", "chain_chance": 0.08},
    {"name": "Surgical Gloves", "chain_chance": 0.10},
    {"name": "Green Thumb Gloves", "chain_chance": 0.15},
    {"name": "Astral Gloves", "chain_chance": 0.20},
    {"name": "Spectral Gloves", "chain_chance": 0.25},
    {"name": "Luminite Mitts", "chain_chance": 0.30},
    {"name": "Plutonium Hands", "chain_chance": 0.35},
    {"name": "Galaxial Gloves", "chain_chance": 0.40},
]

SOIL_UPGRADES = [
    {"name": "Asphalt", "gmo_boost": 0.005},
    {"name": "Dry Soil", "gmo_boost": 0.01},
    {"name": "Grass", "gmo_boost": 0.015},
    {"name": "Wet Soil", "gmo_boost": 0.02},
    {"name": "Fertile Soil", "gmo_boost": 0.03},
    {"name": "Fertile Crescent", "gmo_boost": 0.04},
    {"name": "Oasis Soil", "gmo_boost": 0.05},
    {"name": "Astral Soil", "gmo_boost": 0.06},
    {"name": "Solar Soil", "gmo_boost": 0.08},
    {"name": "The Best Soil In The Entire Universe", "gmo_boost": 0.10},
]

# HARVEST UPGRADE PATHS
# Note: These prices are doubled from the user's specifications
HARVEST_CAR_UPGRADES = [
    {"name": "Smart Car", "extra_items": 1},
    {"name": "Nissan Cube", "extra_items": 2},
    {"name": "Mini Cooper", "extra_items": 3},
    {"name": "2001 Honda Civic", "extra_items": 4},
    {"name": "2010 BMW 528i XDrive", "extra_items": 5},
    {"name": "Minivan", "extra_items": 6},
    {"name": "Ford F150", "extra_items": 7},
    {"name": "Limousine", "extra_items": 8},
    {"name": "Japanese Bullet Train", "extra_items": 9},
    {"name": "Cargo Plane", "extra_items": 10},
]

HARVEST_CAR_PRICES = [1500, 2000, 5000, 20000, 100000, 200000, 1000000, 2000000, 6000000, 15000000]

HARVEST_CHAIN_UPGRADES = [
    {"name": "Winter", "chain_chance": 0.01},
    {"name": "Spring", "chain_chance": 0.02},
    {"name": "Great Season", "chain_chance": 0.03},
    {"name": "Amazing Season", "chain_chance": 0.04},
    {"name": "Bountiful Season", "chain_chance": 0.05},
    {"name": "Floraltastic Season", "chain_chance": 0.075},
    {"name": "Astralicious Season", "chain_chance": 0.10},
    {"name": "Spectral Season", "chain_chance": 0.15},
    {"name": "Galaxial Season", "chain_chance": 0.25},
    {"name": "Universal Season", "chain_chance": 0.40},
]

HARVEST_CHAIN_PRICES = [1000, 5000, 15000, 50000, 100000, 200000, 1000000, 5000000, 10000000, 20000000]

HARVEST_FERTILIZER_UPGRADES = [
    {"name": "Ash", "multiplier": 0.05},
    {"name": "Dirt", "multiplier": 0.10},
    {"name": "Water", "multiplier": 0.25},
    {"name": "Regular Fertilizer", "multiplier": 0.35},
    {"name": "Bone Meal", "multiplier": 0.50},
    {"name": "Enriched Fertilizer", "multiplier": 0.75},
    {"name": "Diamond Enhanced Bone Meal", "multiplier": 1.00},
    {"name": "Luminescent Fertilizer", "multiplier": 1.50},
    {"name": "Astral Fertilizer", "multiplier": 2.50},
    {"name": "Galactic Spectral Fertilizer", "multiplier": 4.00},
]

HARVEST_FERTILIZER_PRICES = [1000, 2000, 5000, 15000, 50000, 125000, 250000, 1000000, 5000000, 15000000]

HARVEST_COOLDOWN_UPGRADES = [
    {"name": "Flexible Schedule & Six Figure Salary", "reduction": 3},
    {"name": "8 Hours/Week", "reduction": 5},
    {"name": "10 Hours/Week", "reduction": 10},
    {"name": "15 Hours/Week", "reduction": 20},
    {"name": "20 Hours/Week", "reduction": 40},  # 40 seconds
    {"name": "40 Hours/Week", "reduction": 60},  # 1 minute = 60 seconds
    {"name": "60 Hours/Week", "reduction": 150},  # 2.5 minutes = 150 seconds
    {"name": "80 Hours/Week", "reduction": 300},  # 5 minutes = 300 seconds
    {"name": "112 Hours/Week", "reduction": 450},  # 7.5 minutes = 450 seconds
    {"name": "Cannot Leave The Farm", "reduction": 600},  # 10 minutes = 600 seconds
]

HARVEST_COOLDOWN_PRICES = [5000, 50000, 100000, 500000, 1000000, 2500000, 5000000, 10000000, 15000000, 20000000]

# Money handling helper functions to fix floating point precision issues
def normalize_money(amount: float) -> float:
    """Round money to exactly 2 decimal places to avoid floating point precision issues."""
    return round(amount, 2)

def validate_money_precision(amount: float) -> bool:
    """Check if a money amount has at most 2 decimal places (no fractional cents)."""
    # Multiply by 100 to convert to cents, then check if it's an integer
    cents = amount * 100
    return abs(cents - round(cents)) < 0.0001  # Small epsilon for floating point comparison

def can_afford_rounded(balance: float, amount: float) -> bool:
    """Check if balance is sufficient for amount, accounting for floating point precision.
    Allows spending entire balance (within 0.01 tolerance)."""
    normalized_balance = normalize_money(balance)
    normalized_amount = normalize_money(amount)
    # Use small epsilon to allow spending entire balance despite floating point errors
    return normalized_balance >= normalized_amount - 0.001

def check_roulette_elimination_cooldown(user_id):
    """Check if user is on Russian Roulette elimination cooldown. Returns (is_on_cooldown: bool, time_left_seconds: int)."""
    roulette_elimination_time = get_user_last_roulette_elimination_time(user_id)
    if roulette_elimination_time > 0:
        current_time = time.time()
        cooldown_end = roulette_elimination_time + ROULETTE_ELIMINATION_COOLDOWN
        if current_time < cooldown_end:
            time_left = int(cooldown_end - current_time)
            return True, time_left
    return False, 0

def can_harvest(user_id):
    last_harvest_time = get_user_last_harvest_time(user_id)
    current_time = time.time()
    
    # Check Russian Roulette elimination cooldown first
    is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
    if is_roulette_cooldown:
        return False, roulette_time_left, True  # Return True as third param to indicate it's a roulette cooldown
    
    if last_harvest_time == 0:
        return True, 0, False
    
    # Get harvest upgrades for cooldown reduction
    harvest_upgrades = get_user_harvest_upgrades(user_id)
    cooldown_tier = harvest_upgrades["cooldown"]
    cooldown_reduction = 0
    if cooldown_tier > 0:
        cooldown_reduction = HARVEST_COOLDOWN_UPGRADES[cooldown_tier - 1]["reduction"]
    
    # Apply event cooldown reductions
    active_events = get_active_events_cached()
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_harvest":
            cooldown_reduction += 30  # Cooldown reduced by 30 seconds
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_day":
            cooldown_reduction += 15  # Cooldown reduced by 15 seconds
    
    # Apply cooldown reduction
    effective_cooldown = max(0, HARVEST_COOLDOWN - cooldown_reduction)
    cooldown_end = last_harvest_time + effective_cooldown
    if current_time >= cooldown_end:
        return True, 0, False
    else:
        time_left = int(cooldown_end - current_time)
        return False, time_left, False

def set_harvest_cooldown(user_id):
    update_user_last_harvest_time(user_id, time.time())

    
async def assign_bloom_rank_role(member: discord.Member, guild: discord.Guild) -> tuple[str | None, str | None]:
    """Assign Bloom Rank role to user based on their bloom_count."""
    user_id = member.id
    bloom_count = get_user_bloom_count(user_id)
    current_rank = get_bloom_rank(user_id)
    
    # All Bloom Rank roles in order
    bloom_rank_roles = [
        "PINE I", "PINE II", "PINE III",
        "CEDAR I", "CEDAR II", "CEDAR III",
        "BIRCH I", "BIRCH II", "BIRCH III",
        "MAPLE I", "MAPLE II", "MAPLE III",
        "OAK I", "OAK II", "OAK III",
        "FIR I", "FIR II", "FIR III",
        "REDWOOD"
    ]
    
    # Find the user's current bloom rank role
    previous_role_name = next((role.name for role in member.roles if role.name in bloom_rank_roles), None)
    
    # Determine the target role based on current rank
    target_role_name = current_rank
    
    # If the target role is the same as current role, no changes needed
    if target_role_name == previous_role_name:
        return previous_role_name, None
    
    # Remove the old bloom rank role if they had one
    if previous_role_name:
        old_role = discord.utils.get(guild.roles, name=previous_role_name)
        if old_role:
            try:
                await member.remove_roles(old_role)
            except Exception as e:
                print(f"Error removing bloom rank role {previous_role_name} from user {user_id}: {e}")
    
    # Assign the new bloom rank role
    if target_role_name:
        new_role = discord.utils.get(guild.roles, name=target_role_name)
        if new_role:
            try:
                await member.add_roles(new_role)
            except Exception as e:
                print(f"Error adding bloom rank role {target_role_name} to user {user_id}: {e}")
                return previous_role_name, None
        else:
            print(f"Bloom rank role {target_role_name} not found in guild {guild.id}")
            return previous_role_name, None
    
    return previous_role_name, target_role_name


async def assign_gatherer_role(member: discord.Member, guild: discord.Guild) -> tuple[str | None, str | None]:
    #assign gatherer role to the user
    #PLANTER I - 0-49 items gathered
    #PLANTER II - 50-149 items gathered
    #PLANTER III - 150-299 items gathered
    #PLANTER IV - 300-499 items gathered
    #PLANTER V - 500-999 items gathered
    #PLANTER VI - 1000-1999 items gathered
    #PLANTER VII - 2000-3999 items gathered
    #PLANTER VIII - 4000-9999 items gathered
    #PLANTER IX - 10000-99999 items gathered
    #PLANTER X - 100000+ items gathered

    user_id = member.id
    total_items = get_user_total_items(user_id)  # Use same counter as userstats to keep them in sync
    planter_roles = ["PLANTER I", "PLANTER II", "PLANTER III", "PLANTER IV", "PLANTER V", "PLANTER VI", "PLANTER VII", "PLANTER VIII", "PLANTER IX", "PLANTER X"]

    # Find the user's current planter role
    previous_role_name = next((role.name for role in member.roles if role.name in planter_roles), None)
    
    # Determine the target role based on total items gathered
    target_role_name = None
    if total_items < 50:
        target_role_name = "PLANTER I"
    elif total_items < 150:
        target_role_name = "PLANTER II"
    elif total_items < 300:  # Fixed: was 299
        target_role_name = "PLANTER III"
    elif total_items < 500:  # Fixed: was 499
        target_role_name = "PLANTER IV"
    elif total_items < 1000:
        target_role_name = "PLANTER V"
    elif total_items < 2000:
        target_role_name = "PLANTER VI"
    elif total_items < 4000:
        target_role_name = "PLANTER VII"
    elif total_items < 10000:
        target_role_name = "PLANTER VIII"
    elif total_items < 100000:
        target_role_name = "PLANTER IX"
    else: #100000+
        target_role_name = "PLANTER X"
    # If the target role is the same as current role, no changes needed
    if target_role_name == previous_role_name:
        return previous_role_name, None


    # Remove the old planter role if they had one
    if previous_role_name:
        old_role = discord.utils.get(guild.roles, name=previous_role_name)
        if old_role:
            try:
                await member.remove_roles(old_role)
            except Exception as e:
                print(f"Error removing role {previous_role_name} from user {user_id}: {e}")

    # Assign the new planter role
    if target_role_name:
        new_role = discord.utils.get(guild.roles, name=target_role_name)
        if new_role:
            try:
                await member.add_roles(new_role)
                return previous_role_name, target_role_name
            except Exception as e:
                print(f"Error adding role {target_role_name} to user {user_id}: {e}")
                return previous_role_name, None
        else:
            print(f"Role {target_role_name} not found for user {user_id}")
            return previous_role_name, None
    
    # Fallback return (should not normally reach here)
    return previous_role_name, None




# Achievement definitions
ACHIEVEMENTS = {
    "gatherer": {
        "name": "Gatherer Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Gatherer Achievement Category",
                "description": "You haven't even gathered yet! Do /gather!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Baby's First Gather",
                "description": "/gather 1 time",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "Smelling What You're Stepping In",
                "description": "/gather 25 times",
                "threshold": 25,
                "boost": 0.01  # 1%
            },
            {
                "level": 3,
                "name": "Getting The Hang Of It",
                "description": "/gather 100 times",
                "threshold": 100,
                "boost": 0.02  # 2%
            },
            {
                "level": 4,
                "name": "Green Thumb",
                "description": "/gather 500 times",
                "threshold": 500,
                "boost": 0.035  # 3.5%
            },
            {
                "level": 5,
                "name": "Serf",
                "description": "/gather 1000 times",
                "threshold": 1000,
                "boost": 0.06  # 6%
            },
            {
                "level": 6,
                "name": "Farmer",
                "description": "/gather 5,000 times",
                "threshold": 5000,
                "boost": 0.10  # 10%
            },
            {
                "level": 7,
                "name": "Gathousand",
                "description": "/gather 10,000 times",
                "threshold": 10000,
                "boost": 0.15  # 15%
            },
            {
                "level": 8,
                "name": "Fifty Gathousand",
                "description": "/gather 50,000 times",
                "threshold": 50000,
                "boost": 0.25  # 25%
            },
            {
                "level": 9,
                "name": "Hundred Gathousand",
                "description": "/gather 100,000 times",
                "threshold": 100000,
                "boost": 0.40  # 40%
            },
            {
                "level": 10,
                "name": "Mikellion",
                "description": "/gather 1,000,000 times",
                "threshold": 1000000,
                "boost": 1.0  # 100%
            }
        ]
    },
    "coinflip_total": {
        "name": "Coinflip Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Goody-Two Shoes",
                "description": "You haven't even coinflipped! Do /coinflip in #coinflip!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Trying My Luck",
                "description": "Perform 1 coinflip",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "Fifth Time's The Charm",
                "description": "Perform 5 coinflips",
                "threshold": 5,
                "boost": 0.01  # 1%
            },
            {
                "level": 3,
                "name": "Tails Never Fails",
                "description": "Perform 15 coinflips",
                "threshold": 15,
                "boost": 0.03  # 3%
            },
            {
                "level": 4,
                "name": "Heads Never Dreads",
                "description": "Perform 50 coinflips",
                "threshold": 50,
                "boost": 0.07  # 7%
            },
            {
                "level": 5,
                "name": "Statistically, It's Just A 50% Chance",
                "description": "Perform 100 coinflips",
                "threshold": 100,
                "boost": 0.15  # 15%
            },
            {
                "level": 6,
                "name": "Gambler",
                "description": "Perform 500 coinflips",
                "threshold": 500,
                "boost": 0.25  # 25%
            },
            {
                "level": 7,
                "name": "Landed On Its Side",
                "description": "Perform 1000 coinflips",
                "threshold": 1000,
                "boost": 0.40  # 40%
            },
            {
                "level": 8,
                "name": "Just One More",
                "description": "Perform 7,777 coinflips",
                "threshold": 7777,
                "boost": 0.77  # 77%
            },
            {
                "level": 9,
                "name": "Almost Breaking Even",
                "description": "Perform 15,000 coinflips",
                "threshold": 15000,
                "boost": 1.50  # 150%
            },
            {
                "level": 10,
                "name": "The House Always Wins",
                "description": "Perform 30,000 coinflips",
                "threshold": 30000,
                "boost": 3.0  # 300%
            }
        ]
    },
    "coinflip_win_streak": {
        "name": "Coinflip Win Streak Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Loser",
                "description": "You haven't even won a coinflip! Do /coinflip in #coinflip!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Just The Start",
                "description": "Win 1 coinflip in a row",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "1/4",
                "description": "Win 2 coinflips in a row",
                "threshold": 2,
                "boost": 0.015  # 1.5%
            },
            {
                "level": 3,
                "name": "These Might Take Some Time",
                "description": "Win 3 coinflips in a row",
                "threshold": 3,
                "boost": 0.03  # 3%
            },
            {
                "level": 4,
                "name": "Gambler's High",
                "description": "Win 4 coinflips in a row",
                "threshold": 4,
                "boost": 0.10  # 10%
            },
            {
                "level": 5,
                "name": "Just a 1/32 Chance BTW",
                "description": "Win 5 coinflips in a row",
                "threshold": 5,
                "boost": 0.50  # 50%
            },
            {
                "level": 6,
                "name": "Still Going",
                "description": "Win 6 coinflips in a row",
                "threshold": 6,
                "boost": 1.0  # 100%
            },
            {
                "level": 7,
                "name": "Just 1 More Win (In a Row)",
                "description": "Win 7 coinflips in a row",
                "threshold": 7,
                "boost": 2.0  # 200%
            },
            {
                "level": 8,
                "name": "Is This Even Possible?",
                "description": "Win 8 coinflips in a row",
                "threshold": 8,
                "boost": 5.0  # 500%
            },
            {
                "level": 9,
                "name": "You Should Buy A Lottery Ticket",
                "description": "Win 9 coinflips in a row",
                "threshold": 9,
                "boost": 20.0  # 2000%
            },
            {
                "level": 10,
                "name": "Struck By Lightning (Twice)",
                "description": "Win 10 coinflips in a row",
                "threshold": 10,
                "boost": 100.0  # 10000%
            }
        ]
    },
    "harvesting": {
        "name": "Harvesting Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Deharvested",
                "description": "You haven't even harvested! Do /harvest!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Baby's First Harvest",
                "description": "Do /harvest once",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "Farmer's Market",
                "description": "Do /harvest 5 times",
                "threshold": 5,
                "boost": 0.01  # 1%
            },
            {
                "level": 3,
                "name": "Combining",
                "description": "Do /harvest 15 times",
                "threshold": 15,
                "boost": 0.05  # 5%
            },
            {
                "level": 4,
                "name": "Have You Done /orchard Yet?",
                "description": "Do /harvest 50 times",
                "threshold": 50,
                "boost": 0.10  # 10%
            },
            {
                "level": 5,
                "name": "Making The World Go Round",
                "description": "Do /harvest 100 times",
                "threshold": 100,
                "boost": 0.25  # 25%
            },
            {
                "level": 6,
                "name": "Locally Known",
                "description": "Do /harvest 500 times",
                "threshold": 500,
                "boost": 0.50  # 50%
            },
            {
                "level": 7,
                "name": "Competing With Publix",
                "description": "Do /harvest 1000 times",
                "threshold": 1000,
                "boost": 1.20  # 120%
            },
            {
                "level": 8,
                "name": "World Renowned",
                "description": "Do /harvest 2000 times",
                "threshold": 2000,
                "boost": 2.50  # 250%
            },
            {
                "level": 9,
                "name": "The Lebron of Plants",
                "description": "Do /harvest 5000 times",
                "threshold": 5000,
                "boost": 4.0  # 400%
            },
            {
                "level": 10,
                "name": "Galactically Known",
                "description": "Do /harvest 10,000 times",
                "threshold": 10000,
                "boost": 9.99  # 999%
            }
        ]
    },
    "planter": {
        "name": "Planter Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Unranked",
                "description": "Play the game!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Just Starting Out",
                "description": "Be PLANTER I",
                "threshold": 1,
                "boost": 0.001  # 0.1%
            },
            {
                "level": 2,
                "name": "Wearing The Plants",
                "description": "Be PLANTER II",
                "threshold": 2,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 3,
                "name": "Planter Tree",
                "description": "Be PLANTER III",
                "threshold": 3,
                "boost": 0.01  # 1%
            },
            {
                "level": 4,
                "name": "I'm Glad You Like Plants",
                "description": "Be PLANTER IV",
                "threshold": 4,
                "boost": 0.02  # 2%
            },
            {
                "level": 5,
                "name": "Eat Your Greens",
                "description": "Be PLANTER V",
                "threshold": 5,
                "boost": 0.05  # 5%
            },
            {
                "level": 6,
                "name": "I Prefer Almond Milk Anyway",
                "description": "Be PLANTER VI",
                "threshold": 6,
                "boost": 0.10  # 10%
            },
            {
                "level": 7,
                "name": "It'd Be Crazy If You Were A Carnivore",
                "description": "Be PLANTER VII",
                "threshold": 7,
                "boost": 0.20  # 20%
            },
            {
                "level": 8,
                "name": "Going Vegan",
                "description": "Be PLANTER VIII",
                "threshold": 8,
                "boost": 0.40  # 40%
            },
            {
                "level": 9,
                "name": "Treehugger",
                "description": "Be PLANTER IX",
                "threshold": 9,
                "boost": 1.0  # 100%
            },
            {
                "level": 10,
                "name": "John Deere Himself",
                "description": "Be PLANTER X",
                "threshold": 10,
                "boost": 3.0  # 300%
            }
        ]
    },
    "water_streak": {
        "name": "Water Streak Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Dry",
                "description": "You haven't watered yet! Do /water!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Sprinkle",
                "description": "Have a /water streak of 1",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "MMM.. Yeah, that's good. -The Plants",
                "description": "Have a /water streak of 3",
                "threshold": 3,
                "boost": 0.02  # 2%
            },
            {
                "level": 3,
                "name": "Just A Week",
                "description": "Have a /water streak of 7",
                "threshold": 7,
                "boost": 0.05  # 5%
            },
            {
                "level": 4,
                "name": "Two Weeks Notice",
                "description": "Have a /water streak of 14",
                "threshold": 14,
                "boost": 0.15  # 15%
            },
            {
                "level": 5,
                "name": "One Month Anniversary",
                "description": "Have a /water streak of 30",
                "threshold": 30,
                "boost": 0.50  # 50%
            },
            {
                "level": 6,
                "name": "Can You Tell I Just Really Like Plants",
                "description": "Have a /water streak of 60",
                "threshold": 60,
                "boost": 1.0  # 100%
            },
            {
                "level": 7,
                "name": "Parks & Rec Department",
                "description": "Have a /water streak of 100",
                "threshold": 100,
                "boost": 3.0  # 300%
            },
            {
                "level": 8,
                "name": "Just Doin' My Dailies",
                "description": "Have a /water streak of 150",
                "threshold": 150,
                "boost": 10.0  # 1000%
            },
            {
                "level": 9,
                "name": "Surely I Can Keep This Going",
                "description": "Have a /water streak of 210",
                "threshold": 210,
                "boost": 20.0  # 2000%
            },
            {
                "level": 10,
                "name": "Year Of The /gather",
                "description": "Have a /water streak of 365",
                "threshold": 365,
                "boost": 100.0  # 10000%
            }
        ]
    }
}

# Hidden achievements definitions
HIDDEN_ACHIEVEMENTS = {
    "john_rockefeller": {
        "name": "John Rockefeller",
        "description": "Pay someone at least $1,000,000",
        "boost": 0.25  # 25%
    },
    "beating_the_odds": {
        "name": "Beating The Odds",
        "description": "Cashout in a game of /russian where there are 5 bullets in the chamber",
        "boost": 0.33  # 33%
    },
    "beneficiary": {
        "name": "Beneficiary",
        "description": "Receive at least $1,000,000 from someone",
        "boost": 0.10  # 10%
    },
    "leap_year": {
        "name": "Leap Year",
        "description": "Have a /water streak of 366",
        "boost": 50.00  # 5000%
    },
    "ceo": {
        "name": "CEO",
        "description": "Own over 50% of the amount of shares for any company in the stock market",
        "boost": 20.00  # 2000%
    },
    "blockchain": {
        "name": "Blockchain",
        "description": "Have at least 1.00 of any cryptocoin",
        "boost": 0.50  # 50%
    },
    "almost_got_it": {
        "name": "Almost Got It",
        "description": "Do a /gather when there's less than 1 second before the cooldown is up, but you're still on cooldown",
        "boost": 0.20  # 20%
    },
    "maxed_out": {
        "name": "Maxed Out",
        "description": "Have all GPUs, all gardeners, all /gear upgrades maxed, all /orchard upgrades maxed",
        "boost": 1.0  # 100%
    }
}

# Total number of hidden achievements
TOTAL_HIDDEN_ACHIEVEMENTS = 8


def check_maxed_out_achievement(user_id: int) -> bool:
    """Check if user has all upgrades maxed (all GPUs, all gardeners, all gear upgrades, all orchard upgrades).
    Returns True if achievement was newly unlocked, False otherwise."""
    # Check all GPUs (10 total)
    user_gpus = get_user_gpus(user_id)
    all_gpu_names = [gpu["name"] for gpu in GPU_SHOP]
    has_all_gpus = all(gpu_name in user_gpus for gpu_name in all_gpu_names)
    
    # Check all gardeners (5 total, with tools)
    user_gardeners = get_user_gardeners(user_id)
    has_all_gardeners = len(user_gardeners) >= 5
    if has_all_gardeners:
        # Check that all gardeners have tools
        for gardener in user_gardeners:
            if not gardener.get("has_tool", False):
                has_all_gardeners = False
                break
    
    # Check all gear upgrades (basket, shoes, gloves, soil - all at tier 10)
    basket_upgrades = get_user_basket_upgrades(user_id)
    all_gear_maxed = (
        basket_upgrades["basket"] >= 10 and
        basket_upgrades["shoes"] >= 10 and
        basket_upgrades["gloves"] >= 10 and
        basket_upgrades["soil"] >= 10
    )
    
    # Check all orchard upgrades (car, chain, fertilizer, cooldown - all at tier 10)
    harvest_upgrades = get_user_harvest_upgrades(user_id)
    all_orchard_maxed = (
        harvest_upgrades["car"] >= 10 and
        harvest_upgrades["chain"] >= 10 and
        harvest_upgrades["fertilizer"] >= 10 and
        harvest_upgrades["cooldown"] >= 10
    )
    
    # If all conditions are met, unlock the achievement
    if has_all_gpus and has_all_gardeners and all_gear_maxed and all_orchard_maxed:
        return unlock_hidden_achievement(user_id, "maxed_out")
    
    return False


def get_achievement_level_for_stat(achievement_name: str, stat_value: int) -> int:
    """Get the achievement level based on a stat value (e.g., total_items gathered)."""
    if achievement_name not in ACHIEVEMENTS:
        return 0
    
    achievement = ACHIEVEMENTS[achievement_name]
    levels = achievement["levels"]
    
    # Find the highest level the user has reached
    current_level = 0
    for level_data in reversed(levels):  # Check from highest to lowest
        if stat_value >= level_data["threshold"]:
            current_level = level_data["level"]
            break
    
    return current_level


def get_planter_level_from_total_items(total_items: int) -> int:
    """Get PLANTER achievement level (0-10) based on total_items gathered.
    Achievement levels match the PLANTER role thresholds:
    0 = Unranked (0 items, no PLANTER role yet - new users/prestige)
    1 = PLANTER I (1-49 items) - achievement "Just Starting Out"
    2 = PLANTER II (50-149 items) - achievement "Wearing The Plants"
    3 = PLANTER III (150-299 items) - achievement "Planter Tree"
    4 = PLANTER IV (300-499 items) - achievement "I'm Glad You Like Plants"
    5 = PLANTER V (500-999 items) - achievement "Eat Your Greens"
    6 = PLANTER VI (1000-1999 items) - achievement "I Prefer Almond Milk Anyway"
    7 = PLANTER VII (2000-3999 items) - achievement "It'd Be Crazy If You Were A Carnivore"
    8 = PLANTER VIII (4000-9999 items) - achievement "Going Vegan"
    9 = PLANTER IX (10000-99999 items) - achievement "Treehugger"
    10 = PLANTER X (100000+ items) - achievement "John Deere Himself"
    """
    if total_items == 0:
        return 0  # Unranked (no PLANTER role yet - new users/prestige)
    elif total_items < 50:
        return 1  # PLANTER I role -> achievement level 1 "Just Starting Out"
    elif total_items < 150:
        return 2  # PLANTER II role -> achievement level 2 "Wearing The Plants"
    elif total_items < 300:
        return 3  # PLANTER III role -> achievement level 3 "Planter Tree"
    elif total_items < 500:
        return 4  # PLANTER IV role -> achievement level 4
    elif total_items < 1000:
        return 5  # PLANTER V role -> achievement level 5
    elif total_items < 2000:
        return 6  # PLANTER VI role -> achievement level 6
    elif total_items < 4000:
        return 7  # PLANTER VII role -> achievement level 7
    elif total_items < 10000:
        return 8  # PLANTER VIII role -> achievement level 8
    elif total_items < 100000:
        return 9  # PLANTER IX role -> achievement level 9
    else:
        return 10  # PLANTER X role -> achievement level 10


def get_achievement_multiplier(user_id: int) -> float:
    """
    Calculate the total achievement multiplier based on all achievement levels.
    Returns a multiplier (e.g., 1.005 for 0.5% boost).
    All achievement boosts stack additively.
    """
    total_boost = 0.0
    
    # Check gatherer achievement
    gatherer_level = get_user_achievement_level(user_id, "gatherer")
    if gatherer_level > 0 and "gatherer" in ACHIEVEMENTS:
        gatherer_achievement = ACHIEVEMENTS["gatherer"]
        if gatherer_level < len(gatherer_achievement["levels"]):
            level_data = gatherer_achievement["levels"][gatherer_level]
            total_boost += level_data["boost"]
    
    # Check coinflip_total achievement
    coinflip_total_level = get_user_achievement_level(user_id, "coinflip_total")
    if coinflip_total_level > 0 and "coinflip_total" in ACHIEVEMENTS:
        coinflip_total_achievement = ACHIEVEMENTS["coinflip_total"]
        if coinflip_total_level < len(coinflip_total_achievement["levels"]):
            level_data = coinflip_total_achievement["levels"][coinflip_total_level]
            total_boost += level_data["boost"]
    
    # Check coinflip_win_streak achievement
    coinflip_streak_level = get_user_achievement_level(user_id, "coinflip_win_streak")
    if coinflip_streak_level > 0 and "coinflip_win_streak" in ACHIEVEMENTS:
        coinflip_streak_achievement = ACHIEVEMENTS["coinflip_win_streak"]
        if coinflip_streak_level < len(coinflip_streak_achievement["levels"]):
            level_data = coinflip_streak_achievement["levels"][coinflip_streak_level]
            total_boost += level_data["boost"]
    
    # Check harvesting achievement
    harvesting_level = get_user_achievement_level(user_id, "harvesting")
    if harvesting_level > 0 and "harvesting" in ACHIEVEMENTS:
        harvesting_achievement = ACHIEVEMENTS["harvesting"]
        if harvesting_level < len(harvesting_achievement["levels"]):
            level_data = harvesting_achievement["levels"][harvesting_level]
            total_boost += level_data["boost"]
    
    # Check planter achievement
    planter_level = get_user_achievement_level(user_id, "planter")
    if planter_level > 0 and "planter" in ACHIEVEMENTS:
        planter_achievement = ACHIEVEMENTS["planter"]
        if planter_level < len(planter_achievement["levels"]):
            level_data = planter_achievement["levels"][planter_level]
            total_boost += level_data["boost"]
    
    # Check water_streak achievement
    water_streak_level = get_user_achievement_level(user_id, "water_streak")
    if water_streak_level > 0 and "water_streak" in ACHIEVEMENTS:
        water_streak_achievement = ACHIEVEMENTS["water_streak"]
        if water_streak_level < len(water_streak_achievement["levels"]):
            level_data = water_streak_achievement["levels"][water_streak_level]
            total_boost += level_data["boost"]
    
    # Check hidden achievements
    for achievement_key, achievement_data in HIDDEN_ACHIEVEMENTS.items():
        if has_hidden_achievement(user_id, achievement_key):
            total_boost += achievement_data["boost"]
    
    return 1.0 + total_boost


def get_rank_perma_buff_multiplier(user_id):
    """
    Calculate the rank perma buff multiplier based on bloom rank.
    Returns a multiplier (e.g., 1.015 for 1.5% boost).
    
    PINE I: 0% (no boost, returns 1.0)
    PINE II: 1.5%
    PINE III: 3%
    CEDAR I: 6%
    CEDAR II: 9%
    CEDAR III: 12%
    BIRCH I: 17%
    BIRCH II: 22%
    BIRCH III: 27%
    MAPLE I: 34.5%
    MAPLE II: 42%
    MAPLE III: 50%
    OAK I: 60%
    OAK II: 70%
    OAK III: 80%
    FIR I: 95%
    FIR II: 110%
    FIR III: 125%
    REDWOOD: 200%
    """
    bloom_rank = get_bloom_rank(user_id)
    
    if bloom_rank == "PINE I":
        return 1.0  # No boost for PINE I
    elif bloom_rank == "PINE II":
        return 1.015  # 1.5%
    elif bloom_rank == "PINE III":
        return 1.03  # 3%
    elif bloom_rank == "CEDAR I":
        return 1.06  # 6%
    elif bloom_rank == "CEDAR II":
        return 1.09  # 9%
    elif bloom_rank == "CEDAR III":
        return 1.12  # 12%
    elif bloom_rank == "BIRCH I":
        return 1.17  # 17%
    elif bloom_rank == "BIRCH II":
        return 1.22  # 22%
    elif bloom_rank == "BIRCH III":
        return 1.27  # 27%
    elif bloom_rank == "MAPLE I":
        return 1.345  # 34.5%
    elif bloom_rank == "MAPLE II":
        return 1.42  # 42%
    elif bloom_rank == "MAPLE III":
        return 1.50  # 50%
    elif bloom_rank == "OAK I":
        return 1.60  # 60%
    elif bloom_rank == "OAK II":
        return 1.70  # 70%
    elif bloom_rank == "OAK III":
        return 1.80  # 80%
    elif bloom_rank == "FIR I":
        return 1.95  # 95%
    elif bloom_rank == "FIR II":
        return 2.10  # 110%
    elif bloom_rank == "FIR III":
        return 2.25  # 125%
    elif bloom_rank == "REDWOOD":
        return 3.0  # 200% (flat increase)
    else:
        # Default to no boost if rank is unknown
        return 1.0


def can_gather(user_id, user_data=None, active_events=None):
    """
    Check if user can gather. Returns (can_gather: bool, time_left: int, is_roulette_cooldown: bool).
    
    Args:
        user_id: User ID
        user_data: Optional pre-fetched user data dict (from get_user_gather_data)
        active_events: Optional pre-fetched active events list
    """
    # Check Russian Roulette elimination cooldown first
    is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
    if is_roulette_cooldown:
        return False, roulette_time_left, True  # Return True as third param to indicate it's a roulette cooldown
    
    # Fetch data if not provided
    if user_data is None:
        user_data = get_user_gather_data(user_id)
    
    if active_events is None:
        active_events = get_active_events_cached()
    
    last_gather_time = user_data["last_gather_time"]
    current_time = time.time()
    #check if the user is on cooldown, return true/false and how much time left
    #right off the bat if the user is new they have no cooldown
    if last_gather_time == 0:
        return True, 0, False
    
    # Get shoes upgrade cooldown reduction
    user_upgrades = user_data["basket_upgrades"]
    shoes_tier = user_upgrades["shoes"]
    cooldown_reduction = 0
    if shoes_tier > 0:
        cooldown_reduction = SHOES_UPGRADES[shoes_tier - 1]["reduction"]
    
    # Apply event cooldown reductions
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_harvest":
            cooldown_reduction += 30  # Cooldown reduced by 30 seconds
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_day":
            cooldown_reduction += 15  # Cooldown reduced by 15 seconds
    
    # Calculate effective cooldown (base cooldown minus reduction, minimum 0)
    effective_cooldown = max(0, GATHER_COOLDOWN - cooldown_reduction)
    cooldown_end = last_gather_time + effective_cooldown
    
    if current_time >= cooldown_end:
        return True, 0, False
    else:
        time_left = int(cooldown_end - current_time)
        return False, time_left, False

def set_cooldown(user_id):
    # set cooldown for user, p self explanatory
    update_user_last_gather_time(user_id, time.time())

async def perform_gather_for_user(user_id: int, apply_cooldown: bool = True, 
                                  user_data=None, active_events=None,
                                  apply_orchard_fertilizer: bool = False) -> dict:
    """
    Perform a gather action for a user. Returns dict with gathered item info.
    When apply_orchard_fertilizer=True (e.g. gardener auto-gather), orchard (harvest) fertilizer
    upgrade is applied to the value. Gardener auto-gathers never get chain chance (chain is only
    in the /gather command, not in this function).
    
    Args:
        user_id: User ID
        apply_cooldown: If True, sets cooldown. If False, skips cooldown (for gardeners).
        user_data: Optional pre-fetched user data dict (from get_user_gather_data)
        active_events: Optional pre-fetched active events list
        apply_orchard_fertilizer: If True, apply orchard (harvest) fertilizer multiplier (for gardener auto-gather).
    """
    # Fetch data if not provided
    if user_data is None:
        user_data = get_user_gather_data(user_id)
    
    if active_events is None:
        active_events = get_active_events_cached()
    
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    
    # Choose a random item, with event modifications
    items_to_choose = GATHERABLE_ITEMS.copy()
    weights = None
    
    # Apply category-specific event effects (May Flowers, Fruit Festival, Vegetable Boom)
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "may_flowers":
            # Increase flower weights by 60%
            weights = []
            for item in GATHERABLE_ITEMS:
                if item["category"] == "Flower":
                    weights.append(1.6)  # 60% increase
                else:
                    weights.append(1.0)
        elif event_id == "fruit_festival":
            # Increase fruit weights by 50%
            weights = []
            for item in GATHERABLE_ITEMS:
                if item["category"] == "Fruit":
                    weights.append(1.5)  # 50% increase
                else:
                    weights.append(1.0)
        elif event_id == "vegetable_boom":
            # Increase vegetable weights by 50%
            weights = []
            for item in GATHERABLE_ITEMS:
                if item["category"] == "Vegetable":
                    weights.append(1.5)  # 50% increase
                else:
                    weights.append(1.0)
    
    if weights:
        item = random.choices(GATHERABLE_ITEMS, weights=weights, k=1)[0]
    else:
        item = random.choice(GATHERABLE_ITEMS)
    
    name = item["name"]
    base_value = item["base_value"]
    
    # Apply event base value modifications (May Flowers, Fruit Festival, Vegetable Boom)
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "may_flowers" and item["category"] == "Flower":
            base_value *= 3  # Triple flower prices
        elif event_id == "fruit_festival" and item["category"] == "Fruit":
            base_value *= 2  # Double fruit prices
        elif event_id == "vegetable_boom" and item["category"] == "Vegetable":
            base_value *= 2  # Double vegetable prices
    
    if item["category"] == "Fruit":
        ripeness_list = LEVEL_OF_RIPENESS_FRUITS
    elif item["category"] == "Vegetable":
        ripeness_list = LEVEL_OF_RIPENESS_VEGETABLES
    elif item["category"] == "Flower":
        ripeness_list = LEVEL_OF_RIPENESS_FLOWERS
    else:
        ripeness_list = []

    if ripeness_list:
        # Use weighted random selection for the chance
        weights = [r["chance"] for r in ripeness_list]
        
        # Apply Perfect Ripeness event (hourly) or Ripeness Rush event (daily)
        hourly_event_id = hourly_event.get("effects", {}).get("event_id", "") if hourly_event else ""
        daily_event_id = daily_event.get("effects", {}).get("event_id", "") if daily_event else ""
        if hourly_event_id == "perfect_ripeness":
            # Increase all ripeness multipliers by 50%
            ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
            ripeness_multiplier = ripeness["multiplier"] * 1.5
        elif daily_event_id == "ripeness_rush":
            # Double perfect ripeness chance
            weights = []
            for r in ripeness_list:
                if "Perfect" in r["name"]:
                    weights.append(r["chance"] * 2)
                else:
                    weights.append(r["chance"])
            ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
            ripeness_multiplier = ripeness["multiplier"]
        else:
            ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
            ripeness_multiplier = ripeness["multiplier"]
        
        final_value = base_value * ripeness_multiplier
    else:
        final_value = base_value
        ripeness = {"name": "Normal"}

    # Get user upgrades from pre-fetched data
    user_upgrades = user_data["basket_upgrades"]
    
    # Apply soil upgrade GMO chance boost
    soil_tier = user_upgrades["soil"]
    base_gmo_chance = 0.05
    soil_gmo_boost = SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0
    gmo_chance = base_gmo_chance + soil_gmo_boost
    
    # Apply event GMO chance modifications
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "radiation_leak":
            # GMO chance +25%
            gmo_chance += 0.25
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "gmo_surge":
            # GMO chance +25%
            gmo_chance += 0.25
    
    # Clamp GMO chance to max 1.0 (100%)
    gmo_chance = min(gmo_chance, 1.0)
    
    # See if the gathered item is a GMO
    is_gmo = random.choices([True, False], weights=[gmo_chance, 1-gmo_chance], k=1)[0]
    if is_gmo:
        final_value *= 2
    
    # Apply basket upgrade money multiplier
    basket_tier = user_upgrades["basket"]
    basket_multiplier = 1.0
    if basket_tier > 0:
        basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"]
    
    # Apply event basket multiplier modifications
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "basket_boost":
            # Basket multiplier +50%
            basket_multiplier *= 1.5
    
    # Apply event value multipliers (Bumper Crop, Harvest Festival, Double Money, Lucky Strike)
    value_multiplier = 1.0
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "bumper_crop":
            value_multiplier *= 2.0  # All item values x2
        elif event_id == "lucky_strike":
            value_multiplier *= 1.25  # All multipliers +25%
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "double_money":
            value_multiplier *= 2.0  # All earnings doubled
        elif event_id == "harvest_festival":
            value_multiplier *= 1.5  # All item values +50%
    
    final_value *= basket_multiplier * value_multiplier

    # Apply orchard (harvest) fertilizer when e.g. gardener auto-gather
    if apply_orchard_fertilizer:
        harvest_upgrades = get_user_harvest_upgrades(user_id)
        fertilizer_tier = harvest_upgrades["fertilizer"]
        if fertilizer_tier > 0:
            fertilizer_multiplier = 1.0 + HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"]
            final_value *= fertilizer_multiplier

    # Apply bloom multiplier
    bloom_multiplier = get_bloom_multiplier(user_id)
    base_final_value = final_value
    final_value *= bloom_multiplier
    extra_money_from_bloom = final_value - base_final_value
    
    # Apply water multiplier (1.01x per water, cumulative)
    water_multiplier = get_water_multiplier(user_id)
    final_value *= water_multiplier
    
    # Apply rank perma buff multiplier
    rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
    base_value_before_rank = final_value
    final_value *= rank_perma_buff_multiplier
    extra_money_from_rank = final_value - base_value_before_rank
    
    # Apply achievement multiplier
    achievement_multiplier = get_achievement_multiplier(user_id)
    base_value_before_achievement = final_value
    final_value *= achievement_multiplier
    extra_money_from_achievement = final_value - base_value_before_achievement
    
    # Apply daily bonus multiplier (1% per consecutive day)
    daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
    base_value_before_daily = final_value
    final_value *= daily_bonus_multiplier
    extra_money_from_daily = final_value - base_value_before_daily

    # Calculate new balance from pre-fetched data
    current_balance = user_data["balance"]
    new_balance = current_balance + final_value
    
    # Perform all database updates in a single batched operation
    perform_gather_update(
        user_id=user_id,
        balance_increment=final_value,
        item_name=name,
        ripeness_name=ripeness["name"],
        category=item["category"],
        apply_cooldown=apply_cooldown
    )
    
    # Note: Achievement checking is now done in the /gather command handler
    # to only count actual /gather commands, not gardener auto-gathers

    return {
        "name": name,
        "value": final_value,
        "base_value": base_final_value,
        "extra_money_from_bloom": extra_money_from_bloom,
        "bloom_multiplier": bloom_multiplier,
        "extra_money_from_rank": extra_money_from_rank,
        "rank_perma_buff_multiplier": rank_perma_buff_multiplier,
        "extra_money_from_achievement": extra_money_from_achievement,
        "achievement_multiplier": achievement_multiplier,
        "extra_money_from_daily": extra_money_from_daily,
        "daily_bonus_multiplier": daily_bonus_multiplier,
        "ripeness": ripeness["name"],
        "is_gmo": is_gmo,
        "category": item["category"],
        "new_balance": new_balance
    }

#gatherable items
GATHERABLE_ITEMS = [
    {"category": "Flower","name": "Rose 🌹", "base_value": 10},
    {"category": "Flower","name": "Lily 🌺", "base_value": 8},
    {"category": "Flower","name": "Sunflower 🌻", "base_value": 6},
    {"category": "Flower","name": "Daisy 🌼", "base_value": 4},
    {"category": "Flower","name": "Tulip 🌷", "base_value": 2},
    {"category": "Flower","name": "Daffodil 🌼", "base_value": 1},
    {"category": "Flower", "name": "Flowey", "base_value": 5},
    {"category": "Flower", "name": "Lotus🪷", "base_value": 6.7},
    {"category": "Flower", "name": "Sakura 🌸", "base_value": 6},
    {"category": "Flower", "name": "Clover 🍀", "base_value": 7.77},
    {"category": "Flower", "name": "Herb 🌿", "base_value": 5},


    {"category": "Fruit","name": "Strawberry 🍓", "base_value": 8},
    {"category": "Fruit","name": "Blueberry 🫐", "base_value": 10},
    {"category": "Fruit","name": "Raspberry", "base_value": 2},
    {"category": "Fruit","name": "Cherry 🍒", "base_value": 1},
    {"category": "Fruit","name": "Apple 🍎", "base_value": 9},
    {"category": "Fruit","name": "Pear 🍐", "base_value": 14},
    {"category": "Fruit","name": "Orange 🍊", "base_value": 6},
    {"category": "Fruit","name": "Grape 🍇", "base_value": 7},
    {"category": "Fruit","name": "Banana 🍌", "base_value": 5},
    {"category": "Fruit","name": "Watermelon 🍉", "base_value": 12},
    {"category": "Fruit","name": "Peach 🍑", "base_value": 8},
    {"category": "Fruit","name": "Mango 🥭", "base_value": 11},
    {"category": "Fruit","name": "Pineapple 🍍", "base_value": 13},
    {"category": "Fruit","name": "Kiwi 🥝", "base_value": 9},
    {"category": "Fruit","name": "Lemon 🍋", "base_value": 4},
    {"category": "Fruit","name": "Coconut 🥥", "base_value": 10},
    {"category": "Fruit","name": "Melon 🍈", "base_value": 7},
    {"category": "Fruit","name": "Green Apple 🍏", "base_value": 8},
    {"category": "Fruit","name": "Olive 🫒", "base_value": 6},

    {"category": "Vegetable","name": "Carrot 🥕", "base_value": 2},
    {"category": "Vegetable","name": "Potato 🥔", "base_value": 1},
    {"category": "Vegetable","name": "Onion 🧅", "base_value": 3},
    {"category": "Vegetable","name": "Garlic 🧄", "base_value": 7},
    {"category": "Vegetable","name": "Tomato 🍅", "base_value": 4},
    {"category": "Vegetable","name": "Lettuce 🥬", "base_value": 3},
    {"category": "Vegetable","name": "Cabbage 🥬", "base_value": 10},
    {"category": "Vegetable","name": "Broccoli 🥦", "base_value": 5},
    {"category": "Vegetable","name": "Corn 🌽", "base_value": 6},
    {"category": "Vegetable","name": "Cucumber 🥒", "base_value": 3},
    {"category": "Vegetable","name": "Bell Pepper 🫑", "base_value": 5},
    {"category": "Vegetable","name": "Hot Pepper 🌶️", "base_value": 8},
    {"category": "Vegetable","name": "Avocado 🥑", "base_value": 11},
    {"category": "Vegetable","name": "Mushroom 🍄", "base_value": 9},
    {"category": "Vegetable","name": "Peanuts 🥜", "base_value": 4},
    {"category": "Vegetable","name": "Beans 🫘", "base_value": 3},
    {"category": "Vegetable","name": "Pea Pod 🫛", "base_value": 2},
    {"category": "Vegetable","name": "Eggplant 🍆", "base_value": 6},
]   

# Item descriptions for almanac
ITEM_DESCRIPTIONS = {
    "Rose 🌹": "A classic symbol of love and passion!",
    "Lily 🌺": "Elegant and fragrant, a garden favorite!",
    "Sunflower 🌻": "Bright and cheerful, follows the sun!",
    "Daisy 🌼": "Simple and pure, a field of dreams!",
    "Tulip 🌷": "Colorful and springy, a Dutch delight!",
    "Daffodil 🌼": "The first sign of spring's arrival!",
    "Flowey": "Your Best Friend!",
    "Lotus🪷": "The Valorant Map, or the Person?",
    "Sakura 🌸": "I really want to go to Japan one day...",
    "Clover 🍀": "Lucky four-leaf clover brings good fortune!",
    "Herb 🌿": "Fresh and aromatic, perfect for cooking!",
    "Strawberry 🍓": "Sweet and juicy, nature's candy!",
    "Blueberry 🫐": "Tiny but packed with flavor!",
    "Raspberry": "Tart and tangy, perfect for desserts!",
    "Cherry 🍒": "Small and sweet, a summer treat!",
    "Apple 🍎": "One a day keeps the doctor away!",
    "Pear 🍐": "Sweet and crisp!",
    "Orange 🍊": "Yeah, we're from Florida. Hey Apple!",
    "Grape 🍇": "Not statuatory!",
    "Banana 🍌": "Ape-approved and potassium-packed!",
    "Watermelon 🍉": "Perfect for hot summer days!",
    "Peach 🍑": "Soft, fuzzy, and oh so sweet!",
    "Mango 🥭": "Tropical treasure with golden flesh!",
    "Pineapple 🍍": "Spiky on the outside, sweet inside!",
    "Kiwi 🥝": "Fuzzy brown exterior, emerald green inside!",
    "Lemon 🍋": "Sour but makes everything better!",
    "Coconut 🥥": "Tropical treat with refreshing water!",
    "Melon 🍈": "Sweet and refreshing, a summer favorite!",
    "Green Apple 🍏": "Crisp and tart, the other apple!",
    "Olive 🫒": "Mediterranean delight, small but mighty!",
    "Carrot 🥕": "Good for your eyes!",
    "Potato 🥔": "An Irish delight!",
    "Onion 🧅": "Makes you cry...!",
    "Garlic 🧄": "Wards off vampires!",
    "Tomato 🍅": "Technically a fruit!",
    "Lettuce 🥬": "THIS is what the Titanic hit?",
    "Cabbage 🥬": "Round and leafy, great for coleslaw!",
    "Broccoli 🥦": "A tiny tree that's super healthy!",
    "Corn 🌽": "Golden kernels of summer sweetness!",
    "Cucumber 🥒": "Cool, crisp, and refreshing!",
    "Bell Pepper 🫑": "Colorful and crunchy, comes in many hues!",
    "Hot Pepper 🌶️": "Spicy and fiery, handle with care!",
    "Avocado 🥑": "Creamy green goodness, toast's best friend!",
    "Mushroom 🍄": "Fungi among us, earthy and savory!",
    "Peanuts 🥜": "Crunchy legumes, great for snacking!",
    "Beans 🫘": "Protein-packed pods of goodness!",
    "Pea Pod 🫛": "Sweet little green pearls in a pod!",
    "Eggplant 🍆": "Purple and versatile, a kitchen staple!",
}

#level of ripeness - FRUITS
LEVEL_OF_RIPENESS_FRUITS = [
    {"name": "Budding", "multiplier": 0.9, "chance": 25},
    {"name": "Flowering", "multiplier": 1.2, "chance": 10},
    {"name": "Raw", "multiplier": 1.3, "chance": 15},
    {"name": "Slightly Ripe", "multiplier": 1.5, "chance": 25},
    {"name": "Perfectly Ripe", "multiplier": 2.5, "chance": 20},
    {"name": "Overripe", "multiplier": 1.6, "chance": 10},  
    {"name": "Spoiled", "multiplier": 0.9, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 1},
]

#level of ripeness - VEGETABLES
LEVEL_OF_RIPENESS_VEGETABLES = [
    {"name": "Sproutling", "multiplier": 1, "chance": 25},
    {"name": "Raw", "multiplier": 1.3, "chance": 15},
    {"name": "Slightly Ripe", "multiplier": 1.5, "chance": 25},
    {"name": "Perfectly Ripe", "multiplier": 2.5, "chance": 20},
    {"name": "Overripe", "multiplier": 1.6, "chance": 10},
    {"name": "Spoiled", "multiplier": 0.9, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 1},
]

#level of ripeness - FLOWERS
LEVEL_OF_RIPENESS_FLOWERS = [
    {"name": "Budded", "multiplier": 0.75, "chance": 30},
    {"name": "Blooming", "multiplier": 1, "chance": 45},
    {"name": "Full Bloom", "multiplier": 1.5, "chance": 20},
    {"name": "Wilted", "multiplier": 0.6, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 1},
]

MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# Event definitions
HOURLY_EVENTS = [
    {
        "id": "radiation_leak",
        "name": "Radiation Leak!",
        "emoji": "☢️",
        "description": "Radiation has leaked into the forest! GMO mutations are more common.",
        "effect": "GMO chance +25%"
    },
    {
        "id": "may_flowers",
        "name": "May Flowers!",
        "emoji": "🌸",
        "description": "Flowers are blooming everywhere! Increased flower gathering and triple prices!",
        "effect": "Flower gather chance +60%, flower prices x3"
    },
    {
        "id": "bumper_crop",
        "name": "Bumper Crop!",
        "emoji": "🌾",
        "description": "An exceptional harvest season! All items are worth double!",
        "effect": "All item values x2"
    },
    {
        "id": "speed_harvest",
        "name": "Speed Harvest!",
        "emoji": "⚡",
        "description": "The forest is buzzing with energy! Gather faster!",
        "effect": "Cooldown reduced by 30 seconds"
    },
    {
        "id": "perfect_ripeness",
        "name": "Perfect Ripeness!",
        "emoji": "⭐",
        "description": "Everything is perfectly ripe! All ripeness multipliers increased!",
        "effect": "All ripeness multipliers +50%"
    },
    {
        "id": "fruit_festival",
        "name": "Fruit Festival!",
        "emoji": "🍎",
        "description": "A celebration of fruits! More fruits and double prices!",
        "effect": "Fruit gather chance +50%, fruit prices x2"
    },
    {
        "id": "vegetable_boom",
        "name": "Vegetable Boom!",
        "emoji": "🥕",
        "description": "Vegetables are thriving! More vegetables and double prices!",
        "effect": "Vegetable gather chance +50%, vegetable prices x2"
    },
    {
        "id": "chain_reaction",
        "name": "Chain Reaction!",
        "emoji": "🔗",
        "description": "The gloves are working overtime! Chain chances doubled!",
        "effect": "Gloves chain chance doubled"
    },
    {
        "id": "basket_boost",
        "name": "Basket Boost!",
        "emoji": "🧺",
        "description": "Your baskets are enhanced! All basket multipliers increased!",
        "effect": "Basket multiplier +50%"
    },
    {
        "id": "lucky_strike",
        "name": "Lucky Strike!",
        "emoji": "🍀",
        "description": "Luck is on your side! All multipliers increased!",
        "effect": "All multipliers +25%"
    }
]

DAILY_EVENTS = [
    {
        "id": "double_money",
        "name": "Double Money Day!",
        "emoji": "💰",
        "description": "Today is a special day! All earnings are doubled!",
        "effect": "All earnings doubled for 24 hours"
    },
    {
        "id": "speed_day",
        "name": "Speed Day!",
        "emoji": "🏃",
        "description": "Move faster today! Cooldowns are reduced!",
        "effect": "Cooldown reduced by 15 seconds for 24 hours"
    },
    {
        "id": "gmo_surge",
        "name": "GMO Surge!",
        "emoji": "✨",
        "description": "GMO mutations are surging! Increased GMO chance all day!",
        "effect": "GMO chance +25% for 24 hours"
    },
    {
        "id": "harvest_festival",
        "name": "Harvest Festival!",
        "emoji": "🎉",
        "description": "A grand festival! All items are worth more today!",
        "effect": "All item values +50% for 24 hours"
    },
    {
        "id": "ripeness_rush",
        "name": "Ripeness Rush!",
        "emoji": "🌿",
        "description": "Perfect ripeness is everywhere! Double the chance for perfect ripeness!",
        "effect": "Perfect ripeness chance doubled for 24 hours"
    }
]

active_roulette_games = {}
user_active_games = {} # user id -> game id
active_roulette_channel_games = {} # to map channel id to game id, so we can have one game per channel
class RouletteGame:
    def __init__(self, game_id, host_id, host_name, bullets, bet_amount, max_players):
        self.game_id = game_id
        self.host_id = host_id
        self.host_name = host_name
        self.bullets = bullets
        self.initial_bullets=bullets
        self.bet_amount = bet_amount
        self.max_players = max_players
        self.players = {host_id: {"name": host_name, "alive": True, "rounds_survived": 0, "current_stake": bet_amount}}
        self.pot = 0
        self.round_number = 0
        self.chamber_size = 6
        self.turn_index = 0
        self.player_order = [host_id]
        self.game_started = False

    #add player to game
    def add_player(self, player_id: int, player_name: str):
        if len(self.players) >= self.max_players:
            return False
        if player_id in self.players:
            return False

        self.players[player_id] = {
            "name": player_name,
            "alive": True,
            "rounds_survived": 0,
            "current_stake": self.bet_amount
        }
        self.player_order.append(player_id)
        return True

    def is_full(self):
        return len(self.players) >= self.max_players

    def get_alive_players(self):
        #return list of player ids that are alive
        return [pid for pid, data in self.players.items() if data["alive"]]

    #get current players turn
    def get_current_player(self):
        alive = self.get_alive_players()
        if not alive:
            return None
        return alive[self.turn_index % len(alive)]

    #move to next player
    def next_turn(self):
        self.turn_index += 1

    #calculate the total multiplier
    def calculate_total_multiplier(self, rounds_survived):
        # Base multiplier from bullets (keep this as is for now, or remove if not needed)
        bullet_multiplier = 1.2 ** self.initial_bullets
        # 1.2x per round survived
        round_multiplier = 1.2 ** rounds_survived
        # 1.4x per ADDITIONAL player (not counting yourself if solo)
        # If solo (max_players == 1), additional_players = 0
        # If 2 players, additional_players = 1, etc.
        additional_players = max(0, len(self.players) - 1)
        player_multiplier = 1.4 ** additional_players
        return bullet_multiplier * round_multiplier * player_multiplier

    #if a player loses, get them out and add their money to the pot
    def eliminate(self, player_id):
        if player_id in self.players:
            self.players[player_id]["alive"] = False
            self.pot += self.players[player_id]["current_stake"]
            # Set 30-minute cooldown on /gather and /harvest for eliminated player
            update_user_last_roulette_elimination_time(player_id, time.time())
        #print the player out
        # print(f"{self.players[player_id]['name']} has been eliminated!")

    #when playersl live, increase their number of rounds
    def player_survived_round(self, player_id):
        if (player_id in self.players and self.players[player_id]["alive"]):
            self.players[player_id]["rounds_survived"] += 1
            # update stack w/ new multiplier
            multiplier = self.calculate_total_multiplier(self.players[player_id]["rounds_survived"])
            self.players[player_id]["current_stake"] = normalize_money(self.bet_amount * multiplier)






#start rusian roulette
async def start_roulette_game(channel, game_id):
    try:
        if game_id not in active_roulette_games:
            print(f"Warning: Game {game_id} not found in active_roulette_games")
            return
        
        game = active_roulette_games[game_id]
        
        # Check if game is already started (race condition protection)
        if game.game_started:
            print(f"Warning: Game {game_id} is already started, ignoring duplicate start request")
            return
        
        # Validate that there are players in the game
        if len(game.players) == 0:
            print(f"Error: Game {game_id} has no players, cannot start")
            # Clean up the game
            if game_id in active_roulette_games:
                del active_roulette_games[game_id]
            for ch_id, tracked_game_id in list(active_roulette_channel_games.items()):
                if tracked_game_id == game_id:
                    del active_roulette_channel_games[ch_id]
            for player_id in list(user_active_games.keys()):
                if user_active_games[player_id] == game_id:
                    # Refund the player
                    user_balance = get_user_balance(player_id)
                    user_balance = normalize_money(user_balance)
                    refund_amount = normalize_money(game.bet_amount)
                    new_balance = normalize_money(user_balance + refund_amount)
                    update_user_balance(player_id, new_balance)
                    del user_active_games[player_id]
            await channel.send("❌ **Error**: Game could not start because there are no players. All bets have been refunded.")
            return
        
        game.game_started = True
        #start on first round
        game.round_number = 1

        await asyncio.sleep(2)

        #start message
        # Ensure pot is set (should already be set by button handler, but set it here as fallback)
        if game.pot == 0:
            game.pot = normalize_money(game.bet_amount * len(game.players))
        
        embed = discord.Embed(
            title = "🎲 RUSSIAN ROULETTE 🎲",
            description = f"**{game.host_name}**'s game has started!\n*The cylinder spins.. click.. click.. click.. click..*",
            color = discord.Color.dark_red()
        )
        embed.add_field(name="🔫 Bullets Loaded", value=f"{game.bullets}/6", inline=True)
        embed.add_field(name="💰 Total Pot", value=f"${game.pot:.2f}", inline=True)
        embed.add_field(name="🎮 Players", value=f"{len(game.players)}/{game.max_players}", inline=True)
        await channel.send(embed=embed)
        await asyncio.sleep(2)

        #play round!!
        await play_roulette_round(channel, game_id)
    except Exception as e:
        print(f"Error starting roulette game {game_id}: {e}")
        import traceback
        traceback.print_exc()
        # Try to refund all players if game fails to start
        if game_id in active_roulette_games:
            game = active_roulette_games[game_id]
            for player_id in game.players.keys():
                try:
                    user_balance = get_user_balance(player_id)
                    user_balance = normalize_money(user_balance)
                    refund_amount = normalize_money(game.bet_amount)
                    new_balance = normalize_money(user_balance + refund_amount)
                    update_user_balance(player_id, new_balance)
                    if player_id in user_active_games:
                        del user_active_games[player_id]
                except Exception as refund_error:
                    print(f"Error refunding player {player_id}: {refund_error}")
            # Clean up game
            del active_roulette_games[game_id]
            for ch_id, tracked_game_id in list(active_roulette_channel_games.items()):
                if tracked_game_id == game_id:
                    del active_roulette_channel_games[ch_id]
            try:
                await channel.send("❌ **Error**: Game failed to start. All bets have been refunded.")
            except:
                pass

#play a round of russian roulette
async def play_roulette_round(channel, game_id):
    if game_id not in active_roulette_games:
        return
    game = active_roulette_games[game_id]
    alive_players = game.get_alive_players()

    #check if  game should end, check if everyone died
    if (len(alive_players) <= 0):
        #game over
        await end_roulette_game(channel, game_id)
        return

    if len(alive_players) == 1 and game.max_players > 1:
        # one player left, player can choose to keep playing
        winner_id = alive_players[0]
        winner = game.players[winner_id]

        #announce winner, but let them keep playing
        embed = discord.Embed(
            title = "LAST PLAYER STANDING!",
            description = f"**{winner['name']}** is the last man standing!\n\n**But the game isn't over. Will they try their luck?**",
            color = discord.Color.gold()
        )
        embed.add_field(name="💰 Current Winnings", value=f"${game.pot + winner['current_stake']:.2f}", inline=True)
        embed.add_field(name="📈 Current Multiplier", value=f"{game.calculate_total_multiplier(winner['rounds_survived']):.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{winner['rounds_survived']}", inline=True)
        embed.add_field(name="🔫 Bullets Left", value=f"{game.bullets}/6", inline=True)

        await channel.send(embed=embed)
        await asyncio.sleep(2)

    current_player_id = game.get_current_player()
    if current_player_id is None:
        await end_roulette_game(channel, game_id)
        return

    current_player = game.players[current_player_id]

    #revolver chamber spinning animation
    embed = discord.Embed(
        title=f"🔫 {current_player['name']}'s Turn",
        description="*The cylinder re-spins...*\n\n🔄 🔄 🔄",
        color=discord.Color.orange()
    )
    embed.add_field(name="💀 Bullets Remaining", value=f"{game.bullets}/6", inline=True)
    embed.add_field(name="💰 Current Stake", value=f"${current_player['current_stake']:.2f}", inline=True)
    embed.add_field(name="🎯 Rounds Survived", value=f"{current_player['rounds_survived']}", inline=True)
    embed.add_field(name="📈 Current Multiplier", value=f"{game.calculate_total_multiplier(current_player['rounds_survived']):.2f}x", inline=True)
    
    msg = await channel.send(embed=embed)
    await asyncio.sleep(2)

    #bullet firing logic
    chambers = [False] * 6
    for i in range(game.bullets):
        chambers[i] = True
    random.shuffle(chambers)

    shot_fired = chambers[0]

    if shot_fired:
        #player eliminated
        game.eliminate(current_player_id)
        game.bullets -= 1

        embed = discord.Embed(
            title="💥 BANG! 💥",
            description=f"**{current_player['name']}** has been eliminated!",
            color=discord.Color.dark_red()
            ) 
        embed.add_field(name="💀 Status", value="ELIMINATED", inline=True)
        embed.add_field(name="💸 Lost", value=f"${current_player['current_stake']:.2f}", inline=True)
        embed.add_field(name="💰 Pot Now", value=f"${game.pot:.2f}", inline=True)
        embed.add_field(name="🔫 Bullets Left", value=f"{game.bullets}/6", inline=True)
        embed.add_field(name="👥 Players Alive", value=f"{len(game.get_alive_players())}", inline=True)
    
        await msg.edit(embed=embed)
    
        # remove player from active games
        if current_player_id in user_active_games:
            del user_active_games[current_player_id]
    
        # check if anyone is left
        await asyncio.sleep(2)
        if len(game.get_alive_players()) == 0:
            await end_roulette_game(channel, game_id)
            return
    
        #continue to next player - give them option to cash out (except first turn)
        game.next_turn()
        await asyncio.sleep(2)
        
        # Check if this is the very first turn (no one has survived a round yet)
        is_first_turn = all(player['rounds_survived'] == 0 for player in game.players.values())
        
        if is_first_turn:
            # First turn - immediately continue to next player's turn
            await play_roulette_round(channel, game_id)
        else:
            # Not first turn - give next player option to cash out or continue
            alive_players = game.get_alive_players()
            if len(alive_players) == 0:
                await end_roulette_game(channel, game_id)
                return
            
            next_player_id = game.get_current_player()
            if next_player_id is None:
                await end_roulette_game(channel, game_id)
                return
            
            next_player = game.players[next_player_id]
            
            # Determine total winnings if they cash out now
            if len(alive_players) == 1:
                potential_winnings = game.pot + next_player['current_stake']
            else:
                potential_winnings = next_player['current_stake']
            
            # Create continue/cashout view (only allow cash out if not first turn)
            is_first_turn_here = all(player['rounds_survived'] == 0 for player in game.players.values())
            view = RouletteContinueView(game_id, timeout=300, allow_cashout=not is_first_turn_here)
            
            if is_first_turn_here:
                embed = discord.Embed(
                    title="⚠️ YOUR TURN ⚠️",
                    description=f"**{next_player['name']}**, it's your turn!\n\nClick **Pull Trigger** to continue.\n\n⏰ **You have 5 minutes to decide, or you'll automatically cash out.**\n\n*Note: Cash out is not available on the very first turn.*",
                    color=discord.Color.gold()
                )
            else:
                embed = discord.Embed(
                    title="⚠️ YOUR TURN ⚠️",
                    description=f"**{next_player['name']}**, it's your turn!\n\nClick **Pull Trigger** to continue or **Cash Out** to leave with your winnings.\n\n⏰ **You have 5 minutes to decide, or you'll automatically cash out.**",
                    color=discord.Color.gold()
                )
            embed.add_field(name="💰 Potential Winnings", value=f"${potential_winnings:.2f}", inline=True)
            embed.add_field(name="🔫 Bullets", value=f"{game.bullets}/6", inline=True)
            embed.add_field(name="💀 Death Odds", value=f"{(game.bullets/6)*100:.1f}%", inline=True)
            embed.add_field(name="📈 Current Multiplier", value=f"{game.calculate_total_multiplier(next_player['rounds_survived']):.2f}x", inline=True)
            embed.add_field(name="🎯 Rounds Survived", value=f"{next_player['rounds_survived']}", inline=True)
            
            if len(alive_players) == 1 and game.max_players > 1:
                embed.add_field(
                    name="🏆 Victory Status",
                    value="You won the multiplayer round! Keep playing to increase your multiplier or cash out now!",
                    inline=False
                )
            await channel.send(f"<@{next_player_id}>", embed=embed, view=view)
        return
    
    else:
        # player survived!
        game.player_survived_round(current_player_id)
    
        embed = discord.Embed(
            title="*click*",
            description=f"**{current_player['name']}** survived!",
            color=discord.Color.green()
            )
    
        new_multiplier = game.calculate_total_multiplier(current_player['rounds_survived'])
        embed.add_field(name="✅ Status", value="ALIVE", inline=True)
        embed.add_field(name="💰 Current Stake", value=f"${current_player['current_stake']:.2f}", inline=True)
        embed.add_field(name="📈 Multiplier", value=f"{new_multiplier:.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{current_player['rounds_survived']}", inline=True)
    
        await msg.edit(embed=embed)

    # If all bullets gone, reload chamber
    if game.bullets == 0:
        game.bullets = game.initial_bullets
        game.round_number += 1
        
        await asyncio.sleep(2)
        
        embed = discord.Embed(
            title=f"🔄 ROUND {game.round_number} 🔄",
            description="*Reloading the chamber...*\n\n**Stakes just got higher!**",
            color=discord.Color.blue()
        )
        embed.add_field(name="🔫 Bullets Reloaded", value=f"{game.bullets}/6", inline=True)
        embed.add_field(name="👥 Players Remaining", value=f"{len(alive_players)}", inline=True)
        embed.add_field(name="💰 Total Pot", value=f"${game.pot:.2f}", inline=True)
        
        await channel.send(embed=embed)
        await asyncio.sleep(2)
    
    # Move to next player (or same player in solo/last-man-standing)
    if len(alive_players) > 1:
        game.next_turn()
    # If solo or last survivor, they go again (don't increment turn)
    
    # Get next player for decision
    next_player_id = game.get_current_player()
    if next_player_id is None:
        await end_roulette_game(channel, game_id)
        return
        
    next_player = game.players[next_player_id]
    
    # Check if this is the very first turn (no one has survived a round yet)
    is_first_turn = all(player['rounds_survived'] == 0 for player in game.players.values())
    
    # Determine total winnings if they cash out now
    if len(alive_players) == 1:
        # Last player standing gets pot + their stake
        potential_winnings = game.pot + next_player['current_stake']
    else:
        # Multiplayer - just show their stake
        potential_winnings = next_player['current_stake']
    
    # Create continue/cashout view (only allow cash out if not first turn)
    view = RouletteContinueView(game_id, timeout=300, allow_cashout=not is_first_turn)
    
    if is_first_turn:
        embed = discord.Embed(
            title="⚠️ YOUR TURN ⚠️",
            description=f"**{next_player['name']}**, it's your turn!\n\nClick **Pull Trigger** to continue.\n\n⏰ **You have 5 minutes to decide, or you'll automatically cash out.**\n\n*Note: Cash out is not available on the very first turn.*",
            color=discord.Color.gold()
        )
    else:
        embed = discord.Embed(
            title="⚠️ YOUR TURN ⚠️",
            description=f"**{next_player['name']}**, it's your turn!\n\nClick **Pull Trigger** to continue or **Cash Out** to leave with your winnings.\n\n⏰ **You have 5 minutes to decide, or you'll automatically cash out.**",
            color=discord.Color.gold()
        )
    
    embed.add_field(name="💰 Potential Winnings", value=f"${potential_winnings:.2f}", inline=True)
    embed.add_field(name="🔫 Bullets", value=f"{game.bullets}/6", inline=True)
    embed.add_field(name="💀 Death Odds", value=f"{(game.bullets/6)*100:.1f}%", inline=True)
    embed.add_field(name="📈 Current Multiplier", value=f"{game.calculate_total_multiplier(next_player['rounds_survived']):.2f}x", inline=True)
    embed.add_field(name="🎯 Rounds Survived", value=f"{next_player['rounds_survived']}", inline=True)
    
    # Show different message for solo vs last-survivor
    if len(alive_players) == 1 and game.max_players > 1:
        embed.add_field(
            name="🏆 Victory Status",
            value="You won the multiplayer round! Keep playing to increase your multiplier or cash out now!",
            inline=False
        )
    
    await channel.send(f"<@{next_player_id}>", embed=embed, view=view)

class RouletteJoinView(discord.ui.View):
    def __init__(self, game_id: str, host_id: int, timeout = 300):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.host_id = host_id

    @discord.ui.button(label = "Join Game", style = discord.ButtonStyle.green, emoji = "🔫")
    async def join_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            user_id = interaction.user.id
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
                
            game = active_roulette_games[self.game_id]
            
            # Check if user is on Russian Roulette elimination cooldown (dead)
            is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
            if is_roulette_cooldown:
                minutes_left = roulette_time_left // 60
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot join Russian Roulette for {minutes_left} minute(s)", ephemeral=True)
                return
            
            # Check if user is already in this game
            if user_id in game.players:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You're already in this game!", ephemeral=True)
                return
                
            # Check if user is in another game
            if user_id in user_active_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You're already in another game!", ephemeral=True)
                return
                
            # Check if game is full
            if len(game.players) >= game.max_players:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game is full!", ephemeral=True)
                return
                
            # Check if game already started
            if game.game_started:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game already started!", ephemeral=True)
                return
                
            # Check user balance
            user_balance = get_user_balance(user_id)
            user_balance = normalize_money(user_balance)
            bet_amount = normalize_money(game.bet_amount)
            
            if not can_afford_rounded(user_balance, bet_amount):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You don't have enough balance to join!", ephemeral=True)
                return
                
            # Join the game
            game.add_player(user_id, interaction.user.name)
            user_active_games[user_id] = self.game_id
            
            # Deduct bet
            new_balance = normalize_money(user_balance - bet_amount)
            update_user_balance(user_id, new_balance)
            
            # Update the embed
            embed = interaction.message.embeds[0]
            embed.description = f"**{game.host_name}** is playing with **{len(game.players)}/{game.max_players}** players!\n\n*How long can you survive?*"
            
            # Update the view (disable join button if full)
            if len(game.players) >= game.max_players:
                button.disabled = True
                button.label = "Game Full"
            
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in join_game: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.blurple, emoji="🚀")
    async def start_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Only host can start
            if interaction.user.id != self.host_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Only the game host can start the game!", ephemeral=True)
                return
                
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
                
            game = active_roulette_games[self.game_id]
            
            # Check if game already started (race condition protection)
            if game.game_started:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game already started!", ephemeral=True)
                return
            
            # Validate that there are players
            if len(game.players) == 0:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Cannot start game: No players in game!", ephemeral=True)
                return
                
            # Set pot before starting (start_roulette_game will set game_started)
            game.pot = normalize_money(game.bet_amount * len(game.players))
            
            await safe_interaction_response(interaction, interaction.response.edit_message, content="🎮 **Game Started!**", view=None)
            
            # Start the actual game (this will set game_started and handle errors)
            await start_roulette_game(interaction.channel, self.game_id)
        except Exception as e:
            print(f"Error in start_game: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Cancel Game", style=discord.ButtonStyle.red, emoji="❌")
    async def cancel_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Only host can cancel
            if interaction.user.id != self.host_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Only the game host can cancel the game!", ephemeral=True)
                return
            
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            
            game = active_roulette_games[self.game_id]
            
            # Check if game already started
            if game.game_started:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Cannot cancel a game that has already started!", ephemeral=True)
                return
            
            # Refund all players
            refunded_count = 0
            for player_id in list(game.players.keys()):
                try:
                    user_balance = get_user_balance(player_id)
                    user_balance = normalize_money(user_balance)
                    refund_amount = normalize_money(game.bet_amount)
                    new_balance = normalize_money(user_balance + refund_amount)
                    update_user_balance(player_id, new_balance)
                    refunded_count += 1
                except Exception as e:
                    print(f"Error refunding player {player_id}: {e}")
            
            # Clean up game from all dictionaries
            if self.game_id in active_roulette_games:
                del active_roulette_games[self.game_id]
            
            for ch_id, tracked_game_id in list(active_roulette_channel_games.items()):
                if tracked_game_id == self.game_id:
                    del active_roulette_channel_games[ch_id]
            
            for player_id in list(user_active_games.keys()):
                if user_active_games[player_id] == self.game_id:
                    del user_active_games[player_id]
            
            # Update the message to show cancellation
            embed = discord.Embed(
                title="❌ Game Cancelled",
                description=f"**{game.host_name}** cancelled the game.\n\nAll bets have been refunded.",
                color=discord.Color.red()
            )
            embed.add_field(name="💰 Refunded", value=f"${game.bet_amount:.2f} to {refunded_count} player(s)", inline=True)
            
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=None)
        except Exception as e:
            print(f"Error in cancel_game: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    async def on_timeout(self):
        # Auto-start the game after 5 minutes if host hasn't started it
        if self.game_id in active_roulette_games:
            game = active_roulette_games[self.game_id]
            if not game.game_started and len(game.players) >= 1:  # At least host is in game
                # Set pot before starting (start_roulette_game will set game_started and validate)
                game.pot = normalize_money(game.bet_amount * len(game.players))
                
                # Find the channel where this game is running
                channel = None
                for ch_id, tracked_game_id in active_roulette_channel_games.items():
                    if tracked_game_id == self.game_id:
                        channel = bot.get_channel(ch_id)
                        break
                
                if channel:
                    try:
                        await channel.send("⏰ **Auto-starting game after 5 minutes!**")
                        await start_roulette_game(channel, self.game_id)
                    except Exception as e:
                        print(f"Error auto-starting roulette game: {e}")
                        import traceback
                        traceback.print_exc()



# roulette continue view
class RouletteContinueView(discord.ui.View):
    def __init__(self, game_id, timeout=300, allow_cashout=True):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.allow_cashout = allow_cashout
    
    @discord.ui.button(label="Pull Trigger", style=discord.ButtonStyle.danger, emoji="🔫")
    async def continue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            
            game = active_roulette_games[self.game_id]
            current_player_id = game.get_current_player()
            
            if interaction.user.id != current_player_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
                return
            
            if not await safe_defer(interaction):
                return
            
            try:
                await interaction.message.delete()
            except:
                pass  # Message might already be deleted
            
            # Continue the game
            await play_roulette_round(interaction.channel, self.game_id)
        except Exception as e:
            print(f"Error in continue_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Cash Out", style=discord.ButtonStyle.secondary, emoji="💰")
    async def cashout_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not self.allow_cashout:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Cash out is not available on the very first turn!", ephemeral=True)
                return
            
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            
            game = active_roulette_games[self.game_id]
            current_player_id = game.get_current_player()
            
            if interaction.user.id != current_player_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
                return
            
            if not await safe_defer(interaction):
                return
            
            # Cash out - player gets their stake back
            player = game.players[current_player_id]
            winnings = normalize_money(player['current_stake'])
            
            # Add winnings to player balance
            current_balance = get_user_balance(current_player_id)
            current_balance = normalize_money(current_balance)
            new_balance = normalize_money(current_balance + winnings)
            update_user_balance(current_player_id, new_balance)
            
            # Remove from active games
            if current_player_id in user_active_games:
                del user_active_games[current_player_id]
            
            # Mark player as eliminated (cashed out)
            game.players[current_player_id]['alive'] = False
            
            embed = discord.Embed(
                title="💰 CASHED OUT! 💰",
                description=f"**{player['name']}** decided to walk away!",
                color=discord.Color.gold()
            )
            embed.add_field(name="💵 Winnings", value=f"${winnings:.2f}", inline=True)
            embed.add_field(name="💸 Profit", value=f"${normalize_money(winnings - normalize_money(game.bet_amount)):.2f}", inline=True)
            embed.add_field(name="📈 Multiplier Achieved", value=f"{game.calculate_total_multiplier(player['rounds_survived']):.2f}x", inline=True)
            embed.add_field(name="🎯 Rounds Survived", value=f"{player['rounds_survived']}", inline=True)
            
            try:
                await interaction.message.edit(embed=embed, view=None)
            except:
                pass  # Message might have been deleted
            
            # Check for hidden achievement: Beating The Odds (cashout with 5 bullets = 5/6 death chance)
            # Send as ephemeral message to the user
            if game.initial_bullets == 5 and unlock_hidden_achievement(current_player_id, "beating_the_odds"):
                try:
                    # Send ephemeral notification to the user who cashed out
                    await send_hidden_achievement_notification(interaction, "beating_the_odds")
                except Exception as e:
                    print(f"Error sending Beating The Odds achievement notification: {e}")
            
            # Check if game ends
            alive_count = len(game.get_alive_players())
            
            if alive_count == 0 or (alive_count == 1 and game.max_players > 1):
                await asyncio.sleep(2)
                await end_roulette_game(interaction.channel, self.game_id)
            else:
                game.next_turn()
                await asyncio.sleep(2)
                await play_roulette_round(interaction.channel, self.game_id)
        except Exception as e:
            print(f"Error in cashout_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    async def on_timeout(self):
        # Auto-cash out when timeout expires
        if self.game_id not in active_roulette_games:
            return
        
        game = active_roulette_games[self.game_id]
        current_player_id = game.get_current_player()
        
        if current_player_id is None:
            return
        
        # Check if player is still alive (hasn't already been eliminated)
        if current_player_id not in game.players or not game.players[current_player_id]['alive']:
            return
        
        # Get the message channel - we need to find it from the game
        channel = None
        for ch_id, tracked_game_id in active_roulette_channel_games.items():
            if tracked_game_id == self.game_id:
                channel = bot.get_channel(ch_id)
                break
        
        if channel is None:
            return
        
        # Cash out - player gets their stake back
        player = game.players[current_player_id]
        winnings = normalize_money(player['current_stake'])
        
        # Add winnings to player balance
        current_balance = get_user_balance(current_player_id)
        current_balance = normalize_money(current_balance)
        new_balance = normalize_money(current_balance + winnings)
        update_user_balance(current_player_id, new_balance)
        
        # Remove from active games
        if current_player_id in user_active_games:
            del user_active_games[current_player_id]
        
        # Mark player as eliminated (cashed out)
        game.players[current_player_id]['alive'] = False
        
        embed = discord.Embed(
            title="💰 AUTO CASHED OUT! 💰",
            description=f"**{player['name']}** timed out and was automatically cashed out!",
            color=discord.Color.orange()
        )
        embed.add_field(name="💵 Winnings", value=f"${winnings:.2f}", inline=True)
        embed.add_field(name="💸 Profit", value=f"${normalize_money(winnings - normalize_money(game.bet_amount)):.2f}", inline=True)
        embed.add_field(name="📈 Multiplier Achieved", value=f"{game.calculate_total_multiplier(player['rounds_survived']):.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{player['rounds_survived']}", inline=True)
        
        await channel.send(embed=embed)
        
        # Check if game ends
        alive_count = len(game.get_alive_players())
        
        if alive_count == 0 or (alive_count == 1 and game.max_players > 1):
            await asyncio.sleep(2)
            await end_roulette_game(channel, self.game_id)
        else:
            game.next_turn()
            await asyncio.sleep(2)
            await play_roulette_round(channel, self.game_id)

# end roulette

async def end_roulette_game(channel, game_id):
    # end the game, give winnings
    if game_id not in active_roulette_games:
        return
    
    game = active_roulette_games[game_id]
    alive_players = game.get_alive_players()
    
    if len(alive_players) == 1:
        # One winner!
        winner_id = alive_players[0]
        winner = game.players[winner_id]
        
        # Winner gets pot + their stake
        total_winnings = game.pot + winner['current_stake']
        
        # Add winnings to balance
        current_balance = get_user_balance(winner_id)
        update_user_balance(winner_id, current_balance + total_winnings)
        
        # Remove from active games
        if winner_id in user_active_games:
            del user_active_games[winner_id]
        
        # Calculate profit
        profit = total_winnings - game.bet_amount
        
        embed = discord.Embed(
            title="🏆 WINNER! 🏆",
            description=f"**{winner['name']}** is the last one standing!",
            color=discord.Color.gold()
        )
        embed.add_field(name="💰 Total Winnings", value=f"${total_winnings:.2f}", inline=True)
        embed.add_field(name="💸 Net Profit", value=f"${profit:.2f}", inline=True)
        embed.add_field(name="📈 Final Multiplier", value=f"{game.calculate_total_multiplier(winner['rounds_survived']):.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{winner['rounds_survived']}", inline=True)
        embed.add_field(name="💀 Opponents Eliminated", value=f"{len(game.players) - 1}", inline=True)
        embed.add_field(name="🔫 Initial Bullets", value=f"{game.initial_bullets}/6", inline=True)
        
        # Add stats for solo mode
        if game.max_players == 1:
            embed.add_field(
                name="🎮 You walked away..", 
                value=f"You survived **{winner['rounds_survived']}** rounds with **{game.initial_bullets}** bullets!",
                inline=False
            )
        
        await channel.send(embed=embed)
        
    elif len(alive_players) == 0:
        # Everyone eliminated (all died on same round)
        embed = discord.Embed(
            title="☠️ EVERYONE ELIMINATED ☠️",
            description="Nobody survived... The pot is lost to the void.",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="💰 Lost Pot", value=f"${game.pot:.2f}", inline=True)
        
        await channel.send(embed=embed)
    
    # Clean up - remove all players from active games tracker
    for player_id in game.players.keys():
        if player_id in user_active_games:
            del user_active_games[player_id]
    
    # Clean up game
    del active_roulette_games[game_id]
    for channel_id, tracked_game_id in list(active_roulette_channel_games.items()):
        if tracked_game_id == game_id:
            del active_roulette_channel_games[channel_id]
            break



# command to add /coinflip, user bets on heads or tails, if they win they get double their bet, if they lose they lose their bet
@bot.tree.command(name="coinflip", description="Bet on heads or tails!")
@app_commands.choices(choice=[
    app_commands.Choice(name="heads", value="heads"),
    app_commands.Choice(name="tails", value="tails")
])
async def coinflip(interaction: discord.Interaction, bet: float, choice: str):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        user_id = interaction.user.id
        
        # Validate bet amount is positive
        if bet <= 0:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Bet amount must be greater than $0.00!", ephemeral=True)
            return
        
        # Validate bet has at most 2 decimal places (no fractional cents)
        if not validate_money_precision(bet):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Bet amount must be in dollars and cents (maximum 2 decimal places)!", ephemeral=True)
            return
        
        # Normalize bet to exactly 2 decimal places
        bet = normalize_money(bet)
        
        current_balance = get_user_balance(user_id)
        current_balance = normalize_money(current_balance)
        
        if not can_afford_rounded(current_balance, bet):
            await safe_interaction_response(interaction, interaction.followup.send, f"You do not have enough balance to bet **${bet:.2f}**, {interaction.user.name}.", ephemeral=False)
            return
        
        # Deduct bet first
        new_balance_after_bet = normalize_money(current_balance - bet)
        update_user_balance(user_id, new_balance_after_bet)
        
        # Flip the coin - randomly choose heads or tails (lowercase)
        coin_result = random.choice(["heads", "tails"])
        
        # Check if they won (their choice matches the result, both lowercase)
        won = choice.lower() == coin_result
        
        # Track coinflip stats
        increment_user_coinflip_count(user_id)
        current_streak = get_user_coinflip_win_streak(user_id)
        
        # Track achievements that will be unlocked
        achievements_unlocked = []
        
        if won:
            # They win - increment streak
            new_streak = current_streak + 1
            set_user_coinflip_win_streak(user_id, new_streak)
            
            # Check and update coinflip_win_streak achievement
            new_streak_level = get_achievement_level_for_stat("coinflip_win_streak", new_streak)
            current_streak_level = get_user_achievement_level(user_id, "coinflip_win_streak")
            if new_streak_level > current_streak_level:
                set_user_achievement_level(user_id, "coinflip_win_streak", new_streak_level)
                achievements_unlocked.append(("coinflip_win_streak", new_streak_level))
        else:
            # They lose - reset streak to 0
            set_user_coinflip_win_streak(user_id, 0)
        
        # Check and update coinflip_total achievement
        coinflip_count = get_user_coinflip_count(user_id)
        new_total_level = get_achievement_level_for_stat("coinflip_total", coinflip_count)
        current_total_level = get_user_achievement_level(user_id, "coinflip_total")
        if new_total_level > current_total_level:
            set_user_achievement_level(user_id, "coinflip_total", new_total_level)
            achievements_unlocked.append(("coinflip_total", new_total_level))
        
        # Calculate new balance
        if won:
            # They win - get double their bet back (bet was already deducted, so add 2*bet)
            new_balance = normalize_money(new_balance_after_bet + (bet * 2))
            update_user_balance(user_id, new_balance)
            message = f"You placed **${bet:.2f}** on **{choice}**!\nThe coin landed on **{coin_result}**! You doubled your bet!!\nYour new balance is **${new_balance:.2f}**."
        else:
            # They lose - bet was already deducted
            new_balance = new_balance_after_bet
            # Get the opposite choice for display (lowercase)
            opposite = "tails" if choice.lower() == "heads" else "heads"
            message = f"You placed **${bet:.2f}** on **{choice}**!\nOuch {interaction.user.name}, the coin landed on **{opposite}**. You lost **${bet:.2f}**.\nYour new balance is **${new_balance:.2f}**."
        
        # Send the main coinflip result message first
        await safe_interaction_response(interaction, interaction.followup.send, message, ephemeral=False)
        
        # Then send all achievement notifications as ephemeral (only visible to user)
        for achievement_name, achievement_level in achievements_unlocked:
            await send_achievement_notification(interaction, achievement_name, achievement_level)
            # Small delay to ensure proper ordering
            await asyncio.sleep(0.5)
    except Exception as e:
        print(f"Error in coinflip command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Slots game implementation
def generate_slot_emoji():
    """Generate a random slot emoji based on probability distribution."""
    rand = random.random() * 100  # 0-100
    
    if rand < 1.0:  # 1% chance
        return "💎"  # JACKPOT (diamond)
    elif rand < 8.5:  # 7.5% chance (1% + 7.5% = 8.5%)
        return "📊"  # BAR (bar chart emoji)
    elif rand < 18.5:  # 10% chance (8.5% + 10% = 18.5%)
        return "7️⃣"  # SEVEN
    elif rand < 33.5:  # 15% chance (18.5% + 15% = 33.5%)
        return "⭐"  # STAR
    elif rand < 58.5:  # 25% chance (33.5% + 25% = 58.5%)
        return "💰"  # MONEY BAG
    else:  # Remaining ~41.5% split evenly among cherry, lemon, orange
        return random.choice(["🍒", "🍋", "🍊"])  # 33% each of remaining


def generate_slot_grid():
    """Generate a 3x3 grid of slot emojis."""
    return [
        [generate_slot_emoji() for _ in range(3)] for _ in range(3)
    ]


def format_slot_grid(grid, locked_columns=None):
    """Format the slot grid as a string, highlighting the middle row."""
    if locked_columns is None:
        locked_columns = set()
    
    # Format as a clean grid with consistent cell width
    # Each cell is 5 characters wide (including borders)
    # Top border
    top_row = "┌─────┬─────┬─────┐\n"
    
    # First row - center emojis with consistent spacing
    row1 = f"│ {grid[0][0]}  │ {grid[0][1]}  │ {grid[0][2]}  │\n"
    row1_sep = "├─────┼─────┼─────┤\n"
    
    # Middle row (the winning row) - highlighted
    row2 = f"│ {grid[1][0]}  │ {grid[1][1]}  │ {grid[1][2]}  │ ⬅️\n"
    row2_sep = "├─────┼─────┼─────┤\n"
    
    # Bottom row
    row3 = f"│ {grid[2][0]}  │ {grid[2][1]}  │ {grid[2][2]}  │\n"
    bottom_row = "└─────┴─────┴─────┘"
    
    # Use code block for monospace font - this helps with alignment
    return f"```\n{top_row}{row1}{row1_sep}{row2}{row2_sep}{row3}{bottom_row}\n```"


def check_win(grid):
    """Check if the middle row has matching emojis."""
    middle_row = grid[1]  # Middle row (index 1)
    return len(set(middle_row)) == 1  # All emojis are the same


class SlotsView(discord.ui.View):
    def __init__(self, user_id: int, bet: float, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.bet = bet
        self.grid = generate_slot_grid()
        self.spinning = False
        self.spun = False
        self.locked_columns = set()  # Track which columns have stopped
        self.final_grid = None  # Store final result
        
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the user who started the game to interact."""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your slots game!", ephemeral=True)
            return False
        return True
    
    def update_embed(self, is_spinning=False, status_text=""):
        """Create or update the embed with current slot state."""
        title = "🎰 SLOTS - SPINNING... 🎰" if is_spinning else "🎰 SLOTS 🎰"
        embed = discord.Embed(
            title=title,
            description=f"Bet: **${self.bet:.2f}**\n\n{format_slot_grid(self.grid, self.locked_columns)}",
            color=discord.Color.gold() if not is_spinning else discord.Color.orange()
        )
        
        if not self.spun:
            embed.set_footer(text="Click SPIN to play!")
        elif is_spinning:
            footer_text = status_text if status_text else "🎰 Spinning... 🎰"
            embed.set_footer(text=footer_text)
        else:
            embed.set_footer(text="Spin complete!")
        
        return embed
    
    async def animate_spin(self, interaction: discord.Interaction):
        """Animate the slots spinning with columns stopping one at a time."""
        # Disable the button during spin
        for item in self.children:
            item.disabled = True
        
        # Generate final result first
        self.final_grid = generate_slot_grid()
        
        # Update embed to show spinning state
        embed = self.update_embed(is_spinning=True, status_text="🎰 All columns spinning... 🎰")
        
        # Respond to the button interaction by editing the message
        await interaction.response.edit_message(embed=embed, view=self)
        
        # Phase 1: All columns spinning (fast)
        for frame in range(8):
            # Randomize all columns
            for row in range(3):
                for col in range(3):
                    self.grid[row][col] = generate_slot_emoji()
            
            embed = self.update_embed(is_spinning=True, status_text="🎰 All columns spinning... 🎰")
            await interaction.message.edit(embed=embed, view=self)
        
        # Lock column 1 - set it to final values
        for row in range(3):
            self.grid[row][0] = self.final_grid[row][0]
        self.locked_columns.add(0)
        
        embed = self.update_embed(is_spinning=True, status_text="✅ Column 1 stopped! 🎰 Columns 2 & 3 spinning...")
        await interaction.message.edit(embed=embed, view=self)
        
        # Phase 2: Columns 2 and 3 spinning
        for frame in range(6):
            # Randomize only columns 2 and 3
            for row in range(3):
                self.grid[row][1] = generate_slot_emoji()
                self.grid[row][2] = generate_slot_emoji()
            
            embed = self.update_embed(is_spinning=True, status_text="✅ Column 1 stopped! 🎰 Columns 2 & 3 spinning...")
            await interaction.message.edit(embed=embed, view=self)
        
        # Lock column 2
        for row in range(3):
            self.grid[row][1] = self.final_grid[row][1]
        self.locked_columns.add(1)
        
        embed = self.update_embed(is_spinning=True, status_text="✅ Columns 1 & 2 stopped! 🎰 Column 3 spinning...")
        await interaction.message.edit(embed=embed, view=self)
        
        # Phase 3: Only column 3 spinning (slower, building suspense)
        for frame in range(5):
            # Randomize only column 3
            for row in range(3):
                self.grid[row][2] = generate_slot_emoji()
            
            embed = self.update_embed(is_spinning=True, status_text="✅ Columns 1 & 2 stopped! 🎰 Column 3 spinning...")
            await interaction.message.edit(embed=embed, view=self)
        
        # Lock column 3 - final result
        for row in range(3):
            self.grid[row][2] = self.final_grid[row][2]
        self.locked_columns.add(2)
        
        # Check for win
        won = check_win(self.grid)
        self.spinning = False
        self.spun = True
        
        # Show final result
        embed = discord.Embed(
            title="🎰 SLOTS - FINAL RESULT 🎰",
            description=f"Bet: **${self.bet:.2f}**\n\n\n\n{format_slot_grid(self.grid, self.locked_columns)}",
            color=discord.Color.green() if won else discord.Color.red()
        )
        
        if won:
            middle_emoji = self.grid[1][0]
            winnings = self.bet * 3  # Triple the bet
            current_balance = get_user_balance(self.user_id)
            current_balance = normalize_money(current_balance)
            new_balance = normalize_money(current_balance + winnings)
            update_user_balance(self.user_id, new_balance)
            
            embed.add_field(
                name="🎉 YOU WON! 🎉",
                value=f"All three **{middle_emoji}** in the middle row!\nYou won **${winnings:.2f}**!\nYour new balance is **${new_balance:.2f}**.",
                inline=False
            )
        else:
            current_balance = get_user_balance(self.user_id)
            current_balance = normalize_money(current_balance)
            
            embed.add_field(
                name="❌ You Lost",
                value=f"No match in the middle row. You lost **${self.bet:.2f}**.\nYour balance is **${current_balance:.2f}**.",
                inline=False
            )
        
        await interaction.message.edit(embed=embed, view=self)
        self.stop()
    
    @discord.ui.button(label="🎰 SPIN 🎰", style=discord.ButtonStyle.success, emoji="🎲", row=0)
    async def spin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Spin the slots!"""
        try:
            if self.spinning or self.spun:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You already spun!", ephemeral=True)
                return
            
            self.spinning = True
            # Start animation in a task so we don't block
            await self.animate_spin(interaction)
        except Exception as e:
            print(f"Error in spin_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="slots", description="Play slots! Match 3 in the middle row to win!")
@app_commands.describe(bet="The amount to bet")
async def slots(interaction: discord.Interaction, bet: float):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        user_id = interaction.user.id
        
        # Check if user is on Russian Roulette elimination cooldown (dead)
        is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
        if is_roulette_cooldown:
            minutes_left = roulette_time_left // 60
            await safe_interaction_response(interaction, interaction.followup.send,
                f"Sorry, {interaction.user.name}, you're dead. You cannot play slots for {minutes_left} minute(s)", ephemeral=True)
            return
        
        # Validate bet amount is positive
        if bet <= 0:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Invalid bet amount!", ephemeral=True)
            return
        
        # Validate bet has at most 2 decimal places (no fractional cents)
        if not validate_money_precision(bet):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Invalid bet amount!", ephemeral=True)
            return
        
        # Normalize bet to exactly 2 decimal places
        bet = normalize_money(bet)
        
        current_balance = get_user_balance(user_id)
        current_balance = normalize_money(current_balance)
        
        if not can_afford_rounded(current_balance, bet):
            await safe_interaction_response(interaction, interaction.followup.send, f"You do not have enough balance to bet **${bet:.2f}**, {interaction.user.name}.", ephemeral=False)
            return
        
        # Deduct bet first
        new_balance_after_bet = normalize_money(current_balance - bet)
        update_user_balance(user_id, new_balance_after_bet)
        
        # Create slots view
        view = SlotsView(user_id, bet)
        embed = view.update_embed()
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in slots command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# user_balances = {}
# def get_user_balance(user_id):
#     # get users balance and start them with 100 if they're new
#     if user_id not in user_balances:
#         user_balances[user_id] = 100.00
#     return user_balances[user_id]

# on ready
@bot.event 
async def on_ready():
    print(f"Slash Gather, {bot.user.name}")
    #set bot status
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.playing,
            name="running /gather on V0.3.3 :3"
        )
    )
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print(f"Error syncing commands: {e}")
    
    # Start the leaderboard update task
    bot.loop.create_task(update_all_leaderboards())
    print("Started automatic leaderboard updates")
    
    
    # Start the marketboard update task
    bot.loop.create_task(update_all_marketboards())
    print("Started automatic marketboard updates")
    
    # Start the market news task
    bot.loop.create_task(send_market_news_loop())
    print("Started automatic market news alerts")
    
    # Start the coinbase update task
    bot.loop.create_task(update_all_coinbase())
    print("Started automatic coinbase updates")
    
    # Start the gardener background task
    bot.loop.create_task(gardener_background_task())
    print("Started automatic gardener gathering")
    
    # Start the GPU background task
    bot.loop.create_task(gpu_background_task())
    print("Started automatic GPU mining")
    
    # Start the event background tasks
    bot.loop.create_task(hourly_event_check())
    print("Started hourly event checking")
    bot.loop.create_task(daily_event_check())
    print("Started daily event checking")
    bot.loop.create_task(event_cleanup_task())
    print("Started event cleanup task")


@bot.event
async def on_member_join(member):
    # Find the welcome channel
    welcome_channel = discord.utils.get(member.guild.text_channels, name="welcome")
    
    if welcome_channel:
        
        #send welcome message to the welcome channel
        await welcome_channel.send(f"🌿 Welcome to /GATHER, {member.mention}! 🌿")
    # else:
    #     #fallback in case it fails
    #     for channel in member.guild.text_channels:
    #         if "welcome" in channel.name.lower():
    #             await channel.send(f"🌿 Welcome to /GATHER, {member.mention}! 🌿")
    #             break
    #     else:
    #         #if no welcome channel found
    #         print(f"Warning: No 'welcome' channel found in {member.guild.name}. Could not welcome {member.name}.")

    #assign gatherer1 role or whatever they have incase they joined and then left, and then rejoined
    try:
        await assign_gatherer_role(member, member.guild)
    except Exception as e:
        print(f"Error assigning gatherer role to user {member.id}: {e}")

@bot.tree.command(name="gather", description="Gather a random item from nature!")
async def gather(interaction: discord.Interaction):
    try:
        #use defer for custom message
        if not await safe_defer(interaction, ephemeral=False):
            return

        # Fetch user data and events once at the start
        user_id = interaction.user.id
        user_data = get_user_gather_data(user_id)   
        active_events = get_active_events_cached()

        #check if the user is on cooldown (default 1 min), if so let them know how much time they have left
        can_user_gather, time_left, is_roulette_cooldown = can_gather(user_id, user_data=user_data, active_events=active_events)
        if not can_user_gather:
            #then user is on cooldown
            if is_roulette_cooldown:
                # Russian Roulette elimination cooldown
                minutes_left = time_left // 60
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot /gather for {minutes_left} minute(s)", ephemeral=True)
            else:
                # Normal gather cooldown
                # Check for hidden achievement: Almost Got It (cooldown says "0" seconds)
                if time_left == 0 and unlock_hidden_achievement(user_id, "almost_got_it"):
                    await safe_interaction_response(interaction, interaction.followup.send,
                        f"You must wait {time_left} seconds before gathering again, {interaction.user.name}.\n\n"
                        f"🎉 **Hidden Achievement Unlocked: Almost Got It!** 🎉", ephemeral=True)
                else:
                    await safe_interaction_response(interaction, interaction.followup.send,
                        f"You must wait {time_left} seconds before gathering again, {interaction.user.name}.", ephemeral=True)
            return

        # Get Tree Rings before gather to check if one was awarded
        tree_rings_before = get_user_tree_rings(user_id)
        
        # Increment gather command count (only for actual /gather commands, not gardeners)
        increment_user_gather_command_count(user_id)
        
        # Perform the gather with pre-fetched data
        gather_result = await perform_gather_for_user(user_id, apply_cooldown=True, 
                                                      user_data=user_data, active_events=active_events)

        # Check if a Tree Ring was awarded
        tree_rings_after = get_user_tree_rings(user_id)
        if tree_rings_after > tree_rings_before:
            tree_rings_awarded = tree_rings_after - tree_rings_before
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🌳 {interaction.user.mention} You've been awarded **{tree_rings_awarded} Tree Ring{'s' if tree_rings_awarded > 1 else ''}**!",
                ephemeral=True)

        # assign role and check for rank-up
        old_role = None
        new_role = None
        try:
            old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
        except Exception as e:
            print(f"Error assigning gatherer role to user {user_id}: {e}")

        # Send rank-up notification if player advanced
        if new_role:
            # Special message for Planter I advancement (when old_role is None)
            if new_role == "PLANTER I" and old_role is None:
                # Assign PINE I bloom rank role
                try:
                    await assign_bloom_rank_role(interaction.user, interaction.guild)
                except Exception as e:
                    print(f"Error assigning bloom rank role to user {user_id}: {e}")
                
                # Get the bloom rank to display
                bloom_rank = get_bloom_rank(user_id)
                rankup_embed = discord.Embed(
                    title="🌱 Rank Up!",
                    description=f"{interaction.user.mention} advanced to **PLANTER I** and is ranked **PINE I**!",
                    color=discord.Color.gold(),
                )
            else:
                rankup_embed = discord.Embed(
                    title="🌱 Rank Up!",
                    description=f"{interaction.user.mention} advanced from **{old_role or 'PLANTER I'}** to **{new_role}**!",
                    color=discord.Color.gold(),
                )
            await safe_interaction_response(interaction, interaction.followup.send, embed=rankup_embed)

        #create discord embed
        embed = discord.Embed(
            title= "You Gathered!",
            description = f"You foraged for a(n) **{gather_result['name']}**!",
            color = discord.Color.green()
        )

        embed.add_field(name="Value", value=f"**${gather_result['value']:.2f}**", inline=True)
        embed.add_field(name="Ripeness", value=f"{gather_result['ripeness']}", inline=True)
        embed.add_field(name="GMO?", value=f"{'Yes ✨' if gather_result['is_gmo'] else 'No'}", inline=False)
        
        # Show bloom multiplier if applicable (only after first bloom)
        bloom_count = get_user_bloom_count(user_id)
        if bloom_count > 0 and gather_result.get('extra_money_from_bloom', 0) > 0:
            tree_rings = get_user_tree_rings(user_id)
            multiplier_percent = (gather_result['bloom_multiplier'] - 1.0) * 100
            embed.add_field(
                name="🌳 Tree Ring Boost", 
                value=f"+{multiplier_percent:.1f}% - **+${gather_result['extra_money_from_bloom']:.2f}**", 
                inline=False
            )
        
        # Show rank perma buff if applicable (only if not PINE I)
        bloom_rank = get_bloom_rank(user_id)
        if bloom_rank != "PINE I" and gather_result.get('extra_money_from_rank', 0) > 0:
            rank_perma_buff_percent = (gather_result['rank_perma_buff_multiplier'] - 1.0) * 100
            embed.add_field(
                name="⭐ Rank Boost",
                value=f"+{rank_perma_buff_percent:.1f}% - **+${gather_result['extra_money_from_rank']:.2f}**",
                inline=False
            )
        
        # Show achievement boost if applicable
        if gather_result.get('extra_money_from_achievement', 0) > 0:
            achievement_percent = (gather_result['achievement_multiplier'] - 1.0) * 100
            embed.add_field(
                name="🏆 Achievement Boost",
                value=f"+{achievement_percent:.1f}% - **+${gather_result['extra_money_from_achievement']:.2f}**",
                inline=False
            )
        
        # Show daily bonus if applicable (1% per consecutive day)
        if gather_result.get('extra_money_from_daily', 0) > 0:
            daily_bonus_percent = (gather_result['daily_bonus_multiplier'] - 1.0) * 100
            embed.add_field(
                name="💧 Water Streak Boost",
                value=f"+{daily_bonus_percent:.1f}% - **+${gather_result['extra_money_from_daily']:.2f}**",
                inline=False
            )
        
        # add a line to show [username] in [month]
        embed.add_field(name="~", value=f"{interaction.user.name} in {MONTHS[random.randint(0, 11)]}", inline=False)
        embed.add_field(name="new balance: ", value=f"**${gather_result['new_balance']:.2f}**", inline=False)
        
        # Check for chain chance (gloves upgrade) - use pre-fetched data
        user_upgrades = user_data["basket_upgrades"]
        gloves_tier = user_upgrades["gloves"]
        chain_triggered = False
        if gloves_tier > 0:
            chain_chance = GLOVES_UPGRADES[gloves_tier - 1]["chain_chance"]
            
            # Apply Chain Reaction event (hourly) - use pre-fetched events
            hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
            if hourly_event:
                event_id = hourly_event.get("effects", {}).get("event_id", "")
                if event_id == "chain_reaction":
                    chain_chance *= 2  # Double the chain chance
            
            chain_triggered = random.random() < chain_chance
            if chain_triggered:
                # Reset cooldown by setting last_gather_time to 0 (allows immediate next gather)
                update_user_last_gather_time(user_id, 0)
        
        # Send the main gather embed first
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
        
        # Check and update planter achievement level (after main response)
        total_items = get_user_total_items(user_id)
        new_planter_level = get_planter_level_from_total_items(total_items)
        current_planter_achievement_level = get_user_achievement_level(user_id, "planter")
        if new_planter_level > current_planter_achievement_level:
            set_user_achievement_level(user_id, "planter", new_planter_level)
            # Send achievement notification (ephemeral, only visible to user)
            await send_achievement_notification(interaction, "planter", new_planter_level)
        
        # Check and update achievement levels based on gather_command_count (after main response)
        gather_command_count = get_user_gather_command_count(user_id)
        new_gatherer_level = get_achievement_level_for_stat("gatherer", gather_command_count)
        current_gatherer_level = get_user_achievement_level(user_id, "gatherer")
        if new_gatherer_level > current_gatherer_level:
            set_user_achievement_level(user_id, "gatherer", new_gatherer_level)
            # Send achievement notification (ephemeral, only visible to user)
            await send_achievement_notification(interaction, "gatherer", new_gatherer_level)
        
        # Send separate chain message if triggered
        if chain_triggered:
            await safe_interaction_response(interaction, interaction.followup.send, f"🔗🔗 **CHAIN!** Your cooldown has been reset! Gather again! 🔗🔗")
    except Exception as e:
        print(f"Error in gather command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


#/water command - daily watering system
@bot.tree.command(name="water", description="Water your plants daily for bonus rewards! (Resets at midnight)")
async def water(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        current_time = time.time()
        last_water_time = get_user_last_water_time(user_id)
        
        # Convert to EST (UTC-5)
        EST_OFFSET = datetime.timedelta(hours=-5)
        now_utc = datetime.datetime.utcnow()
        now_est = now_utc + EST_OFFSET
        
        # Get next midnight in EST
        next_midnight_est = (now_est + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time_until_midnight = (next_midnight_est - now_est).total_seconds()
        
        # Check if user has already watered today
        if last_water_time > 0:
            # Check if last water was today (same calendar day in EST)
            last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
            last_water_est = last_water_utc + EST_OFFSET
            last_water_date = last_water_est.date()
            current_date = now_est.date()
            
            if last_water_date == current_date:
                # Already watered today, show time until midnight
                time_left = int(time_until_midnight)
                
                # Format time remaining based on duration
                if time_left < 60:
                    # Less than 1 minute - show seconds only
                    time_msg = f"{time_left} second{'s' if time_left != 1 else ''}"
                elif time_left < 3600:
                    # Less than 1 hour - show minutes and seconds
                    minutes_left = time_left // 60
                    seconds_left = time_left % 60
                    time_msg = f"{minutes_left} minute{'s' if minutes_left != 1 else ''} and {seconds_left} second{'s' if seconds_left != 1 else ''}"
                else:
                    # 1 hour or more - show hours, minutes, and seconds
                    hours_left = time_left // 3600
                    minutes_left = (time_left % 3600) // 60
                    seconds_left = time_left % 60
                    time_msg = f"{hours_left} hour{'s' if hours_left != 1 else ''}, {minutes_left} minute{'s' if minutes_left != 1 else ''}, and {seconds_left} second{'s' if seconds_left != 1 else ''}"
                
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"💧 {interaction.user.mention}, you need to wait **{time_msg}** before watering your plants again!", ephemeral=False)
                return
        
        # Calculate consecutive days
        consecutive_days = get_user_consecutive_water_days(user_id)
        
        # Check if streak should be reset (last water was not yesterday)
        if last_water_time > 0:
            last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
            last_water_est = last_water_utc + EST_OFFSET
            last_water_date = last_water_est.date()
            yesterday_date = (now_est - datetime.timedelta(days=1)).date()
            
            # If last water was not yesterday, reset streak
            if last_water_date != yesterday_date:
                consecutive_days = 0  # Reset streak
        else:
            consecutive_days = 0  # First time watering
        
        # Increment consecutive days
        consecutive_days += 1
        set_user_consecutive_water_days(user_id, consecutive_days)
        
        # Update last water time and increment water count
        update_user_last_water_time(user_id, current_time)
        increment_user_water_count(user_id)
        
        # Calculate money reward: $7,500 per day (day 1 = $7.5k, day 2 = $15k, etc.)
        money_reward = consecutive_days * 7500.0
        money_reward = normalize_money(money_reward)
        
        # Update user balance with reward
        current_balance = get_user_balance(user_id)
        new_balance = normalize_money(current_balance + money_reward)
        update_user_balance(user_id, new_balance)
        
        # Get water count and multiplier
        water_count = get_user_water_count(user_id)
        water_multiplier = get_water_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        
        # Award 10 Tree Rings on 5th consecutive day
        tree_rings_awarded = 0
        if consecutive_days == 5:
            increment_tree_rings(user_id, 10)
            tree_rings_awarded = 10
        
        # Build the message
        message = f"{interaction.user.mention}, you've been rewarded with **${money_reward:,.2f}**. Your streak is **{consecutive_days}**! (**{daily_bonus_multiplier:.2f}x**)"
        
        # Add Tree Rings message if it's the 5th day
        if tree_rings_awarded > 0:
            message += f" You've been awarded **{tree_rings_awarded} Tree Rings**!"
        
        await safe_interaction_response(interaction, interaction.followup.send, message, ephemeral=False)
        
        # Check and update water_streak achievement level (after main response)
        new_water_streak_level = get_achievement_level_for_stat("water_streak", consecutive_days)
        current_water_streak_level = get_user_achievement_level(user_id, "water_streak")
        if new_water_streak_level > current_water_streak_level:
            set_user_achievement_level(user_id, "water_streak", new_water_streak_level)
            await send_achievement_notification(interaction, "water_streak", new_water_streak_level)
        
        # Check for hidden achievement: Leap Year (water streak of 366)
        if consecutive_days == 366 and unlock_hidden_achievement(user_id, "leap_year"):
            await send_hidden_achievement_notification(interaction, "leap_year")
    except Exception as e:
        print(f"Error in water command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


async def perform_harvest_for_user(user_id: int, allow_chain: bool = True) -> dict:
    """Perform a full harvest for a user (balance, items, stats, tree rings). Does NOT set harvest cooldown.
    Uses orchard (harvest) upgrades: car (extra items), fertilizer, basket, etc., so gardener auto-harvests
    benefit from the user's /orchard upgrades. Set allow_chain=False for gardener-triggered harvests (no chain).
    Returns dict with total_value, gathered_items, current_balance, chain_chance, total_base_value,
    bloom_multiplier, water_multiplier, total_value_before_daily, tree_rings_to_award."""
    active_events = get_active_events_cached()
    user_upgrades = get_user_basket_upgrades(user_id)
    basket_tier = user_upgrades["basket"]
    soil_tier = user_upgrades["soil"]
    harvest_upgrades = get_user_harvest_upgrades(user_id)
    car_tier = harvest_upgrades["car"]
    chain_tier = harvest_upgrades["chain"]
    fertilizer_tier = harvest_upgrades["fertilizer"]
    base_items = 10
    extra_items = HARVEST_CAR_UPGRADES[car_tier - 1]["extra_items"] if car_tier > 0 else 0
    total_items_to_harvest = base_items + extra_items
    basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"] if basket_tier > 0 else 1.0
    base_gmo_chance = 0.05
    soil_gmo_boost = SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0
    gmo_chance = base_gmo_chance + soil_gmo_boost
    fertilizer_multiplier = 1.0
    if fertilizer_tier > 0:
        fertilizer_multiplier = 1.0 + HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"]
    chain_chance = HARVEST_CHAIN_UPGRADES[chain_tier - 1]["chain_chance"] if chain_tier > 0 else 0.0
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    if allow_chain and hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "chain_reaction":
        chain_chance *= 2
    if not allow_chain:
        chain_chance = 0.0  # Gardener auto-harvests/auto-gathers never get chain
    if hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "radiation_leak":
        gmo_chance += 0.25
    if daily_event and daily_event.get("effects", {}).get("event_id", "") == "gmo_surge":
        gmo_chance += 0.25
    gmo_chance = min(gmo_chance, 1.0)
    if hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "basket_boost":
        basket_multiplier *= 1.5
    bloom_multiplier = get_bloom_multiplier(user_id)
    water_multiplier = get_water_multiplier(user_id)
    plants_before_harvest = get_user_total_items(user_id)
    gathered_items = []
    total_value = 0.0
    total_base_value = 0.0
    total_value_before_daily = 0.0
    current_balance = get_user_balance(user_id)
    for i in range(total_items_to_harvest):
        item = random.choice(GATHERABLE_ITEMS)
        name = item["name"]
        if item["category"] == "Fruit":
            ripeness_list = LEVEL_OF_RIPENESS_FRUITS
        elif item["category"] == "Vegetable":
            ripeness_list = LEVEL_OF_RIPENESS_VEGETABLES
        elif item["category"] == "Flower":
            ripeness_list = LEVEL_OF_RIPENESS_FLOWERS
        else:
            ripeness_list = []
        base_value = item["base_value"]
        if hourly_event:
            event_id = hourly_event.get("effects", {}).get("event_id", "")
            if event_id == "may_flowers" and item["category"] == "Flower":
                base_value *= 3
            elif event_id == "fruit_festival" and item["category"] == "Fruit":
                base_value *= 2
            elif event_id == "vegetable_boom" and item["category"] == "Vegetable":
                base_value *= 2
        if ripeness_list:
            weights = [r["chance"] for r in ripeness_list]
            hourly_event_id = hourly_event.get("effects", {}).get("event_id", "") if hourly_event else ""
            daily_event_id = daily_event.get("effects", {}).get("event_id", "") if daily_event else ""
            if hourly_event_id == "perfect_ripeness":
                ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
                ripeness_multiplier = ripeness["multiplier"] * 1.5
            elif daily_event_id == "ripeness_rush":
                weights = [r["chance"] * 2 if "Perfect" in r["name"] else r["chance"] for r in ripeness_list]
                ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
                ripeness_multiplier = ripeness["multiplier"]
            else:
                ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
                ripeness_multiplier = ripeness["multiplier"]
            final_value = base_value * ripeness_multiplier
        else:
            final_value = base_value
            ripeness = {"name": "Normal"}
        is_gmo = random.choices([True, False], weights=[gmo_chance, 1 - gmo_chance], k=1)[0]
        if is_gmo:
            final_value *= 2
        final_value *= basket_multiplier
        final_value *= fertilizer_multiplier
        value_multiplier = 1.0
        if hourly_event:
            event_id = hourly_event.get("effects", {}).get("event_id", "")
            if event_id == "bumper_crop":
                value_multiplier *= 2.0
            elif event_id == "lucky_strike":
                value_multiplier *= 1.25
        if daily_event:
            event_id = daily_event.get("effects", {}).get("event_id", "")
            if event_id == "double_money":
                value_multiplier *= 2.0
            elif event_id == "harvest_festival":
                value_multiplier *= 1.5
        final_value *= value_multiplier
        base_value_before_bloom = final_value
        final_value *= bloom_multiplier
        final_value *= water_multiplier
        final_value *= get_rank_perma_buff_multiplier(user_id)
        final_value *= get_achievement_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        value_before_daily = final_value
        final_value *= daily_bonus_multiplier
        current_balance += final_value
        total_value += final_value
        total_base_value += base_value_before_bloom
        total_value_before_daily += value_before_daily
        add_user_item(user_id, name)
        add_ripeness_stat(user_id, ripeness["name"])
        # Increment total_items for userstats display (harvests should count towards total plants)
        # But do NOT increment gather_stats categories/items (those are for gatherer achievements only)
        increment_total_items_only(user_id)
        gathered_items.append({"name": name, "value": final_value, "ripeness": ripeness["name"], "is_gmo": is_gmo})
    current_total = get_user_total_items(user_id)
    tree_rings_to_award = 0
    for milestone in range(200, current_total + 1, 200):
        if plants_before_harvest < milestone <= current_total:
            tree_rings_to_award += 1
    if tree_rings_to_award > 0:
        increment_tree_rings(user_id, tree_rings_to_award)
    
    # Note: Gatherer achievement check removed - harvests should not contribute to gatherer achievements
    # Achievement checking is now done in the /harvest command handler to only count actual /harvest commands
    
    update_user_balance(user_id, current_balance)
    return {
        "total_value": total_value,
        "gathered_items": gathered_items,
        "current_balance": current_balance,
        "chain_chance": chain_chance,
        "total_base_value": total_base_value,
        "bloom_multiplier": bloom_multiplier,
        "water_multiplier": water_multiplier,
        "total_value_before_daily": total_value_before_daily,
        "tree_rings_to_award": tree_rings_to_award,
        "achievement_unlocked": None
    }


#/harvest command, basically /castnet
@bot.tree.command(name="harvest", description="Harvest a bunch of plants at once!")
async def harvest(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        user_id = interaction.user.id
        can_user_harvest, time_left, is_roulette_cooldown = can_harvest(user_id)
        if not can_user_harvest:
            if is_roulette_cooldown:
                # Russian Roulette elimination cooldown
                minutes_left = time_left // 60
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot /harvest for {minutes_left} minute(s)", ephemeral=True)
            else:
                # Normal harvest cooldown
                minutes_left = time_left // 60
                seconds_left = time_left % 60   
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"You must wait {minutes_left} minutes and {seconds_left} seconds before harvesting again, {interaction.user.name}.", ephemeral=True)
            return
        set_harvest_cooldown(user_id)
        result = await perform_harvest_for_user(user_id)
        gathered_items = result["gathered_items"]
        total_value = result["total_value"]
        current_balance = result["current_balance"]
        chain_chance = result["chain_chance"]
        total_base_value = result["total_base_value"]
        bloom_multiplier = result["bloom_multiplier"]
        water_multiplier = result["water_multiplier"]
        total_value_before_daily = result["total_value_before_daily"]
        tree_rings_to_award = result["tree_rings_to_award"]

        if tree_rings_to_award > 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🌳 {interaction.user.mention} You've been awarded **{tree_rings_to_award} Tree Ring{'s' if tree_rings_to_award > 1 else ''}**!",
                ephemeral=True)

        #assign role in case they hit a new gatherer level in a harvest
        old_role = None
        new_role = None
        try:
            old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
        except Exception as e:
            print(f"Error assigning gatherer role to user {user_id}: {e}")


        # Send rank-up notification if player advanced
        if new_role:
            # Special message for Planter I advancement (when old_role is None)
            if new_role == "PLANTER I" and old_role is None:
                # Assign PINE I bloom rank role
                try:
                    await assign_bloom_rank_role(interaction.user, interaction.guild)
                except Exception as e:
                    print(f"Error assigning bloom rank role to user {user_id}: {e}")
                
                # Get the bloom rank to display
                bloom_rank = get_bloom_rank(user_id)
                rankup_embed = discord.Embed(
                    title="🌾 Rank Up!",
                    description=f"{interaction.user.mention} advanced to **PLANTER I** and is ranked **PINE I**!",
                    color=discord.Color.gold(),
                )
            else:
                rankup_embed = discord.Embed(
                    title="🌾 Rank Up!",
                    description=f"{interaction.user.mention} advanced from **{old_role or 'PLANTER I'}** to **{new_role}**!",
                    color=discord.Color.gold(),
                )
            await safe_interaction_response(interaction, interaction.followup.send, embed=rankup_embed)

        # Check for chain chance (season upgrade)
        chain_triggered = False
        if chain_chance > 0:
            chain_triggered = random.random() < chain_chance
            if chain_triggered:
                # Reset cooldown by setting last_harvest_time to 0 (allows immediate next harvest)
                update_user_last_harvest_time(user_id, 0)
        
        #create harvest embed
        embed = discord.Embed(
            title = "You Harvested!",
            color = discord.Color.green()
        )

        #show gathered items, just using 20 for now
        items_text = ""
        for item in gathered_items[:20]:
            gmo_text = " GMO! ✨" if item["is_gmo"] else ""
            items_text += f"• **{item['name']}** - ${item['value']:.2f} ({item['ripeness']}){gmo_text}\n"

        embed.add_field(name="📦 Items Gathered", value=items_text or "No items", inline=False)
        embed.add_field(name="💰 Total Value", value=f"**${total_value:.2f}**", inline=True)
        embed.add_field(name="💵 New Balance", value=f"**${current_balance:.2f}**", inline=True)
        
        # Show bloom multiplier if applicable (only after first bloom)
        bloom_count = get_user_bloom_count(user_id)
        # Calculate extra money from bloom correctly: base * (bloom_multiplier - 1) * water_multiplier
        # This accounts for water multiplier being applied after bloom
        extra_money_from_bloom = total_base_value * (bloom_multiplier - 1.0) * water_multiplier
        if bloom_count > 0 and extra_money_from_bloom > 0:
            tree_rings = get_user_tree_rings(user_id)
            multiplier_percent = (bloom_multiplier - 1.0) * 100
            embed.add_field(
                name="🌳 Tree Ring Boost", 
                value=f"+{multiplier_percent:.1f}% - **+${extra_money_from_bloom:.2f}**", 
                inline=False
            )
        
        # Show rank perma buff if applicable (only if not PINE I)
        bloom_rank = get_bloom_rank(user_id)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
        achievement_multiplier = get_achievement_multiplier(user_id)
        # Calculate extra money from rank: value after water * (rank_multiplier - 1)
        value_after_water = total_base_value * bloom_multiplier * water_multiplier
        extra_money_from_rank = value_after_water * (rank_perma_buff_multiplier - 1.0)
        # Calculate extra money from achievement: value after rank * (achievement_multiplier - 1)
        value_after_rank = value_after_water * rank_perma_buff_multiplier
        extra_money_from_achievement = value_after_rank * (achievement_multiplier - 1.0)
        if bloom_rank != "PINE I" and extra_money_from_rank > 0:
            rank_perma_buff_percent = (rank_perma_buff_multiplier - 1.0) * 100
            embed.add_field(
                name="⭐ Rank Boost",
                value=f"+{rank_perma_buff_percent:.1f}% - **+${extra_money_from_rank:.2f}**",
                inline=False
            )
        
        # Show achievement boost if applicable
        if extra_money_from_achievement > 0:
            achievement_percent = (achievement_multiplier - 1.0) * 100
            embed.add_field(
                name="🏆 Achievement Boost",
                value=f"+{achievement_percent:.1f}% - **+${extra_money_from_achievement:.2f}**",
                inline=False
            )
        
        # Show daily bonus if applicable (1% per consecutive day)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        extra_money_from_daily = total_value_before_daily * (daily_bonus_multiplier - 1.0)
        if extra_money_from_daily > 0:
            daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
            embed.add_field(
                name="💧 Water Streak Boost",
                value=f"+{daily_bonus_percent:.1f}% - **+${extra_money_from_daily:.2f}**",
                inline=False
            )
        
        embed.add_field(name="~", value=f"{interaction.user.name} in {MONTHS[random.randint(0, 11)]}", inline=False)

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
        
        # Check and update planter achievement level (after main response)
        total_items = get_user_total_items(user_id)
        new_planter_level = get_planter_level_from_total_items(total_items)
        current_planter_achievement_level = get_user_achievement_level(user_id, "planter")
        if new_planter_level > current_planter_achievement_level:
            set_user_achievement_level(user_id, "planter", new_planter_level)
            # Send achievement notification (ephemeral, only visible to user)
            await send_achievement_notification(interaction, "planter", new_planter_level)
        
        # Increment harvest command count and check for harvesting achievements (after main response)
        # IMPORTANT: This counts the number of times /harvest command is used, NOT the number of plants gathered
        increment_user_harvest_command_count(user_id)
        harvest_command_count = get_user_harvest_command_count(user_id)
        # Use harvest_command_count (number of /harvest commands), NOT total_items (number of plants)
        new_harvesting_level = get_achievement_level_for_stat("harvesting", harvest_command_count)
        current_harvesting_level = get_user_achievement_level(user_id, "harvesting")
        if new_harvesting_level > current_harvesting_level:
            set_user_achievement_level(user_id, "harvesting", new_harvesting_level)
            # Send achievement notification (ephemeral, only visible to user)
            await send_achievement_notification(interaction, "harvesting", new_harvesting_level)
        
        # Send achievement notification if unlocked (for backward compatibility, though this should be None now)
        if result.get('achievement_unlocked'):
            achievement_name, achievement_level = result['achievement_unlocked']
            await send_achievement_notification(interaction, achievement_name, achievement_level)
        
        # Send separate chain message if triggered
        if chain_triggered:
            await safe_interaction_response(interaction, interaction.followup.send, f"🔗🔗 **CHAIN!** Your harvest cooldown has been reset! Harvest again! 🔗🔗")
    except Exception as e:
        print(f"Error in harvest command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="achievements", description="View your achievements and progress!")
async def achievements(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        user_id = interaction.user.id
        total_items = get_user_total_items(user_id)
        hidden_achievements_count = get_user_hidden_achievements_count(user_id)
        
        # Create embed
        embed = discord.Embed(
            title=f"🏆 {interaction.user.name}'s Achievements",
            color=discord.Color.gold()
        )
        
        # Calculate total boost
        total_boost_multiplier = get_achievement_multiplier(user_id)
        total_boost_percent = (total_boost_multiplier - 1.0) * 100
        
        # Process each achievement category
        for achievement_name, achievement_data in ACHIEVEMENTS.items():
            current_level = get_user_achievement_level(user_id, achievement_name)
            levels = achievement_data["levels"]
            
            # Get the current level data
            if current_level < len(levels):
                current_level_data = levels[current_level]
            else:
                current_level_data = levels[-1]  # Use max level if somehow exceeded
            
            # Build progress bar (10 squares for levels 0-10)
            # current_level represents the level achieved (0-10)
            # Level 0 = all grey (0 green), Level 1 = 1 green + 9 grey, Level 10 = 10 green (all green)
            progress_bar = ""
            # Show current_level green squares (0 for level 0, 1 for level 1, ..., 10 for level 10)
            num_green_squares = current_level
            for i in range(10):
                if i < num_green_squares:
                    progress_bar += "🟩"  # Green square for completed
                else:
                    progress_bar += "⬜"  # Grey square for not completed
            
            # Get achievement name and description
            achievement_display_name = current_level_data["name"]
            achievement_description = current_level_data["description"]
            boost_percent = current_level_data["boost"] * 100
            
            # Build field value with bold description
            field_value = f"**{achievement_description}**\n{progress_bar}\n"
            if current_level > 0:
                field_value += f"💰 Boost: **{boost_percent:.1f}%**"
            else:
                field_value += "💰 Boost: **0%**"
            
            embed.add_field(
                name=achievement_display_name,
                value=field_value,
                inline=False
            )
        
        # Add total boost at the bottom
        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━",
            value=f"💰 **Total Boost: {total_boost_percent:.1f}%**",
            inline=False
        )
        
        # Add hidden achievements count at the end
        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━",
            value=f"**Hidden Achievements:** {hidden_achievements_count}/{TOTAL_HIDDEN_ACHIEVEMENTS}",
            inline=False
        )
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in achievements command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="bloom", description="Bloom to reset your progress and advance your Bloom Rank! (Requires 3000 plants)")
async def bloom(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        total_items = get_user_total_items(user_id)
        
        # Check if user has at least 3000 plants
        if total_items < 3000:
            plants_needed = 3000 - total_items
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You need **{plants_needed}** more plants to bloom, {interaction.user.name}!",
                ephemeral=True)
            return
        
        # Get current stats before blooming
        old_rank = get_bloom_rank(user_id)
        tree_rings = get_user_tree_rings(user_id)
        
        # Perform bloom
        perform_bloom(user_id)
        
        # Get new stats
        new_rank = get_bloom_rank(user_id)
        
        # Assign Bloom Rank role
        old_bloom_role = None
        new_bloom_role = None
        try:
            old_bloom_role, new_bloom_role = await assign_bloom_rank_role(interaction.user, interaction.guild)
        except Exception as e:
            print(f"Error assigning bloom rank role to user {user_id}: {e}")
        
        # Create confirmation embed
        embed = discord.Embed(
            title="🌳 You Bloomed!",
            description=f"{interaction.user.mention} has advanced to **{new_rank}**!",
            color=discord.Color.gold()
        )
        
        embed.add_field(name="🌲 Bloom Rank", value=f"**{old_rank}** → **{new_rank}**", inline=False)
        embed.add_field(name="🌳 Tree Rings", value=f"**{tree_rings}** Tree Rings", inline=False)
        
        if tree_rings > 0:
            multiplier = get_bloom_multiplier(user_id)
            multiplier_percent = (multiplier - 1.0) * 100
            embed.add_field(
                name="💰 Money Boost", 
                value=f"+{multiplier_percent:.1f}% on all earnings", 
                inline=False
            )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in bloom command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# # balance command
# commented since we have userstats
# @bot.tree.command(name="balance", description="Check your current balance")
# #use defer for thinking message
# async def balance(interaction: discord.Interaction):
#     await interaction.response.defer(ephemeral=True)
#     user_id = interaction.user.id
#     user_balance = get_user_balance(user_id)
#     await interaction.followup.send(f"{interaction.user.name}, you have **${user_balance:.2f}**.")


# userstats command
@bot.tree.command(name="userstats", description="View your statistics!")
async def userstats(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        user_balance = get_user_balance(user_id)
        total_items = get_user_total_items(user_id)
        
        # Calculate items needed for next rankup
        # PLANTER I: 0-49 (need 50 for PLANTER II)
        # PLANTER II: 50-149 (need 150 for PLANTER III)
        # PLANTER III: 150-299 (need 300 for PLANTER IV)
        # PLANTER IV: 300-499 (need 500 for PLANTER V)
        # PLANTER V: 500-999 (need 1000 for PLANTER VI)
        # PLANTER VI: 1000-1999 (need 2000 for PLANTER VII)
        # PLANTER VII: 2000-3999 (need 4000 for PLANTER VIII)
        # PLANTER VIII: 4000-9999 (need 10000 for PLANTER IX)
        # PLANTER IX: 10000-99999 (need 100000 for PLANTER X)
        # PLANTER X: 100000+ (max rank)
        items_needed = None
        next_rank = None
        
        if total_items < 50:
            items_needed = 50 - total_items
            next_rank = "PLANTER II"
        elif total_items < 150:
            items_needed = 150 - total_items
            next_rank = "PLANTER III"
        elif total_items < 300:
            items_needed = 300 - total_items
            next_rank = "PLANTER IV"
        elif total_items < 500:
            items_needed = 500 - total_items
            next_rank = "PLANTER V"
        elif total_items < 1000:
            items_needed = 1000 - total_items
            next_rank = "PLANTER VI"
        elif total_items < 2000:
            items_needed = 2000 - total_items
            next_rank = "PLANTER VII"
        elif total_items < 4000:
            items_needed = 4000 - total_items
            next_rank = "PLANTER VIII"
        elif total_items < 10000:
            items_needed = 10000 - total_items
            next_rank = "PLANTER IX"
        elif total_items < 100000:
            items_needed = 100000 - total_items
            next_rank = "PLANTER X"
        else:
            # Max rank achieved
            items_needed = 0
            next_rank = "MAX RANK"
        
        embed = discord.Embed(
            title=f"📊 {interaction.user.name}'s Stats",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="💰 Balance", value=f"**${user_balance:.2f}**", inline=True)
        embed.add_field(name="🌱 Plants Gathered", value=f"**{total_items}** plants", inline=True)
        
        # Add Bloom Rank and Tree Rings
        bloom_rank = get_bloom_rank(user_id)
        tree_rings = get_user_tree_rings(user_id)
        bloom_multiplier = get_bloom_multiplier(user_id)
        embed.add_field(name="🌲 Bloom Rank", value=f"**{bloom_rank}**", inline=True)
        embed.add_field(name="🌳 Tree Rings", value=f"**{tree_rings}** ({bloom_multiplier:.2f}x)", inline=True)
        
        # Add Rank Perma Buff (only if not PINE I)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
        if bloom_rank != "PINE I":
            rank_perma_buff_percent = (rank_perma_buff_multiplier - 1.0) * 100
            embed.add_field(name="⭐ Rank Boost", value=f"**+{rank_perma_buff_percent:.1f}%**", inline=True)
        
        # Add Water Streak
        water_streak = get_user_consecutive_water_days(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        day_text = "day" if water_streak == 1 else "days"
        embed.add_field(name="💧 Water Streak", value=f"**{water_streak}** {day_text} ({daily_bonus_multiplier:.2f}x)", inline=True)
        
        if items_needed == 0:
            embed.add_field(name="🏆 Rank Status", value=f"**{next_rank}** - You've reached the maximum rank!", inline=False)
        else:
            embed.add_field(name="📈 Next Rank", value=f"**{items_needed}** more plants until **{next_rank}**", inline=False)
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in userstats command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# almanac command
@bot.tree.command(name="almanac", description="View your collection of your gathered items!")
async def almanac(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        user_items = get_user_items(user_id)
        
        if not user_items:
            embed = discord.Embed(
                title=f"{interaction.user.name}'s Almanac",
                description="Your collection is empty! Start gathering items with `/gather` or `/harvest` to fill it up!",
                color=discord.Color.orange()
            )
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
            return
        
        # Build the almanac text
        almanac_text = ""
        for item_name, count in user_items.items():
            # Get description or use a default one
            description = ITEM_DESCRIPTIONS.get(item_name, "A mysterious item from nature!")
            # Format: [ ITEM NAME ] xCount : "Description"
            almanac_text += f"[ {item_name.upper()} ] x{count} : \"{description}\"\n"
        
        embed = discord.Embed(
            title=f"{interaction.user.name}'s Almanac",
            description=almanac_text,
            color=discord.Color.green()
        )
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in almanac command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Basket Upgrade View with buttons
class BasketUpgradeView(discord.ui.View):
    def __init__(self, user_id: int, guild: discord.Guild, timeout=300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.guild = guild
    
    def create_embed(self) -> discord.Embed:
        """Create the basket upgrade embed."""
        upgrades = get_user_basket_upgrades(self.user_id)
        balance = get_user_balance(self.user_id)
        
        embed = discord.Embed(
            title="🛒 Gear Upgrade Shop",
            description=f"💰 Your Balance: **${balance:,.2f}**\n\nChoose an upgrade path to purchase!",
            color=discord.Color.gold()
        )
        
        # Path 1: Baskets (Money Multiplier)
        basket_tier = upgrades["basket"]
        current_basket = "No Basket" if basket_tier == 0 else BASKET_UPGRADES[basket_tier - 1]["name"]
        current_multiplier = 1.0 if basket_tier == 0 else BASKET_UPGRADES[basket_tier - 1]["multiplier"]
        if basket_tier < 10:
            next_basket = BASKET_UPGRADES[basket_tier]["name"]
            next_multiplier = BASKET_UPGRADES[basket_tier]["multiplier"]
            next_cost = UPGRADE_PRICES[basket_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            basket_text = f"**Upgrade {basket_tier + 1}/10**\n**Current:** {current_basket} ({current_multiplier}x money)\n**Next:** {next_basket} ({next_multiplier}x money)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            basket_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_basket} ({current_multiplier}x money)"
        
        embed.add_field(
            name="🧺 PATH 1: BASKETS",
            value=basket_text,
            inline=False
        )
        
        # Path 2: Shoes (Cooldown Reduction)
        shoes_tier = upgrades["shoes"]
        current_shoes = "Bare Feet" if shoes_tier == 0 else SHOES_UPGRADES[shoes_tier - 1]["name"]
        current_reduction = 0 if shoes_tier == 0 else SHOES_UPGRADES[shoes_tier - 1]["reduction"]
        if shoes_tier < 10:
            next_shoes = SHOES_UPGRADES[shoes_tier]["name"]
            next_reduction = SHOES_UPGRADES[shoes_tier]["reduction"]
            next_cost = UPGRADE_PRICES[shoes_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            shoes_text = f"**Upgrade {shoes_tier + 1}/10**\n**Current:** {current_shoes} (-{current_reduction}s cooldown)\n**Next:** {next_shoes} (-{next_reduction}s cooldown)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            shoes_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_shoes} (-{current_reduction}s cooldown)"
        
        embed.add_field(
            name="👟 PATH 2: RUNNING SHOES",
            value=shoes_text,
            inline=False
        )
        
        # Path 3: Gloves (Chain Chance)
        gloves_tier = upgrades["gloves"]
        current_gloves = "Bare Hands" if gloves_tier == 0 else GLOVES_UPGRADES[gloves_tier - 1]["name"]
        current_chain = 0 if gloves_tier == 0 else GLOVES_UPGRADES[gloves_tier - 1]["chain_chance"] * 100
        if gloves_tier < 10:
            next_gloves = GLOVES_UPGRADES[gloves_tier]["name"]
            next_chain = GLOVES_UPGRADES[gloves_tier]["chain_chance"] * 100
            next_cost = UPGRADE_PRICES[gloves_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            gloves_text = f"**Upgrade {gloves_tier + 1}/10**\n**Current:** {current_gloves} ({current_chain}% chain chance)\n**Next:** {next_gloves} ({next_chain}% chain chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            gloves_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_gloves} ({current_chain}% chain chance)"
        
        embed.add_field(
            name="🧤 PATH 3: GLOVES",
            value=gloves_text,
            inline=False
        )
        
        # Path 4: Soil (GMO Chance)
        soil_tier = upgrades["soil"]
        current_soil = "Regular Soil" if soil_tier == 0 else SOIL_UPGRADES[soil_tier - 1]["name"]
        current_gmo = 0 if soil_tier == 0 else SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] * 100
        if soil_tier < 10:
            next_soil = SOIL_UPGRADES[soil_tier]["name"]
            next_gmo = SOIL_UPGRADES[soil_tier]["gmo_boost"] * 100
            next_cost = UPGRADE_PRICES[soil_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            soil_text = f"**Upgrade {soil_tier + 1}/10**\n**Current:** {current_soil} (+{current_gmo}% GMO chance)\n**Next:** {next_soil} (+{next_gmo}% GMO chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            soil_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_soil} (+{current_gmo}% GMO chance)"
        
        embed.add_field(
            name="🌱 PATH 4: SOIL",
            value=soil_text,
            inline=False
        )
        
        embed.set_footer(text="Click a button below to purchase an upgrade!")
        
        return embed
    
    @discord.ui.button(label="🧺 Buy Basket", style=discord.ButtonStyle.primary, row=0)
    async def buy_basket(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "basket", BASKET_UPGRADES, "Basket")
        except Exception as e:
            print(f"Error in buy_basket: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="👟 Buy Shoes", style=discord.ButtonStyle.primary, row=0)
    async def buy_shoes(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "shoes", SHOES_UPGRADES, "Shoes")
        except Exception as e:
            print(f"Error in buy_shoes: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🧤 Buy Gloves", style=discord.ButtonStyle.primary, row=1)
    async def buy_gloves(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "gloves", GLOVES_UPGRADES, "Gloves")
        except Exception as e:
            print(f"Error in buy_gloves: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🌱 Buy Soil", style=discord.ButtonStyle.primary, row=1)
    async def buy_soil(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "soil", SOIL_UPGRADES, "Soil")
        except Exception as e:
            print(f"Error in buy_soil: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, row=2)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your gear shop!", ephemeral=True)
                return
            
            embed = self.create_embed()
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in refresh (gear): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    async def handle_purchase(self, interaction: discord.Interaction, upgrade_type: str, upgrade_list: list, upgrade_name: str):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ This is not your gear shop!", ephemeral=True)
                return
            
            upgrades = get_user_basket_upgrades(self.user_id)
            current_tier = upgrades[upgrade_type]
            
            if current_tier >= 10:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ You already have the maximum {upgrade_name} upgrade!", ephemeral=True)
                return
            
            cost = UPGRADE_PRICES[current_tier]
            balance = get_user_balance(self.user_id)
            
            if balance < cost:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"❌ You don't have enough money! You need **${cost:,.2f}** but only have **${balance:,.2f}**.", 
                    ephemeral=True)
                return
            
            # Deduct money and upgrade
            new_balance = balance - cost
            update_user_balance(self.user_id, new_balance)
            set_user_basket_upgrade(self.user_id, upgrade_type, current_tier + 1)
            
            next_upgrade = upgrade_list[current_tier]
            
            # Send quick confirmation and update the main embed
            await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.", ephemeral=True)
            
            embed = self.create_embed()
            try:
                await interaction.message.edit(embed=embed, view=self)
            except:
                pass  # Message might have been deleted
        except Exception as e:
            print(f"Error in handle_purchase (gear): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


# Gear command
@bot.tree.command(name="gear", description="Upgrade your gathering equipment!")
async def gear(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        view = BasketUpgradeView(user_id, interaction.guild)
        embed = view.create_embed()
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in gear command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Harvest Upgrade View with buttons
class HarvestUpgradeView(discord.ui.View):
    def __init__(self, user_id: int, guild: discord.Guild, timeout=300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.guild = guild
    
    def create_embed(self) -> discord.Embed:
        """Create the harvest upgrade embed."""
        upgrades = get_user_harvest_upgrades(self.user_id)
        balance = get_user_balance(self.user_id)
        
        embed = discord.Embed(
            title="🚜 Harvest Upgrade Shop",
            description=f"💰 Your Balance: **${balance:,.2f}**\n\nChoose an upgrade path to purchase!",
            color=discord.Color.green()
        )
        
        # Path 1: Car (Extra Items)
        car_tier = upgrades["car"]
        current_car = "Just Yourself" if car_tier == 0 else HARVEST_CAR_UPGRADES[car_tier - 1]["name"]
        current_extra = 0 if car_tier == 0 else HARVEST_CAR_UPGRADES[car_tier - 1]["extra_items"]
        if car_tier < 10:
            next_car = HARVEST_CAR_UPGRADES[car_tier]["name"]
            next_extra = HARVEST_CAR_UPGRADES[car_tier]["extra_items"]
            next_cost = HARVEST_CAR_PRICES[car_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            car_text = f"**Upgrade {car_tier + 1}/10**\n**Current:** {current_car} (+{current_extra} extra items)\n**Next:** {next_car} (+{next_extra} extra items)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            car_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_car} (+{current_extra} extra items)"
        
        embed.add_field(
            name="🚗 PATH 1: VEHICLE",
            value=car_text,
            inline=False
        )
        
        # Path 2: Chain Chance (Season)
        chain_tier = upgrades["chain"]
        current_season = "No Season" if chain_tier == 0 else HARVEST_CHAIN_UPGRADES[chain_tier - 1]["name"]
        current_chain = 0 if chain_tier == 0 else HARVEST_CHAIN_UPGRADES[chain_tier - 1]["chain_chance"] * 100
        if chain_tier < 10:
            next_season = HARVEST_CHAIN_UPGRADES[chain_tier]["name"]
            next_chain = HARVEST_CHAIN_UPGRADES[chain_tier]["chain_chance"] * 100
            next_cost = HARVEST_CHAIN_PRICES[chain_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            chain_text = f"**Upgrade {chain_tier + 1}/10**\n**Current:** {current_season} ({current_chain}% chain chance)\n**Next:** {next_season} ({next_chain}% chain chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            chain_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_season} ({current_chain}% chain chance)"
        
        embed.add_field(
            name="🌾 PATH 2: YIELD",
            value=chain_text,
            inline=False
        )
        
        # Path 3: Fertilizer (Money Multiplier)
        fertilizer_tier = upgrades["fertilizer"]
        current_fertilizer = "No Fertilizer" if fertilizer_tier == 0 else HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["name"]
        current_multiplier = 0 if fertilizer_tier == 0 else HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"] * 100
        if fertilizer_tier < 10:
            next_fertilizer = HARVEST_FERTILIZER_UPGRADES[fertilizer_tier]["name"]
            next_multiplier = HARVEST_FERTILIZER_UPGRADES[fertilizer_tier]["multiplier"] * 100
            next_cost = HARVEST_FERTILIZER_PRICES[fertilizer_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            fertilizer_text = f"**Upgrade {fertilizer_tier + 1}/10**\n**Current:** {current_fertilizer} (+{current_multiplier}% money)\n**Next:** {next_fertilizer} (+{next_multiplier}% money)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            fertilizer_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_fertilizer} (+{current_multiplier}% money)"
        
        embed.add_field(
            name="💩 PATH 3: FERTILIZER",
            value=fertilizer_text,
            inline=False
        )
        
        # Path 4: Cooldown Reduction (Workers)
        cooldown_tier = upgrades["cooldown"]
        current_workers = "No Workers" if cooldown_tier == 0 else HARVEST_COOLDOWN_UPGRADES[cooldown_tier - 1]["name"]
        current_reduction = 0 if cooldown_tier == 0 else HARVEST_COOLDOWN_UPGRADES[cooldown_tier - 1]["reduction"]
        if cooldown_tier < 10:
            next_workers = HARVEST_COOLDOWN_UPGRADES[cooldown_tier]["name"]
            next_reduction = HARVEST_COOLDOWN_UPGRADES[cooldown_tier]["reduction"]
            next_cost = HARVEST_COOLDOWN_PRICES[cooldown_tier]
            can_afford = "✅" if balance >= next_cost else "❌"
            # Format reduction time nicely
            if next_reduction < 60:
                reduction_text = f"-{next_reduction}s"
            elif next_reduction < 3600:
                minutes = next_reduction // 60
                seconds = next_reduction % 60
                if seconds > 0:
                    reduction_text = f"-{minutes}m {seconds}s"
                else:
                    reduction_text = f"-{minutes}m"
            else:
                hours = next_reduction // 3600
                minutes = (next_reduction % 3600) // 60
                if minutes > 0:
                    reduction_text = f"-{hours}h {minutes}m"
                else:
                    reduction_text = f"-{hours}h"
            
            if current_reduction < 60:
                current_reduction_text = f"-{current_reduction}s"
            elif current_reduction < 3600:
                minutes = current_reduction // 60
                seconds = current_reduction % 60
                if seconds > 0:
                    current_reduction_text = f"-{minutes}m {seconds}s"
                else:
                    current_reduction_text = f"-{minutes}m"
            else:
                hours = current_reduction // 3600
                minutes = (current_reduction % 3600) // 60
                if minutes > 0:
                    current_reduction_text = f"-{hours}h {minutes}m"
                else:
                    current_reduction_text = f"-{hours}h"
            
            cooldown_text = f"**Upgrade {cooldown_tier + 1}/10**\n**Current:** {current_workers} ({current_reduction_text} cooldown)\n**Next:** {next_workers} ({reduction_text} cooldown)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            if current_reduction < 60:
                current_reduction_text = f"-{current_reduction}s"
            elif current_reduction < 3600:
                minutes = current_reduction // 60
                seconds = current_reduction % 60
                if seconds > 0:
                    current_reduction_text = f"-{minutes}m {seconds}s"
                else:
                    current_reduction_text = f"-{minutes}m"
            else:
                hours = current_reduction // 3600
                minutes = (current_reduction % 3600) // 60
                if minutes > 0:
                    current_reduction_text = f"-{hours}h {minutes}m"
                else:
                    current_reduction_text = f"-{hours}h"
            cooldown_text = f"**Upgrade 10/10 (MAX)**\n**Current:** {current_workers} ({current_reduction_text} cooldown)"
        
        embed.add_field(
            name="⚡ PATH 4: COOLDOWN REDUCTION",
            value=cooldown_text,
            inline=False
        )
        
        embed.set_footer(text="Click a button below to purchase an upgrade!")
        
        return embed
    
    @discord.ui.button(label="🚗 Buy Vehicle", style=discord.ButtonStyle.primary, row=0)
    async def buy_car(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "car", HARVEST_CAR_UPGRADES, HARVEST_CAR_PRICES, "Vehicle")
        except Exception as e:
            print(f"Error in buy_car: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🌾 Buy Yield", style=discord.ButtonStyle.primary, row=0)
    async def buy_chain(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "chain", HARVEST_CHAIN_UPGRADES, HARVEST_CHAIN_PRICES, "Yield")
        except Exception as e:
            print(f"Error in buy_chain: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="💩 Buy Fertilizer", style=discord.ButtonStyle.primary, row=1)
    async def buy_fertilizer(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "fertilizer", HARVEST_FERTILIZER_UPGRADES, HARVEST_FERTILIZER_PRICES, "Fertilizer")
        except Exception as e:
            print(f"Error in buy_fertilizer: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="⚡ Buy Workers", style=discord.ButtonStyle.primary, row=1)
    async def buy_cooldown(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "cooldown", HARVEST_COOLDOWN_UPGRADES, HARVEST_COOLDOWN_PRICES, "Workers")
        except Exception as e:
            print(f"Error in buy_cooldown: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, row=2)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your harvest shop!", ephemeral=True)
                return
            
            embed = self.create_embed()
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in refresh (harvest): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    async def handle_purchase(self, interaction: discord.Interaction, upgrade_type: str, upgrade_list: list, price_list: list, upgrade_name: str):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ This is not your harvest shop!", ephemeral=True)
                return
            
            upgrades = get_user_harvest_upgrades(self.user_id)
            current_tier = upgrades[upgrade_type]
            
            if current_tier >= 10:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ You already have the maximum {upgrade_name} upgrade!", ephemeral=True)
                return
            
            cost = price_list[current_tier]
            balance = get_user_balance(self.user_id)
            
            if balance < cost:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"❌ You don't have enough money! You need **${cost:,.2f}** but only have **${balance:,.2f}**.", 
                    ephemeral=True)
                return
            
            # Deduct money and upgrade
            new_balance = balance - cost
            update_user_balance(self.user_id, new_balance)
            set_user_harvest_upgrade(self.user_id, upgrade_type, current_tier + 1)
            
            next_upgrade = upgrade_list[current_tier]
            
            # Check for Maxed Out achievement
            achievement_unlocked = check_maxed_out_achievement(self.user_id)
            
            # Send quick confirmation and update the main embed
            if achievement_unlocked:
                await safe_interaction_response(interaction, interaction.response.send_message, 
                    f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.\n\n"
                    f"🎉 **Hidden Achievement Unlocked: Maxed Out!** 🎉", ephemeral=True)
            else:
                await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.", ephemeral=True)
            
            embed = self.create_embed()
            try:
                await interaction.message.edit(embed=embed, view=self)
            except:
                pass  # Message might have been deleted
        except Exception as e:
            print(f"Error in handle_purchase (harvest): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


# Orchard command
@bot.tree.command(name="orchard", description="Upgrade your harvest equipment!")
async def orchard(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        view = HarvestUpgradeView(user_id, interaction.guild)
        embed = view.create_embed()
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in orchard command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Hire View with pagination
class HireView(discord.ui.View):
    def __init__(self, user_id: int, timeout=300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.current_page = 0  # 0-4 for gardeners 1-5
        self.total_pages = 5
    
    def create_embed(self, page: int) -> discord.Embed:
        """Create the embed for a specific gardener page."""
        slot_id = page + 1  # Convert 0-4 to 1-5
        gardeners = get_user_gardeners(self.user_id)
        balance = get_user_balance(self.user_id)
        gardener_dict = {g["id"]: g for g in gardeners}
        gardener = gardener_dict.get(slot_id)
        price = GARDENER_PRICES[slot_id - 1]
        
        # Get the chance for this gardener level
        gardener_chance = GARDENER_CHANCES.get(slot_id, 0.05) * 100  # Convert to percentage
        description_text = f"💰 Your Balance: **${balance:,.2f}**\n\nHire gardeners to automatically gather items for you! This gardener has a **{gardener_chance:.0f}%** chance to gather every minute."
        
        embed = discord.Embed(
            title=f"🌱 Gardener #{slot_id}",
            description=description_text,
            color=discord.Color.green()
        )
        
        if gardener:
            # Gardener is hired - show stats and tool info
            plants_gathered = gardener.get("plants_gathered", 0)
            total_money = gardener.get("total_money_earned", 0.0)
            has_tool = gardener.get("has_tool", False)
            tool_info = GARDENER_TOOLS.get(slot_id, {"name": "Tool", "cost": 0, "chance": 0})
            tool_chance_pct = tool_info["chance"] * 100
            
            embed.add_field(
                name="Status",
                value="**HIRED** ✅",
                inline=False
            )
            embed.add_field(
                name="Plants Gathered",
                value=f"**{plants_gathered}**",
                inline=True
            )
            embed.add_field(
                name="Total Money Earned",
                value=f"**${total_money:,.2f}**",
                inline=True
            )
            if has_tool:
                embed.add_field(
                    name="Tool",
                    value=f"**{tool_info['name']}** ✅ — **{tool_chance_pct}%** chance to auto harvest",
                    inline=False
                )
            else:
                embed.add_field(
                    name="Tool",
                    value=f"Buy **{tool_info['name']}** for **${tool_info['cost']:,.0f}** — **{tool_chance_pct}%** chance to auto harvest",
                    inline=False
                )
        else:
            # Gardener slot is available
            can_afford = "✅" if balance >= price else "❌"
            embed.add_field(
                name="Status",
                value="**Available**",
                inline=False
            )
            embed.add_field(
                name="Price",
                value=f"**${price:,.2f}** {can_afford}",
                inline=True
            )
        
        embed.set_footer(text=f"Page {page + 1} of {self.total_pages}")
        
        return embed
    
    def update_buttons(self):
        """Update button states based on current page and gardener status."""
        # Update navigation buttons
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        
        # Update hire button
        slot_id = self.current_page + 1
        gardeners = get_user_gardeners(self.user_id)
        gardener_dict = {g["id"]: g for g in gardeners}
        gardener = gardener_dict.get(slot_id)
        balance = get_user_balance(self.user_id)
        price = GARDENER_PRICES[slot_id - 1]
        
        if gardener:
            # Already hired
            self.hire_button.disabled = True
            self.hire_button.label = "Already Hired"
            self.hire_button.style = discord.ButtonStyle.secondary
        elif balance < price:
            # Can't afford
            self.hire_button.disabled = True
            self.hire_button.label = f"Hire (Need ${price:,.0f})"
            self.hire_button.style = discord.ButtonStyle.secondary
        elif len(gardeners) >= 5:
            # Max gardeners reached
            self.hire_button.disabled = True
            self.hire_button.label = "Max Gardeners"
            self.hire_button.style = discord.ButtonStyle.secondary
        else:
            # Can hire
            self.hire_button.disabled = False
            self.hire_button.label = f"Hire for ${price:,.0f}"
            self.hire_button.style = discord.ButtonStyle.success
        
        # Update buy tool button
        tool_info = GARDENER_TOOLS.get(slot_id, {"name": "Tool", "cost": 0})
        if gardener:
            if gardener.get("has_tool", False):
                self.buy_tool_button.disabled = True
                self.buy_tool_button.label = f"Tool: {tool_info['name']} ✓"
                self.buy_tool_button.style = discord.ButtonStyle.secondary
            else:
                tool_cost = tool_info["cost"]
                if balance < tool_cost:
                    self.buy_tool_button.disabled = True
                    self.buy_tool_button.label = f"Buy {tool_info['name']} (${tool_cost:,.0f})"
                    self.buy_tool_button.style = discord.ButtonStyle.secondary
                else:
                    self.buy_tool_button.disabled = False
                    self.buy_tool_button.label = f"Buy {tool_info['name']} (${tool_cost:,.0f})"
                    self.buy_tool_button.style = discord.ButtonStyle.primary
        else:
            self.buy_tool_button.disabled = True
            self.buy_tool_button.label = "Buy Tool"
            self.buy_tool_button.style = discord.ButtonStyle.secondary
    
    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, row=0)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your hiring center!", ephemeral=True)
                return
            
            if self.current_page > 0:
                self.current_page -= 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in previous_button (hire): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your hiring center!", ephemeral=True)
                return
            
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in next_button (hire): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Hire", style=discord.ButtonStyle.success, row=1)
    async def hire_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your hiring center!", ephemeral=True)
                return
            
            slot_id = self.current_page + 1
            gardeners = get_user_gardeners(self.user_id)
            gardener_dict = {g["id"]: g for g in gardeners}
            
            # Check if slot is already taken
            if slot_id in gardener_dict:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ Gardener #{slot_id} is already hired!", ephemeral=True)
                return
            
            # Check if max gardeners reached
            if len(gardeners) >= 5:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You already have the maximum of 5 gardeners!", ephemeral=True)
                return
            
            price = GARDENER_PRICES[slot_id - 1]
            balance = get_user_balance(self.user_id)
            
            if balance < price:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"❌ You don't have enough money! You need **${price:,.2f}** but only have **${balance:,.2f}**.",
                    ephemeral=True)
                return
            
            # Hire the gardener
            success = add_gardener(self.user_id, slot_id, price)
            if not success:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Failed to hire gardener. Please try again.", ephemeral=True)
                return
            
            # Check for Maxed Out achievement
            achievement_unlocked = check_maxed_out_achievement(self.user_id)
            
            # Send confirmation and update embed
            if achievement_unlocked:
                await safe_interaction_response(interaction, interaction.response.send_message, 
                    f"✅ Hired **Gardener #{slot_id}** for ${price:,.2f}! They'll start gathering for you automatically.\n\n"
                    f"🎉 **Hidden Achievement Unlocked: Maxed Out!** 🎉", ephemeral=True)
            else:
                await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Hired **Gardener #{slot_id}** for ${price:,.2f}! They'll start gathering for you automatically.", ephemeral=True)
            
            embed = self.create_embed(self.current_page)
            self.update_buttons()
            try:
                await interaction.message.edit(embed=embed, view=self)
            except:
                pass  # Message might have been deleted
        except Exception as e:
            print(f"Error in hire_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Buy Tool", style=discord.ButtonStyle.secondary, row=1)
    async def buy_tool_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your hiring center!", ephemeral=True)
                return
            
            slot_id = self.current_page + 1
            gardeners = get_user_gardeners(self.user_id)
            gardener_dict = {g["id"]: g for g in gardeners}
            gardener = gardener_dict.get(slot_id)
            
            if not gardener:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Hire this gardener first before buying their tool!", ephemeral=True)
                return
            
            if gardener.get("has_tool", False):
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ This gardener already has their tool!", ephemeral=True)
                return
            
            tool_info = GARDENER_TOOLS.get(slot_id, {"name": "Tool", "cost": 0})
            tool_cost = tool_info["cost"]
            balance = get_user_balance(self.user_id)
            
            if balance < tool_cost:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"❌ You don't have enough money! The **{tool_info['name']}** costs **${tool_cost:,.2f}** but you only have **${balance:,.2f}**.", ephemeral=True)
                return
            
            success = set_gardener_has_tool(self.user_id, slot_id, tool_cost)
            if not success:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Failed to buy tool. Please try again.", ephemeral=True)
                return
            
            # Check for Maxed Out achievement
            achievement_unlocked = check_maxed_out_achievement(self.user_id)
            
            chance_pct = tool_info["chance"] * 100
            if achievement_unlocked:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"✅ **{tool_info['name']}** purchased for ${tool_cost:,.2f}! This gardener's auto gather now has a **{chance_pct}%** chance to upgrade to a full harvest!\n\n"
                    f"🎉 **Hidden Achievement Unlocked: Maxed Out!** 🎉", ephemeral=True)
            else:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"✅ **{tool_info['name']}** purchased for ${tool_cost:,.2f}! This gardener's auto gather now has a **{chance_pct}%** chance to upgrade to a full harvest!", ephemeral=True)
            
            embed = self.create_embed(self.current_page)
            self.update_buttons()
            try:
                await interaction.message.edit(embed=embed, view=self)
            except:
                pass
        except Exception as e:
            print(f"Error in buy_tool_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


# Hire command
@bot.tree.command(name="hire", description="Hire gardeners to automatically gather items for you!")
async def hire(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        
        view = HireView(user_id)
        embed = view.create_embed(0)  # Start on page 0 (Gardener #1)
        view.update_buttons()
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in hire command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# GPU View with pagination
class GpuView(discord.ui.View):
    def __init__(self, user_id: int, timeout=300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.current_page = 0  # 0-9 for GPUs 1-10
        self.total_pages = 10
    
    def create_embed(self, page: int) -> discord.Embed:
        """Create the embed for a specific GPU page."""
        gpu_info = GPU_SHOP[page]
        gpu_name = gpu_info["name"]
        balance = get_user_balance(self.user_id)
        user_gpus = get_user_gpus(self.user_id)
        
        # Check if user owns this GPU
        already_owned = gpu_name in user_gpus
        
        embed = discord.Embed(
            title=f"🖥️ {gpu_name}",
            description=f"💰 Your Balance: **${balance:,.2f}**\n\nBuy GPUs to boost your mining! You can own one of each GPU.",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Mining Boost",
            value=f"**+{gpu_info['percent_increase']}%** amount gained",
            inline=True
        )
        embed.add_field(
            name="Time Boost",
            value=f"**+{gpu_info['seconds_increase']}** seconds",
            inline=True
        )
        embed.add_field(
            name="Price",
            value=f"**${gpu_info['price']:,.2f}** {'✅' if balance >= gpu_info['price'] else '❌'}",
            inline=True
        )
        embed.add_field(
            name="Status",
            value="**OWNED** ✅" if already_owned else "**Available**",
            inline=False
        )
        
        embed.set_footer(text=f"Page {page + 1} of {self.total_pages}")
        
        return embed
    
    def update_buttons(self):
        """Update button states based on current page and user balance."""
        # Update navigation buttons
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        
        # Update buy button
        gpu_info = GPU_SHOP[self.current_page]
        gpu_name = gpu_info["name"]
        balance = get_user_balance(self.user_id)
        price = gpu_info["price"]
        user_gpus = get_user_gpus(self.user_id)
        already_owned = gpu_name in user_gpus
        
        if already_owned:
            # Already owned
            self.buy_button.disabled = True
            self.buy_button.label = "Already Owned"
            self.buy_button.style = discord.ButtonStyle.secondary
        elif balance < price:
            # Can't afford
            self.buy_button.disabled = True
            self.buy_button.label = f"Buy (Need ${price:,.0f})"
            self.buy_button.style = discord.ButtonStyle.secondary
        else:
            # Can buy
            self.buy_button.disabled = False
            self.buy_button.label = f"Buy for ${price:,.0f}"
            self.buy_button.style = discord.ButtonStyle.success
    
    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, row=0)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your GPU shop!", ephemeral=True)
                return
            
            if self.current_page > 0:
                self.current_page -= 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in previous_button (gpu): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your GPU shop!", ephemeral=True)
                return
            
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in next_button (gpu): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Buy", style=discord.ButtonStyle.success, row=1)
    async def buy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your GPU shop!", ephemeral=True)
                return
            
            gpu_info = GPU_SHOP[self.current_page]
            gpu_name = gpu_info["name"]
            price = gpu_info["price"]
            balance = get_user_balance(self.user_id)
            user_gpus = get_user_gpus(self.user_id)
            
            # Check if already owned
            if gpu_name in user_gpus:
                await safe_interaction_response(interaction, interaction.response.send_message, f"❌ You already own **{gpu_name}**!", ephemeral=True)
                return
            
            if balance < price:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"❌ You don't have enough money! You need **${price:,.2f}** but only have **${balance:,.2f}**.",
                    ephemeral=True)
                return
            
            # Buy the GPU
            success = add_gpu(self.user_id, gpu_name, price)
            if not success:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Failed to buy GPU. Please try again.", ephemeral=True)
                return
            
            # Check for Maxed Out achievement
            achievement_unlocked = check_maxed_out_achievement(self.user_id)
            
            # Send confirmation and update embed
            if achievement_unlocked:
                await safe_interaction_response(interaction, interaction.response.send_message, 
                    f"✅ Purchased **{gpu_name}** for ${price:,.2f}! It will boost your mining!\n\n"
                    f"🎉 **Hidden Achievement Unlocked: Maxed Out!** 🎉", ephemeral=True)
            else:
                await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{gpu_name}** for ${price:,.2f}! It will boost your mining!", ephemeral=True)
            
            embed = self.create_embed(self.current_page)
            self.update_buttons()
            try:
                await interaction.message.edit(embed=embed, view=self)
            except:
                pass  # Message might have been deleted
        except Exception as e:
            print(f"Error in buy_button (gpu): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


# GPU command
@bot.tree.command(name="gpu", description="Buy GPUs to boost your cryptocurrency mining!")
async def gpu(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        
        view = GpuView(user_id)
        embed = view.create_embed(0)  # Start on page 0 (GPU 1)
        view.update_buttons()
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in gpu command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Admin Event Commands
@bot.tree.command(name="starthourlyevent", description="[ADMIN] Start a specific hourly event manually")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(event=[
    app_commands.Choice(name=f"{e['emoji']} {e['name']}", value=e['id'])
    for e in HOURLY_EVENTS
])
@app_commands.choices(duration=[
    app_commands.Choice(name="30 minutes", value=30),
    app_commands.Choice(name="45 minutes", value=45),
    app_commands.Choice(name="60 minutes", value=60)
])
async def starthourlyevent(interaction: discord.Interaction, event: str, duration: int):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
    
        # Check if there's already an active hourly event
        existing_hourly = get_active_event_by_type("hourly")
        if existing_hourly:
            current_time = time.time()
            if existing_hourly.get("end_time", 0) > current_time:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ **Error**: An hourly event is already active: **{existing_hourly['event_name']}** "
                    f"(ends in {int((existing_hourly.get('end_time', 0) - current_time) / 60)} minutes). "
                    f"Use `/endevent hourly` to end it first.",
                    ephemeral=True)
                return
            else:
                # Clean up expired event
                clear_event(existing_hourly.get("event_id", ""))
        
        # Find the event info
        event_info = next((e for e in HOURLY_EVENTS if e["id"] == event), None)
        if not event_info:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Event not found.", ephemeral=True)
            return
        
        # Create the event
        duration_minutes = duration
        duration_seconds = duration_minutes * 60
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        event_id = f"hourly_{int(start_time)}_{event_info['id']}"
        
        set_active_event(
            event_id=event_id,
            event_type="hourly",
            event_name=event_info["name"],
            start_time=start_time,
            end_time=end_time,
            effects={"event_id": event_info["id"]}
        )
        
        # Send announcement to all guilds
        guilds_sent = 0
        for guild in bot.guilds:
            try:
                await send_event_start_embed(guild, {
                    "event_type": "hourly",
                    "event_id": event_info["id"],
                    "event_name": event_info["name"]
                }, duration_minutes)
                guilds_sent += 1
            except Exception as e:
                print(f"Error sending start embed to {guild.name} for hourly event: {e}")
        
        embed = discord.Embed(
            title=f"✅ Event Started Successfully",
            description=f"**{event_info['emoji']} {event_info['name']}**",
            color=discord.Color.green()
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} started hourly event: {event_info['name']} for {duration_minutes} minutes")
    except Exception as e:
        print(f"Error in starthourlyevent command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="startdailyevent", description="[ADMIN] Start a specific daily event manually")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(event=[
    app_commands.Choice(name=f"{e['emoji']} {e['name']}", value=e['id'])
    for e in DAILY_EVENTS
])
async def startdailyevent(interaction: discord.Interaction, event: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
    
        # Check if there's already an active daily event
        existing_daily = get_active_event_by_type("daily")
        if existing_daily:
            current_time = time.time()
            if existing_daily.get("end_time", 0) > current_time:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ **Error**: A daily event is already active: **{existing_daily['event_name']}** "
                    f"(ends in {int((existing_daily.get('end_time', 0) - current_time) / 3600)} hours). "
                    f"Use `/endevent daily` to end it first.",
                    ephemeral=True)
                return
            else:
                # Clean up expired event
                clear_event(existing_daily.get("event_id", ""))
        
        # Find the event info
        event_info = next((e for e in DAILY_EVENTS if e["id"] == event), None)
        if not event_info:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Event not found.", ephemeral=True)
            return
        
        # Create the event (fixed 24 hour duration)
        duration_minutes = 24 * 60
        duration_seconds = duration_minutes * 60
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        event_id = f"daily_{int(start_time)}_{event_info['id']}"
        
        set_active_event(
            event_id=event_id,
            event_type="daily",
            event_name=event_info["name"],
            start_time=start_time,
            end_time=end_time,
            effects={"event_id": event_info["id"]}
        )
        
        # Send announcement to all guilds
        guilds_sent = 0
        for guild in bot.guilds:
            try:
                await send_event_start_embed(guild, {
                    "event_type": "daily",
                    "event_id": event_info["id"],
                    "event_name": event_info["name"]
                }, duration_minutes)
                guilds_sent += 1
            except Exception as e:
                print(f"Error sending start embed to {guild.name} for daily event: {e}")
        
        embed = discord.Embed(
            title=f"✅ Event Started Successfully",
            description=f"**{event_info['emoji']} {event_info['name']}**",
            color=discord.Color.green()
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} started daily event: {event_info['name']}")
    except Exception as e:
        print(f"Error in startdailyevent command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="endevent", description="[ADMIN] End the currently active hourly or daily event")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(event_type=[
    app_commands.Choice(name="hourly", value="hourly"),
    app_commands.Choice(name="daily", value="daily")
])
async def endevent(interaction: discord.Interaction, event_type: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        
        # Get the active event
        active_event = get_active_event_by_type(event_type)
        if not active_event:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ **Error**: No active {event_type} event found.",
                ephemeral=True)
            return
        
        # Get event info for the embed
        event_type_id = active_event.get("effects", {}).get("event_id")
        if not event_type_id:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Could not find event information.", ephemeral=True)
            return
        
        event_info = None
        if event_type == "hourly":
            event_info = next((e for e in HOURLY_EVENTS if e["id"] == event_type_id), None)
        elif event_type == "daily":
            event_info = next((e for e in DAILY_EVENTS if e["id"] == event_type_id), None)
        
        if not event_info:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Event info not found.", ephemeral=True)
            return
        
        # Clear the event from database
        clear_event(active_event.get("event_id", ""))
        
        # Send end message to all guilds
        guilds_sent = 0
        for guild in bot.guilds:
            try:
                await send_event_end_embed(guild, active_event)
                guilds_sent += 1
            except Exception as e:
                print(f"Error sending end embed to {guild.name}: {e}")
        
        embed = discord.Embed(
            title=f"✅ Event Ended Successfully",
            description=f"**{event_info['emoji']} {event_info['name']}**",
            color=discord.Color.orange()
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} ended {event_type} event: {event_info['name']}")
    except Exception as e:
        print(f"Error in endevent command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Reset command - Admin only, #hidden channel
@bot.tree.command(name="reset", description="[ADMIN] Reset cooldowns or crypto prices")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(type=[
    app_commands.Choice(name="cooldowns", value="cooldowns"),
    app_commands.Choice(name="cryptoprices", value="cryptoprices")
])
async def reset(interaction: discord.Interaction, type: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        
        # Check if command is being used in the #hidden channel
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return
        
        if type == "cooldowns":
            # Get all members in the guild
            guild = interaction.guild
            if not guild:
                await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Could not get guild information.", ephemeral=True)
                return
            
            members = guild.members
            reset_count = 0
            
            # Reset cooldowns for all members
            for member in members:
                if not member.bot:  # Skip bots
                    reset_user_cooldowns(member.id)
                    reset_count += 1
            
            embed = discord.Embed(
                title="✅ Cooldowns Reset",
                description=f"Reset cooldowns for **{reset_count}** users.",
                color=discord.Color.green()
            )
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
            print(f"Admin {interaction.user.name} reset cooldowns for {reset_count} users")
        
        elif type == "cryptoprices":
            # Reset crypto prices to base prices
            base_prices = {coin["symbol"]: coin["base_price"] for coin in CRYPTO_COINS}
            update_crypto_prices(base_prices)
            
            # Also reset price history
            initialize_crypto_history()
            
            embed = discord.Embed(
                title="✅ Crypto Prices Reset",
                description="Cryptocurrency prices have been reset to their base values:",
                color=discord.Color.green()
            )
            for coin in CRYPTO_COINS:
                embed.add_field(
                    name=f"{coin['name']} ({coin['symbol']})",
                    value=f"${coin['base_price']:,.2f}",
                    inline=True
                )
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
            print(f"Admin {interaction.user.name} reset crypto prices to base values")
    except Exception as e:
        print(f"Error in reset command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Wipe command - Admin only, #hidden channel
@bot.tree.command(name="wipe", description="[ADMIN] Wipe user data (money or all)")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(type=[
    app_commands.Choice(name="money", value="money"),
    app_commands.Choice(name="plants", value="plants"),
    app_commands.Choice(name="crypto", value="crypto"),
    app_commands.Choice(name="all", value="all")
])
async def wipe(interaction: discord.Interaction, type: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        
        # Check if command is being used in the #hidden channel
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return
        
        # Get all members in the guild
        guild = interaction.guild
        if not guild:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Could not get guild information.", ephemeral=True)
            return
    
        members = guild.members
        wiped_count = 0
        
        if type == "money":
            # Reset money to default, keep upgrades
            for member in members:
                if not member.bot:  # Skip bots
                    wipe_user_money(member.id)
                    wiped_count += 1
            
            embed = discord.Embed(
                title="✅ Money Wiped",
                description=f"Reset money to default for **{wiped_count}** users in this server.\n\n**Stock market has been reset** - all shares returned, making all stocks available at max capacity.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Money (balance)\n• Stock holdings (shares)\n• Crypto holdings (portfolio)", inline=False)
            embed.add_field(name="What was kept", value="• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Plants", inline=False)
        elif type == "plants":
            # Reset plants and update ranks
            for member in members:
                if not member.bot:  # Skip bots
                    wipe_user_plants(member.id)
                    # Update their rank to PLANTER I
                    try:
                        await assign_gatherer_role(member, guild)
                    except Exception as e:
                        print(f"Error updating role for user {member.id}: {e}")
                    wiped_count += 1
            
            embed = discord.Embed(
                title="✅ Plants Wiped",
                description=f"Reset collected plants for **{wiped_count}** users in this server.\nAll users have been set to **PLANTER I** rank.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Collected items\n• Gather stats\n• Ripeness stats\n• Rank (set to PLANTER I)\n• Planter achievement\n• All cooldowns", inline=False)
            embed.add_field(name="What was kept", value="• Money (balance)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs", inline=False)
        elif type == "crypto":
            # Reset crypto holdings only
            for member in members:
                if not member.bot:  # Skip bots
                    wipe_user_crypto(member.id)
                    wiped_count += 1
            
            embed = discord.Embed(
                title="✅ Crypto Wiped",
                description=f"Reset crypto holdings to 0 for **{wiped_count}** users in this server.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Crypto holdings (portfolio)", inline=False)
            embed.add_field(name="What was kept", value="• Money (balance)\n• Stock holdings (shares)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Plants", inline=False)
        else:  # type == "all"
            # Reset money, all upgrades, and plants
            for member in members:
                if not member.bot:  # Skip bots
                    wipe_user_all(member.id)
                    # Update their rank to PLANTER I
                    try:
                        await assign_gatherer_role(member, guild)
                    except Exception as e:
                        print(f"Error updating gatherer role for user {member.id}: {e}")
                    # Update their Bloom rank to PINE I
                    try:
                        await assign_bloom_rank_role(member, guild)
                    except Exception as e:
                        print(f"Error updating bloom rank role for user {member.id}: {e}")
                    wiped_count += 1
            
            embed = discord.Embed(
                title="✅ All Data Wiped",
                description=f"Reset everything for **{wiped_count}** users in this server.\nAll users have been set to **PLANTER I** rank and **PINE I** Bloom rank.\n\n**Market has been reset** - all shares returned, making all stocks available at max capacity.",
                color=discord.Color.red()
            )
            embed.add_field(name="What was reset", value="• Money (balance)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Stock holdings (shares)\n• Crypto holdings (portfolio)\n• Collected items\n• Gather stats\n• Ripeness stats\n• Rank (set to PLANTER I)\n• Bloom rank (set to PINE I)\n• All achievements and achievement stats\n• All cooldowns", inline=False)
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} wiped {type} data for {wiped_count} users")
    except Exception as e:
        print(f"Error in wipe command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Set command - Admin only, #hidden channel
@bot.tree.command(name="set", description="[ADMIN] Set a user's money, plants, or crypto amount")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    user="The user to set the value for (defaults to yourself)",
    amount="The amount to set",
    type="The type of value to set: money, plants, or crypto",
    coin="The crypto coin type (RTC, TER, or CNY) - required if type is crypto"
)
@app_commands.choices(type=[
    app_commands.Choice(name="Money", value="money"),
    app_commands.Choice(name="Plants", value="plants"),
    app_commands.Choice(name="Crypto", value="crypto")
])
@app_commands.choices(coin=[
    app_commands.Choice(name="RTC", value="RTC"),
    app_commands.Choice(name="TER", value="TER"),
    app_commands.Choice(name="CNY", value="CNY")
])
async def set_command(
    interaction: discord.Interaction,
    amount: float,
    type: str,
    user: discord.Member = None,
    coin: str = None
):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        # Check if user has administrator permissions
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        
        # Check if command is being used in the #hidden channel
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return
        
        # Determine target user (default to command user if not specified)
        target_user = user if user else interaction.user
        user_id = target_user.id
        
        # Validate and normalize type
        type_lower = type.lower()
        if type_lower not in ["money", "plants", "crypto"]:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Type must be one of: `money`, `plants`, or `crypto`.",
                ephemeral=True)
            return
        
        # Validate amount
        if amount < 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Amount cannot be negative.",
                ephemeral=True)
            return
        
        users = _get_users_collection()
        
        # Handle each type
        if type_lower == "money":
            update_user_balance(user_id, amount)
            embed = discord.Embed(
                title="✅ Money Set",
                description=f"{target_user.mention}'s balance has been set to **${amount:,.2f}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s money to ${amount:,.2f}")
        
        elif type_lower == "plants":
            # Set gather_stats.total_items and total_forage_count
            users.update_one(
                {"_id": int(user_id)},
                {
                    "$set": {
                        "gather_stats.total_items": int(amount),
                        "total_forage_count": int(amount)
                    }
                },
                upsert=True
            )
            embed = discord.Embed(
                title="✅ Plants Set",
                description=f"{target_user.mention}'s plant count has been set to **{int(amount):,}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s plants to {int(amount):,}")
        
        elif type_lower == "crypto":
            # Validate coin parameter
            if not coin:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: `coin` parameter is required when type is `crypto`. Choose from: `RTC`, `TER`, or `CNY`.",
                    ephemeral=True)
                return
            
            coin_upper = coin.upper()
            if coin_upper not in ["RTC", "TER", "CNY"]:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Coin must be one of: `RTC`, `TER`, or `CNY`.",
                    ephemeral=True)
                return
            
            # Set crypto holdings directly
            users.update_one(
                {"_id": int(user_id)},
                {
                    "$set": {
                        f"crypto_holdings.{coin_upper}": float(amount)
                    }
                },
                upsert=True
            )
            embed = discord.Embed(
                title="✅ Crypto Set",
                description=f"{target_user.mention}'s {coin_upper} holdings have been set to **{amount:,.2f}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s {coin_upper} to {amount:,.2f}")
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in set command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Pay command
@bot.tree.command(name="pay", description="Pay money to another user!")
async def pay(interaction: discord.Interaction, amount: float, user: discord.Member):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        sender_id = interaction.user.id
        recipient_id = user.id
        
        # Can't pay yourself
        if sender_id == recipient_id:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't pay yourself!", ephemeral=True)
            return
        
        # Can't pay the bot
        if recipient_id == bot.user.id:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't pay the bot!", ephemeral=True)
            return
        
        # Validate amount is positive
        if amount <= 0:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Payment amount must be greater than $0!", ephemeral=True)
            return
        
        # Validate amount has at most 2 decimal places (no fractional cents)
        if not validate_money_precision(amount):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Invalid payment amount!", ephemeral=True)
            return
        
        # Normalize amount to exactly 2 decimal places
        amount = normalize_money(amount)
        
        # Check sender balance
        sender_balance = get_user_balance(sender_id)
        sender_balance = normalize_money(sender_balance)
        
        if not can_afford_rounded(sender_balance, amount):
            await safe_interaction_response(interaction, interaction.followup.send, f"❌ You don't have enough balance!", ephemeral=True)
            return
        
        # Get recipient balance
        recipient_balance = get_user_balance(recipient_id)
        recipient_balance = normalize_money(recipient_balance)
        
        # Transfer money
        new_sender_balance = normalize_money(sender_balance - amount)
        new_recipient_balance = normalize_money(recipient_balance + amount)
        update_user_balance(sender_id, new_sender_balance)
        update_user_balance(recipient_id, new_recipient_balance)
        
        # Send confirmation message first
        await safe_interaction_response(interaction, interaction.followup.send, f"{interaction.user.mention} has paid {user.mention} **${amount:.2f}**! 💰")
        
        # Check for hidden achievements (after main response)
        # John Rockefeller: Pay someone over $1,000,000
        if amount >= 1000000.0 and unlock_hidden_achievement(sender_id, "john_rockefeller"):
            await send_hidden_achievement_notification(interaction, "john_rockefeller")
        
        # Beneficiary: Receive over $1,000,000 from someone
        # Send as channel message mentioning the recipient (visible to everyone, but clearly for them)
        # Note: Cannot send ephemeral message to recipient since they didn't initiate the interaction
        if amount >= 1000000.0:
            newly_unlocked = unlock_hidden_achievement(recipient_id, "beneficiary")
            if newly_unlocked:
                # Send as regular channel message with recipient mention
                channel = getattr(interaction, 'channel', None)
                if channel:
                    await send_hidden_achievement_notification_async(channel, user.mention, "beneficiary")
    except Exception as e:
        print(f"Error in pay command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Leaderboard pagination view
class LeaderboardView(discord.ui.View):
    def __init__(self, leaderboard_data: list[tuple[int, float | int | str]], leaderboard_type: str, guild: discord.Guild, timeout=300):
        super().__init__(timeout=timeout)
        self.leaderboard_data = leaderboard_data
        self.leaderboard_type = leaderboard_type
        self.guild = guild
        self.current_page = 0
        self.items_per_page = 10
        self.total_pages = (len(leaderboard_data) + self.items_per_page - 1) // self.items_per_page
        
    def get_page_data(self, page: int) -> list[tuple[int, float | int | str]]:
        """Get the data for a specific page."""
        start_idx = page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        return self.leaderboard_data[start_idx:end_idx]
    
    def get_username(self, user_id: int) -> str:
        """Get username from guild, or return 'Unknown User' if not found."""
        member = self.guild.get_member(user_id)
        if member:
            return member.display_name or member.name
        return "Unknown User"
    
    def create_embed(self, page: int) -> discord.Embed:
        """Create the embed for a specific page."""
        page_data = self.get_page_data(page)
        
        if self.leaderboard_type == "plants":
            title = "**🌱 PLANTS**"
            description = ""
            value_name = "Items"
        elif self.leaderboard_type == "money":
            title = "**💰 MONEY**"
            description = ""
            value_name = "Balance"
        else:  # ranks
            title = "**🏆 RANKS**"
            description = ""
            value_name = "Rank"
        
        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.gold()
        )
        
        leaderboard_text = ""
        start_rank = page * self.items_per_page + 1
        
        for idx, (user_id, value) in enumerate(page_data):
            rank = start_rank + idx
            username = self.get_username(user_id)
            
            if self.leaderboard_type == "plants":
                # Top 3 get different tree emojis, bottom 7 get plant emoji
                if rank == 1:
                    emoji = "🌳"
                elif rank == 2:
                    emoji = "🎄"
                elif rank == 3:
                    emoji = "🌲"
                else:
                    emoji = "🌱"
                leaderboard_text += f"{emoji} **{rank}.** {username}: **{value}** items\n"
            elif self.leaderboard_type == "money":
                # Top 3 get money bag, bottom 7 get cash emoji
                if rank <= 3:
                    emoji = "💰"
                else:
                    emoji = "💵"
                leaderboard_text += f"{emoji} **{rank}.** {username}: **${value:.2f}**\n"
            else:  # ranks
                # Top 3 get trophy emojis, bottom 7 get medal emoji
                if rank == 1:
                    emoji = "🥇"
                elif rank == 2:
                    emoji = "🥈"
                elif rank == 3:
                    emoji = "🥉"
                else:
                    emoji = "🏅"
                leaderboard_text += f"{emoji} **{rank}.** {username}: **{value}**\n"
        
        if not leaderboard_text:
            leaderboard_text = "No data available"
        
        embed.add_field(name="Rankings", value=leaderboard_text, inline=False)
        embed.set_footer(text=f"Page {page + 1} of {self.total_pages} | Total: {len(self.leaderboard_data)} users")
        
        return embed
    
    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.current_page > 0:
                self.current_page -= 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in previous_button (leaderboard): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            else:
                await safe_defer(interaction)
        except Exception as e:
            print(f"Error in next_button (leaderboard): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
        else:
            await interaction.response.defer()
    
    def update_buttons(self):
        """Update button states based on current page."""
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1


# Store leaderboard message IDs per guild and type
leaderboard_messages = {}  # {guild_id: {"plants": message_id, "money": message_id, "ranks": message_id}}

async def update_leaderboard_message(guild: discord.Guild, leaderboard_type: str):
    """Update or create a leaderboard message in the #leaderboard channel."""
    # Find the leaderboard channel
    leaderboard_channel = discord.utils.get(guild.text_channels, name="leaderboard")
    
    if not leaderboard_channel:
        return  # Channel doesn't exist, skip
    
    # Get all guild member IDs
    guild_member_ids = {member.id for member in guild.members}
    
    # Get leaderboard data
    if leaderboard_type == "plants":
        all_data = get_all_users_total_items()
    elif leaderboard_type == "money":
        all_data = get_all_users_balance()
    else:  # ranks
        all_data = get_all_users_ranks()
    
    # Filter to only include users in the guild
    leaderboard_data = [(user_id, value) for user_id, value in all_data if user_id in guild_member_ids]
    
    if not leaderboard_data:
        return  # No data available
    
    # Create embed (first page only, no pagination for auto-updates)
    if leaderboard_type == "plants":
        title = "**🌱 PLANTS**"
        description = ""
    elif leaderboard_type == "money":
        title = "**💰 MONEY**"
        description = ""
    else:  # ranks
        title = "**🏆 RANKS**"
        description = ""
    
    embed = discord.Embed(
        title=title,
        description=description,
        color=discord.Color.gold()
    )
    
    # Show top 10
    leaderboard_text = ""
    for idx, (user_id, value) in enumerate(leaderboard_data[:10]):
        rank = idx + 1
        member = guild.get_member(user_id)
        username = member.display_name or member.name if member else "Unknown User"
        
        if leaderboard_type == "plants":
            # Top 3 get different tree emojis, bottom 7 get plant emoji
            if rank == 1:
                emoji = "🌳"
            elif rank == 2:
                emoji = "🎄"
            elif rank == 3:
                emoji = "🌲"
            else:
                emoji = "🌱"
            leaderboard_text += f"{emoji} **{rank}.** {username}: **{value}** items\n"
        elif leaderboard_type == "money":
            # Top 3 get money bag, bottom 7 get cash emoji
            if rank <= 3:
                emoji = "💰"
            else:
                emoji = "💵"
            leaderboard_text += f"{emoji} **{rank}.** {username}: **${value:.2f}**\n"
        else:  # ranks
            # Top 3 get trophy emojis, bottom 7 get medal emoji
            if rank == 1:
                emoji = "🥇"
            elif rank == 2:
                emoji = "🥈"
            elif rank == 3:
                emoji = "🥉"
            else:
                emoji = "🏅"
            leaderboard_text += f"{emoji} **{rank}.** {username}: **{value}**\n"
    
    if not leaderboard_text:
        leaderboard_text = "No data available"
    
    embed.add_field(name="Top 10 Rankings", value=leaderboard_text, inline=False)
    embed.set_footer(text=f"Total: {len(leaderboard_data)} users")
    embed.timestamp = discord.utils.utcnow()
    
    # Try to edit existing message, or create new one
    guild_id = guild.id
    if guild_id not in leaderboard_messages:
        leaderboard_messages[guild_id] = {}
    
    message_id = leaderboard_messages[guild_id].get(leaderboard_type)
    
    try:
        if message_id:
            # Try to edit existing message
            try:
                message = await leaderboard_channel.fetch_message(message_id)
                
                # Always try to edit the existing message, regardless of age
                try:
                    await message.edit(embed=embed)
                    return
                except discord.HTTPException as e:
                    # Check if it's a rate limit error
                    if e.status == 429:
                        # Rate limited, wait and retry once
                        retry_after = e.retry_after if hasattr(e, 'retry_after') else 1.0
                        await asyncio.sleep(retry_after)
                        try:
                            await message.edit(embed=embed)
                            return
                        except discord.HTTPException as retry_e:
                            # If retry also fails, log but don't create new message
                            logging.warning(f"Rate limited retry failed for {leaderboard_type} leaderboard in {guild.name}: {retry_e}")
                            return  # Skip this update rather than creating new message
                    elif e.code == 30046:  # Maximum edits to old messages reached
                        # Discord limit reached, but we still want to keep the message
                        # Log and skip this update rather than creating new message
                        logging.warning(f"Maximum edits reached for {leaderboard_type} leaderboard message in {guild.name}, skipping update")
                        return
                    else:
                        # Other error, log but don't create new message
                        logging.warning(f"Error editing {leaderboard_type} leaderboard in {guild.name}: {e}")
                        return  # Skip this update rather than creating new message
            except discord.NotFound:
                # Message was deleted, search for existing one
                message_id = None
            except discord.HTTPException as e:
                # Other error (permissions, etc.), search for existing one
                if e.status == 429:
                    # Rate limited, skip this update
                    logging.warning(f"Rate limited while fetching leaderboard message in {guild.name}, skipping update")
                    return
                logging.warning(f"Error fetching {leaderboard_type} leaderboard message in {guild.name}: {e}")
                message_id = None
        
        # If no valid message_id, search for existing leaderboard message in channel
        if not message_id:
            try:
                # Search through recent messages to find existing leaderboard
                async for message in leaderboard_channel.history(limit=50):
                    if message.author.id == bot.user.id and message.embeds:
                        embed_title = message.embeds[0].title if message.embeds[0].title else ""
                        # Check if this is the leaderboard message we're looking for
                        if (leaderboard_type == "plants" and "🌱 PLANTS" in embed_title) or \
                           (leaderboard_type == "money" and "💰 MONEY" in embed_title) or \
                           (leaderboard_type == "ranks" and "🏆 RANKS" in embed_title):
                            # Found existing message, update it (regardless of age)
                            message_id = message.id
                            leaderboard_messages[guild_id][leaderboard_type] = message_id
                            try:
                                await message.edit(embed=embed)
                                return
                            except discord.HTTPException as e:
                                if e.status == 429:
                                    # Rate limited, skip
                                    logging.warning(f"Rate limited while editing {leaderboard_type} leaderboard in {guild.name}, skipping update")
                                    return
                                elif e.code == 30046:
                                    # Max edits reached, skip this update but keep the message
                                    logging.warning(f"Maximum edits reached for {leaderboard_type} leaderboard message in {guild.name}, skipping update")
                                    return
                                else:
                                    # Other error, try next message or skip
                                    continue
            except discord.HTTPException as e:
                if e.status == 429:
                    logging.warning(f"Rate limited while searching for leaderboard message in {guild.name}, skipping update")
                    return
                logging.warning(f"Error searching for existing leaderboard message in {guild.name}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error searching for leaderboard message in {guild.name}: {e}", exc_info=True)
        
        # Create new message only if we truly couldn't find an existing one (message was deleted)
        # This should be rare - we only create if no message exists at all
        try:
            message = await leaderboard_channel.send(embed=embed)
            leaderboard_messages[guild_id][leaderboard_type] = message.id
            logging.info(f"Created new {leaderboard_type} leaderboard message in {guild.name} (no existing message found)")
        except discord.HTTPException as e:
            if e.status == 429:
                logging.warning(f"Rate limited while creating new {leaderboard_type} leaderboard message in {guild.name}, skipping update")
            else:
                logging.error(f"Error creating new {leaderboard_type} leaderboard message in {guild.name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error updating {leaderboard_type} leaderboard in {guild.name}: {e}", exc_info=True)

async def update_all_leaderboards():
    """Background task to update all leaderboards every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Update leaderboards for all guilds the bot is in
            for guild in bot.guilds:
                try:
                    await update_leaderboard_message(guild, "plants")
                    await asyncio.sleep(2)  # Delay between updates to avoid rate limits
                    await update_leaderboard_message(guild, "money")
                    await asyncio.sleep(2)  # Delay between updates
                    await update_leaderboard_message(guild, "ranks")
                    await asyncio.sleep(2)  # Delay between updates
                except Exception as e:
                    logging.error(f"Error updating leaderboards for guild {guild.name}: {e}", exc_info=True)
                # Delay between guilds to prevent rate limiting
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in leaderboard update task: {e}", exc_info=True)
        
        # Wait 60 seconds before next update
        await asyncio.sleep(60)


# STALK MARKET - Stock Market System
# Mapping from fictional stock symbols to real-world stock tickers
REAL_STOCK_MAPPING = {
    "M": "M",        # Maizy's -> Macy's
    "MEDO": "META",  # Meadow -> Meta Platforms
    "IVM": "IBM",    # IVM -> IBM
    "CSGO": "CSCO",  # CisGrow -> Cisco
    "SWNY": "SONY",  # Sowny -> Sony Group
    "GM": "GM",      # General Mowers -> General Motors
    "RTH": "RTX",    # Raytheorn -> Raytheon
    "WFG": "WFC",    # Wells Fargrow -> Wells Fargo
    "AAPL": "AAPL",  # Apple -> Apple
    "SPRT": "SPOT"   # Sproutify -> Spotify
}

# Stock ticker definitions
STOCK_TICKERS = [
    {"name": "Maizy's", "symbol": "M", "base_price": 50.0, "max_shares": 10000},
    {"name": "Meadow", "symbol": "MEDO", "base_price": 75.0, "max_shares": 20000},
    {"name": "IVM", "symbol": "IVM", "base_price": 100.0, "max_shares": 15000},
    {"name": "CisGrow", "symbol": "CSGO", "base_price": 60.0, "max_shares": 12000},
    {"name": "Sowny", "symbol": "SWNY", "base_price": 90.0, "max_shares": 11000},
    {"name": "General Mowers", "symbol": "GM", "base_price": 45.0, "max_shares": 20000},
    {"name": "Raytheorn", "symbol": "RTH", "base_price": 125.0, "max_shares": 16000},
    {"name": "Wells Fargrow", "symbol": "WFG", "base_price": 70.0, "max_shares": 18000},
    {"name": "Apple", "symbol": "AAPL", "base_price": 150.0, "max_shares": 17000},
    {"name": "Sproutify", "symbol": "SPRT", "base_price": 55.0, "max_shares": 16000},
]

# Stock data storage: {guild_id: {ticker_symbol: {"price": float, "price_history": [float], "available_shares": int, "real_price": float, "shares_outstanding": int, "market_cap": float, "news_multiplier": float, "last_api_fetch": float}}}
stock_data = {}

def fetch_real_stock_data(real_ticker: str) -> dict:
    """Fetch real-world stock data from yfinance API.
    
    Args:
        real_ticker: Real stock ticker symbol (e.g., "AAPL", "META")
    
    Returns:
        dict with keys: price, shares_outstanding, market_cap, company_name
        Returns None if API call fails
    """
    try:
        ticker = yf.Ticker(real_ticker)
        info = ticker.info
        
        # Extract data from yfinance info
        price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
        shares_outstanding = info.get('sharesOutstanding') or info.get('impliedSharesOutstanding')
        market_cap = info.get('marketCap')
        company_name = info.get('longName') or info.get('shortName') or real_ticker
        
        if price is None or price <= 0:
            logging.warning(f"Invalid price for {real_ticker}: {price}")
            return None
        
        if shares_outstanding is None or shares_outstanding <= 0:
            logging.warning(f"Invalid shares outstanding for {real_ticker}: {shares_outstanding}")
            return None
        
        return {
            "price": float(price),
            "shares_outstanding": int(shares_outstanding),
            "market_cap": float(market_cap) if market_cap else None,
            "company_name": str(company_name)
        }
    except Exception as e:
        logging.error(f"Error fetching stock data for {real_ticker}: {e}", exc_info=True)
        return None

async def initialize_stocks(guild_id: int):
    """Initialize stock data for a guild if it doesn't exist, fetching real stock data."""
    if guild_id not in stock_data:
        stock_data[guild_id] = {}
        current_time = time.time()
        
        for ticker in STOCK_TICKERS:
            symbol = ticker["symbol"]
            real_ticker = REAL_STOCK_MAPPING.get(symbol)
            
            # Initialize with base values
            stock_data[guild_id][symbol] = {
                "price": ticker["base_price"],
                "price_history": [ticker["base_price"]] * 6,
                "real_price": ticker["base_price"],
                "shares_outstanding": ticker.get("max_shares", 0),
                "market_cap": None,
                "news_multiplier": 1.0,
                "last_api_fetch": 0,
                "available_shares": 0
            }
            
            # Try to fetch real data immediately
            if real_ticker:
                real_data = await asyncio.to_thread(fetch_real_stock_data, real_ticker)
                if real_data:
                    stock_data[guild_id][symbol]["real_price"] = real_data["price"]
                    stock_data[guild_id][symbol]["shares_outstanding"] = real_data["shares_outstanding"]
                    stock_data[guild_id][symbol]["market_cap"] = real_data.get("market_cap")
                    stock_data[guild_id][symbol]["price"] = real_data["price"]  # Initial price is real price
                    stock_data[guild_id][symbol]["price_history"] = [real_data["price"]] * 6
                    stock_data[guild_id][symbol]["last_api_fetch"] = current_time

async def update_stock_prices(guild_id: int):
    """Update stock prices with real-world data from API, then apply market news multipliers."""
    if guild_id not in stock_data:
        await initialize_stocks(guild_id)
    
    current_time = time.time()
    cache_duration = 120  # Cache for 120 seconds (2 minutes)
    
    for ticker in STOCK_TICKERS:
        symbol = ticker["symbol"]
        real_ticker = REAL_STOCK_MAPPING.get(symbol)
        
        if not real_ticker:
            logging.warning(f"No real ticker mapping found for {symbol}")
            continue
        
        # Initialize stock data structure if needed
        if symbol not in stock_data[guild_id]:
            stock_data[guild_id][symbol] = {
                "price": ticker["base_price"],
                "price_history": [ticker["base_price"]] * 6,
                "real_price": ticker["base_price"],
                "shares_outstanding": ticker.get("max_shares", 0),
                "market_cap": None,
                "news_multiplier": 1.0,
                "last_api_fetch": 0,
                "available_shares": 0
            }
        
        stock_info = stock_data[guild_id][symbol]
        
        # Check if we need to fetch new data (cache expired or missing)
        last_fetch = stock_info.get("last_api_fetch", 0)
        needs_fetch = (current_time - last_fetch) > cache_duration or stock_info.get("real_price") is None
        
        if needs_fetch:
            # Fetch real stock data (run in thread since yfinance is synchronous)
            # Add small delay between API calls to avoid rate limiting
            try:
                real_data = await asyncio.to_thread(fetch_real_stock_data, real_ticker)
                
                if real_data:
                    # Update real price and market data
                    stock_info["real_price"] = real_data["price"]
                    stock_info["shares_outstanding"] = real_data["shares_outstanding"]
                    stock_info["market_cap"] = real_data.get("market_cap")
                    stock_info["last_api_fetch"] = current_time
                    logging.info(f"Fetched real stock data for {symbol} ({real_ticker}): price=${real_data['price']:.2f}, shares={real_data['shares_outstanding']:,}")
                else:
                    # API failed, use fallback
                    if stock_info.get("real_price") is None:
                        stock_info["real_price"] = ticker["base_price"]
                        stock_info["shares_outstanding"] = ticker.get("max_shares", 0)
                    logging.warning(f"Failed to fetch stock data for {symbol} ({real_ticker}), using cached/fallback data")
            except Exception as e:
                # Handle any unexpected errors during API fetch
                logging.error(f"Unexpected error fetching stock data for {symbol} ({real_ticker}): {e}", exc_info=True)
                if stock_info.get("real_price") is None:
                    stock_info["real_price"] = ticker["base_price"]
                    stock_info["shares_outstanding"] = ticker.get("max_shares", 0)
            
            # Small delay between API calls to avoid rate limiting (0.1 second)
            await asyncio.sleep(0.1)
        
        # Get current news multiplier (default 1.0)
        news_multiplier = stock_info.get("news_multiplier", 1.0)
        
        # Calculate final price: real_price * news_multiplier
        real_price = stock_info.get("real_price", ticker["base_price"])
        final_price = real_price * news_multiplier
        
        # Update price
        stock_info["price"] = final_price
        
        # Update price history (keep last 6 minutes)
        price_history = stock_info.get("price_history", [ticker["base_price"]] * 6)
        price_history.append(final_price)
        if len(price_history) > 6:
            price_history.pop(0)
        stock_info["price_history"] = price_history

def get_5min_change(guild_id: int, symbol: str) -> float:
    """Get the percent change over the last 5 minutes."""
    if guild_id not in stock_data or symbol not in stock_data[guild_id]:
        return 0.0
    
    price_history = stock_data[guild_id][symbol]["price_history"]
    if len(price_history) < 6:
        return 0.0
    
    # Price 5 minutes ago is at index -6 (6th from the end), current price is at index -1
    # We keep 6 prices: [5min_ago, 4min_ago, 3min_ago, 2min_ago, 1min_ago, current]
    old_price = price_history[-6]
    current_price = price_history[-1]
    
    if old_price == 0:
        return 0.0
    
    change_percent = ((current_price - old_price) / old_price) * 100
    return change_percent

def get_change_emoji(change_5min: float) -> str:
    """Get emoji based on 5-minute change."""
    if change_5min < -0.1:  # Negative (more than -0.1%)
        return "🔴"
    elif -0.1 <= change_5min <= 0.1:  # Slightly negative or slightly positive
        return "🟨"
    else:  # Positive (more than 0.1%)
        return "🟢"

def calculate_available_shares(guild_id: int, symbol: str) -> int:
    """Calculate available shares by summing all user holdings and subtracting from real shares outstanding."""
    from database import _get_users_collection
    
    ticker_info = next((t for t in STOCK_TICKERS if t["symbol"] == symbol), None)
    if not ticker_info:
        return 0
    
    # Get shares outstanding from stock_data (from API) or fallback to max_shares
    shares_outstanding = ticker_info.get("max_shares", 0)  # Fallback
    if guild_id in stock_data and symbol in stock_data[guild_id]:
        api_shares = stock_data[guild_id][symbol].get("shares_outstanding")
        if api_shares and api_shares > 0:
            shares_outstanding = api_shares
    
    # Get all users' stock holdings for this symbol
    users = _get_users_collection()
    total_owned = 0
    
    # Query all users who have stock holdings
    cursor = users.find({}, {"stock_holdings": 1})
    for doc in cursor:
        holdings = doc.get("stock_holdings", {})
        user_shares = int(holdings.get(symbol, 0))
        if user_shares > 0:
            total_owned += user_shares
    
    available = shares_outstanding - total_owned
    return max(0, available)  # Ensure it doesn't go negative

async def update_marketboard_message(guild: discord.Guild):
    """Update or create the marketboard message in #grow-jones channel."""
    # Find the grow-jones channel
    market_channel = discord.utils.get(guild.text_channels, name="grow-jones")
    
    if not market_channel:
        return  # Channel doesn't exist, skip
    
    # Initialize stocks for this guild
    await initialize_stocks(guild.id)
    
    # Update stock prices
    await update_stock_prices(guild.id)
    
    # Create embed
    embed = discord.Embed(
        title="📈 GROW JONES INDUSTRIAL AVERAGE 📈",
        description="\n\n",
        color=discord.Color.green()
    )
    
    
    # Add each stock to the embed
    stock_lines = []
    for ticker in STOCK_TICKERS:
        symbol = ticker["symbol"]
        stock_info = stock_data[guild.id][symbol]
        current_price = stock_info["price"]
        base_price = ticker["base_price"]
        
        # Get shares outstanding from API data or fallback to max_shares
        shares_outstanding = stock_info.get("shares_outstanding") or ticker.get("max_shares", 0)
        
        # Calculate available shares from database
        available_shares = calculate_available_shares(guild.id, symbol)
        # Update stock_data with calculated available_shares
        stock_info["available_shares"] = available_shares
        
        # Calculate percent increase from base
        percent_from_base = ((current_price - base_price) / base_price) * 100
        percent_sign = "+" if percent_from_base >= 0 else ""
        percent_str = f"{percent_sign}{percent_from_base:.2f}%"
        
        # Calculate 5-minute change
        change_5min = get_5min_change(guild.id, symbol)
        change_emoji = get_change_emoji(change_5min)
        
        # Format 5-minute change with sign
        change_sign = "+" if change_5min >= 0 else ""
        change_str = f"{change_sign}{change_5min:.2f}%"
        
        # Format price
        price_str = f"${current_price:.2f}"
        
        # Format shares as available/max (using real shares outstanding)
        shares_str = f"{available_shares:,}/{shares_outstanding:,}"
        
        # Create stock line
        stock_line = f"**{ticker['name']} ({symbol})**\n"
        stock_line += f"   Price: **{price_str}** | Δ5m: **{change_str}** | Shares: **{shares_str}** {change_emoji}\n"
        
        stock_lines.append(stock_line)
    
    # Combine all stock lines
    embed.description += "\n".join(stock_lines)
    embed.set_footer(text="Last updated")
    embed.timestamp = discord.utils.utcnow()
    
    # Try to edit existing message, or create new one
    guild_id = guild.id
    if guild_id not in leaderboard_messages:
        leaderboard_messages[guild_id] = {}
    
    message_id = leaderboard_messages[guild_id].get("marketboard")
    
    try:
        if message_id:
            # Try to edit existing message
            try:
                message = await market_channel.fetch_message(message_id)
                
                # Always try to edit the existing message, regardless of age
                try:
                    await message.edit(embed=embed)
                    return
                except discord.HTTPException as e:
                    # Check if it's a rate limit error
                    if e.status == 429:
                        # Rate limited, wait and retry once
                        retry_after = e.retry_after if hasattr(e, 'retry_after') else 1.0
                        await asyncio.sleep(retry_after)
                        try:
                            await message.edit(embed=embed)
                            return
                        except discord.HTTPException as retry_e:
                            # If retry also fails, log but don't create new message
                            logging.warning(f"Rate limited retry failed for marketboard in {guild.name}: {retry_e}")
                            return  # Skip this update rather than creating new message
                    elif e.code == 30046:  # Maximum edits to old messages reached
                        # Discord limit reached, but we still want to keep the message
                        # Log and skip this update rather than creating new message
                        logging.warning(f"Maximum edits reached for marketboard message in {guild.name}, skipping update")
                        return
                    else:
                        # Other error, log but don't create new message
                        logging.warning(f"Error editing marketboard in {guild.name}: {e}")
                        return  # Skip this update rather than creating new message
            except discord.NotFound:
                # Message was deleted, search for existing one
                message_id = None
            except discord.HTTPException as e:
                # Other error (permissions, etc.), search for existing one
                if e.status == 429:
                    # Rate limited, skip this update
                    logging.warning(f"Rate limited while fetching marketboard message in {guild.name}, skipping update")
                    return
                logging.warning(f"Error fetching marketboard message in {guild.name}: {e}")
                message_id = None
        
        # If no valid message_id, search for existing marketboard message in channel
        if not message_id:
            try:
                # Search through recent messages to find existing marketboard
                async for message in market_channel.history(limit=50):
                    if message.author.id == bot.user.id and message.embeds:
                        embed_title = message.embeds[0].title if message.embeds[0].title else ""
                        # Check if this is the marketboard message
                        if "GROW JONES INDUSTRIAL AVERAGE" in embed_title:
                            # Found existing message, update it (regardless of age)
                            message_id = message.id
                            leaderboard_messages[guild_id]["marketboard"] = message_id
                            try:
                                await message.edit(embed=embed)
                                return
                            except discord.HTTPException as e:
                                if e.status == 429:
                                    # Rate limited, skip
                                    logging.warning(f"Rate limited while editing marketboard in {guild.name}, skipping update")
                                    return
                                elif e.code == 30046:
                                    # Max edits reached, skip this update but keep the message
                                    logging.warning(f"Maximum edits reached for marketboard message in {guild.name}, skipping update")
                                    return
                                else:
                                    # Other error, try next message or skip
                                    continue
            except discord.HTTPException as e:
                if e.status == 429:
                    logging.warning(f"Rate limited while searching for marketboard message in {guild.name}, skipping update")
                    return
                logging.warning(f"Error searching for existing marketboard message in {guild.name}: {e}")
            except Exception as e:
                logging.error(f"Unexpected error searching for marketboard message in {guild.name}: {e}", exc_info=True)
        
        # Create new message only if we truly couldn't find an existing one (message was deleted)
        # This should be rare - we only create if no message exists at all
        try:
            message = await market_channel.send(embed=embed)
            leaderboard_messages[guild_id]["marketboard"] = message.id
            logging.info(f"Created new marketboard message in {guild.name} (no existing message found)")
        except discord.HTTPException as e:
            if e.status == 429:
                logging.warning(f"Rate limited while creating new marketboard message in {guild.name}, skipping update")
            else:
                logging.error(f"Error creating new marketboard message in {guild.name}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error updating marketboard in {guild.name}: {e}", exc_info=True)

async def update_all_marketboards():
    """Background task to update all marketboards every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Update marketboards for all guilds the bot is in
            for guild in bot.guilds:
                try:
                    await update_marketboard_message(guild)
                    await asyncio.sleep(2)  # Delay after marketboard update
                    # Update leaderboards after stock prices change
                    await update_leaderboard_message(guild, "plants")
                    await asyncio.sleep(2)  # Delay between updates to avoid rate limits
                    await update_leaderboard_message(guild, "money")
                    await asyncio.sleep(2)  # Delay between updates
                    await update_leaderboard_message(guild, "ranks")
                    await asyncio.sleep(2)  # Delay between updates
                except Exception as e:
                    logging.error(f"Error updating marketboard/leaderboards for guild {guild.name}: {e}", exc_info=True)
                # Delay between guilds to prevent rate limiting
                await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Error in marketboard update task: {e}", exc_info=True)
        
        # Wait 60 seconds before next update
        await asyncio.sleep(60)


# MARKET NEWS - News Alert System
# News templates (positive and negative)
POSITIVE_NEWS = [
    "{company} just signed an exclusive deal with ArborTech, integrating new smart-root sensors!",
    "{company} announces a new fertilizer that cuts plant growth time in half",
    "{company} approves the use of new experimental seeds, theorized to increase yield by 30%!",
    "{company} reports record-breaking harvest season with 40% increase in production",
    "{company} launches revolutionary vertical farming initiative in major cities",
    "{company} partners with leading agricultural universities for R&D breakthrough",
    "{company} stock surges after announcing breakthrough in drought-resistant crops",
    "{company} expands operations to three new continents, doubling market reach",
]

NEGATIVE_NEWS = [
    "{company} faces major recall after contaminated seed batch discovered",
    "{company} stock plummets following unexpected crop failure in key regions",
    "{company} under investigation for environmental violations at multiple facilities",
    "{company} reports significant losses due to unexpected pest infestation",
    "{company} CEO steps down amid controversy over pesticide usage",
    "{company} faces lawsuit from farmers over failed crop yields",
    "{company} announces layoffs after disappointing quarterly earnings",
    "{company} stock drops after major client terminates partnership agreement",
]

async def send_market_news(guild: discord.Guild):
    """Send a random news alert to the #market-news channel and affect stock price."""
    try:
        # Find the market-news channel
        news_channel = discord.utils.get(guild.text_channels, name="market-news")
        
        if not news_channel:
            logging.warning(f"Market news channel not found in guild '{guild.name}' (ID: {guild.id}). Skipping market news.")
            return  # Channel doesn't exist, skip
        
        # Check if bot has permission to send messages
        if not news_channel.permissions_for(guild.me).send_messages:
            logging.warning(f"Bot lacks permission to send messages in #market-news channel in guild '{guild.name}' (ID: {guild.id}). Skipping market news.")
            return
        
        # Check if bot can embed links (required for embeds)
        if not news_channel.permissions_for(guild.me).embed_links:
            logging.warning(f"Bot lacks permission to embed links in #market-news channel in guild '{guild.name}' (ID: {guild.id}). Skipping market news.")
            return
        
        # Initialize stocks for this guild if needed
        await initialize_stocks(guild.id)
        
        # Pick a random company
        ticker = random.choice(STOCK_TICKERS)
        company_name = ticker["name"]
        symbol = ticker["symbol"]
        
        # Pick positive or negative news (50/50 chance)
        is_positive = random.choice([True, False])
        
        # Randomly select price change percentage: 1% to 10%
        price_change_percent = random.randint(1, 10) / 100.0
        
        if is_positive:
            news_template = random.choice(POSITIVE_NEWS)
            color = discord.Color.green()
            emoji = "📈"
            price_multiplier = 1 + price_change_percent  # Increase price
        else:
            news_template = random.choice(NEGATIVE_NEWS)
            color = discord.Color.red()
            emoji = "📉"
            price_multiplier = 1 - price_change_percent  # Decrease price
        
        # Apply price change to stock using news multiplier system
        if symbol in stock_data[guild.id]:
            stock_info = stock_data[guild.id][symbol]
            
            # Ensure we have real_price (fetch if needed)
            real_price = stock_info.get("real_price")
            if real_price is None or real_price <= 0:
                # Try to fetch real price if missing
                real_ticker = REAL_STOCK_MAPPING.get(symbol)
                if real_ticker:
                    real_data = await asyncio.to_thread(fetch_real_stock_data, real_ticker)
                    if real_data:
                        real_price = real_data["price"]
                        stock_info["real_price"] = real_price
                        stock_info["shares_outstanding"] = real_data["shares_outstanding"]
                        stock_info["market_cap"] = real_data.get("market_cap")
                    else:
                        # Fallback to base_price if API fails
                        real_price = ticker["base_price"]
                        stock_info["real_price"] = real_price
                else:
                    real_price = ticker["base_price"]
                    stock_info["real_price"] = real_price
            
            # Get current news multiplier (default 1.0)
            current_multiplier = stock_info.get("news_multiplier", 1.0)
            
            # Apply new news multiplier (cumulative)
            new_multiplier = current_multiplier * price_multiplier
            stock_info["news_multiplier"] = new_multiplier
            
            # Calculate final price: real_price * news_multiplier
            final_price = real_price * new_multiplier
            stock_info["price"] = final_price
            
            # Update price history (keep last 6 minutes)
            price_history = stock_info.get("price_history", [real_price] * 6)
            price_history.append(final_price)
            if len(price_history) > 6:
                price_history.pop(0)
            stock_info["price_history"] = price_history
            
            price_change_display = f"{'+' if is_positive else '-'}{price_change_percent * 100:.0f}%"
        else:
            # Stock not initialized, skip price update
            price_change_display = f"{'+' if is_positive else '-'}{price_change_percent * 100:.0f}%"
        
        # Format the news message with company name
        news_message = news_template.format(company=company_name)
        
        # Create embed
        embed = discord.Embed(
            title=f"{emoji} ***THIS JUST IN!***",
            description=news_message,
            color=color
        )
        embed.add_field(name="Company", value=f"**{company_name} ({symbol})**", inline=True)
        embed.add_field(name="Price Impact", value=f"**{price_change_display}**", inline=True)
        embed.timestamp = discord.utils.utcnow()
        
        await news_channel.send(embed=embed)
        logging.info(f"Successfully sent market news for {company_name} ({symbol}) in guild '{guild.name}' (ID: {guild.id})")
        
    except discord.Forbidden:
        logging.error(f"Forbidden error sending market news in guild '{guild.name}' (ID: {guild.id}): Bot lacks permissions")
    except discord.HTTPException as e:
        logging.error(f"HTTP error sending market news in guild '{guild.name}' (ID: {guild.id}): {e}")
    except Exception as e:
        logging.error(f"Unexpected error sending market news in guild '{guild.name}' (ID: {guild.id}): {e}", exc_info=True)

async def send_market_news_loop():
    """Background task to send market news alerts at random intervals."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(10)
    
    logging.info(f"Market news loop started. Bot is in {len(bot.guilds)} guild(s)")
    
    while not bot.is_closed():
        try:
            # Send news to all guilds the bot is in
            guilds_processed = 0
            for guild in bot.guilds:
                try:
                    await send_market_news(guild)
                    guilds_processed += 1
                    await asyncio.sleep(1)  # Small delay between guilds
                except Exception as e:
                    # Log error but continue with other guilds
                    logging.error(f"Error processing market news for guild '{guild.name}' (ID: {guild.id}): {e}", exc_info=True)
            
            if guilds_processed > 0:
                logging.info(f"Market news cycle completed. Processed {guilds_processed} guild(s)")
            else:
                logging.warning("Market news cycle completed but no guilds were processed (no valid channels found?)")
                
        except Exception as e:
            logging.error(f"Critical error in market news task: {e}", exc_info=True)
        
        # Wait random interval between 2-5 minutes (120-300 seconds)
        wait_time = random.randint(120, 300)
        logging.info(f"Market news loop: Waiting {wait_time} seconds until next cycle")
        await asyncio.sleep(wait_time)


# CRYPTOCURRENCY SYSTEM
# Cryptocurrency definitions
CRYPTO_COINS = [
    {"name": "RootCoin", "symbol": "RTC", "base_price": 90000.0},
    {"name": "Terrarium", "symbol": "TER", "base_price": 3100.0},
    {"name": "Canopy", "symbol": "CNY", "base_price": 855.0},
]

# Crypto price history storage: {symbol: [float]} - keeps last 6 prices (5 minutes + current)
crypto_price_history = {}

def initialize_crypto_history():
    """Initialize crypto price history if not already initialized."""
    global crypto_price_history
    if not crypto_price_history:
        for coin in CRYPTO_COINS:
            base_price = coin["base_price"]
            crypto_price_history[coin["symbol"]] = [base_price] * 6

async def fetch_real_crypto_prices() -> dict[str, float] | None:
    """Fetch real-world cryptocurrency prices from CoinGecko API (free tier, rate-limited).
    
    Returns:
        dict mapping game symbols to prices: {"RTC": btc_price, "TER": eth_price, "CNY": bnb_price}
        Returns None if API call fails
    """
    try:
        async with aiohttp.ClientSession() as session:
            # CoinGecko API endpoint - fetches all three coins in one request
            # Maps: bitcoin -> RTC, ethereum -> TER, binancecoin -> CNY
            url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,binancecoin&vs_currencies=usd"
            
            # Mapping from CoinGecko coin IDs to game symbols
            coin_mapping = {
                "bitcoin": "RTC",
                "ethereum": "TER",
                "binancecoin": "CNY"
            }
            
            timeout = aiohttp.ClientTimeout(total=10)  # 10 second timeout
            
            try:
                logging.debug(f"Fetching crypto prices from CoinGecko: {url}")
                async with session.get(url, timeout=timeout) as response:
                    if response.status == 200:
                        data = await response.json()
                        prices = {}
                        
                        # Extract prices from CoinGecko response
                        for coin_id, symbol in coin_mapping.items():
                            if coin_id in data and "usd" in data[coin_id]:
                                price = float(data[coin_id]["usd"])
                                prices[symbol] = price
                                logging.info(f"Successfully fetched {symbol} price: {price}")
                            else:
                                logging.warning(f"CoinGecko response missing data for {coin_id} ({symbol})")
                        
                        if not prices:
                            logging.error("CoinGecko API returned empty price data")
                            return None
                        
                        logging.info(f"Fetched {len(prices)} crypto prices successfully: {prices}")
                        return prices
                    elif response.status == 429:
                        # Rate limit exceeded
                        logging.warning(f"CoinGecko API rate limit exceeded (429). Will retry on next cycle.")
                        return None
                    else:
                        logging.warning(f"CoinGecko API returned status {response.status}")
                        return None
                        
            except (aiohttp.ClientError, asyncio.TimeoutError, KeyError, ValueError) as e:
                logging.warning(f"Error fetching crypto prices from CoinGecko: {e}", exc_info=True)
                return None
            
    except Exception as e:
        logging.error(f"Error in fetch_real_crypto_prices: {e}", exc_info=True)
        return None

async def update_crypto_prices_market():
    """Update cryptocurrency prices with real-world prices from CoinGecko API."""
    # Initialize history if needed
    initialize_crypto_history()
    
    # Get current prices as fallback
    current_prices = get_crypto_prices()
    
    # Fetch real-world prices
    real_prices = await fetch_real_crypto_prices()
    
    # If ALL API calls failed, keep current prices and return early
    if real_prices is None:
        logging.warning("Failed to fetch real crypto prices, keeping current prices")
        return current_prices
    
    # Log successful fetch
    logging.info(f"Successfully fetched crypto prices: {real_prices}")
    
    # Update prices with real-world data
    prices = {}
    for coin in CRYPTO_COINS:
        symbol = coin["symbol"]
        new_price = real_prices.get(symbol)
        
        # If we got a price for this symbol, use it
        if new_price is not None and new_price > 0:
            prices[symbol] = new_price
            logging.info(f"Updated {symbol} price: {current_prices.get(symbol, 'N/A')} -> {new_price}")
        else:
            # Keep the previous price if API didn't return a valid price for this symbol
            prices[symbol] = current_prices.get(symbol, coin["base_price"])
            logging.warning(f"Keeping previous price for {symbol}: {prices[symbol]} (API didn't return valid price)")
        
        # Update price history (keep last 6 prices)
        if symbol not in crypto_price_history:
            crypto_price_history[symbol] = [coin["base_price"]] * 6
        price_history = crypto_price_history[symbol]
        price_history.append(prices[symbol])
        if len(price_history) > 6:
            price_history.pop(0)
    
    # Update prices in database
    update_crypto_prices(prices)
    logging.info(f"Updated crypto prices in database: {prices}")
    return prices

def get_crypto_5min_change(symbol: str) -> float:
    """Get the percent change over the last 5 minutes for a crypto coin."""
    initialize_crypto_history()
    
    if symbol not in crypto_price_history:
        return 0.0
    
    price_history = crypto_price_history[symbol]
    if len(price_history) < 6:
        return 0.0
    
    # Price 5 minutes ago is at index -6 (6th from the end), current price is at index -1
    old_price = price_history[-6]
    current_price = price_history[-1]
    
    if old_price == 0:
        return 0.0
    
    change_percent = ((current_price - old_price) / old_price) * 100
    return change_percent

def get_crypto_change_emoji(change_5min: float) -> str:
    """Get emoji based on 5-minute change for crypto."""
    if change_5min < -0.1:  # Negative (more than -0.1%)
        return "🔴"
    elif -0.1 <= change_5min <= 0.1:  # Slightly negative or slightly positive
        return "🟨"
    else:  # Positive (more than 0.1%)
        return "🟢"

async def update_coinbase_message(guild: discord.Guild):
    """Update or create the coinbase message in #coinbase channel."""
    # Find the coinbase channel
    coinbase_channel = discord.utils.get(guild.text_channels, name="coinbase")
    
    if not coinbase_channel:
        return  # Channel doesn't exist, skip
    
    try:
        # Initialize crypto history if needed
        initialize_crypto_history()
        
        # Get current prices
        prices = get_crypto_prices()
        
        # Create embed
        embed = discord.Embed(
            title="💰 CRYPTO MARKET 💰",
            description="\n\n",
            color=discord.Color.blue()
        )
        
        # Add each coin to the embed
        coin_lines = []
        for coin in CRYPTO_COINS:
            symbol = coin["symbol"]
            current_price = prices.get(symbol, coin["base_price"])
            base_price = coin["base_price"]
            
            # Calculate percent increase from base
            percent_from_base = ((current_price - base_price) / base_price) * 100
            percent_sign = "+" if percent_from_base >= 0 else ""
            percent_str = f"{percent_sign}{percent_from_base:.2f}%"
            
            # Calculate 5-minute change
            change_5min = get_crypto_5min_change(symbol)
            change_emoji = get_crypto_change_emoji(change_5min)
            
            # Format 5-minute change with sign
            change_sign = "+" if change_5min >= 0 else ""
            change_str = f"{change_sign}{change_5min:.2f}%"
            
            # Format price
            price_str = f"${current_price:.2f}"
            
            # Create coin line
            coin_line = f"**{coin['name']} ({symbol})**\n"
            coin_line += f"   Price: **{price_str}** | Δ5m: **{change_str}** {change_emoji}\n"
            
            coin_lines.append(coin_line)
        
        # Combine all coin lines
        embed.description += "\n".join(coin_lines)
        embed.set_footer(text="Last updated")
        embed.timestamp = discord.utils.utcnow()
        
        # Try to edit existing message, or create new one
        async for message in coinbase_channel.history(limit=50):
            if message.author == bot.user and message.embeds and message.embeds[0].title == "💰 CRYPTO MARKET 💰":
                await message.edit(embed=embed)
                return
        
        # No existing message found, create new one
        await coinbase_channel.send(embed=embed)
        
    except Exception as e:
        print(f"Error updating coinbase in {guild.name}: {e}")

async def update_all_coinbase():
    """Background task to update all coinbase channels every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(5)
    
    logging.info("Coinbase update task started")
    
    while not bot.is_closed():
        try:
            # Update prices first (now async)
            logging.info("Starting crypto price update...")
            await update_crypto_prices_market()
            
            # Update coinbase channels for all guilds the bot is in
            for guild in bot.guilds:
                await update_coinbase_message(guild)
                await asyncio.sleep(1)  # Small delay between updates
        except Exception as e:
            logging.error(f"Error in coinbase update task: {e}", exc_info=True)
        
        # Wait 60 seconds before next update
        await asyncio.sleep(60)


async def gardener_background_task():
    """Background task to check gardener actions every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for bot to fully initialize
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Get all users with gardeners
            users_with_gardeners = get_all_users_with_gardeners()
            
            for user_id, gardeners in users_with_gardeners:
                # Process each gardener
                for gardener in gardeners:
                    gardener_id = gardener.get("id")
                    if not gardener_id:
                        continue
                    
                    # Get chance based on gardener level
                    gardener_chance = GARDENER_CHANCES.get(gardener_id, 0.05)  # Default to 5% if invalid ID
                    if random.random() < gardener_chance:
                        try:
                            # Stacked tool chance: any gardener with a tool can trigger harvest upgrade
                            total_harvest_upgrade_chance = sum(
                                GARDENER_TOOLS.get(g["id"], {}).get("chance", 0)
                                for g in gardeners
                                if g.get("has_tool")
                            )
                            upgraded_to_harvest = total_harvest_upgrade_chance > 0 and random.random() < total_harvest_upgrade_chance
                            
                            if upgraded_to_harvest:
                                # Perform full harvest instead of single gather (orchard upgrades apply; no chain)
                                harvest_result = await perform_harvest_for_user(user_id, allow_chain=False)
                                total_value = harvest_result["total_value"]
                                current_balance = harvest_result["current_balance"]
                                item_count = len(harvest_result["gathered_items"])
                                
                                # Update gardener stats with total money earned from harvest and plant count
                                update_gardener_stats(user_id, gardener_id, total_value, plants_count=item_count)
                                
                                # Send cool upgrade message to #lawn
                                for guild in bot.guilds:
                                    member = guild.get_member(user_id)
                                    if member:
                                        lawn_channel = discord.utils.get(guild.text_channels, name="lawn")
                                        if lawn_channel and lawn_channel.permissions_for(guild.me).send_messages:
                                            try:
                                                mention = member.mention
                                                embed = discord.Embed(
                                                    title="🌾✨ GATHER UPGRADED TO HARVEST! ✨🌾",
                                                    description=f"{mention}, **the gardener's tool sparked!**",
                                                    color=discord.Color.gold()
                                                )
                                                
                                                # Display actual items (up to 20)
                                                items_text = ""
                                                for item in harvest_result["gathered_items"][:20]:
                                                    gmo_text = " GMO! ✨" if item["is_gmo"] else ""
                                                    items_text += f"• **{item['name']}** - ${item['value']:.2f} ({item['ripeness']}){gmo_text}\n"
                                                
                                                embed.add_field(name="📦 Items Harvested", value=items_text or "No items", inline=False)
                                                embed.add_field(name="💰 Total Value", value=f"**${total_value:,.2f}**", inline=True)
                                                embed.add_field(name="💵 New Balance", value=f"**${current_balance:,.2f}**", inline=True)
                                                await lawn_channel.send(embed=embed)
                                                break
                                            except Exception as e:
                                                print(f"Error sending gardener harvest-upgrade notification to #lawn in {guild.name} for user {user_id}: {e}")
                                        break
                            else:
                                # Normal single gather (orchard fertilizer applies; no chain)
                                gather_result = await perform_gather_for_user(user_id, apply_cooldown=False, apply_orchard_fertilizer=True)
                                # Single gather = 1 plant
                                update_gardener_stats(user_id, gardener_id, gather_result["value"], plants_count=1)
                                
                                user_name = "User"
                                for guild in bot.guilds:
                                    member = guild.get_member(user_id)
                                    if member:
                                        user_name = member.display_name or member.name
                                        break
                                
                                for guild in bot.guilds:
                                    member = guild.get_member(user_id)
                                    if member:
                                        lawn_channel = discord.utils.get(guild.text_channels, name="lawn")
                                        if lawn_channel:
                                            try:
                                                if lawn_channel.permissions_for(guild.me).send_messages:
                                                    embed = discord.Embed(
                                                        title=f"🌿 {user_name}'s Gardener gathered!",
                                                        description=f"Their gardener found a **{gather_result['name']}**!",
                                                        color=discord.Color.green()
                                                    )
                                                    embed.add_field(name="Value", value=f"**${gather_result['value']:.2f}**", inline=True)
                                                    embed.add_field(name="Ripeness", value=gather_result['ripeness'], inline=True)
                                                    embed.add_field(name="GMO?", value="Yes ✨" if gather_result['is_gmo'] else "No", inline=False)
                                                    await lawn_channel.send(embed=embed)
                                                    break
                                            except Exception as e:
                                                print(f"Error sending gardener notification to #lawn channel in {guild.name} for user {user_id}: {e}")
                                        break
                        except Exception as e:
                            print(f"Error processing gather for gardener {gardener_id} of user {user_id}: {e}")
            
            # Small delay to avoid overwhelming the system
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in gardener background task: {e}")
        
        # Wait 60 seconds (1 minute) before next check
        await asyncio.sleep(60)


async def gpu_background_task():
    """Background task to check GPU mining actions every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for bot to fully initialize
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Get all users with GPUs
            users_with_gpus = get_all_users_with_gpus()
            
            for user_id, gpus in users_with_gpus:
                # Process each GPU
                for gpu_name in gpus:
                    # Find GPU info in shop to get tier index and percent_increase
                    gpu_info = None
                    tier_index = 0
                    for idx, gpu in enumerate(GPU_SHOP):
                        if gpu["name"] == gpu_name:
                            gpu_info = gpu
                            tier_index = idx
                            break
                    
                    if not gpu_info:
                        continue  # Skip if GPU not found in shop
                    
                    # Calculate mining chance based on GPU tier
                    # Formula: base_chance * (1 + tier_index * 0.5) where base_chance = 0.03 (3%)
                    base_chance = 0.03
                    mining_chance = base_chance * (1 + tier_index * 0.5)
                    
                    if random.random() < mining_chance:
                        try:
                            # Randomly select a coin to mine
                            coin = random.choice(CRYPTO_COINS)
                            symbol = coin["symbol"]
                            base_price = coin["base_price"]
                            
                            # Calculate mining amount based on coin's base price (proportional to old 200.0 base)
                            # Target: $50-60 per session average (assuming up to 60 clicks per 60s session, 1 per second)
                            # This means ~$0.83-$1.00 per click average, so $0.60-$1.40 range with RNG
                            # At $200 base: $0.60-$1.40 = 0.003-0.007 coins = 30-70 thousandths
                            # New system: scale by price ratio (200.0 / base_price)
                            price_ratio = 200.0 / base_price
                            # Reduced range: 30-70 thousandths (0.003-0.007) at $200 base
                            # Scaled range: multiply by price_ratio
                            min_thousandths = int(30 * price_ratio)
                            max_thousandths = int(70 * price_ratio)
                            # Ensure at least 1 thousandth
                            min_thousandths = max(1, min_thousandths)
                            max_thousandths = max(min_thousandths, max_thousandths)
                            random_thousandths = random.randint(min_thousandths, max_thousandths)
                            base_amount = round(random_thousandths / 10000, 4)
                            
                            # Apply GPU percent boost
                            percent_increase = gpu_info["percent_increase"]
                            percent_multiplier = 1.0 + (percent_increase / 100.0)
                            amount = round(base_amount * percent_multiplier, 4)
                            
                            # Add crypto to user's holdings
                            update_user_crypto_holdings(user_id, symbol, amount)
                            
                            # Optional: Could send notification to #gathercoin channel
                            # (Skipping for now to reduce spam, similar to plan note)
                            
                        except Exception as e:
                            print(f"Error processing GPU mining for {gpu_name} of user {user_id}: {e}")
            
            # Small delay to avoid overwhelming the system
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in GPU background task: {e}")
        
        # Wait 60 seconds (1 minute) before next check
        await asyncio.sleep(60)


# Event system functions
async def send_event_start_embed(guild: discord.Guild, event: dict, duration_minutes: int):
    """Send event start embed to #events channel."""
    # Try exact match first
    events_channel = discord.utils.get(guild.text_channels, name="events")
    
    # If not found, try case-insensitive search
    if not events_channel:
        for channel in guild.text_channels:
            if channel.name.lower() == "events":
                events_channel = channel
                break
    
    # If still not found, try to find any channel with "event" in the name
    if not events_channel:
        for channel in guild.text_channels:
            if "event" in channel.name.lower():
                events_channel = channel
                break
    
    if not events_channel:
        print(f"ERROR: #events channel not found in {guild.name}. Available text channels: {[ch.name for ch in guild.text_channels]}")
        return False
    
    # Check if bot has permission to send messages in the channel
    if not events_channel.permissions_for(guild.me).send_messages:
        print(f"ERROR: Bot does not have permission to send messages in #events channel in {guild.name}")
        return False
    
    event_info = None
    if event["event_type"] == "hourly":
        event_info = next((e for e in HOURLY_EVENTS if e["id"] == event["event_id"]), None)
    elif event["event_type"] == "daily":
        event_info = next((e for e in DAILY_EVENTS if e["id"] == event["event_id"]), None)
    
    if not event_info:
        print(f"ERROR: Event info not found for event_id={event.get('event_id')}, event_type={event.get('event_type')} in {guild.name}")
        return False
    
    event_name = event_info['name'].rstrip('!')
    embed = discord.Embed(
        title=f"{event_info['emoji']} {event_name} Event Has Started!",
        description=event_info["description"],
        color=discord.Color.green()
    )
    # Display duration appropriately (seconds if < 1 minute, minutes otherwise)
    if duration_minutes < 1:
        duration_seconds = int(duration_minutes * 60)
        duration_display = f"{duration_seconds} Seconds"
    else:
        duration_display = f"{duration_minutes} Minutes"
    embed.add_field(name="Duration", value=duration_display, inline=False)
    embed.add_field(name="Effect", value=event_info["effect"], inline=False)
    embed.set_footer(text="Go /gather!!")
    
    try:
        await events_channel.send("@here", embed=embed)
        return True
    except Exception as e:
        print(f"ERROR sending event start embed in {guild.name}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def send_event_end_embed(guild: discord.Guild, event: dict):
    """Send event end embed to #events channel."""
    # Try exact match first
    events_channel = discord.utils.get(guild.text_channels, name="events")
    
    # If not found, try case-insensitive search
    if not events_channel:
        for channel in guild.text_channels:
            if channel.name.lower() == "events":
                events_channel = channel
                break
    
    # If still not found, try to find any channel with "event" in the name
    if not events_channel:
        for channel in guild.text_channels:
            if "event" in channel.name.lower():
                events_channel = channel
                break
    
    if not events_channel:
        print(f"ERROR: #events channel not found in {guild.name}. Available text channels: {[ch.name for ch in guild.text_channels]}")
        return False
    
    # Check if bot has permission to send messages in the channel
    if not events_channel.permissions_for(guild.me).send_messages:
        print(f"ERROR: Bot does not have permission to send messages in #events channel in {guild.name}")
        return False
    
    # Get the actual event ID from effects (not the database event_id)
    event_type_id = event.get("effects", {}).get("event_id")
    if not event_type_id:
        print(f"No event_id found in effects for event: {event}")
        return
    
    event_info = None
    if event["event_type"] == "hourly":
        event_info = next((e for e in HOURLY_EVENTS if e["id"] == event_type_id), None)
    elif event["event_type"] == "daily":
        event_info = next((e for e in DAILY_EVENTS if e["id"] == event_type_id), None)
    
    if not event_info:
        print(f"Event info not found for {event_type_id} in {event['event_type']} events")
        return
    
    event_name = event_info['name'].rstrip('!')
    embed = discord.Embed(
        title=f"{event_info['emoji']} {event_name} Event Has Ended",
        description="Event has ended! Forest conditions go back to normal.\n\nThe event is over. Stay tuned for any future events..",
        color=discord.Color.red()
    )
    
    try:
        await events_channel.send(embed=embed)
    except Exception as e:
        print(f"Error sending event end embed in {guild.name}: {e}")


async def hourly_event_check():
    """Background task to trigger hourly events at configurable intervals with 50% chance."""
    await bot.wait_until_ready()
    
    # Wait a short initial delay before first check
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # CRITICAL: Clean up expired events BEFORE checking for existing events
            # This prevents stuck/expired events from blocking new events
            clear_expired_events()
            
            # Check if there's already an active hourly event
            existing_hourly = get_active_event_by_type("hourly")
            if existing_hourly:
                # Verify the event is actually still valid (double-check)
                current_time = time.time()
                if existing_hourly.get("end_time", 0) > current_time:
                    # Event already active and valid, skip this hour
                    print(f"Skipping hourly event - event already active: {existing_hourly['event_name']} (ends at {existing_hourly.get('end_time', 0)})")
                else:
                    # Event found but expired, clean it up and proceed
                    print(f"Found expired hourly event: {existing_hourly['event_name']}, cleaning up and proceeding")
                    clear_event(existing_hourly.get("event_id", ""))
                    existing_hourly = None
            
            if not existing_hourly:
                # 50% chance to trigger an event
                if random.random() < 0.5:
                    # Select random hourly event
                    event_info = random.choice(HOURLY_EVENTS)
                    
                    # Random duration: 40% = 30min, 35% = 45min, 25% = 60min
                    rand = random.random()
                    if rand < 0.40:
                        duration_minutes = 30
                    elif rand < 0.75:  # 0.40 + 0.35
                        duration_minutes = 45
                    else:
                        duration_minutes = 60
                    
                    duration_seconds = duration_minutes * 60
                    start_time = time.time()
                    end_time = start_time + duration_seconds
                    
                    # Create event ID
                    event_id = f"hourly_{int(start_time)}_{event_info['id']}"
                    
                    # Store event
                    set_active_event(
                        event_id=event_id,
                        event_type="hourly",
                        event_name=event_info["name"],
                        start_time=start_time,
                        end_time=end_time,
                        effects={"event_id": event_info["id"]}
                    )
                    
                    # Send announcement to all guilds
                    for guild in bot.guilds:
                        try:
                            await send_event_start_embed(guild, {
                                "event_type": "hourly",
                                "event_id": event_info["id"],
                                "event_name": event_info["name"]
                            }, duration_minutes)
                            print(f"Sent start embed to #events channel in {guild.name} for hourly event: {event_info['name']}")
                        except Exception as e:
                            print(f"Error sending start embed to {guild.name} for hourly event: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    print(f"Started hourly event: {event_info['name']} for {duration_minutes} minutes")
                    
                    # Wait until 5 seconds before event ends
                    wait_seconds = duration_seconds - 5
                    await asyncio.sleep(wait_seconds)
                    
                    # Send end message 5 seconds before event actually ends
                    event = {
                        "event_id": event_id,
                        "event_type": "hourly",
                        "event_name": event_info["name"],
                        "start_time": start_time,
                        "end_time": end_time,
                        "effects": {"event_id": event_info["id"]}
                    }
                    
                    # Send end embed to #events channel in all guilds
                    for guild in bot.guilds:
                        try:
                            await send_event_end_embed(guild, event)
                            print(f"Sent end embed to #events channel in {guild.name}")
                        except Exception as e:
                            print(f"Error sending end embed to {guild.name}: {e}")
                    
                    print(f"Sent end message for hourly event: {event_info['name']} (5 seconds remaining)")
                    
                    # Wait for remaining 5 seconds until event actually ends
                    await asyncio.sleep(5)
                    
                    # Remove event from database (ensure it's cleared)
                    clear_event(event_id)
                    clear_expired_events()  # Double-check cleanup
                    print(f"Ended hourly event: {event_info['name']} and cleared from database")
            
            # Wait for the configured interval before next check
            print(f"Hourly event check completed. Waiting {HOURLY_EVENT_INTERVAL} seconds until next check...")
            await asyncio.sleep(HOURLY_EVENT_INTERVAL)
            
        except Exception as e:
            print(f"Error in hourly_event_check: {e}")
            import traceback
            traceback.print_exc()
            # Wait for the configured interval on error
            await asyncio.sleep(HOURLY_EVENT_INTERVAL)


async def daily_event_check():
    """Background task to trigger daily events at configurable intervals with 10% chance."""
    await bot.wait_until_ready()
    
    # Wait a short initial delay before first check
    await asyncio.sleep(10)
    
    while not bot.is_closed():
        try:
            # CRITICAL: Clean up expired events BEFORE checking for existing events
            # This prevents stuck/expired events from blocking new events
            clear_expired_events()
            
            # Check if there's already an active daily event
            existing_daily = get_active_event_by_type("daily")
            if existing_daily:
                # Verify the event is actually still valid (double-check)
                current_time = time.time()
                if existing_daily.get("end_time", 0) > current_time:
                    # Event already active and valid, skip this day
                    print(f"Skipping daily event - event already active: {existing_daily['event_name']} (ends at {existing_daily.get('end_time', 0)})")
                else:
                    # Event found but expired, clean it up and proceed
                    print(f"Found expired daily event: {existing_daily['event_name']}, cleaning up and proceeding")
                    clear_event(existing_daily.get("event_id", ""))
                    existing_daily = None
            
            if not existing_daily:
                # 10% chance to trigger an event
                if random.random() < 0.10:
                    # Select random daily event
                    event_info = random.choice(DAILY_EVENTS)
                    
                    # Fixed 24 hour duration
                    duration_minutes = 24 * 60
                    duration_seconds = duration_minutes * 60
                    start_time = time.time()
                    end_time = start_time + duration_seconds
                    
                    # Create event ID
                    event_id = f"daily_{int(start_time)}_{event_info['id']}"
                    
                    # Store event
                    set_active_event(
                        event_id=event_id,
                        event_type="daily",
                        event_name=event_info["name"],
                        start_time=start_time,
                        end_time=end_time,
                        effects={"event_id": event_info["id"]}
                    )
                    
                    # Send announcement to all guilds
                    for guild in bot.guilds:
                        try:
                            await send_event_start_embed(guild, {
                                "event_type": "daily",
                                "event_id": event_info["id"],
                                "event_name": event_info["name"]
                            }, duration_minutes)
                            print(f"Sent start embed to #events channel in {guild.name} for daily event: {event_info['name']}")
                        except Exception as e:
                            print(f"Error sending start embed to {guild.name} for daily event: {e}")
                            import traceback
                            traceback.print_exc()
                    
                    print(f"Started daily event: {event_info['name']} for 24 hours")
                    
                    # Wait until 5 seconds before event ends
                    wait_seconds = duration_seconds - 5
                    await asyncio.sleep(wait_seconds)
                    
                    # Send end message 5 seconds before event actually ends
                    event = {
                        "event_id": event_id,
                        "event_type": "daily",
                        "event_name": event_info["name"],
                        "start_time": start_time,
                        "end_time": end_time,
                        "effects": {"event_id": event_info["id"]}
                    }
                    
                    # Send end embed to #events channel in all guilds
                    for guild in bot.guilds:
                        try:
                            await send_event_end_embed(guild, event)
                            print(f"Sent end embed to #events channel in {guild.name}")
                        except Exception as e:
                            print(f"Error sending end embed to {guild.name}: {e}")
                    
                    print(f"Sent end message for daily event: {event_info['name']} (5 seconds remaining)")
                    
                    # Wait for remaining 5 seconds until event actually ends
                    await asyncio.sleep(5)
                    
                    # Remove event from database
                    clear_event(event_id)
                    clear_expired_events()
                    print(f"Ended daily event: {event_info['name']} and cleared from database")
            
            # Wait for the configured interval before next check
            print(f"Daily event check completed. Waiting {DAILY_EVENT_INTERVAL} seconds until next check...")
            await asyncio.sleep(DAILY_EVENT_INTERVAL)
            
        except Exception as e:
            print(f"Error in daily_event_check: {e}")
            import traceback
            traceback.print_exc()
            # Wait for the configured interval on error
            await asyncio.sleep(DAILY_EVENT_INTERVAL)


async def event_cleanup_task():
    """Background task to clean up any orphaned expired events."""
    await bot.wait_until_ready()
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Clean up any expired events in database (shouldn't be needed with timing-based approach, but safety net)
            clear_expired_events()
            await asyncio.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Error in event_cleanup_task: {e}")
            await asyncio.sleep(60)


# Mining View with button
class MiningView(discord.ui.View):
    def __init__(self, user_id: int, message=None, timeout=60, gpu_percent_boost=0, gpu_seconds_boost=0, gpus_used=None):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.message = message  # Store message reference for timeout updates
        self.start_time = None  # Track when the session actually starts (after first button click)
        self.session_started = False  # Track if session has been started
        self.total_mines = 0
        self.session_mined = {}  # Track coins mined in this session: {symbol: amount}
        self.session_value = 0.0  # Total value mined in this session (base value only)
        self.timed_out = False  # Track if session has timed out
        self.last_embed_update = 0.0  # Track last embed update time for rate limiting
        self.timer_task = None  # Background task for monitoring timer
        # Ensure GPU boosts are numbers (convert to float/int if needed)
        self.gpu_percent_boost = float(gpu_percent_boost) if gpu_percent_boost else 0.0  # Total percent increase from GPUs
        self.gpu_seconds_boost = int(gpu_seconds_boost) if gpu_seconds_boost else 0  # Total seconds increase from GPUs
        self.gpus_used = gpus_used if gpus_used else []  # List of GPU names being used
        self.blockchain_achievement_unlocked = False  # Track if Blockchain achievement was unlocked
    
    async def _timer_monitor_task(self):
        """Background task that monitors the timer and disables button when time expires."""
        max_time = 60 + self.gpu_seconds_boost
        
        while not self.timed_out and self.session_started and self.start_time:
            current_time = time.time()
            elapsed_time = current_time - self.start_time
            time_remaining = max_time - elapsed_time
            
            if time_remaining <= 0:
                # Time expired - disable button and show expiration message instantly
                if not self.timed_out:
                    # Disable the button immediately
                    for item in self.children:
                        item.disabled = True
                    # Update the message with disabled button state immediately
                    if self.message:
                        try:
                            await self.message.edit(view=self)
                        except Exception as e:
                            print(f"Error disabling button: {e}")
                    # Handle timeout (this will set timed_out and show the expiration message)
                    await self._handle_timeout()
                break
            
            # Update embed periodically (every 0.5 seconds) to show countdown
            await self._update_timer_embed(time_remaining, max_time)
            await asyncio.sleep(0.5)
    
    async def _update_timer_embed(self, time_remaining: float, max_time: int, force_update: bool = False):
        """Update the embed with the current timer countdown. Rate limited to avoid spam."""
        if self.timed_out or not self.message:
            return
        
        # Rate limit embed updates: only update every 0.5 seconds or if forced
        current_time = time.time()
        if not force_update and (current_time - self.last_embed_update) < 0.5:
            return
        
        self.last_embed_update = current_time
        
        # Ensure time_remaining is not negative
        time_remaining = max(0, time_remaining)
        
        # Create session summary
        session_summary = ""
        for sym, amt in self.session_mined.items():
            coin_name = next(c["name"] for c in CRYPTO_COINS if c["symbol"] == sym)
            session_summary += f"{coin_name} ({sym}): {amt:.4f}\n"
        
        # Create GPU info text
        gpu_text = ""
        if self.gpus_used:
            gpu_text = "\n".join(self.gpus_used)
        
        # Create description with countdown timer
        description_text = f"Click the button as many times as you can in {max_time} seconds!"
        if self.gpu_percent_boost > 0:
            description_text += f"\n💰 **GPU Boost: +{self.gpu_percent_boost}%**"
        
        # Show time remaining in integer seconds - BOLD THE SECONDS
        description_text += f"\n\n⏰ Time Remaining: **{int(time_remaining)}** seconds"
        
        success_embed = discord.Embed(
            title="⛏️ /mine",
            description=description_text,
            color=discord.Color.light_grey()
        )
        success_embed.add_field(name="This Session", value=f"Total Mines: **{self.total_mines}**", inline=True)
        if gpu_text:
            success_embed.add_field(name="GPUs Active", value=gpu_text, inline=False)
        if session_summary:
            success_embed.add_field(name="Session Mined", value=session_summary.strip(), inline=False)
        success_embed.set_footer(text="Keep clicking! (Use /sell to sell your cryptocurrency!)")
        
        try:
            await self.message.edit(embed=success_embed, view=self)
        except Exception as e:
            # If edit fails (e.g., message deleted), just log and continue
            print(f"Error updating timer embed: {e}")
    
    async def _handle_timeout(self):
        """Handle timeout by updating the message with timeout embed."""
        if self.timed_out:
            return  # Already handled
        
        self.timed_out = True
        
        # Cancel timer task if it's still running
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
            try:
                await self.timer_task
            except asyncio.CancelledError:
                pass
        
        # Disable the button
        for item in self.children:
            item.disabled = True
        
        # Create timeout embed
        timeout_embed = discord.Embed(
            title="⏰ Mining Session Expired",
            description="Time's up! Your mining session has ended.",
            color=discord.Color.orange()
        )
        
        if self.total_mines > 0:
            timeout_embed.add_field(
                name="Session Summary",
                value=f"Total Mines: **{self.total_mines}**",
                inline=False
            )
            
            # Add GPU info if GPUs were used
            if self.gpus_used:
                timeout_embed.add_field(name="GPUs Used", value="\n".join(self.gpus_used), inline=False)
            
            session_summary = ""
            for sym, amt in self.session_mined.items():
                coin_name = next(c["name"] for c in CRYPTO_COINS if c["symbol"] == sym)
                session_summary += f"{coin_name} ({sym}): {amt:.4f}\n"
            
            if session_summary:
                timeout_embed.add_field(name="Mined This Session", value=session_summary.strip(), inline=False)
            
            # Add hidden achievement message if unlocked
            if self.blockchain_achievement_unlocked:
                timeout_embed.add_field(name="🎉 Hidden Achievement Unlocked!", value="**Blockchain**", inline=False)

            timeout_embed.set_footer(text="Use /sell to sell your cryptocurrency!")
        else:
            timeout_embed.description = "Time's up! You didn't mine anything this session."
        
        # Update the message with timeout embed if we have a reference
        if self.message:
            try:
                await self.message.edit(embed=timeout_embed, view=self)
            except Exception as e:
                print(f"Error updating timeout message: {e}")
    
    @discord.ui.button(label="MINE!", style=discord.ButtonStyle.success, emoji="⛏️")
    async def mine_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check user authorization first
            if interaction.user.id != self.user_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your mining session!", ephemeral=True)
                return
            
            # DEFER IMMEDIATELY - This is critical to prevent interaction timeouts
            if not await safe_defer(interaction, ephemeral=False):
                return
            
            # Check if already timed out - do this early
            if self.timed_out:
                return
            
            # Start the session on first button click
            if not self.session_started:
                self.session_started = True
                self.start_time = time.time()
                # Set cooldown when session actually starts
                update_user_last_mine_time(self.user_id, self.start_time)
                # Start the timer monitor task
                self.timer_task = asyncio.create_task(self._timer_monitor_task())
                # Update embed to show timer has started (force update on first click)
                await self._update_timer_embed(60 + self.gpu_seconds_boost, 60 + self.gpu_seconds_boost, force_update=True)
                # Continue to mine on this first click
            else:
                # Check if session has timed out - early check before processing
                # The timer task handles the main timeout, but we check here as a safety measure
                elapsed_time = time.time() - self.start_time
                max_time = 60 + self.gpu_seconds_boost
                if elapsed_time >= max_time:
                    # Session has expired - return early (timer task will handle the rest)
                    return
            
            # Randomly select a coin to mine
            coin = random.choice(CRYPTO_COINS)
            symbol = coin["symbol"]
            base_price = coin["base_price"]
            
            # Calculate mining amount based on coin's base price (proportional to old 200.0 base)
            # Target: $50-60 per session average (assuming up to 60 clicks per 60s session, 1 per second)
            # This means ~$0.83-$1.00 per click average, so $0.60-$1.40 range with RNG
            # At $200 base: $0.60-$1.40 = 0.003-0.007 coins = 30-70 thousandths
            # New system: scale by price ratio (200.0 / base_price)
            price_ratio = 200.0 / base_price
            # Reduced range: 30-70 thousandths (0.003-0.007) at $200 base
            # Scaled range: multiply by price_ratio
            min_thousandths = int(30 * price_ratio)
            max_thousandths = int(70 * price_ratio)
            # Ensure at least 1 thousandth
            min_thousandths = max(1, min_thousandths)
            max_thousandths = max(min_thousandths, max_thousandths)
            random_thousandths = random.randint(min_thousandths, max_thousandths)
            base_amount = round(random_thousandths / 10000, 4)
            
            # Apply GPU percent boost (e.g., 5% = 0.05, so multiply by 1.05)
            # Ensure gpu_percent_boost is a number (convert to float if needed)
            gpu_boost = float(self.gpu_percent_boost) if self.gpu_percent_boost else 0.0
            percent_multiplier = 1.0 + (gpu_boost / 100.0)
            amount = round(base_amount * percent_multiplier, 4)
            
            # Add crypto to user's holdings (NO BOOSTS APPLIED DURING MINING)
            update_user_crypto_holdings(interaction.user.id, symbol, amount)
            
            # Check for hidden achievement: Blockchain (have at least 1.00 of any cryptocoin)
            # Check all holdings after this update
            crypto_holdings = get_user_crypto_holdings(interaction.user.id)
            has_blockchain = False
            for coin_symbol, coin_amount in crypto_holdings.items():
                if coin_amount >= 1.0:
                    has_blockchain = True
                    break
            
            if has_blockchain and unlock_hidden_achievement(interaction.user.id, "blockchain"):
                # Achievement unlocked - we'll show it in the timeout message
                self.blockchain_achievement_unlocked = True
            else:
                self.blockchain_achievement_unlocked = False
            
            # Update session tracking
            self.total_mines += 1
            if symbol not in self.session_mined:
                self.session_mined[symbol] = 0.0
            self.session_mined[symbol] += amount
            
            # Calculate value of this mine (base value only, no boosts)
            prices = get_crypto_prices()
            coin_price = prices.get(symbol, base_price)
            mine_value = amount * coin_price
            self.session_value += mine_value
            
            # Check timeout again after processing (in case processing took time)
            # The timer task handles the main timeout, but we check here as a safety measure
            if self.timed_out:
                return
            
            # Update embed only if not timed out (rate limited to avoid slowing down clicks)
            if self.session_started and not self.timed_out:
                elapsed_time = time.time() - self.start_time
                max_time = 60 + self.gpu_seconds_boost
                time_remaining = max(0, max_time - elapsed_time)
                
                # Update embed asynchronously - don't block button processing
                # Use create_task so it doesn't delay the button response
                asyncio.create_task(self._update_timer_embed(time_remaining, max_time))
                
        except Exception as e:
            print(f"Error in mine_button: {e}")
            # Already deferred, so use followup if needed
            try:
                if hasattr(interaction, 'followup'):
                    await interaction.followup.send("❌ An error occurred. Please try again.", ephemeral=True)
            except:
                pass
    
    async def on_timeout(self):
        # Discord's timeout callback - use shared handler
        if not self.timed_out:
            await self._handle_timeout()
    
    def stop(self):
        """Stop the view and cancel any running tasks."""
        # Cancel timer task if it's still running
        if self.timer_task and not self.timer_task.done():
            self.timer_task.cancel()
        super().stop()


@bot.tree.command(name="mine", description="Mine cryptocurrency! (1 hour cooldown)")
async def mine(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        
        # Check if user is on Russian Roulette elimination cooldown (dead)
        is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
        if is_roulette_cooldown:
            if roulette_time_left < 60:
                # Show seconds when less than 1 minute left
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot mine for {roulette_time_left} second(s)", ephemeral=True)
            else:
                # Show minutes when 1 minute or more left
                minutes_left = roulette_time_left // 60
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot mine for {minutes_left} minute(s)", ephemeral=True)
            return
        
        # Check if command is being used in the correct channel
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "gathercoin":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #gathercoin channel, {interaction.user.name}!",
                ephemeral=True)
            return
        
        user_id = interaction.user.id
        
        # Check cooldown
        last_mine_time = get_user_last_mine_time(user_id)
        current_time = time.time()
        
        if last_mine_time > 0:
            cooldown_end = last_mine_time + MINE_COOLDOWN
            if current_time < cooldown_end:
                time_left = int(cooldown_end - current_time)
                minutes_left = time_left // 60
                seconds_left = time_left % 60
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"⏰ You must wait {minutes_left} minutes and {seconds_left} seconds before mining again, {interaction.user.name}.",
                    ephemeral=True)
                return
        
        # Don't set cooldown here - it will be set when the user clicks the button to start the session
        
        # Get user's GPUs and calculate bonuses
        user_gpus = get_user_gpus(user_id)
        total_percent_boost = 0
        total_seconds_boost = 0
        gpus_used = []
        
        for gpu_name in user_gpus:
            # Find GPU info in shop
            gpu_info = next((gpu for gpu in GPU_SHOP if gpu["name"] == gpu_name), None)
            if gpu_info:
                total_percent_boost += gpu_info["percent_increase"]
                total_seconds_boost += gpu_info["seconds_increase"]
                gpus_used.append(gpu_name)
        
        # Create mining embed with button
        base_time = 60
        total_time = base_time + total_seconds_boost
        description_text = f"Click the **MINE!** button below to start mining!\n\nYou will have **{total_time}** seconds to click as many times as you can once you start!"
        if total_percent_boost > 0:
            description_text += f"\n💰 **GPU Boost: +{total_percent_boost}%**"
        if total_seconds_boost > 0:
            description_text += f"\n⏱️ **Time Boost: +{total_seconds_boost} seconds**"
        
        embed = discord.Embed(
            title="⛏️ Cryptocurrency Mining",
            description=description_text,
            color=discord.Color.blue()
        )
        
        # Add GPU info if user has GPUs
        if gpus_used:
            embed.add_field(name="GPUs Active", value="\n".join(gpus_used), inline=False)
        
        view = MiningView(user_id, timeout=total_time, gpu_percent_boost=total_percent_boost, gpu_seconds_boost=total_seconds_boost, gpus_used=gpus_used)
        message = await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
        # Store message reference in view for timeout handling
        if message:
            view.message = message
        # Don't start the timeout checker here - it will start when the user clicks the button
    except Exception as e:
        print(f"Error in mine command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="sell", description="Sell your cryptocurrency holdings")
@app_commands.choices(coin=[
    app_commands.Choice(name="RootCoin (RTC)", value="RTC"),
    app_commands.Choice(name="Terrarium (TER)", value="TER"),
    app_commands.Choice(name="Canopy (CNY)", value="CNY"),
    app_commands.Choice(name="All", value="all"),
])
async def sell(interaction: discord.Interaction, coin: str, amount: float = None):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        holdings = get_user_crypto_holdings(user_id)
        prices = get_crypto_prices()
        
        # Handle "all" option
        if coin == "all":
            # Sell all crypto holdings
            base_sale_value = 0.0
            sold_items = []
            
            for crypto_coin in CRYPTO_COINS:
                symbol = crypto_coin["symbol"]
                user_holding = holdings.get(symbol, 0.0)
                
                if user_holding > 0:
                    coin_base_price = crypto_coin["base_price"]
                    coin_price = prices.get(symbol, coin_base_price)
                    sale_value = user_holding * coin_price
                    base_sale_value += sale_value
                    
                    # Update holdings (subtract)
                    update_user_crypto_holdings(user_id, symbol, -user_holding)
                    
                    sold_items.append(f"{symbol}: {user_holding:.4f} (${sale_value:.2f})")
            
            if base_sale_value == 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You don't have any cryptocurrency to sell, {interaction.user.name}!",
                    ephemeral=True)
                return
            
            # Apply boosts to sale value
            bloom_multiplier = get_bloom_multiplier(user_id)
            water_multiplier = get_water_multiplier(user_id)
            rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
            achievement_multiplier = get_achievement_multiplier(user_id)
            daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
            
            # Calculate boosted value (apply all multipliers)
            value_after_bloom = base_sale_value * bloom_multiplier
            value_after_water = value_after_bloom * water_multiplier
            value_after_rank = value_after_water * rank_perma_buff_multiplier
            value_after_achievement = value_after_rank * achievement_multiplier
            total_sale_value = value_after_achievement * daily_bonus_multiplier
            
            # Calculate extra value from boosts (only show tree ring and water streak)
            extra_from_bloom = value_after_bloom - base_sale_value
            # Water multiplier is applied but not shown separately
            extra_from_rank = value_after_rank - value_after_water
            extra_from_achievement = value_after_achievement - value_after_rank
            extra_from_daily = total_sale_value - value_after_achievement
            
            # Add money to balance (with boosts)
            current_balance = get_user_balance(user_id)
            new_balance = current_balance + total_sale_value
            update_user_balance(user_id, new_balance)
            
            # Get updated holdings
            updated_holdings = get_user_crypto_holdings(user_id)
            
            # Create success embed
            embed = discord.Embed(
                title="💰 Sale Successful!",
                description=f"You sold all your cryptocurrency for **${total_sale_value:.2f}**!",
                color=discord.Color.green()
            )
            embed.add_field(name="Sold", value="\n".join(sold_items) if sold_items else "None", inline=False)
            
            # Show boosts if applicable (only tree ring and water streak)
            bloom_count = get_user_bloom_count(user_id)
            if bloom_count > 0 and extra_from_bloom > 0:
                multiplier_percent = (bloom_multiplier - 1.0) * 100
                embed.add_field(
                    name="🌳 Tree Ring Boost",
                    value=f"+{multiplier_percent:.1f}% - **+${extra_from_bloom:.2f}**",
                    inline=False
                )
            # Show rank perma buff if applicable (only if not PINE I)
            bloom_rank = get_bloom_rank(user_id)
            if bloom_rank != "PINE I" and extra_from_rank > 0:
                rank_perma_buff_percent = (rank_perma_buff_multiplier - 1.0) * 100
                embed.add_field(
                    name="⭐ Rank Boost",
                    value=f"+{rank_perma_buff_percent:.1f}% - **+${extra_from_rank:.2f}**",
                    inline=False
                )
            # Show achievement boost if applicable
            if extra_from_achievement > 0:
                achievement_percent = (achievement_multiplier - 1.0) * 100
                embed.add_field(
                    name="🏆 Achievement Boost",
                    value=f"+{achievement_percent:.1f}% - **+${extra_from_achievement:.2f}**",
                    inline=False
                )
            if extra_from_daily > 0:
                daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
                embed.add_field(
                    name="💧 Water Streak Boost",
                    value=f"+{daily_bonus_percent:.1f}% - **+${extra_from_daily:.2f}**",
                    inline=False
                )
            
            embed.add_field(name="Remaining Holdings", value=f"RTC: {updated_holdings['RTC']:.4f}\nTER: {updated_holdings['TER']:.4f}\nCNY: {updated_holdings['CNY']:.4f}", inline=False)
            embed.add_field(name="New Balance", value=f"${new_balance:.2f}", inline=False)
            
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
            return
        
        # Original logic for selling a specific coin
        # Check if user has any of this coin
        user_holding = holdings.get(coin, 0.0)
        
        if user_holding <= 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You don't have any {coin} to sell, {interaction.user.name}!",
                ephemeral=True)
            return
        
        # If amount not specified, sell all
        if amount is None:
            amount = user_holding
        elif amount > user_holding:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You only have {user_holding:.4f} {coin}, but tried to sell {amount:.4f} {coin}!",
                ephemeral=True)
            return
        elif amount <= 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Invalid amount! Please sell a positive amount.",
                ephemeral=True)
            return
        
        # Calculate base sale value
        # Get base price for the coin
        coin_info = next((c for c in CRYPTO_COINS if c["symbol"] == coin), None)
        coin_base_price = coin_info["base_price"] if coin_info else 855.0
        coin_price = prices.get(coin, coin_base_price)
        base_sale_value = amount * coin_price
        
        # Apply boosts to sale value
        bloom_multiplier = get_bloom_multiplier(user_id)
        water_multiplier = get_water_multiplier(user_id)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
        achievement_multiplier = get_achievement_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        
        # Calculate boosted value (apply all multipliers)
        value_after_bloom = base_sale_value * bloom_multiplier
        value_after_water = value_after_bloom * water_multiplier
        value_after_rank = value_after_water * rank_perma_buff_multiplier
        value_after_achievement = value_after_rank * achievement_multiplier
        sale_value = value_after_achievement * daily_bonus_multiplier
        
        # Calculate extra value from boosts (only show tree ring and water streak)
        extra_from_bloom = value_after_bloom - base_sale_value
        # Water multiplier is applied but not shown separately
        extra_from_rank = value_after_rank - value_after_water
        extra_from_achievement = value_after_achievement - value_after_rank
        extra_from_daily = sale_value - value_after_achievement
        
        # Update holdings (subtract)
        update_user_crypto_holdings(user_id, coin, -amount)
        
        # Add money to balance (with boosts)
        current_balance = get_user_balance(user_id)
        new_balance = current_balance + sale_value
        update_user_balance(user_id, new_balance)
        
        # Get updated holdings
        updated_holdings = get_user_crypto_holdings(user_id)
        
        # Create success embed
        embed = discord.Embed(
            title="💰 Sale Successful!",
            description=f"You sold **{amount:.4f} {coin}** for **${sale_value:.2f}**!",
            color=discord.Color.green()
        )
        
        # Show boosts if applicable (only tree ring and water streak)
        bloom_count = get_user_bloom_count(user_id)
        if bloom_count > 0 and extra_from_bloom > 0:
            multiplier_percent = (bloom_multiplier - 1.0) * 100
            embed.add_field(
                name="🌳 Tree Ring Boost",
                value=f"+{multiplier_percent:.1f}% - **+${extra_from_bloom:.2f}**",
                inline=False
            )
        # Show rank perma buff if applicable (only if not PINE I)
        bloom_rank = get_bloom_rank(user_id)
        if bloom_rank != "PINE I" and extra_from_rank > 0:
            rank_perma_buff_percent = (rank_perma_buff_multiplier - 1.0) * 100
            embed.add_field(
                name="⭐ Rank Boost",
                value=f"+{rank_perma_buff_percent:.1f}% - **+${extra_from_rank:.2f}**",
                inline=False
            )
        # Show achievement boost if applicable
        if extra_from_achievement > 0:
            achievement_percent = (achievement_multiplier - 1.0) * 100
            embed.add_field(
                name="🏆 Achievement Boost",
                value=f"+{achievement_percent:.1f}% - **+${extra_from_achievement:.2f}**",
                inline=False
            )
        if extra_from_daily > 0:
            daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
            embed.add_field(
                name="💧 Water Streak Boost",
                value=f"+{daily_bonus_percent:.1f}% - **+${extra_from_daily:.2f}**",
                inline=False
            )
        
        embed.add_field(name="Remaining Holdings", value=f"RTC: {updated_holdings['RTC']:.4f}\nTER: {updated_holdings['TER']:.4f}\nCNY: {updated_holdings['CNY']:.4f}", inline=False)
        embed.add_field(name="New Balance", value=f"${new_balance:.2f}", inline=False)
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in sell command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="portfolio", description="View your cryptocurrency and stock portfolio")
async def portfolio(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        guild_id = interaction.guild.id if interaction.guild else None
        
        # Get crypto holdings and prices
        crypto_holdings = get_user_crypto_holdings(user_id)
        crypto_prices = get_crypto_prices()
        
        # Get stock holdings
        stock_holdings = get_user_stock_holdings(user_id)
        
        # Initialize stocks for guild if needed to get current prices
        if guild_id:
            await initialize_stocks(guild_id)
            stock_prices = {}
            for ticker in STOCK_TICKERS:
                symbol = ticker["symbol"]
                if symbol in stock_data.get(guild_id, {}):
                    stock_prices[symbol] = stock_data[guild_id][symbol]["price"]
                else:
                    stock_prices[symbol] = ticker["base_price"]
        else:
            # Fallback to base prices if no guild
            stock_prices = {ticker["symbol"]: ticker["base_price"] for ticker in STOCK_TICKERS}
        
        # Calculate crypto values
        crypto_values = {}
        crypto_total = 0.0
        
        for coin in CRYPTO_COINS:
            symbol = coin["symbol"]
            amount = crypto_holdings.get(symbol, 0.0)
            price = crypto_prices.get(symbol, coin["base_price"])
            value = amount * price
            crypto_values[symbol] = value
            crypto_total += value
        
        # Calculate stock values
        stock_values = {}
        stock_total = 0.0
        
        for ticker in STOCK_TICKERS:
            symbol = ticker["symbol"]
            shares = stock_holdings.get(symbol, 0)
            price = stock_prices.get(symbol, ticker["base_price"])
            value = shares * price
            stock_values[symbol] = value
            stock_total += value
        
        # Total portfolio value
        total_value = crypto_total + stock_total
        
        # Create portfolio embed
        embed = discord.Embed(
            title="💼 Your Portfolio",
            description=f"**Total Portfolio Value: ${total_value:.2f}**",
            color=discord.Color.blue()
        )
        
        # Add cryptocurrency section
        if crypto_total > 0:
            embed.description += "\n**💰 Cryptocurrency:**"
            for coin in CRYPTO_COINS:
                symbol = coin["symbol"]
                amount = crypto_holdings.get(symbol, 0.0)
                if amount > 0:
                    price = crypto_prices.get(symbol, coin["base_price"])
                    value = crypto_values.get(symbol, 0.0)
                    embed.add_field(
                        name=f"{coin['name']} ({symbol})",
                        value=f"Amount: {amount:.4f}\nValue: ${value:.2f}",
                        inline=True
                    )
            # Add total as a field right after crypto holdings
            embed.add_field(
                name="\u200b",
                value=f"**Total: ${crypto_total:.2f}**",
                inline=False
            )
        
        # Add stock section
        if stock_total > 0:
            embed.description += "\n**📈 Stocks:**"
            for ticker in STOCK_TICKERS:
                symbol = ticker["symbol"]
                shares = stock_holdings.get(symbol, 0)
                if shares > 0:
                    price = stock_prices.get(symbol, ticker["base_price"])
                    value = stock_values.get(symbol, 0.0)
                    embed.add_field(
                        name=f"{ticker['name']} ({symbol})",
                        value=f"Shares: {shares:,}\nValue: ${value:.2f}",
                        inline=True
                    )
            # Add total as a field right after stock holdings
            embed.add_field(
                name="\u200b",
                value=f"**Total: ${stock_total:.2f}**",
                inline=False
            )
        
        if total_value == 0:
            embed.description = "You don't have any holdings yet!\n\nUse `/mine` to mine cryptocurrency or buy stocks to get started!"
        
        embed.set_footer(text="Do /mine to get crypto, /sell to sell it, and /stocks to buy/sell shares!")
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in portfolio command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="stocks", description="Buy or sell stocks")
@app_commands.choices(action=[
    app_commands.Choice(name="buy", value="buy"),
    app_commands.Choice(name="sell", value="sell"),
])
@app_commands.choices(ticker=[
    app_commands.Choice(name="Maizy's (M)", value="M"),
    app_commands.Choice(name="Meadow (MEDO)", value="MEDO"),
    app_commands.Choice(name="IVM (IVM)", value="IVM"),
    app_commands.Choice(name="CisGrow (CSGO)", value="CSGO"),
    app_commands.Choice(name="Sowny (SWNY)", value="SWNY"),
    app_commands.Choice(name="General Mowers (GM)", value="GM"),
    app_commands.Choice(name="Raytheorn (RTH)", value="RTH"),
    app_commands.Choice(name="Wells Fargrow (WFG)", value="WFG"),
    app_commands.Choice(name="Apple (AAPL)", value="AAPL"),
    app_commands.Choice(name="Sproutify (SPRT)", value="SPRT"),
])
async def stocks(interaction: discord.Interaction, action: str, ticker: str, amount: int):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        
        user_id = interaction.user.id
        
        # Check if user is on Russian Roulette elimination cooldown (dead)
        is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
        if is_roulette_cooldown:
            minutes_left = roulette_time_left // 60
            await safe_interaction_response(interaction, interaction.followup.send,
                f"Sorry, {interaction.user.name}, you're dead. You cannot buy or sell stocks for {minutes_left} minute(s)", ephemeral=True)
            return
        
        guild_id = interaction.guild.id if interaction.guild else None
        
        # Validate amount
        if amount <= 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Invalid amount! Please buy or sell a positive number of shares.",
                ephemeral=True)
            return
        
        # Find the ticker info
        ticker_info = None
        for t in STOCK_TICKERS:
            if t["symbol"] == ticker:
                ticker_info = t
                break
        
        if not ticker_info:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Invalid ticker symbol!",
                ephemeral=True)
            return
        
        # Initialize stocks for guild if needed to get current prices
        if not guild_id:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command must be used in a server!",
                ephemeral=True)
            return
    
        await initialize_stocks(guild_id)
        
        # Get current stock price
        if ticker not in stock_data.get(guild_id, {}):
            current_price = ticker_info["base_price"]
        else:
            current_price = stock_data[guild_id][ticker]["price"]
        
        # Get user's current stock holdings
        stock_holdings = get_user_stock_holdings(user_id)
        current_shares = stock_holdings.get(ticker, 0)
        
        if action == "buy":
            # Check if enough shares are available in the market
            available_shares = calculate_available_shares(guild_id, ticker)
            if available_shares == 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ No shares available! All shares of {ticker_info['name']} ({ticker}) have been purchased.",
                    ephemeral=True)
                return
            
            if available_shares < amount:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ Not enough shares available!\n\n"
                    f"Only **{available_shares:,} share(s)** of {ticker_info['name']} ({ticker}) are available, "
                    f"but you tried to buy **{amount:,} share(s)**.",
                    ephemeral=True)
                return
            
            # Calculate total cost
            total_cost = amount * current_price
            
            # Check if user has enough balance
            user_balance = get_user_balance(user_id)
            if user_balance < total_cost:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You don't have enough balance to buy {amount} share(s) of {ticker_info['name']} ({ticker})!\n\n"
                    f"You need **${total_cost:.2f}** but only have **${user_balance:.2f}**.",
                    ephemeral=True)
                return
            
            # Check if user would exceed max shares (per user limit)
            max_shares = ticker_info["max_shares"]
            if (current_shares + amount) > max_shares:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You cannot buy {amount:,} share(s)! Maximum shares per user for {ticker_info['name']} is {max_shares:,}.\n\n"
                    f"You currently own {current_shares:,} share(s).",
                    ephemeral=True)
                return
            
            # Deduct money and add shares
            new_balance = user_balance - total_cost
            update_user_balance(user_id, new_balance)
            update_user_stock_holdings(user_id, ticker, amount)
            
            # Check for hidden achievement: CEO (own over 50% of shares for any company)
            # Get shares outstanding from stock_data or fallback to max_shares
            shares_outstanding = ticker_info.get("max_shares", 0)
            if guild_id and guild_id in stock_data and ticker in stock_data[guild_id]:
                api_shares = stock_data[guild_id][ticker].get("shares_outstanding")
                if api_shares and api_shares > 0:
                    shares_outstanding = api_shares
            
            user_owned_shares = current_shares + amount
            if shares_outstanding > 0 and (user_owned_shares / shares_outstanding) > 0.5:
                if unlock_hidden_achievement(user_id, "ceo"):
                    achievement_msg = f"\n\n🎉 **Hidden Achievement Unlocked: CEO!** 🎉"
                else:
                    achievement_msg = ""
            else:
                achievement_msg = ""
            
            # Create success embed
            embed = discord.Embed(
                title="✅ Purchase Successful!",
                description=f"You bought **{amount:,} share(s)** of **{ticker_info['name']} ({ticker})** at **${current_price:.2f}** each.{achievement_msg}",
                color=discord.Color.green()
            )
            embed.add_field(name="Cost", value=f"**${total_cost:.2f}**", inline=True)
            embed.add_field(name="New Balance", value=f"**${new_balance:.2f}**", inline=True)
            embed.add_field(name="Total Shares Owned", value=f"**{current_shares + amount:,}**", inline=False)
            
            # Update marketboard immediately
            try:
                await update_marketboard_message(interaction.guild)
            except Exception as e:
                print(f"Error updating marketboard after buy: {e}")
            
        else:  # sell
            # Check if user has enough shares
            if current_shares < amount:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You don't have enough shares to sell!\n\n"
                    f"You only have **{current_shares:,} share(s)** of {ticker_info['name']} ({ticker}), "
                    f"but tried to sell **{amount:,} share(s)**.",
                    ephemeral=True)
                return
            
            # Calculate total value
            total_value = amount * current_price
            
            # Add money and remove shares
            user_balance = get_user_balance(user_id)
            new_balance = user_balance + total_value
            update_user_balance(user_id, new_balance)
            update_user_stock_holdings(user_id, ticker, -amount)
            
            # Create success embed
            embed = discord.Embed(
                title="✅ Sale Successful!",
                description=f"You sold **{amount:,} share(s)** of **{ticker_info['name']} ({ticker})** at **${current_price:.2f}** each.",
                color=discord.Color.green()
            )
            embed.add_field(name="Revenue", value=f"**${total_value:.2f}**", inline=True)
            embed.add_field(name="New Balance", value=f"**${new_balance:.2f}**", inline=True)
            embed.add_field(name="Remaining Shares", value=f"**{current_shares - amount:,}**", inline=False)
            
            # Update marketboard immediately
            try:
                await update_marketboard_message(interaction.guild)
            except Exception as e:
                print(f"Error updating marketboard after sell: {e}")
        
        embed.set_footer(text=f"Use /portfolio to view all your holdings")
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in stocks command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# gambling commands
# russian roulette

@bot.tree.command(name="russian", description="Play Russian Roulette!")
async def russian(
    interaction: discord.Interaction,
    bullets: int,
    bet: float,
    players: int = 1 # default 1
):
    try:
        if not await safe_defer(interaction):
            return
        #start russian roullette
        user_id = interaction.user.id
        user_name = interaction.user.name
        channel_id = interaction.channel.id
        channel_name = interaction.channel.name.lower() if hasattr(interaction.channel, 'name') else ""

        # Check if user is on Russian Roulette elimination cooldown
        is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
        if is_roulette_cooldown:
            minutes_left = roulette_time_left // 60
            await safe_interaction_response(interaction, interaction.followup.send,
                f"Sorry, {interaction.user.name}, you're dead. You cannot /russian for {minutes_left} minute(s)", ephemeral=True)
            return

        # Check if this is a Russian Roulette channel (by name pattern)
        is_roulette_channel = "russian" in channel_name or "roulette" in channel_name
        
        # If in a Russian Roulette channel, require Planter II or above
        if is_roulette_channel:
            planter_roles = ["PLANTER I", "PLANTER II", "PLANTER III", "PLANTER IV", "PLANTER V", "PLANTER VI", "PLANTER VII", "PLANTER VIII", "PLANTER IX", "PLANTER X"]
            user_roles = [role.name for role in interaction.user.roles]
            has_planter_role = any(role in planter_roles for role in user_roles)
            
            if not has_planter_role:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You must be at least **Planter II** to play Russian Roulette. (Go /gather!!)\n\n",
                    ephemeral=True)
                return
            
            # Check if they have Planter II or higher (not Planter I)
            planter_levels = ["PLANTER I", "PLANTER II", "PLANTER III", "PLANTER IV", "PLANTER V", "PLANTER VI", "PLANTER VII", "PLANTER VIII", "PLANTER IX", "PLANTER X"]
            user_planter_role = next((role for role in user_roles if role in planter_levels), None)
            
            if user_planter_role == "PLANTER I":
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You must be at least **Planter II** to play Russian Roulette. (Go /gather!!)\n\n",
                    ephemeral=True)
                return

        # make sure game is actually valid and the person can do it

        # first, check if game already in channel
        if channel_id in active_roulette_channel_games:
            existing_game_id = active_roulette_channel_games[channel_id]
            if existing_game_id in active_roulette_games:
                await safe_interaction_response(interaction, interaction.followup.send, f"There's already a Russian Roulette game running in this channel!", ephemeral=True)
                return
            else:
                #clean up orphaned ref
                del active_roulette_channel_games[channel_id]
        

        # make sure user is not already in a game
        if user_id in user_active_games:
            existing_game_id = user_active_games[user_id]
            if existing_game_id in active_roulette_games:
                await safe_interaction_response(interaction, interaction.followup.send, f"You're already in a game! Finish it or cash out first!", ephemeral=True)
                return
            else:
                #clean up orphaned ref
                del user_active_games[user_id]


        if bullets < 1 or bullets > 5:
            await safe_interaction_response(interaction, interaction.followup.send, f"Invalid number of bullets", ephemeral=True)
            return

        if players < 1 or players > 6:
            await safe_interaction_response(interaction, interaction.followup.send, f"Invalid number of players", ephemeral=True)
            return
        if bet <= 0:
            await safe_interaction_response(interaction, interaction.followup.send, f"❌ Bet amount must be greater than $0.00!", ephemeral=True)
            return

        # Validate bet has at most 2 decimal places (no fractional cents)
        if not validate_money_precision(bet):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Bet amount must be in dollars and cents (maximum 2 decimal places)!", ephemeral=True)
            return

        # Normalize bet to exactly 2 decimal places
        bet = normalize_money(bet)

        # get user balance
        user_balance = get_user_balance(user_id)
        user_balance = normalize_money(user_balance)
        
        if not can_afford_rounded(user_balance, bet):
            await safe_interaction_response(interaction, interaction.followup.send, f"You don't have enough balance to play Russian Roulette.", ephemeral=True)
            return

        #create unique game ID
        game_id = str(uuid.uuid4())[:8]

        #create new game (bet is already normalized)
        game = RouletteGame(game_id, user_id, user_name, bullets, bet, players)
        active_roulette_games[game_id] = game
        user_active_games[user_id] = game_id
        active_roulette_channel_games[channel_id] = game_id

        # deduct bet from host
        new_balance = normalize_money(user_balance - bet)
        update_user_balance(user_id, new_balance)
        # increase bullet multiplier
        bullet_multiplier = 1.2 ** bullets

        # # SOLO MODE
        # if players == 1:
        #     game.game_started = True
        #     game.pot = bet

        #     embed = discord.Embed(
        #         title="🎲 RUSSIAN ROULETTE 🎲",
        #         description=f"**{user_name}** is playing!\n\n*How long can you survive?*",
        #         color=discord.Color.dark_red()
        #     )

        #     embed.add_field(name="🔫 Bullets", value=f"{bullets}/6", inline=True)
        #     embed.add_field(name="💰 Buy-in", value=f"${bet:.2f}", inline=True)
        #     embed.add_field(name="📈 Base Multiplier", value=f"{bullet_multiplier:.2f}x", inline=True)
        #     embed.add_field(name="💀 Death Chance", value=f"{(bullets/6)*100:.1f}%", inline=True)
        #     embed.add_field(name="✅ Survival Chance", value=f"{((6-bullets)/6)*100:.1f}%", inline=True)
        #     embed.add_field(name="🎮 Game ID", value=f"`{game_id}`", inline=True)

        #     embed.add_field(
        #         name="ℹ️ Rules", 
        #         value="Each round you survive increases your winnings by **1.3x**!\nCash out anytime to keep your winnings, or keep playing for more!",
        #         inline=False
        #     )

        #     await interaction.followup.send(embed=embed)

        #     # auto-start first round after delay
        #     await asyncio.sleep(3)
        #     await start_roulette_game(interaction.channel, game_id)
        #     return

        # MULTIPLAYER MODE
        embed = discord.Embed(
            title="🎲 RUSSIAN ROULETTE 🎲",
            description=f"**{user_name}** is playing with **{len(game.players)}/{players}** players!\n\n*How long can you survive?*",
            color = discord.Color.red()
        )
        embed.add_field(name="🔫 Bullets", value=f"{bullets}/6", inline=True)
        embed.add_field(name="💰 Buy-in", value=f"${bet:.2f}", inline=True)
        embed.add_field(name="📈 Base Multiplier", value=f"{bullet_multiplier:.2f}x", inline=True)
        embed.add_field(name="💀 Death Chance", value=f"{(bullets/6)*100:.1f}%", inline=True)
        embed.add_field(name="✅ Survival Chance", value=f"{((6-bullets)/6)*100:.1f}%", inline=True)
        #embed.add_field(name="🎮 Game ID", value=f"`{game_id}`", inline=True)
        embed.add_field(
        name="📋 Rules",
        value="Cash out anytime to keep your winnings, or keep playing for more!",
        inline=False
    )
        
        #create join button
        view = RouletteJoinView(game_id, user_id,timeout = 300)

        if players == 1:
            embed.add_field(name="ℹ️ How to Play", value="Click **Start Game** to begin your solo adventure!", inline=False)
        else:
            embed.add_field(name="ℹ️ How to Play", value=f"Waiting for {players-1} more players to join! Host can click **Start Game** when ready!", inline=False)

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in russian command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)








#END

# Cloud Run compatibility - add simple HTTP server
# Note: threading, time, os already imported at top
from http.server import HTTPServer, BaseHTTPRequestHandler

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress HTTP server logs
        pass

def start_http_server():
    """Start a simple HTTP server for Cloud Run health checks"""
    try:
        port = int(os.environ.get('PORT', 8080))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"HTTP server listening on port {port}")
        server.serve_forever()
    except Exception as e:
        print(f"HTTP server error: {e}")
        import traceback
        traceback.print_exc()
        # Re-raise to prevent silent failures
        raise

def start_discord_bot():
    """Start the Discord bot with error handling"""
    try:
        print("Starting Discord bot...")
        print(
            "Environment resolved as: "
            f"{environment} (ENVIRONMENT={os.getenv('ENVIRONMENT')}, "
            f"ENVIRONMENT_PROD={os.getenv('ENVIRONMENT_PROD')})"
        )
        mongo_uri = os.getenv('MONGODB_URI')
        print(f"Mongo URI: {_redact_mongo_uri(mongo_uri)}")
        print(
            f"Discord token source: {token_env_key} "
            f"({'set' if token else 'missing'})"
        )
        print(f"Token length: {len(token) if token else 'None'}")
        print("About to call bot.run()...")
        
        # Use INFO level for discord.py to reduce terminal clutter
        # All logs still go to discord.log file via the configured handler
        try:
            bot.run(token, log_handler=None, log_level=logging.INFO)
        except KeyboardInterrupt:
            # Handle graceful shutdown on Ctrl+C
            print("\nShutting down bot...")
            # The bot.run() method should handle cleanup, but we catch this to prevent the RuntimeWarning
            # from appearing in the terminal
            pass
    except Exception as e:
        print(f"Discord bot error: {e}")
        import traceback
        traceback.print_exc()
        # Keep the process alive even if bot fails (so HTTP server keeps running)
        while True:
            time.sleep(60)

if __name__ == "__main__":
    print("Starting SlashGather Discord Bot...")
    if is_production:
        # Start HTTP server as non-daemon so it keeps running
        http_thread = threading.Thread(target=start_http_server, daemon=False)
        http_thread.start()
        # Give the HTTP server time to bind to the port
        time.sleep(2)
        print("HTTP health check server started and bound to port")
    else:
        print("Health check server disabled in development mode")

    start_discord_bot()