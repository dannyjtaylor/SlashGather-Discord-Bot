import discord
from discord import app_commands
from discord.ext import commands
import logging
from collections import Counter
from dotenv import load_dotenv
import os
import random
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
import uuid
import threading
import datetime
import subprocess
import sys
import traceback
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
    get_user_almanac_entries,
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
    get_expired_events,
    set_active_event,
    clear_event,
    clear_expired_events,
    get_user_gather_data,
    perform_gather_update,
    perform_batch_gather_update,
    get_user_tree_rings,
    increment_tree_rings,
    recalculate_user_tree_rings,
    recalculate_guild_tree_rings,
    get_bloom_multiplier,
    get_bloom_rank,
    get_user_bloom_count,
    set_user_bloom_count,
    perform_bloom,
    get_user_bloom_cycle_plants,
    deduct_user_bloom_cycle_plants,
    add_user_bloom_cycle_plants,
    set_user_bloom_cycle_plants,
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
    wipe_guild_money,
    wipe_guild_plants,
    wipe_guild_crypto,
    wipe_guild_all,
    _get_users_collection,
    get_user_achievement_level,
    set_user_achievement_level,
    get_user_achievements_display_data,
    get_user_hidden_achievements_count,
    increment_hidden_achievements_count,
    has_hidden_achievement,
    unlock_hidden_achievement,
    add_pve_defeat,
    get_user_pve_defeated,
    get_user_total_pve_defeats,
    get_user_total_steals,
    get_user_critical_gathers_count,
    increment_critical_gathers_count,
    get_user_coinflip_count,
    increment_user_coinflip_count,
    get_user_coinflip_win_streak,
    set_user_coinflip_win_streak,
    get_user_last_coinflip_loss_time,
    update_user_last_coinflip_loss_time,
    get_user_slots_spin_count,
    increment_user_slots_spin_count,
    get_user_slots_win_streak,
    set_user_slots_win_streak,
    get_user_gather_command_count,
    increment_user_gather_command_count,
    get_user_harvest_command_count,
    increment_user_harvest_command_count,
    get_user_invite_stats,
    increment_invite_joins,
    track_invite_created,
    has_user_joined_before,
    mark_user_as_joined,
    increment_invite_joins_new_user,
    increment_invite_joins_count_only,
    claim_invite_reward,
    get_user_claimed_invite_rewards,
    has_secret_gardener,
    has_secret_gardener_harvest,
    get_invite_cooldown_reductions,
    get_all_users_with_secret_gardener,
    get_user_hoe_attunement,
    set_user_hoe_attunement,
    get_user_tractor_attunement,
    set_user_tractor_attunement,
    get_user_russian_games_played,
    increment_user_russian_games_played,
    get_user_unlocked_areas,
    unlock_user_area,
    reset_user_areas,
    atomic_deduct_balance,
    refund_balance,
    get_user_gather_full_data,
    get_user_harvest_full_data,
    get_user_dossier,
    perform_harvest_batch_update,
    get_user_shop_inventory,
    has_shop_item,
    get_user_daily_shop_purchases,
    purchase_daily_shop_item,
    get_roulette_elimination_cooldown_seconds,
    get_slot_token_free_spin_used_date_est,
    set_slot_token_free_spin_used_date_est,
    get_user_ids_with_shop_item,
    add_shop_item_to_user,
    add_dayboost,
    get_dayboost_count,
    get_all_dayboosts,
    steal_revert_gather,
    steal_apply_gather,
    steal_revert_harvest,
    steal_apply_harvest,
    get_user_beta_tester,
    set_user_beta_tester,
    get_user_server_booster,
    set_user_server_booster,
    get_user_premium_tier,
    set_user_premium_tier,
    get_all_user_ids_with_premium_tier,
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
intents.invites = True
bot = commands.Bot(command_prefix='/', intents=intents)

# Global invite cache for tracking who invited whom
_invite_cache = {}

# Per-user locks to prevent concurrent /imbue operations for the same user
_imbue_locks: dict[int, asyncio.Lock] = {}

# Per-user locks so only one of gather/harvest post-response sends a rank-up embed for the same user
_planter_role_locks: dict[int, asyncio.Lock] = {}

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
    """Safely defer an interaction, handling all possible errors. Call this FIRST in every button/callback to avoid 'This interaction failed.'"""
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
        boost_percent = level_data["boost"] * 100
        
        embed = discord.Embed(
            title="\U0001f3c6 Achievement Unlocked!",
            description=f"**{achievement_display_name}**\n{achievement_description}",
            color=discord.Color.gold()
        )
        if boost_percent > 0:
            embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
        
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
    boost_percent = achievement_data["boost"] * 100
    
    embed = discord.Embed(
        title="\U0001f3c6 Hidden Achievement Unlocked!",
        description=f"**{achievement_name}**\n{achievement_description}",
        color=discord.Color.gold()
    )
    if boost_percent > 0:
        embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
    
    try:
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error sending hidden achievement notification: {e}")

async def send_achievement_notification_async(channel, user_mention: str, achievement_name: str, level: int):
    """Send an achievement notification to a channel (e.g. after PvE defeat)."""
    if achievement_name not in ACHIEVEMENTS or not channel:
        return
    achievement_data = ACHIEVEMENTS[achievement_name]
    levels = achievement_data["levels"]
    if level < len(levels):
        level_data = levels[level]
        achievement_display_name = level_data["name"]
        achievement_description = level_data["description"]
        boost_percent = level_data["boost"] * 100
        embed = discord.Embed(
            title="\U0001f3c6 Achievement Unlocked!",
            description=f"{user_mention}\n\n**{achievement_display_name}**\n{achievement_description}",
            color=discord.Color.gold()
        )
        if boost_percent > 0:
            embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
        try:
            await channel.send(embed=embed)
        except Exception as e:
            print(f"Error sending achievement notification to channel: {e}")


# Helper function to send hidden achievement notification (for non-interaction contexts like background tasks)
async def send_hidden_achievement_notification_async(channel, user_mention: str, achievement_key: str):
    """Send a hidden achievement notification embed to a channel."""
    if achievement_key not in HIDDEN_ACHIEVEMENTS:
        return
    
    achievement_data = HIDDEN_ACHIEVEMENTS[achievement_key]
    achievement_name = achievement_data["name"]
    achievement_description = achievement_data["description"]
    boost_percent = achievement_data["boost"] * 100
    
    embed = discord.Embed(
        title="\U0001f3c6 Hidden Achievement Unlocked!",
        description=f"{user_mention}\n\n**{achievement_name}**\n{achievement_description}",
        color=discord.Color.gold()
    )
    if boost_percent > 0:
        embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
    
    try:
        await channel.send(embed=embed)
    except Exception as e:
        print(f"Error sending hidden achievement notification: {e}")


async def send_hidden_achievement_notification_dm(user_id: int, achievement_key: str):
    """Send a hidden achievement notification via DM when it can't be sent as ephemeral."""
    if achievement_key not in HIDDEN_ACHIEVEMENTS:
        return
    
    achievement_data = HIDDEN_ACHIEVEMENTS[achievement_key]
    achievement_name = achievement_data["name"]
    achievement_description = achievement_data["description"]
    boost_percent = achievement_data["boost"] * 100
    
    embed = discord.Embed(
        title="\U0001f3c6 Hidden Achievement Unlocked!",
        description=f"**{achievement_name}**\n{achievement_description}",
        color=discord.Color.gold()
    )
    if boost_percent > 0:
        embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
    
    try:
        user = bot.get_user(user_id) or await bot.fetch_user(user_id)
        if user:
            await user.send(embed=embed)
    except Exception as e:
        print(f"Error sending hidden achievement DM to user {user_id}: {e}")


async def send_achievement_notification_dm(user_id: int, achievement_name: str, level: int):
    """Send an achievement notification via DM when no interaction is available (e.g. Russian Roulette)."""
    if achievement_name not in ACHIEVEMENTS:
        return
    
    achievement_data = ACHIEVEMENTS[achievement_name]
    levels = achievement_data["levels"]
    
    if level < len(levels):
        level_data = levels[level]
        achievement_display_name = level_data["name"]
        achievement_description = level_data["description"]
        boost_percent = level_data["boost"] * 100
        
        embed = discord.Embed(
            title="\U0001f3c6 Achievement Unlocked!",
            description=f"**{achievement_display_name}**\n{achievement_description}",
            color=discord.Color.gold()
        )
        if boost_percent > 0:
            embed.add_field(name="\U0001f4b0 Boost", value=f"**+{boost_percent:.1f}%**", inline=False)
        
        try:
            user = bot.get_user(user_id) or await bot.fetch_user(user_id)
            if user:
                await user.send(embed=embed)
        except Exception as e:
            print(f"Error sending achievement DM to user {user_id}: {e}")


async def check_russian_roulette_achievement(player_id: int, interaction=None):
    """Increment russian games played and check/award achievement. Sends notification via interaction (ephemeral) or DM."""
    try:
        new_count = increment_user_russian_games_played(player_id)
        new_level = get_achievement_level_for_stat("russian_roulette", new_count)
        current_level = get_user_achievement_level(player_id, "russian_roulette")
        if new_level > current_level:
            set_user_achievement_level(player_id, "russian_roulette", new_level)
            if interaction is not None:
                await send_achievement_notification(interaction, "russian_roulette", new_level)
            else:
                await send_achievement_notification_dm(player_id, "russian_roulette", new_level)
    except Exception as e:
        print(f"Error checking russian roulette achievement for {player_id}: {e}")

GATHER_COOLDOWN = 60 #(seconds)
HARVEST_COOLDOWN = 60 * 30 #(30 minutes)
MINE_COOLDOWN = 60 * 60 #(1 hour)
ROULETTE_ELIMINATION_COOLDOWN = 60 * 30 #(30 minutes)
COINFLIP_LOSS_COOLDOWN = 10  # seconds before next /coinflip after losing

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
    {"name": "NATIVIDIA RooTX 3050", "percent_increase": 50, "seconds_increase": 3, "price": 600},
    {"name": "NATIVIDIA RooTX 2060", "percent_increase": 70, "seconds_increase": 4, "price": 1500},
    {"name": "Plantel Barc B580", "percent_increase": 100, "seconds_increase": 5, "price": 4000},
    {"name": "NATIVIDIA RooTX 3070", "percent_increase": 150, "seconds_increase": 8, "price": 10000},
    {"name": "RayMD RX 5700XT", "percent_increase": 300, "seconds_increase": 12, "price": 50000},
    {"name": "NATIVIDIA GrowTX 1080-Ti", "percent_increase": 500, "seconds_increase": 20, "price": 110000},
    {"name": "RayMD RX 9060-XT", "percent_increase": 750, "seconds_increase": 30, "price": 200000},
    {"name": "NATIVIDIA RooTX 4070 Ti Super", "percent_increase": 1200, "seconds_increase": 45, "price": 400000},
    {"name": "NATIVIDIA RooTX 4090", "percent_increase": 2000, "seconds_increase": 60, "price": 1000000},
    {"name": "NATIVIDIA RooTX 5090", "percent_increase": 6000, "seconds_increase": 100, "price": 2000000},
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
    {"name": "Oven Mitts", "chain_chance": 0.02},
    {"name": "Latex Gloves", "chain_chance": 0.04},
    {"name": "Surgical Gloves", "chain_chance": 0.065},
    {"name": "Green Thumb Gloves", "chain_chance": 0.10},
    {"name": "Astral Gloves", "chain_chance": 0.14},
    {"name": "Spectral Gloves", "chain_chance": 0.1875},
    {"name": "Luminite Mitts", "chain_chance": 0.24},
    {"name": "Plutonium Hands", "chain_chance": 0.30},
    {"name": "Galaxial Gloves", "chain_chance": 0.35},
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
    {"name": "Spectral Season", "chain_chance": 0.14},
    {"name": "Galaxial Season", "chain_chance": 0.20},
    {"name": "Universal Season", "chain_chance": 0.30},
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

# ============================================================
# ENCHANTMENT SYSTEM (/imbue)
# ============================================================

IMBUE_HOE_COST = 750_000
IMBUE_TRACTOR_COST = 4_000_000

# Rarity weights (sum to 100 — each weight IS the % chance)
ENCHANTMENT_RARITIES = [
    {"name": "COMMON",     "weight": 49.35994},  # ~49.36%
    {"name": "UNCOMMON",   "weight": 28.0},       # 28%
    {"name": "RARE",       "weight": 12.0},       # 12%
    {"name": "SUPER RARE", "weight": 6.0},        # 6%
    {"name": "LEGENDARY",  "weight": 3.0},        # 3%
    {"name": "NETHERITE",  "weight": 1.0},        # 1%
    {"name": "LUMINITE",   "weight": 0.5},        # 0.5%
    {"name": "CELESTIAL",  "weight": 0.1234},     # 0.1234%
    {"name": "SECRET",     "weight": 0.01666},    # 0.01666%
]

RARITY_COLORS = {
    "COMMON":     0x808080,  # gray
    "UNCOMMON":   0x2ecc71,  # green
    "RARE":       0x3498db,  # blue
    "SUPER RARE": 0x9b59b6,  # purple
    "LEGENDARY":  0xe67e22,  # orange
    "NETHERITE":  0x5c3a1e,  # dark brown
    "LUMINITE":   0x7fdbda,  # light teal
    "CELESTIAL":  0x8a2be2,  # bright violet purple
    "SECRET":     0x000000,  # black
}

# Custom emoji IDs from Discord CDN (format <:name:id> for display in messages)
RARITY_EMOJI = {
    "COMMON":      "<:IMBUE_C:1472431378900451441>",
    "UNCOMMON":    "<:IMBUE_UC:1472432117966307349>",
    "RARE":        "<:IMBUE_R:1472431562564833361>",
    "SUPER RARE":  "<:IMBUE_SR:1472431974428704999>",
    "LEGENDARY":   "<:IMBUE_L:1472431641975717971>",
    "NETHERITE":   "<:IMBUE_N:1472431697642520576>",
    "LUMINITE":    "<:IMBUE_LUM:1477751680199164075>",
    "CELESTIAL":   "<:IMBUE_CE:1472431208859439295>",
    "SECRET":      "<:IMBUE_SEC:1473395529554591848>"
}

# Progress bar emojis for /gear, /orchard, /achievements (white = not filled, green = filled)
PROGRESS_N = "<:PROGRESS_N:1477736801480474634>"  # white square
PROGRESS_Y = "<:PROGRESS_Y:1477736340941701416>"  # green square
PROGRESS_Y_PARTIAL = discord.PartialEmoji(name="PROGRESS_Y", id=1477736340941701416, animated=False)  # for giveaway entry reaction

# Ripeness name -> imbue tier for plant display (COMMON/UNCOMMON/RARE + rares LEGENDARY/NETHERITE/LUMINITE/CELESTIAL/SECRET)
RIPENESS_IMBUE_TIER = {
    "Budding": "COMMON", "Flowering": "UNCOMMON", "Raw": "COMMON", "Slightly Ripe": "UNCOMMON",
    "Perfectly Ripe": "RARE", "Overripe": "UNCOMMON", "Spoiled": "COMMON",
    "Sproutling": "COMMON", "Budded": "COMMON", "Blooming": "UNCOMMON", "Full Bloom": "RARE", "Wilted": "COMMON",
    "Legendary": "LEGENDARY", "Netherite": "NETHERITE", "Luminite": "LUMINITE", "Celestial": "CELESTIAL", "Mikellion": "SECRET",
}

def get_ripeness_imbue_emoji(ripeness_name: str) -> str:
    """Return the IMBUE emoji for a plant's ripeness tier, or empty string if unknown."""
    tier = RIPENESS_IMBUE_TIER.get((ripeness_name or "").strip())
    return RARITY_EMOJI.get(tier, "") if tier else ""

# GIF emojis used to mask SECRET imbue stats until claimed.
IMBUE_SEC_GIF_1 = "<a:IMBUE_SEC_1:1474405245563175175>"
IMBUE_SEC_GIF_2 = "<a:IMBUE_SEC_2:1474405298385981502>"

def _to_roman(num):
    """Convert integer to Roman numeral string (supports negatives)."""
    if num == 0:
        return "0"
    negative = num < 0
    n = abs(num)
    vals = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
    syms = ['M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I']
    result = ''
    for i in range(len(vals)):
        while n >= vals[i]:
            result += syms[i]
            n -= vals[i]
    return f"-{result}" if negative else result


def _make_hoe_enchant(name, rarity, description, resonance=0, prosperity=0, renewal=0, abundance=0):
    """Create a standard hoe imbuement from level values.
    Hoe formulas: chain=resonance*5%, money=prosperity*20%, cooldown=renewal*1s, crit=abundance*2.5%"""
    return {
        "name": name, "rarity": rarity, "description": description,
        "chain_chance": resonance * 0.05,
        "money_bonus": prosperity * 0.20,
        "cooldown_reduction": renewal,  # seconds (positive = faster)
        "critical_chance": abundance * 0.025,
        "additional_plants": 0,
        "levels": {"resonance": resonance, "prosperity": prosperity, "renewal": renewal, "abundance": abundance, "natures_favor": 0},
    }


def _make_custom_hoe_enchant(name, rarity, description, chain_chance=0, money_bonus=0, cooldown_reduction=0, critical_chance=0, display_levels=None):
    """Create a custom hoe imbuement with explicit effect values (for Netherite+ rarities)."""
    return {
        "name": name, "rarity": rarity, "description": description,
        "chain_chance": chain_chance,
        "money_bonus": money_bonus,
        "cooldown_reduction": cooldown_reduction,
        "critical_chance": critical_chance,
        "additional_plants": 0,
        "levels": display_levels or {},
    }


def _make_tractor_enchant(name, rarity, description, resonance=0, prosperity=0, renewal=0, natures_favor=0):
    """Create a standard tractor imbuement from level values.
    Tractor formulas: chain=resonance*2.5%, money=prosperity*10%, cooldown=renewal*30s, plants=natures_favor*1"""
    return {
        "name": name, "rarity": rarity, "description": description,
        "chain_chance": resonance * 0.025,
        "money_bonus": prosperity * 0.10,
        "cooldown_reduction": renewal * 30,  # seconds
        "critical_chance": 0,
        "additional_plants": natures_favor,
        "levels": {"resonance": resonance, "prosperity": prosperity, "renewal": renewal, "abundance": 0, "natures_favor": natures_favor},
    }


def _make_custom_tractor_enchant(name, rarity, description, chain_chance=0, money_bonus=0, cooldown_reduction=0, additional_plants=0, display_levels=None):
    """Create a custom tractor imbuement with explicit effect values (for Netherite+ rarities)."""
    return {
        "name": name, "rarity": rarity, "description": description,
        "chain_chance": chain_chance,
        "money_bonus": money_bonus,
        "cooldown_reduction": cooldown_reduction,
        "critical_chance": 0,
        "additional_plants": additional_plants,
        "levels": display_levels or {},
    }


# ---- HOE ENCHANTMENTS (for /gather) ----
HOE_ENCHANTMENTS = {
    "COMMON": [
        _make_hoe_enchant("CURSED TILL", "COMMON", "Your hoe is cursed!", resonance=-2, prosperity=1),
        _make_hoe_enchant("HANDLEBROKE", "COMMON", "Your hoe's handle is completely broken!", resonance=-2, prosperity=-1, renewal=3),
        _make_hoe_enchant("RUSTED", "COMMON", "Your hoe's rusted!", resonance=1, prosperity=1, renewal=-3),
        _make_hoe_enchant("SPLINTERED", "COMMON", "Your hoe's handle gives you splinters!", resonance=-2, renewal=-5, abundance=1),
        _make_hoe_enchant("DULLBLADE", "COMMON", "Your hoe's blade is dulling!", prosperity=-1, abundance=2, renewal=1, resonance=1),
        _make_hoe_enchant("DUSTWORN", "COMMON", "Your hoe has been worn with time!", prosperity=1, renewal=1),
        _make_hoe_enchant("VOIDGRAIN", "COMMON", "Your plants are coming straight from the void!", prosperity=1, renewal=2),
        _make_hoe_enchant("TARNISHED TILL", "COMMON", "Your hoe is tarnished!", prosperity=-1, renewal=-5),
        _make_hoe_enchant("SHALLOWCUT", "COMMON", "Your hoe's handle is shorter!", renewal=4, prosperity=-1, resonance=1),
        _make_hoe_enchant("WORMGNAW", "COMMON", "Worms have been nibbling at the handle!", abundance=2, renewal=-2, resonance=-1),
        _make_hoe_enchant("CLAYCAKED", "COMMON", "Dried clay is caked on the blade!", prosperity=2, renewal=-1, abundance=-1),
        _make_hoe_enchant("ROOTBOUND", "COMMON", "Tiny roots have grown into the wood!", renewal=3, abundance=1, prosperity=-1),
        _make_hoe_enchant("SUNCRACKED", "COMMON", "The handle is cracked from the sun!", resonance=2, renewal=-3, prosperity=1),
    ],
    "UNCOMMON": [
        _make_hoe_enchant("WINDBLESSED", "UNCOMMON", "Your hoe has been blessed by the wind!", prosperity=2, renewal=3, resonance=2),
        _make_hoe_enchant("DEWFORGED", "UNCOMMON", "Your hoe has been infused with water!", prosperity=1, renewal=1, resonance=1, abundance=1),
        _make_hoe_enchant("CROPWOVEN", "UNCOMMON", "Your hoe can get more valuable crops!", prosperity=4),
        _make_hoe_enchant("ABYSSALSEED", "UNCOMMON", "Your hoe is from the abyss!", prosperity=1, abundance=2, resonance=1),
        _make_hoe_enchant("THISTLEBOUND", "UNCOMMON", "Your hoe is prone to thistles!", abundance=4, renewal=-5, resonance=-1),
        _make_hoe_enchant("THORNWOVEN", "UNCOMMON", "Your hoe's handle is reinforced with thorns!", abundance=3, renewal=-2, resonance=1),
        _make_hoe_enchant("SAPLINE", "UNCOMMON", "Your hoe has been made with the finest of saplings!", abundance=1, prosperity=3, renewal=3, resonance=1),
    ],
    "RARE": [
        _make_hoe_enchant("SUNCREST", "RARE", "Your hoe has been blessed with the sun!", prosperity=6, renewal=1),
        _make_hoe_enchant("BLOOMGOLD", "RARE", "Your hoe has been infused with blooming flowers!", prosperity=5, renewal=-5, resonance=-2),
        _make_hoe_enchant("STORMROOT", "RARE", "Your hoe has the power of storms!", prosperity=4, abundance=1, resonance=-1, renewal=-2),
        _make_hoe_enchant("EMBERWIND", "RARE", "Your hoe has been imbued with embers!", prosperity=2, abundance=3, resonance=1, renewal=2),
        _make_hoe_enchant("SCYTHEREAP", "RARE", "Your hoe's blade has been sharpened into a scythe!", prosperity=5, abundance=1, resonance=2, renewal=1),
        _make_hoe_enchant("RAINSPROUT", "RARE", "Your hoe calls upon the rain to nourish the soil!", prosperity=4, renewal=2, abundance=2, resonance=1),
        _make_hoe_enchant("GOLDENFURROW", "RARE", "Your furrows gleam like gold!", prosperity=6, abundance=1, resonance=-1, renewal=-1),
        _make_hoe_enchant("FROSTTHAW", "RARE", "Your hoe breaks the frost and wakes the earth!", prosperity=3, renewal=4, abundance=2, resonance=1),
        _make_hoe_enchant("HARVESTCROWN", "RARE", "Your hoe wears a crown of harvest!", prosperity=5, abundance=2, renewal=1, resonance=2),
    ],
    "SUPER RARE": [
        _make_hoe_enchant("TITANBLOOM", "SUPER RARE", "Your hoe's been blessed by the titans!", prosperity=6, abundance=3, renewal=-5, resonance=5),
        _make_hoe_enchant("LIGHTGROWN", "SUPER RARE", "Your hoe's been blessed by light!", prosperity=6, abundance=2, renewal=8),
        _make_hoe_enchant("SOL'S MEMORY", "SUPER RARE", "An ancient hoe, thought to be forged from within the sun itself!", prosperity=7, abundance=3, renewal=5, resonance=3),
        _make_hoe_enchant("THUNDERSOW", "SUPER RARE", "Your hoe has been charged by lightning!", prosperity=5, abundance=2, renewal=9, resonance=2),
        _make_hoe_enchant("MOONBLESSED", "SUPER RARE", "Your hoe has been blessed by the moon!", prosperity=6, abundance=3, renewal=6, resonance=3),
        _make_hoe_enchant("FIRST LIGHT'S KISS", "SUPER RARE", "The first ray of sun blesses every furrow you draw!", prosperity=7, abundance=2, renewal=7, resonance=2),
        _make_hoe_enchant("VOIDBLOSSOM", "SUPER RARE", "Your hoe blooms from the void!", prosperity=5, abundance=4, renewal=5, resonance=4),
    ],
    "LEGENDARY": [
        _make_hoe_enchant("STARFORGED", "LEGENDARY", "Your hoe has been forged from the cosmos!", prosperity=9, abundance=3, renewal=8, resonance=3),
        _make_hoe_enchant("MONARCH'S RAKE", "LEGENDARY", "A pristine, auric hoe from an old king, thousands of years past!", prosperity=10, abundance=5, renewal=10, resonance=4),
        _make_hoe_enchant("SOULBOUND", "LEGENDARY", "Your hoe has been infused with souls of past gardeners!", prosperity=9, abundance=4, renewal=7, resonance=4),
        _make_hoe_enchant("ETERNAL HARVEST", "LEGENDARY", "Your hoe shall reap forever!", prosperity=10, abundance=4, renewal=9, resonance=4),
        _make_hoe_enchant("ELDER EARTH", "LEGENDARY", "This hoe remembers when the soil was new!", prosperity=9, abundance=5, renewal=8, resonance=5),
        _make_hoe_enchant("COSMOS REAPER", "LEGENDARY", "Your hoe reaps across the cosmos!", prosperity=10, abundance=3, renewal=10, resonance=4),
    ],
    "NETHERITE": [
        _make_custom_hoe_enchant("GRANDMASTER'S FURROW", "NETHERITE", "This tool once belonged to a valiant hero!",
            critical_chance=0.15, money_bonus=3.00, chain_chance=0.15, cooldown_reduction=10,
            display_levels={"abundance": 6, "prosperity": 15, "resonance": 3, "renewal": 10}),
        _make_custom_hoe_enchant("EDEN'S GENESIS", "NETHERITE", "A holy hoe, one who gave birth to an ancient garden!",
            critical_chance=0.20, money_bonus=4.00,
            display_levels={"abundance": 8, "prosperity": 20}),
        _make_custom_hoe_enchant("CINDER'S EMBRACE", "NETHERITE", "Warm embers linger in every cut your hoe makes!",
            critical_chance=0.12, money_bonus=2.80, chain_chance=0.12, cooldown_reduction=8,
            display_levels={"abundance": 5, "prosperity": 14, "resonance": 2, "renewal": 8}),
        _make_custom_hoe_enchant("HOLLOW HARVEST", "NETHERITE", "Your hoe reaps what grows in the quiet between worlds!",
            critical_chance=0.18, money_bonus=3.50, chain_chance=0.10, cooldown_reduction=12,
            display_levels={"abundance": 7, "prosperity": 17, "resonance": 2, "renewal": 12}),
    ],
    "LUMINITE": [
        _make_custom_hoe_enchant("EARTHSHAPER", "LUMINITE", "This tool can shape the earth to its will!",
            critical_chance=0.20, money_bonus=3.00, chain_chance=0.15, cooldown_reduction=10,
            display_levels={"abundance": 6, "prosperity": 15, "resonance": 3, "renewal": 10}),
        _make_custom_hoe_enchant("AURORABORN RELIC", "LUMINITE", "An ancient tool born out of heavenly lights!",
            critical_chance=0.05, money_bonus=5.00, chain_chance=0.10, cooldown_reduction=20,
            display_levels={"abundance": 2, "prosperity": 25, "resonance": 2, "renewal": 20}),
        _make_custom_hoe_enchant("STARCRASH TILL", "LUMINITE", "Your hoe tills soil that remembers falling stars!",
            critical_chance=0.18, money_bonus=3.80, chain_chance=0.14, cooldown_reduction=14,
            display_levels={"abundance": 7, "prosperity": 19, "resonance": 3, "renewal": 14}),
        _make_custom_hoe_enchant("LIMINAL FURROW", "LUMINITE", "Your hoe works the threshold between day and night!",
            critical_chance=0.15, money_bonus=4.20, chain_chance=0.12, cooldown_reduction=18,
            display_levels={"abundance": 6, "prosperity": 21, "resonance": 2, "renewal": 18}),
    ],
    "CELESTIAL": [
        _make_custom_hoe_enchant("CULTISCYTHE OF THE LIGHTBRINGER", "CELESTIAL", "Razor-sharp & blessed by the sun!",
            critical_chance=0.225, money_bonus=8.00, chain_chance=0.20, cooldown_reduction=15,
            display_levels={"abundance": 7, "prosperity": 40, "resonance": 4, "renewal": 9}),
        _make_custom_hoe_enchant("NOONSTRIKE", "CELESTIAL", "Your hoe strikes with the weight of the high sun!",
            critical_chance=0.20, money_bonus=7.50, chain_chance=0.18, cooldown_reduction=18,
            display_levels={"abundance": 8, "prosperity": 37, "resonance": 4, "renewal": 18}),
        _make_custom_hoe_enchant("PLANTA MAXIMA LUNAE", "CELESTIAL", "The moon's greatest plant blesses every seed you sow!",
            critical_chance=0.22, money_bonus=8.50, chain_chance=0.19, cooldown_reduction=16,
            display_levels={"abundance": 9, "prosperity": 42, "resonance": 4, "renewal": 16}),
    ],
    "SECRET": [
        _make_custom_hoe_enchant("FLORAL BANE OF VEGETABLES", "SECRET", "The ultimate hoe for /gather!",
            critical_chance=0.30, money_bonus=16.00, chain_chance=0.25, cooldown_reduction=25,
            display_levels={"abundance": 12, "prosperity": 80, "resonance": 5, "renewal": 25}),
    ],
}

# ---- TRACTOR ENCHANTMENTS (for /harvest) ----
TRACTOR_ENCHANTMENTS = {
    "COMMON": [
        _make_tractor_enchant("CURSED WHEEL", "COMMON", "Your wheels on the tractor are cursed!", resonance=-1, prosperity=1),
        _make_tractor_enchant("STEERBROKE", "COMMON", "The steering wheel isn't functioning properly!", resonance=-2, prosperity=-1, renewal=-3),
        _make_tractor_enchant("LEAKOIL", "COMMON", "Your tractor is leaking oil!", resonance=1, prosperity=1, renewal=-3),
        _make_tractor_enchant("REINFORCED", "COMMON", "The metal in your tractor is stronger!", resonance=1, renewal=-1, prosperity=1),
        _make_tractor_enchant("HIGHMILE", "COMMON", "Your tractor now gets high gas mileage!", renewal=-3, prosperity=2),
        _make_tractor_enchant("PRESSURELESS", "COMMON", "Your tractor's tires have low air pressure!", prosperity=1, resonance=-1, renewal=1),
        _make_tractor_enchant("GREASEWORN", "COMMON", "Your tractor's gears are greasy!", prosperity=1, renewal=2),
        _make_tractor_enchant("MUDCLOGGED", "COMMON", "Your tractor's clogged with mud!", prosperity=-2, renewal=3, resonance=-1),
        _make_tractor_enchant("HAYBOUND", "COMMON", "Your tractor is prone to collect more!", prosperity=2),
        _make_tractor_enchant("CHOKEDUST", "COMMON", "Dust has choked the air filter!", prosperity=1, renewal=-2, resonance=-1),
        _make_tractor_enchant("TIREBALD", "COMMON", "Your tires are bald!", resonance=1, renewal=1, prosperity=-1),
        _make_tractor_enchant("RATTLESHAFT", "COMMON", "The driveshaft rattles but holds!", renewal=2, resonance=-1, prosperity=1),
        _make_tractor_enchant("SPUTTERFUEL", "COMMON", "The engine sputters on bad fuel!", resonance=-2, renewal=2, prosperity=1),
        _make_tractor_enchant("WHEATDUST", "COMMON", "Wheat dust coats everything!", prosperity=1, renewal=1, resonance=1),
    ],
    "UNCOMMON": [
        _make_tractor_enchant("FIELDRUNNER", "UNCOMMON", "Your tractor gets across the field faster!", prosperity=1, renewal=5, resonance=-1),
        _make_tractor_enchant("HEAVYWHEEL", "UNCOMMON", "Your wheels are heavier!", prosperity=2, renewal=2),
        _make_tractor_enchant("GROUNDBREAKER", "UNCOMMON", "The soil lets you pass through easier!", prosperity=1, renewal=5, resonance=1),
        _make_tractor_enchant("LOADBEARER'S DRIVE", "UNCOMMON", "Your tractor will follow an ancient, optimized route!", prosperity=3),
        _make_tractor_enchant("SMOKEBOUND", "UNCOMMON", "Your engine is smoking!", prosperity=3, resonance=1, renewal=-4),
        _make_tractor_enchant("TREADWOVEN", "UNCOMMON", "Your tire treads were made stronger!", prosperity=2, renewal=1, resonance=1),
        _make_tractor_enchant("TORQUETUNED", "UNCOMMON", "Your tractor gets more torque!", natures_favor=1),
    ],
    "RARE": [
        _make_tractor_enchant("GOLDENCREST", "RARE", "Your tractor is gilded with gold!", prosperity=4, renewal=3, resonance=2),
        _make_tractor_enchant("BLOOMINION", "RARE", "Your tractor has dominion over flowers!", natures_favor=1, prosperity=4),
        _make_tractor_enchant("OVERDRIVE", "RARE", "Your tractor can now shift into overdrive!", prosperity=3, renewal=7),
        _make_tractor_enchant("COMBUSTINE", "RARE", "Your tractor can transform into a combine!", prosperity=4, natures_favor=2, renewal=-5),
        _make_tractor_enchant("TITANDEEP FIELDBREAKER", "RARE", "Your tractor once belonged to the titans!", prosperity=3, resonance=2, renewal=1, natures_favor=1),
        _make_tractor_enchant("SILVERPLOW", "RARE", "Your tractor is plated with silver!", prosperity=4, renewal=4, resonance=2),
        _make_tractor_enchant("RAINFIELD", "RARE", "Your tractor thrives in the rain!", prosperity=3, renewal=5, resonance=1, natures_favor=1),
        _make_tractor_enchant("GRAINMASTER", "RARE", "Your tractor was built for grain!", prosperity=5, renewal=2, natures_favor=1),
        _make_tractor_enchant("IRONROOT", "RARE", "Your tractor's frame is forged like roots!", prosperity=4, renewal=3, resonance=2, natures_favor=1),
    ],
    "SUPER RARE": [
        _make_tractor_enchant("MONARCH'S MOTOR", "SUPER RARE", "Your tractor has been passed down by an ancient king!", prosperity=6, renewal=5, resonance=3),
        _make_tractor_enchant("HARVEST LIGHTCORE", "SUPER RARE", "Your tractor's been infused with light!", prosperity=7, natures_favor=2, renewal=8, resonance=-1),
        _make_tractor_enchant("LUNA'S MEMORY", "SUPER RARE", "An ancient tractor, thought to be forged from within the moon itself!", prosperity=8, natures_favor=2),
        _make_tractor_enchant("THUNDERPLOW", "SUPER RARE", "Your tractor's engine has been charged by lightning!", prosperity=5, natures_favor=1, renewal=9, resonance=-1),
        _make_tractor_enchant("MOONDRIVE", "SUPER RARE", "Your tractor runs under the moon's blessing!", prosperity=7, natures_favor=2, renewal=6, resonance=2),
        _make_tractor_enchant("DAYBREAK'S BEGINNING", "SUPER RARE", "Your tractor plows at the break of dawn!", prosperity=6, renewal=8, resonance=3, natures_favor=1),
        _make_tractor_enchant("STARFIELD", "SUPER RARE", "Your tractor harvests under the stars!", prosperity=6, natures_favor=2, renewal=7, resonance=3),
    ],
    "LEGENDARY": [
        _make_tractor_enchant("BOREALIS ENGINE", "LEGENDARY", "Your tractor has been forged from the cosmos!", prosperity=9, natures_favor=2, renewal=8, resonance=3),
        _make_tractor_enchant("WORLDPLOW", "LEGENDARY", "Your tractor can run through any terrain!", prosperity=10, renewal=10, resonance=2, natures_favor=3),
        _make_tractor_enchant("SOULBINDED DRIVESHAFT", "LEGENDARY", "Your tractor has been infused with souls of past gardeners!", prosperity=9, natures_favor=2, renewal=9, resonance=3),
        _make_tractor_enchant("ETERNAL PLOW", "LEGENDARY", "Your tractor shall plow forever!", prosperity=10, natures_favor=3, renewal=10, resonance=4),
        _make_tractor_enchant("PRIMORDIAL AXLES", "LEGENDARY", "Your tractor runs on the fire of the first dawn!", prosperity=9, natures_favor=2, renewal=9, resonance=4),
        _make_tractor_enchant("COSMOS DRIVE", "LEGENDARY", "Your tractor drives across the cosmos!", prosperity=10, natures_favor=3, renewal=10, resonance=3),
    ],
    "NETHERITE": [
        _make_custom_tractor_enchant("ASHEN COLOSSUS", "NETHERITE", "A tractor to plow through volcanic lava, rock, and ash!",
            money_bonus=1.70, chain_chance=0.075, cooldown_reduction=300, additional_plants=4,
            display_levels={"prosperity": 17, "resonance": 3, "renewal": 10, "natures_favor": 4}),
        _make_custom_tractor_enchant("DEERE OF EDEN", "NETHERITE", "A holy tractor, one who tilled an ancient garden!",
            additional_plants=5, money_bonus=2.00,
            display_levels={"natures_favor": 5, "prosperity": 20}),
        _make_custom_tractor_enchant("EMBERKISSED DRIVESHAFT", "NETHERITE", "Your tractor runs on eternal embers!",
            money_bonus=1.50, chain_chance=0.065, cooldown_reduction=270, additional_plants=3,
            display_levels={"prosperity": 15, "resonance": 2, "renewal": 9, "natures_favor": 3}),
        _make_custom_tractor_enchant("ESSENCE OF THE VOID", "NETHERITE", "Your tractor plows from the edge of the void!",
            money_bonus=1.90, chain_chance=0.085, cooldown_reduction=330, additional_plants=4,
            display_levels={"prosperity": 19, "resonance": 3, "renewal": 11, "natures_favor": 4}),
    ],
    "LUMINITE": [
        _make_custom_tractor_enchant("RIG OF RADIANCE", "LUMINITE", "This tractor radiates solar energy!",
            money_bonus=2.50, chain_chance=0.10, cooldown_reduction=420,
            display_levels={"prosperity": 25, "resonance": 4, "renewal": 14}),
        _make_custom_tractor_enchant("ABYSSAL OVERDRIVE", "LUMINITE", "Your engine is powered from within the abyss!",
            money_bonus=2.40, chain_chance=0.05, cooldown_reduction=300, additional_plants=6,
            display_levels={"prosperity": 25, "resonance": 2, "renewal": 10, "natures_favor": 6}),
        _make_custom_tractor_enchant("STARDUST GEAR RATIO", "LUMINITE", "Your tractor is woven from stardust!",
            money_bonus=2.80, chain_chance=0.08, cooldown_reduction=390, additional_plants=5,
            display_levels={"prosperity": 28, "resonance": 3, "renewal": 13, "natures_favor": 5}),
        _make_custom_tractor_enchant("TWILIGHTED DUSK TO DAWN", "LUMINITE", "Your tractor runs between day and night!",
            money_bonus=2.60, chain_chance=0.09, cooldown_reduction=360, additional_plants=5,
            display_levels={"prosperity": 26, "resonance": 4, "renewal": 12, "natures_favor": 5}),
    ],
    "CELESTIAL": [
        _make_custom_tractor_enchant("TRACTIC SUPERNOVA CORE", "CELESTIAL", "Blessed by the cosmos!",
            money_bonus=4.50, chain_chance=0.125, cooldown_reduction=450, additional_plants=10,
            display_levels={"prosperity": 45, "resonance": 5, "renewal": 15, "natures_favor": 10}),
        _make_custom_tractor_enchant("SUNSPOT SPARKED ENGINE", "CELESTIAL", "Your tractor is forged in the heart of the sun!",
            money_bonus=4.20, chain_chance=0.12, cooldown_reduction=480, additional_plants=8,
            display_levels={"prosperity": 42, "resonance": 5, "renewal": 16, "natures_favor": 8}),
        _make_custom_tractor_enchant("ECLIPSED PLOWTHROUGH TREADS", "CELESTIAL", "Your tractor plows through the shadow of the eclipse!",
            money_bonus=4.80, chain_chance=0.13, cooldown_reduction=420, additional_plants=9,
            display_levels={"prosperity": 48, "resonance": 5, "renewal": 14, "natures_favor": 9}),
    ],
    "SECRET": [
        _make_custom_tractor_enchant("PROTOTYPE 13: ORBITAL \u03A9", "SECRET", "The ultimate tractor for /harvest!",
            money_bonus=8.50, chain_chance=0.15, cooldown_reduction=480, additional_plants=10,
            display_levels={"prosperity": 85, "resonance": 6, "renewal": 16, "natures_favor": 10}),
    ],
}


def roll_attunement(tool_type: str, user_id: int = None, exclude_enchant: dict = None) -> dict:
    """Roll a random imbuement for the given tool type ('hoe' or 'tractor').
    If user_id is provided and has Commoner's Respite, COMMON rarity is excluded.
    If exclude_enchant is provided (e.g. current imbuement), will not return the same enchant by name.
    Returns a copy of the imbuement dict."""
    enchant_pool = HOE_ENCHANTMENTS if tool_type == "hoe" else TRACTOR_ENCHANTMENTS
    exclude_name = (exclude_enchant or {}).get("name")

    for _ in range(100):  # max attempts to avoid infinite loop
        # Pick rarity using weighted random
        rarity_names = [r["name"] for r in ENCHANTMENT_RARITIES]
        rarity_weights = [r["weight"] for r in ENCHANTMENT_RARITIES]
        if user_id and has_shop_item(user_id, "commoners_respite"):
            # Exclude COMMON: set its weight to 0 and renormalize
            rarity_weights = [0.0 if name == "COMMON" else w for name, w in zip(rarity_names, rarity_weights)]
            total = sum(rarity_weights)
            if total <= 0:
                total = 1.0
            rarity_weights = [w / total for w in rarity_weights]
        chosen_rarity = random.choices(rarity_names, weights=rarity_weights, k=1)[0]

        # Pick random enchant from that rarity
        enchant = random.choice(enchant_pool[chosen_rarity])
        if exclude_name and enchant.get("name") == exclude_name:
            continue  # re-roll same enchant
        # Return a copy so we don't mutate the template
        return dict(enchant)

    # Fallback: return first enchant of a random rarity (should not happen in practice)
    chosen_rarity = random.choice(rarity_names)
    enchant = random.choice(enchant_pool[chosen_rarity])
    return dict(enchant)


def format_enchant_effects(enchant: dict, tool_type: str) -> str:
    """Format imbuement effects for display in an embed.
    Shows only the bold all-caps enchant type and roman numeral level, no percentages, no emojis."""
    parts = []
    levels = enchant.get("levels", {})

    resonance = levels.get("resonance", 0)
    if resonance != 0:
        parts.append(f"**RESONANCE {_to_roman(resonance)}**")

    prosperity = levels.get("prosperity", 0)
    if prosperity != 0:
        parts.append(f"**PROSPERITY {_to_roman(prosperity)}**")

    renewal = levels.get("renewal", 0)
    if renewal != 0:
        parts.append(f"**RENEWAL {_to_roman(renewal)}**")

    abundance = levels.get("abundance", 0)
    if abundance != 0:
        parts.append(f"**ABUNDANCE {_to_roman(abundance)}**")

    natures_favor = levels.get("natures_favor", 0)
    if natures_favor != 0:
        parts.append(f"**NATURE'S FAVOR {_to_roman(natures_favor)}**")

    return "\n".join(parts) if parts else "No effects"


def format_enchant_block(enchant: dict, tool_type: str) -> str:
    """Format a full imbuement block (name, rarity, description, effects) for embed display."""
    name = enchant.get("name", "Unknown")
    rarity = enchant.get("rarity", "COMMON")
    rarity_display = RARITY_EMOJI.get(rarity, f"[{rarity}]")
    desc = enchant.get("description", "")
    effects = format_enchant_effects(enchant, tool_type)
    return f"**{name}** {rarity_display}\n*\"{desc}\"*\n{effects}"


# Money handling helper functions to fix floating point precision issues
def normalize_money(amount: float) -> float:
    """Round money to exactly 2 decimal places to avoid floating point precision issues."""
    return round(amount, 2)


def validate_money_precision(amount: float) -> bool:
    """Check if a money amount has at most 2 decimal places (no fractional cents)."""
    # Multiply by 100 to convert to cents, then check if it's an integer
    cents = amount * 100
    return abs(cents - round(cents)) < 0.0001  # Small epsilon for floating point comparison


def format_money(amount: float | int) -> str:
    """Format a money value with commas, at most 2 decimals, and hide .00.

    Examples:
    - 1234        -> "$1,234"
    - 1234.5      -> "$1,234.50"
    - 1234.567    -> "$1,234.57"
    - 336000000.00000006 -> "$336,000,000"
    """
    # Normalize to two decimal places first to clamp any floating point noise
    normalized = normalize_money(float(amount))
    # Treat tiny values as zero
    if abs(normalized) < 0.0005:
        normalized = 0.0

    # If effectively a whole dollar amount, omit the decimal places
    if abs(normalized - int(normalized)) < 0.0005:
        return f"${int(normalized):,}"
    # Otherwise, show exactly two decimal places
    return f"${normalized:,.2f}"


def format_signed_money(amount: float | int) -> str:
    """Format money with an explicit + / - sign, hiding .00 when possible.

    Used for profit displays where sign matters.
    """
    normalized = normalize_money(float(amount))
    if abs(normalized) < 0.0005:
        return "$0"
    sign = "+" if normalized > 0 else "-"
    value = abs(normalized)
    if abs(value - int(value)) < 0.0005:
        return f"{sign}${int(value):,}"
    return f"{sign}${value:,.2f}"

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
        cooldown_sec = get_roulette_elimination_cooldown_seconds(user_id)
        cooldown_end = roulette_elimination_time + cooldown_sec
        if current_time < cooldown_end:
            time_left = int(cooldown_end - current_time)
            return True, time_left
    return False, 0

def can_harvest(user_id, full_data=None):
    """Check if user can harvest. Returns (can_harvest, time_left, is_roulette_cooldown).
    When *full_data* is provided, avoids all extra DB reads."""

    # Russian Roulette elimination cooldown
    if full_data is not None:
        roulette_time = full_data.get("last_roulette_elimination_time", 0)
    else:
        roulette_time = get_user_last_roulette_elimination_time(user_id)

    current_time = time.time()
    if roulette_time > 0:
        cooldown_sec = get_roulette_elimination_cooldown_seconds(user_id)
        cooldown_end = roulette_time + cooldown_sec
        if current_time < cooldown_end:
            return False, int(cooldown_end - current_time), True

    if full_data is not None:
        last_harvest_time = full_data.get("last_harvest_time", 0)
    else:
        last_harvest_time = get_user_last_harvest_time(user_id)

    if last_harvest_time == 0:
        return True, 0, False
    
    # Get harvest upgrades for cooldown reduction
    if full_data is not None:
        harvest_upgrades = full_data.get("harvest_upgrades", {})
    else:
        harvest_upgrades = get_user_harvest_upgrades(user_id)
    cooldown_tier = harvest_upgrades.get("cooldown", 0)
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
            cooldown_reduction += 120  # 2 minutes
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_day":
            cooldown_reduction += 60
    
    # Apply tractor imbuement cooldown reduction (Renewal)
    if full_data is not None:
        tractor_enchant = full_data.get("tractor_enchantment")
    else:
        tractor_enchant = get_user_tractor_attunement(user_id)
    if tractor_enchant:
        enchant_cd = tractor_enchant.get("cooldown_reduction", 0)
        cooldown_reduction += enchant_cd
    
    # Apply invite reward cooldown reduction (tier 14: -5 minutes = -300 seconds)
    if full_data is not None:
        claimed = full_data.get("invite_claimed_rewards", [])
        cooldown_reduction += 300 if 14 in claimed else 0
    else:
        invite_reductions = get_invite_cooldown_reductions(user_id)
        cooldown_reduction += invite_reductions.get("harvest_reduction", 0)
    premium_cd = get_premium_cooldown_reductions(user_id)
    cooldown_reduction += premium_cd.get("harvest_reduction", 0)
    
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


async def assign_gatherer_role(member: discord.Member, guild: discord.Guild, force_planter_role: str | None = None) -> tuple[str | None, str | None]:
    #assign gatherer role to the user based on bloom cycle plants (resets per bloom)
    #PLANTER I - 0-49 items gathered this cycle
    #PLANTER II - 50-149 items gathered this cycle
    #PLANTER III - 150-299 items gathered this cycle
    #PLANTER IV - 300-499 items gathered this cycle
    #PLANTER V - 500-999 items gathered this cycle
    #PLANTER VI - 1000-1999 items gathered this cycle
    #PLANTER VII - 2000-3999 items gathered this cycle
    #PLANTER VIII - 4000-9999 items gathered this cycle
    #PLANTER IX - 10000-14999 items gathered this cycle
    #PLANTER X - 15000+ items gathered this cycle
    # When force_planter_role is set (e.g. "PLANTER I" after bloom), use it instead of DB cycle_plants so reset is guaranteed.

    user_id = member.id
    # Refetch member so we see current Discord roles (avoids double rank-up when gather and harvest run close together)
    try:
        member = await guild.fetch_member(user_id)
    except Exception:
        pass  # use passed-in member if fetch fails
    planter_roles = ["PLANTER I", "PLANTER II", "PLANTER III", "PLANTER IV", "PLANTER V", "PLANTER VI", "PLANTER VII", "PLANTER VIII", "PLANTER IX", "PLANTER X"]

    # Find the user's current planter role
    previous_role_name = next((role.name for role in member.roles if role.name in planter_roles), None)

    # Target role: forced (e.g. after bloom) or derived from bloom_cycle_plants
    if force_planter_role and force_planter_role in planter_roles:
        target_role_name = force_planter_role
    else:
        cycle_plants = get_user_bloom_cycle_plants(user_id)  # Use bloom cycle counter (resets per bloom)
        target_role_name = None
        if cycle_plants < 50:
            target_role_name = "PLANTER I"
        elif cycle_plants < 150:
            target_role_name = "PLANTER II"
        elif cycle_plants < 300:
            target_role_name = "PLANTER III"
        elif cycle_plants < 500:
            target_role_name = "PLANTER IV"
        elif cycle_plants < 1000:
            target_role_name = "PLANTER V"
        elif cycle_plants < 2000:
            target_role_name = "PLANTER VI"
        elif cycle_plants < 4000:
            target_role_name = "PLANTER VII"
        elif cycle_plants < 10000:
            target_role_name = "PLANTER VIII"
        elif cycle_plants < 15000:
            target_role_name = "PLANTER IX"
        else:  # 15000+
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



# ─── Gathering Areas Configuration ───
# Each area has: multiplier (on base plant values), required planter rank, unlock cost, and unlock order.
# "forest" is always available (default). Other areas must be unlocked with /unlock.
GATHERING_AREAS = {
    "forest": {
        "display_name": "#forest",
        "emoji": "🌲",
        "multiplier": 1.0,
        "required_planter_rank": None,
        "required_planter_level": 0,
        "unlock_cost": 0,
        "unlocked_by_default": True,
        "previous_area": None,
        "order": 0
    },
    "grove": {
        "display_name": "#grove",
        "emoji": "<:TreeRing:1474244868288282817>",
        "multiplier": 1.2,
        "required_planter_rank": "PLANTER III",
        "required_planter_level": 3,
        "unlock_cost": 250_000,
        "unlocked_by_default": False,
        "previous_area": None,
        "order": 1
    },
    "marsh": {
        "display_name": "#marsh",
        "emoji": "🏞️",
        "multiplier": 2.0,
        "required_planter_rank": "PLANTER V",
        "required_planter_level": 5,
        "unlock_cost": 5_000_000,
        "unlocked_by_default": False,
        "previous_area": "grove",
        "order": 2
    },
    "bog": {
        "display_name": "#bog",
        "emoji": "🌿",
        "multiplier": 5.0,
        "required_planter_rank": "PLANTER VII",
        "required_planter_level": 7,
        "unlock_cost": 75_000_000,
        "unlocked_by_default": False,
        "previous_area": "marsh",
        "order": 3
    },
    "mire": {
        "display_name": "#mire",
        "emoji": "🏝️",
        "multiplier": 10.0,
        "required_planter_rank": "PLANTER IX",
        "required_planter_level": 9,
        "unlock_cost": 300_000_000,
        "unlocked_by_default": False,
        "previous_area": "bog",
        "order": 4
    },
    "underground-jungle": {
        "display_name": "#underground-jungle",
        "emoji": "🌴",
        "multiplier": 1.1,
        "gather_boost": 1.1,
        "required_planter_rank": "PLANTER II",
        "required_planter_level": 2,
        "unlock_cost": 100_000,
        "unlocked_by_default": False,
        "previous_area": None,  # Can be unlocked first (with grove); not required for marsh/bog/mire
        "order": 5
    }
}

VALID_GATHERING_CHANNELS = set(GATHERING_AREAS.keys())

# Order of unlockable areas for areas_unlocked achievement (forest is default; jungle before grove)
AREA_ORDER_FOR_ACHIEVEMENT = ["underground-jungle", "grove", "marsh", "bog", "mire"]

# Day/Night (Terraria-style): Solar Eclipse = day (4:30 AM–7:29 PM EST), Blood Moon = night (7:30 PM–4:29 AM EST). Spawn boost during these.
def _is_solar_eclipse_or_blood_moon():
    """Return (is_solar_eclipse, is_blood_moon) based on EST hour. Day 4:30–19:29 EST, Night 19:30–4:29 EST."""
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    now_utc = datetime.now(timezone.utc)
    now_est = now_utc.astimezone(ZoneInfo("America/New_York"))
    h = now_est.hour + now_est.minute / 60.0 + now_est.second / 3600.0
    is_day = 4.5 <= h < 19.5   # 4:30 AM – 7:29 PM EST
    is_night = not is_day
    return is_day, is_night

def _pve_spawn_multiplier():
    """Return (animal_mult, boss_mult) for spawn chances. During active Solar Eclipse or Blood Moon (from DB): 5.0x each."""
    if get_active_event_by_type("solar_eclipse") or get_active_event_by_type("blood_moon"):
        return 5.0, 5.0
    return 1.0, 1.0

# Solar Eclipse and Blood Moon: 50% chance at respective start times (4:30 EST, 19:30 EST). Display in #events with start/end embeds.
SOLAR_ECLIPSE_TITLE = "A Solar Eclipse is happening!"
SOLAR_ECLIPSE_DESCRIPTION = "A Solar Eclipse is happening!"
SOLAR_ECLIPSE_FOOTER = "Wild animals and bosses are more common!"
BLOOD_MOON_TITLE = "The Blood Moon is rising..."
BLOOD_MOON_DESCRIPTION = "The Blood Moon is rising..."
BLOOD_MOON_FOOTER = "Wild animals and bosses are more common!"
SOLAR_ECLIPSE_COLOR = 0xFF8C00   # bright orange
BLOOD_MOON_COLOR = 0xDC143C      # bright red (crimson, distinct from discord red)
CELESTIAL_DAY_START_EST = (4, 30)   # 4:30 AM EST (Solar Eclipse start)
CELESTIAL_NIGHT_START_EST = (19, 30)  # 7:30 PM EST (Blood Moon start)
CELESTIAL_TRIGGER_CHANCE = 0.2   # 20% at start time, Terraria-style

# ═══════════════════════════════════════════════════════════════════════════════
# PvE Wild Animal Event System
# ═══════════════════════════════════════════════════════════════════════════════

PVE_TRIGGER_CHANCE_GATHER = 0.01   # 1% per /gather for animals (event multiplies); animals much more common than bosses
PVE_TRIGGER_CHANCE_HARVEST = 0.005  # 0.5% per /harvest for animals (event multiplies)

# Steal: stealable chance per successful gather/harvest (no PvE when stealable; crits can be stolen)
STEAL_CHANCE_GATHER = 0.04   # 4% per /gather (4x previous)
STEAL_CHANCE_HARVEST = 0.02  # 2% per /harvest (4x previous)
STEAL_WINDOW_GATHER_SEC = 5
STEAL_WINDOW_HARVEST_SEC = 3

PVE_WILD_ANIMALS = [
    {
        "id": "grizzly_bear",
        "name": "Grizzly Bear",
        "emoji": "🐻",
        "hp_range": (50, 75),
        "color": 0x8B4513,
        "description": "A massive **Grizzly Bear** has burst through the treeline! Its eyes are locked on your crops!",
        "defeat_msg": "The **Grizzly Bear** lets out a final roar and lumbers back into the wilderness!",
    },
    {
        "id": "black_bear",
        "name": "Black Bear",
        "emoji": "🐾",
        "hp_range": (35, 45),
        "color": 0x2C2C2C,
        "description": "A scrappy **Black Bear** has wandered into the area, sniffing around for easy meals! It's raiding your plants!",
        "defeat_msg": "The **Black Bear** whimpers and scurries back into the shadows!",
    },
    {
        "id": "polar_bear",
        "name": "Polar Bear",
        "emoji": "🐻‍❄️",
        "hp_range": (90, 125),
        "color": 0xE0F0FF,
        "description": "A colossal **Polar Bear** has appeared from nowhere!",
        "defeat_msg": "The **Polar Bear** shakes off the blows and trudges away, perhaps to be seen again...!",
    },
    {
        "id": "tiger",
        "name": "Tiger",
        "emoji": "🐅",
        "hp_range": (150, 160),
        "color": 0xFF8C00,
        "description": "A ferocious **Tiger** has leapt into the clearing! It paces around, growling at everyone!",
        "defeat_msg": "The **Tiger** snarls one last time and vanishes into the tall grass...",
    },
    {
        "id": "panther",
        "name": "Panther",
        "emoji": "🐆",
        "hp_range": (80, 100),
        "color": 0x1C1C1C,
        "description": "A sleek **Panther** has emerged from the darkness! It stalks through the gathering grounds!",
        "defeat_msg": "The **Panther** hisses and recinds back into the shadows, defeated!",
    },
    {
        "id": "homeless_man_on_fent",
        "name": "Homeless Man on Fent",
        "emoji": "🧟",
        "hp_range": (67, 175),
        "color": 0x6B8E23,
        "description": "A **Homeless Man on Fent** has stumbled into the area! He's mumbling incoherently and swatting at invisible bees! He seems... unreasonably durable!",
        "defeat_msg": "The **Homeless Man on Fent** finally passes out and is carried away by local authorities!",
    },
    {
        "id": "monkey",
        "name": "Monkey",
        "emoji": "🐵",
        "hp_range": (15, 30),
        "color": 0x8B7355,
        "description": "A Monkey has swung in from the trees, snatching up all the fruit!",
        "defeat_msg": "The Monkey chatters in outrage and scrambles back into the canopy!",
    },
    {
        "id": "bullet_ant",
        "name": "Bullet Ant",
        "emoji": "🐜",
        "hp_range": (3, 7),
        "color": 0x2C1810,
        "description": "A **Bullet Ant** from the swarm! Squash it!!",
        "defeat_msg": "This ant is squashed!",
        "is_swarm_ant": True,
    },
]

# Bullet Ant Swarm: intro/outro text only; individual ants use "Bullet Ant" above.
BULLET_ANT_SWARM_INTRO = "A Bullet Ant Swarm has poured out of the undergrowth!"
BULLET_ANT_SWARM_DEFEAT = "After squashing the Bullet Ants, the swarm retreats back into their anthill!"
BULLET_ANT_SWARM_COUNT_RANGE = (4, 9)
PVE_SWARM_TRIGGER_CHANCE = 0.08   # 8% chance to spawn swarm instead of single animal (when an animal spawn triggers)
PVE_WILD_ANIMALS_SINGLE = [a for a in PVE_WILD_ANIMALS if not a.get("is_swarm_ant")]
BULLET_ANT_ANIMAL = next(a for a in PVE_WILD_ANIMALS if a.get("is_swarm_ant"))

# ═══════════════════════════════════════════════════════════════════════════════
# PvE Wild Animals — UNDERGROUND JUNGLE ONLY (#underground-jungle channel only)
# ═══════════════════════════════════════════════════════════════════════════════

PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE = [
    {
        "id": "hornet",
        "name": "Hornet",
        "emoji": "🐝",
        "hp_range": (30, 32),
        "color": 0xFFD700,
        "description": "A Hornet buzzes into your way, stinger aimed at anyone who tries to get close!",
        "defeat_msg": "The Hornet flees!",
    },
    {
        "id": "bee",
        "name": "Bee",
        "emoji": "🐝",
        "hp_range": (1, 3),
        "color": 0xFFC800,
        "description": "A Bee from the swarm! Swat it!",
        "defeat_msg": "The bee was killed!",
        "is_swarm_bee": True,
    },
    {
        "id": "angry_trapper",
        "name": "Angry Trapper",
        "emoji": "🪤",
        "hp_range": (70, 77),
        "color": 0x4A3728,
        "description": "An Angry Trapper springs from the jungle floor, snapping its jaws at everyone!",
        "defeat_msg": "The Angry Trapper wilts and collapses back into the soil!",
    },
    {
        "id": "man_eater",
        "name": "Man Eater",
        "emoji": "🌿",
        "hp_range": (60, 70),
        "color": 0x228B22,
        "description": "A Man Eater slithers through the vines, beckoning anyone to put a limb in its jaws!",
        "defeat_msg": "The Man Eater retracts into the jungle, defeated!",
    },
    {
        "id": "giant_tortoise",
        "name": "Giant Tortoise",
        "emoji": "🐢",
        "hp_range": (110, 124),
        "color": 0x3D5C3D,
        "description": "A Giant Tortoise soars through the underground jungle, attempting to squash anyone that moves!",
        "defeat_msg": "The Giant Tortoise retreats into its shell and jumps away!",
    },
    {
        "id": "arapaima",
        "name": "Arapaima",
        "emoji": "🐟",
        "hp_range": (25, 31),
        "color": 0x8B7355,
        "description": "An Arapaima has leapt from the Jungle water, thrashing around everywhere!",
        "defeat_msg": "The Arapaima flops back into the water and disappears!",
    },
    {
        "id": "piranha",
        "name": "Piranha",
        "emoji": "🐠",
        "hp_range": (15, 18),
        "color": 0xCD5C5C,
        "description": "A Piranha has swam to the surface of the water, snapping its teeth at you!",
        "defeat_msg": "The Piranha sinks beneath the surface, defeated!",
    },
    {
        "id": "moth",
        "name": "Moth",
        "emoji": "🦋",
        "hp_range": (100, 100),
        "color": 0x2F1B2E,
        "description": "A huge Moth flutters down from the canopy, mutated due to jungle toxins!",
        "defeat_msg": "The Moth dissolves into Butterfly Dust!",
        "spawn_weight": 0.25,  # rarer
    },
    {
        "id": "spiked_jungle_slime",
        "name": "Spiked Jungle Slime",
        "emoji": "🟢",
        "hp_range": (18, 22),
        "color": 0x2D5A27,
        "description": "A **Spiked Jungle Slime** oozes out of the mud, covered in sharp thorns!",
        "defeat_msg": "The **Spiked Jungle Slime** bursts into goo and soaks into the ground!",
    },
    {
        "id": "jungle_bat",
        "name": "Jungle Bat",
        "emoji": "🦇",
        "hp_range": (10, 17),
        "color": 0x1C1C1C,
        "description": "A **Jungle Bat** swoops down from the cavern ceiling, screeching!",
        "defeat_msg": "The **Jungle Bat** flutters away into the shadows!",
    },
]

BEE_SWARM_INTRO = "A swarm of bees block the path! Take down each bee!"
BEE_SWARM_DEFEAT = "The rest of the swarm disperses back into the jungle!"
BEE_SWARM_COUNT_RANGE = (4, 6)
BEE_SWARM_LARVA_CHANCE = 0.40  # 40% chance for Larva after defeating Bee Swarm
LARVA_TIMEOUT_SEC = 10.0

PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE_SINGLE = [a for a in PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE if not a.get("is_swarm_bee")]
BEE_ANIMAL = next(a for a in PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE if a.get("is_swarm_bee"))

# Tracks active PvE events per channel: channel_id -> PvE event data
active_pve_events: dict[int, dict] = {}


def _guild_has_active_pve(guild: discord.Guild) -> bool:
    """True if any gathering channel in this guild currently has an active enemy (wild animal or swarm). Used to block boss spawn when an enemy is up."""
    for ch in guild.text_channels:
        if ch.name in VALID_GATHERING_CHANNELS and ch.id in active_pve_events:
            return True
    return False


# Bosses: block all gather channels in guild, @here on warning, rarer spawn. 1-min warning embed before spawn.
PVE_BOSS_TRIGGER_CHANCE = 0.001  # 0.1% per gather/harvest (when no boss and no enemy active); event multiplies; rarer than animals
PVE_BOSSES = [
    {
        "id": "skunkape",
        "name": "Skunkape",
        "emoji": "🦍",
        "hp_range": (175, 200),
        "color": 0x3d2914,
        "description": "The ground is shaking... A Skunkape emerges from the wood, wearing a Winter Haven Technology Services polo!",
        "defeat_msg": "The Skunkape bellows one last time and lumbers into the mist, back to his desk!",
        "pre_spawn_text": "A foul stench fills the air…",
        "server_defeat_msg": "The Skunkape has been defeated! Conditions return to normal.",
    },
    {
        "id": "godzilla",
        "name": "Godzilla",
        "emoji": "🦖",
        "hp_range": (300, 425),
        "color": 0x2d5016,
        "description": "The king of the monsters is trampling everything!",
        "defeat_msg": "Godzilla roars in defeat and sinks beneath the waves. The gathering grounds are safe... for now...",
        "pre_spawn_text": "The ground trembles...",
        "server_defeat_msg": "Godzilla has been defeated! Conditions return to normal.",
    },
    {
        "id": "mothron",
        "name": "Mothron",
        "emoji": "🦋",
        "hp_range": (215, 225),
        "color": 0x1a0d1a,
        "description": "The sky darkens as Mothron descends—a monstrous moth of nightmare! It's blocking all gathering channels; only a united effort can drive it off!",
        "defeat_msg": "Mothron shrieks and flees into the night!",
        "pre_spawn_text": "Wings blot out the Solar Eclipse...",
        "server_defeat_msg": "Mothron has been defeated! Conditions return to normal.",
    },
    {
        "id": "plantera",
        "name": "Plantera",
        "emoji": "🌸",
        "hp_range": (450, 535),
        "color": 0x2d5a1a,
        "description": "Somebody broke a Plantera Bulb!",
        "defeat_msg": "Plantera crumples into a heap of vines and petals!",
        "pre_spawn_text": "The jungle grows restless...",
        "server_defeat_msg": "Plantera has been defeated! Conditions return to normal.",
    },
    {
        "id": "wither",
        "name": "The Wither",
        "emoji": "💀💀💀",
        "hp_range": (300, 300),
        "color": 0x1a0a1a,
        "description": "Somebody summoned the Wither! It's blowing up all the gathering grounds!",
        "defeat_msg": "The Wither has been slain.. It drops a Nether Star!",
        "pre_spawn_text": "The air grows cold and heavy...",
        "server_defeat_msg": "The Wither has been defeated! Conditions return to normal.",
    },
    {
        "id": "retinazer",
        "name": "The Twins – Retinazer",
        "emoji": "👁️",
        "hp_range": (350, 350),
        "color": 0xc41e3a,
        "description": "It's hovering above all the gathering grounds, firing lasers at anything that moves!",
        "defeat_msg": "Retinazer explodes into Hallowed Bars and Souls of Sight!",
        "pre_spawn_text": "This is going to be a terrible night...",
        "server_defeat_msg": "The Twins have been defeated! Conditions return to normal.",
        "twins_pair": "spazmatism",
    },
    {
        "id": "spazmatism",
        "name": "The Twins – Spazmatism",
        "emoji": "🔥",
        "hp_range": (350, 350),
        "color": 0x228b22,
        "description": "It's tearing through the area, lighting the forest on fire with Cursed Flame!",
        "defeat_msg": "Spazmatism sputters out and falls, crashing into the ground and exploding into Hallowed Bars and Souls of Sight!",
        "pre_spawn_text": "This is going to be a terrible night...",
        "server_defeat_msg": "The Twins have been defeated! Conditions return to normal.",
        "twins_pair": "retinazer",
    },
    {
        "id": "ender_dragon",
        "name": "Ender Dragon",
        "emoji": "🐉",
        "hp_range": (500, 500),
        "color": 0x4a0080,
        "description": "The Ender Dragon appears!",
        "defeat_msg": "The Ender Dragon lets out a final roar and dissolves into motes of light. The End has been conquered!",
        "pre_spawn_text": "The End Portal opens...",
        "server_defeat_msg": "The **Ender Dragon** has been defeated! The gathering grounds are safe once more.",
    },
    {
        "id": "queen_bee",
        "name": "Queen Bee🐝",
        "emoji": "👑",
        "hp_range": (220, 260),
        "color": 0xFFD700,
        "description": "You disturbed the bees.. Queen Bee is out to get everyone in the Underground Jungle!",
        "defeat_msg": "The **Queen Bee** buzzes one last time and collapses. The hive is quiet again!",
        "pre_spawn_text": "Queen Bee has awoken!",
        "server_defeat_msg": "The **Queen Bee** has been defeated! The underground jungle is safe again.",
    },
    {
        "id": "the_roaring_knight",
        "name": "The Roaring Knight",
        "emoji": "⚔️",
        "hp_range": (410, 500),
        "color": 0x000000,
        "description": "A tall, armored figure emerges from the darkness... The Roaring Knight has arrived!",
        "defeat_msg": "A Black Shard chips off The Roaring Knight's blade! It promptly retreats!",
        "pre_spawn_text": "A Dark Fountain erupts in the distance...",
        "server_defeat_msg": "The Roaring Knight retreats! The Dark Fountain closes and conditions return to normal.",
    },
    {
        "id": "nergigante",
        "name": "Nergigante",
        "emoji": "🐉",
        "hp_range": (400, 600),
        "color": 0x2d1a1a,
        "description": "Nergigante appears, smashing its horns into the surrounding trees!",
        "defeat_msg": "Nergigante flys away into the wilderness!",
        "pre_spawn_text": "A fight brews among Elder Dragons...",
        "server_defeat_msg": "Nergigante is defeated!",
    },
    {
        "id": "rathalos",
        "name": "Rathalos",
        "emoji": "🐉",
        "hp_range": (700, 700),
        "color": 0x8b0000,
        "description": "Rathalos appeared! He's breathing fire on all the gathering grounds!",
        "defeat_msg": "Rathalos retreats, flying away into the clouds!",
        "pre_spawn_text": "Deafening flaps of wings come from overhead...",
        "server_defeat_msg": "Rathalos has been defeated!",
    },
    {
        "id": "sans",
        "name": "Sans",
        "emoji": "<:sans:1479162217525154007>",
        "hp_range": (1, 1),
        "color": 0xffa500,
        "description": "The easist enemy. Can't keep dodging forever. Keep attacking.",
        "defeat_msg": "Sans bleeds for a few moments... and then blows away into dust!",
        "pre_spawn_text": "You feel like you're going to have a bad time...",
        "server_defeat_msg": "Sans has been defeated!",
    },
]
# Twins: spawn both Retinazer and Spazmatism together; both must be defeated to unblock channels.
PVE_BOSSES_TWINS_IDS = {"retinazer", "spazmatism"}
# Ender Dragon: spawns in trigger channel; other channels get Obsidian Towers (End Crystals). Dragon regens 2 HP/sec per standing tower.
ENDER_DRAGON_ID = "ender_dragon"
ENDER_DRAGON_TOWER_HP = 20
ENDER_DRAGON_REGEN_PER_CRYSTAL = 2
# Enemy id -> hidden achievement key (for first-time defeat)
PVE_ENEMY_TO_HIDDEN_ACHIEVEMENT = {
    "monkey": "no_monkey_business",
    "grizzly_bear": "grizzly_victory",
    "black_bear": "black_bear_blues",
    "polar_bear": "polar_power",
    "tiger": "tiger_tamer",
    "panther": "panther_pounce",
    "homeless_man_on_fent": "homeless_hero",
    "bullet_ant": "bullet_ant_squasher",
    "skunkape": "skunkape_slayer",
    "godzilla": "godzilla_king",
    "mothron": "mothron_masher",
    "plantera": "plantera_crusher",
    "wither": "wither_slayer",
    "retinazer": "retinazer_retired",
    "spazmatism": "spazmatism_silenced",
    "ender_dragon": "ender_dragon_slayer",
    "hornet": "hornet_hunter",
    "bee": "bee_swarm_squasher",
    "angry_trapper": "angry_trapper_crusher",
    "man_eater": "man_eater_muncher",
    "giant_tortoise": "giant_tortoise_tamer",
    "arapaima": "arapaima_slayer",
    "piranha": "piranha_predator",
    "moth": "moth_masher",
    "spiked_jungle_slime": "spiked_slime_squasher",
    "jungle_bat": "jungle_bat_basher",
    "queen_bee": "queen_bee_slayer",
    "the_roaring_knight": "the_roaring_knight_repelled",
    "nergigante": "nergigante_slayer",
    "rathalos": "rathalos_slayer",
    "sans": "sans_slayer",
}
PVE_MASTER_REQUIRED_IDS = frozenset(PVE_ENEMY_TO_HIDDEN_ACHIEVEMENT.keys())
active_boss_events: dict[int, list] = {}  # guild_id -> list of {channel_id, boss, hp, max_hp}
pending_boss_spawn_guild_ids: set[int] = set()  # guilds with a 1-min delayed boss spawn scheduled (prevents double intro/spawn)
# Ender Dragon: guild_id -> set of channel_ids that still have an active (not broken) Obsidian Tower
ender_dragon_towers: dict[int, set[int]] = {}
# Ender Dragon: guild_id -> {user_id: total_tower_hits} for plant rewards when dragon is defeated (towers are NOT counted as enemies defeated)
ender_dragon_tower_attackers: dict[int, dict[int, int]] = {}
# Ender Dragon regen task per guild (so we can cancel when dragon is defeated)
ender_dragon_regen_tasks: dict[int, asyncio.Task] = {}
# When a boss is defeated we append (boss, attackers); when all bosses in the event are dead we distribute and broadcast
_pve_boss_defeated_pending: dict[int, list] = {}  # guild_id -> [(boss, attackers), ...]
# Users who have defeated PvE but reward distribution hasn't finished yet — block bloom until they get their rewards
users_pending_pve_rewards: set[int] = set()
plantera_bulb_eligible_guilds: dict[int, float] = {}  # guild_id -> timestamp when last boss was defeated (for Plantera bulb chance)
PLANTERA_BULB_CHANCE = 0.02
PLANTERA_BULB_MAX_AGE_SEC = 86400  # 24h
# #underground-jungle: 2x wild animal and boss spawn
UNDERGROUND_JUNGLE_ANIMAL_SPAWN_MULT = 2.0
UNDERGROUND_JUNGLE_BOSS_SPAWN_MULT = 2.0

# Channel name for auto-logging rare occurrences (e.g. Legendary/Netherite/Luminite/Celestial/Mikellion plants, netherite+ imbues)
RARES_CHANNEL_NAME = "rares"

# Planter rank name -> numeric level mapping for area requirement checks
PLANTER_RANK_ORDER = {
    "PLANTER I": 1, "PLANTER II": 2, "PLANTER III": 3, "PLANTER IV": 4,
    "PLANTER V": 5, "PLANTER VI": 6, "PLANTER VII": 7, "PLANTER VIII": 8,
    "PLANTER IX": 9, "PLANTER X": 10
}


def get_user_planter_level(member) -> int:
    """Get user's numeric planter rank level (1-10) from their Discord roles. Returns 0 if no planter role."""
    for role in member.roles:
        if role.name in PLANTER_RANK_ORDER:
            return PLANTER_RANK_ORDER[role.name]
    return 0


def check_area_access(member, channel_name: str, user_id: int) -> tuple[bool, str]:
    """
    Check if a user can gather/harvest in the given channel.
    Returns (allowed, error_message). If allowed, error_message is empty.
    """
    # Not a gathering channel at all
    if channel_name not in VALID_GATHERING_CHANNELS:
        channels_list = ", ".join(f"**{GATHERING_AREAS[a]['display_name']}**" for a in GATHERING_AREAS)
        return False, f"❌ You can only use this command in gathering channels: {channels_list}"

    area = GATHERING_AREAS[channel_name]

    # Forest is always accessible
    if area["unlocked_by_default"]:
        return True, ""

    # Fetch unlocked areas once for all checks
    unlocked_areas = get_user_unlocked_areas(user_id)

    # Check if the previous area is unlocked (progression order)
    if area["previous_area"] and not unlocked_areas.get(area["previous_area"], False):
        prev_display = GATHERING_AREAS[area["previous_area"]]["display_name"]
        return False, f"❌ You must unlock **{prev_display}** before accessing **{area['display_name']}**!"

    # Check if this area is unlocked
    if not unlocked_areas.get(channel_name, False):
        return False, f"❌ You haven't unlocked **{area['display_name']}** yet! Use `/unlock {channel_name}` to unlock it for **{format_money(area['unlock_cost'])}**."

    # Check planter rank: use the higher of Discord role level and DB-derived level (bloom_cycle_plants)
    # so e.g. PLANTER X who just leveled up can use mire even if role hasn't synced yet
    role_level = get_user_planter_level(member)
    cycle_plants = get_user_bloom_cycle_plants(user_id)
    db_level = get_planter_level_from_total_items(cycle_plants)
    user_planter_level = max(role_level, db_level)
    if user_planter_level < area["required_planter_level"]:
        return False, f"❌ You must be **{area['required_planter_rank']}** or above to gather in **{area['display_name']}**! You need to gather more plants to rank up."

    return True, ""


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
                "boost": 0.015  # 1.5%
            },
            {
                "level": 2,
                "name": "Smelling What You're Stepping In",
                "description": "/gather 50 times",
                "threshold": 50,
                "boost": 0.03  # 3%
            },
            {
                "level": 3,
                "name": "Getting The Hang Of It",
                "description": "/gather 250 times",
                "threshold": 250,
                "boost": 0.06  # 6%
            },
            {
                "level": 4,
                "name": "Green Thumb",
                "description": "/gather 1,000 times",
                "threshold": 1000,
                "boost": 0.10  # 10%
            },
            {
                "level": 5,
                "name": "Serf",
                "description": "/gather 4,000 times",
                "threshold": 4000,
                "boost": 0.15  # 15%
            },
            {
                "level": 6,
                "name": "Gathousand",
                "description": "/gather 10,000 times",
                "threshold": 10000,
                "boost": 0.30  # 30% (capped)
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
                "boost": 0.55  # 55%
            },
            {
                "level": 9,
                "name": "Almost Breaking Even",
                "description": "Perform 15,000 coinflips",
                "threshold": 15000,
                "boost": 0.70  # 70%
            },
            {
                "level": 10,
                "name": "The House Always Wins",
                "description": "Perform 30,000 coinflips",
                "threshold": 30000,
                "boost": 0.80  # 80% (capped)
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
                "boost": 0.25  # 25%
            },
            {
                "level": 6,
                "name": "Still Going",
                "description": "Win 6 coinflips in a row",
                "threshold": 6,
                "boost": 0.50  # 50%
            },
            {
                "level": 7,
                "name": "Just 1 More Win (In a Row)",
                "description": "Win 7 coinflips in a row",
                "threshold": 7,
                "boost": 0.65  # 65%
            },
            {
                "level": 8,
                "name": "Is This Even Possible?",
                "description": "Win 8 coinflips in a row",
                "threshold": 8,
                "boost": 0.80  # 80%
            },
            {
                "level": 9,
                "name": "You Should Buy A Lottery Ticket",
                "description": "Win 9 coinflips in a row",
                "threshold": 9,
                "boost": 0.90  # 90%
            },
            {
                "level": 10,
                "name": "Struck By Lightning (Twice)",
                "description": "Win 10 coinflips in a row",
                "threshold": 10,
                "boost": 1.0  # 100% (capped)
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
                "name": "Locally Known",
                "description": "Do /harvest 15 times",
                "threshold": 15,
                "boost": 0.05  # 5%
            },
            {
                "level": 4,
                "name": "Making The World Go Round",
                "description": "Do /harvest 50 times",
                "threshold": 50,
                "boost": 0.10  # 10%
            },
            {
                "level": 5,
                "name": "Competing With Publix",
                "description": "Do /harvest 100 times",
                "threshold": 100,
                "boost": 0.25  # 25%
            },
            {
                "level": 6,
                "name": "World Renowned",
                "description": "Do /harvest 500 times",
                "threshold": 500,
                "boost": 0.50  # 50%
            },
            {
                "level": 7,
                "name": "The Lebron of Plants",
                "description": "Do /harvest 1,000 times",
                "threshold": 1000,
                "boost": 1.20  # 120%
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
                "boost": 1.0  # 100% (capped)
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
    },
    "russian_roulette": {
        "name": "Playing Russian! Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Scaredy-Cat",
                "description": "You haven't even played a game of russian! Go do /russian in one of the russian-roulette channels!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Maybe Just One Spin",
                "description": "Play /russian once",
                "threshold": 1,
                "boost": 0.01  # 1%
            },
            {
                "level": 2,
                "name": "Risk Taker",
                "description": "Play /russian 3 times",
                "threshold": 3,
                "boost": 0.02  # 2%
            },
            {
                "level": 3,
                "name": "It's Only A Bullet, Right?",
                "description": "Play /russian 10 times",
                "threshold": 10,
                "boost": 0.10  # 10%
            },
            {
                "level": 4,
                "name": "Desensitized",
                "description": "Play /russian 50 times",
                "threshold": 50,
                "boost": 0.25  # 25%
            },
            {
                "level": 5,
                "name": "I Can Stop Anytime",
                "description": "Play /russian 125 times",
                "threshold": 125,
                "boost": 0.50  # 50%
            },
            {
                "level": 6,
                "name": "16.67's My Favorite Percentage",
                "description": "Play /russian 300 times",
                "threshold": 300,
                "boost": 1.0  # 100%
            }
        ]
    },
    "blooming": {
        "name": "Blooming Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Just A Bud",
                "description": "You haven't even bloomed! Try to bloom with /bloom!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Gravity Falls",
                "description": "Reach Pine II",
                "threshold": 1,
                "boost": 0.05  # 5%
            },
            {
                "level": 2,
                "name": "Insect Repelling",
                "description": "Reach Cedar Rank",
                "threshold": 3,
                "boost": 0.125  # 12.5%
            },
            {
                "level": 3,
                "name": "Eye See You",
                "description": "Reach Birch Rank",
                "threshold": 6,
                "boost": 0.30  # 30%
            },
            {
                "level": 4,
                "name": "Canadian",
                "description": "Reach Maple Rank",
                "threshold": 9,
                "boost": 0.50  # 50%
            },
            {
                "level": 5,
                "name": "Just Like In Minecraft",
                "description": "Reach Oak Rank",
                "threshold": 12,
                "boost": 0.80  # 80%
            },
            {
                "level": 6,
                "name": "How Many Tree Rings Is That?",
                "description": "Reach Fir Rank",
                "threshold": 15,
                "boost": 1.25  # 125%
            },
            {
                "level": 7,
                "name": "The Tallest",
                "description": "Reach Redwood Rank",
                "threshold": 18,
                "boost": 2.00  # 200%
            }
        ]
    },
    "areas_unlocked": {
        "name": "Areas Unlocked Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Starting Area",
                "description": "You've only seen the #forest! Do /unlock to get to other areas!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Mahogany",
                "description": "Unlock the #underground-jungle",
                "threshold": 1,
                "boost": 0.05  # 5%
            },
            {
                "level": 2,
                "name": "Treasure Grove",
                "description": "Unlock the #grove.",
                "threshold": 2,
                "boost": 0.10  # 10%
            },
            {
                "level": 3,
                "name": "Marshing On",
                "description": "Unlock the #marsh",
                "threshold": 3,
                "boost": 0.15  # 15%
            },
            {
                "level": 4,
                "name": "Not To Be Confused With Bug",
                "description": "Unlock the #bog",
                "threshold": 4,
                "boost": 0.22  # 22%
            },
            {
                "level": 5,
                "name": "But Not Loot Lake",
                "description": "Unlock the #mire",
                "threshold": 5,
                "boost": 0.30  # 30% (capped)
            }
        ]
    },
    "slayer": {
        "name": "Slayer Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Pacifist",
                "description": "You haven't defeated any enemies yet! Fight animals and bosses in gathering channels!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "First Blood",
                "description": "Defeat 1 enemy",
                "threshold": 1,
                "boost": 0.01  # 1%
            },
            {
                "level": 2,
                "name": "Pest Control",
                "description": "Defeat 5 enemies",
                "threshold": 5,
                "boost": 0.025  # 2.5%
            },
            {
                "level": 3,
                "name": "Hunter",
                "description": "Defeat 15 enemies",
                "threshold": 15,
                "boost": 0.05  # 5%
            },
            {
                "level": 4,
                "name": "Bounty Hunter",
                "description": "Defeat 40 enemies",
                "threshold": 40,
                "boost": 0.10  # 10%
            },
            {
                "level": 5,
                "name": "Slayer",
                "description": "Defeat 100 enemies",
                "threshold": 100,
                "boost": 0.20  # 20%
            },
            {
                "level": 6,
                "name": "Assassin",
                "description": "Defeat 250 enemies",
                "threshold": 250,
                "boost": 0.35  # 35%
            },
            {
                "level": 7,
                "name": "Taro Sakamoto",
                "description": "Defeat 500 enemies",
                "threshold": 500,
                "boost": 0.50  # 50%
            },
            {
                "level": 8,
                "name": "Frieren The Slayer",
                "description": "Defeat 1,000 enemies",
                "threshold": 1000,
                "boost": 0.75  # 75%
            },
            {
                "level": 9,
                "name": "Ryomen Sukuna",
                "description": "Defeat 2,500 enemies",
                "threshold": 2500,
                "boost": 1.0  # 100%
            },
            {
                "level": 10,
                "name": "Eren Yeager",
                "description": "Defeat 5,000 enemies",
                "threshold": 5000,
                "boost": 1.5  # 150%
            }
        ]
    },
    "slots": {
        "name": "Slots Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Lever? I Hardly Know Her",
                "description": "You haven't clicked SPIN yet! Do /slots, pick 0.1% or 1%, then click SPIN!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "First Pull",
                "description": "Click SPIN (0.1% or 1% bet) 1 time",
                "threshold": 1,
                "boost": 0.005  # 0.5%
            },
            {
                "level": 2,
                "name": "Gamble Addict",
                "description": "Click SPIN 5 times",
                "threshold": 5,
                "boost": 0.01  # 1%
            },
            {
                "level": 3,
                "name": "I Love Reels",
                "description": "Click SPIN 25 times",
                "threshold": 25,
                "boost": 0.03  # 3%
            },
            {
                "level": 4,
                "name": "House Favorite",
                "description": "Click SPIN 100 times",
                "threshold": 100,
                "boost": 0.07  # 7%
            },
            {
                "level": 5,
                "name": "Slot Addict",
                "description": "Click SPIN 500 times",
                "threshold": 500,
                "boost": 0.15  # 15%
            },
            {
                "level": 6,
                "name": "SURELY this one will be the jackpot, right?",
                "description": "Click SPIN 1,000 times",
                "threshold": 1000,
                "boost": 0.25  # 25%
            },
            {
                "level": 7,
                "name": "Vegas Regular",
                "description": "Click SPIN 5,000 times",
                "threshold": 5000,
                "boost": 0.40  # 40%
            },
            {
                "level": 8,
                "name": "I Prefer YouTube Shorts Instead Of Reels Anyway",
                "description": "Click SPIN 10,000 times",
                "threshold": 10000,
                "boost": 0.60  # 60%
            }
        ]
    },
    "stealing": {
        "name": "Stealing Achievement Category",
        "levels": [
            {
                "level": 0,
                "name": "Honest Gardener",
                "description": "You haven't stolen any harvests or gathers yet!",
                "threshold": 0,
                "boost": 0.0
            },
            {
                "level": 1,
                "name": "Level 1 Crook",
                "description": "Steal 1 gather or harvest",
                "threshold": 1,
                "boost": 0.02  # 2%
            },
            {
                "level": 2,
                "name": "Bandit",
                "description": "Steal 5 gathers/harvests",
                "threshold": 5,
                "boost": 0.05  # 5%
            },
            {
                "level": 3,
                "name": "Highway Robbery",
                "description": "Steal 15 gathers/harvests",
                "threshold": 15,
                "boost": 0.10  # 10%
            },
            {
                "level": 4,
                "name": "Pillager",
                "description": "Steal 35 gathers/harvests",
                "threshold": 35,
                "boost": 0.20  # 20%
            },
            {
                "level": 5,
                "name": "Grand Theft Auto",
                "description": "Steal 75 gathers/harvests",
                "threshold": 75,
                "boost": 0.40  # 40%
            },
            {
                "level": 6,
                "name": "Phantom Thief",
                "description": "Steal 150 gathers/harvests",
                "threshold": 150,
                "boost": 0.70  # 70%
            },
            {
                "level": 7,
                "name": "They'll Never Know It Was Me",
                "description": "Steal 250 gathers/harvests",
                "threshold": 250,
                "boost": 1.0  # 100%
            },
            {
                "level": 8,
                "name": "Pickpocket 100",
                "description": "Steal 350 gathers/harvests",
                "threshold": 350,
                "boost": 1.5  # 150%
            },
            {
                "level": 9,
                "name": "Untouchable",
                "description": "Steal 425 gathers/harvests",
                "threshold": 425,
                "boost": 2.0  # 200%
            },
            {
                "level": 10,
                "name": "Level 99 Mafia Boss",
                "description": "Steal 500 gathers/harvests",
                "threshold": 500,
                "boost": 2.5  # 250%
            }
        ]
    },
    "almanac": {
        "name": "Almanac Achievement Category",
        "levels": [
            {"level": 0, "name": "Zero to Hero", "description": "You have no plants?? Do /gather and then check your /almanac!", "threshold": 0, "boost": 0.0},
            {"level": 1, "name": "Field Notes", "description": "Have 10% of the /almanac filled out", "threshold": 10, "boost": 0.05},
            {"level": 2, "name": "Initiate", "description": "Have 20% of the /almanac filled out", "threshold": 20, "boost": 0.10},
            {"level": 3, "name": "Sproutling Scholar", "description": "Have 30% of the /almanac filled out", "threshold": 30, "boost": 0.15},
            {"level": 4, "name": "Archivist", "description": "Have 40% of the /almanac filled out", "threshold": 40, "boost": 0.20},
            {"level": 5, "name": "Thorns & Theories", "description": "Have 50% of the /almanac filled out", "threshold": 50, "boost": 0.25},
            {"level": 6, "name": "Canopy Chronicler", "description": "Have 60% of the /almanac filled out", "threshold": 60, "boost": 0.30},
            {"level": 7, "name": "Botanist", "description": "Have 70% of the /almanac filled out", "threshold": 70, "boost": 0.35},
            {"level": 8, "name": "Graduate's Thesis", "description": "Have 80% of the /almanac filled out", "threshold": 80, "boost": 0.40},
            {"level": 9, "name": "Enlightened Eucalyptus", "description": "Have 90% of the /almanac filled out", "threshold": 90, "boost": 0.45},
            {"level": 10, "name": "Root of All Knowledge", "description": "Max out your /almanac!", "threshold": 100, "boost": 0.50},
        ]
    },
}

# Hidden achievements definitions
HIDDEN_ACHIEVEMENTS = {
    "john_rockefeller": {
        "name": "John Rockefeller",
        "description": "Pay someone at least $1,000,000",
        "boost": 0.10  # 10%
    },
    "beating_the_odds": {
        "name": "Beating The Odds",
        "description": "Cashout in a game of /russian where there are 5 bullets in the chamber",
        "boost": 0.15  # 15%
    },
    "beneficiary": {
        "name": "Beneficiary",
        "description": "Receive at least $1,000,000 from someone",
        "boost": 0.10  # 10%
    },
    "leap_year": {
        "name": "Leap Day",
        "description": "Have a /water streak of 29 days",
        "boost": 0.15  # 15%
    },
    "ceo": {
        "name": "CEO",
        "description": "Own over 50% of the amount of shares for any company in the stock market",
        "boost": 0.70  # 70%
    },
    "blockchain": {
        "name": "Blockchain",
        "description": "Have at least 1.00 of any cryptocoin",
        "boost": 0.05  # 5%
    },
    "almost_got_it": {
        "name": "Almost Got It",
        "description": "Do a /gather when there's less than 1 second before the cooldown is up, but you're still on cooldown",
        "boost": 0.03  # 3%
    },
    "almost_got_it_too": {
        "name": "Almost Got It, Too",
        "description": "Do a /harvest when there's 0 seconds remaining, but you're still on cooldown",
        "boost": 0.06  # 6%
    },
    "maxed_out": {
        "name": "Maxed Out",
        "description": "Have all GPUs, all gardeners, all /gear upgrades maxed, all /orchard upgrades maxed",
        "boost": 0.50  # 50%
    },
    "high_reroller": {
        "name": "High Reroller",
        "description": "Get an imbue enchantment that is NETHERITE, LUMINITE, CELESTIAL, or SECRET",
        "boost": 0.25  # 25%
    },
    "social_butterfly": {
        "name": "Social Butterfly",
        "description": "Invite 20 people to the server",
        "boost": 0.35  # 35%
    },
    "no_monkey_business": {
        "name": "No Monkey Business",
        "description": "Defeat the Monkey",
        "boost": 0.025  # 2.5%
    },
    "grizzly_victory": {
        "name": "If It's Brown, Lay Down",
        "description": "Defeat the Grizzly Bear",
        "boost": 0.025  # 2.5%
    },
    "black_bear_blues": {
        "name": "If It's Black, Fight Back",
        "description": "Defeat the Black Bear",
        "boost": 0.025  # 2.5%
    },
    "polar_power": {
        "name": "If It's White, Goodnight",
        "description": "Defeat the Polar Bear",
        "boost": 0.025  # 2.5%
    },
    "tiger_tamer": {
        "name": "Tiger Tamer",
        "description": "Defeat the Tiger",
        "boost": 0.025  # 2.5%
    },
    "panther_pounce": {
        "name": "SFSC'S Mascot",
        "description": "Defeat the Panther",
        "boost": 0.025  # 2.5%
    },
    "homeless_hero": {
        "name": "Lean No More",
        "description": "Defeat the Homeless Man on Fent",
        "boost": 0.025  # 2.5%
    },
    "bullet_ant_squasher": {
        "name": "Dodged",
        "description": "Defeat a Bullet Ant or Bullet Ant Swarm",
        "boost": 0.025  # 2.5%
    },
    "skunkape_slayer": {
        "name": "Give Me The Werx",
        "description": "Defeat the Skunkape",
        "boost": 0.025  # 2.5%
    },
    "godzilla_king": {
        "name": "Kick 'Em In The Shin",
        "description": "Defeat Godzilla",
        "boost": 0.025  # 2.5%
    },
    "mothron_masher": {
        "name": "Not To Be Confused With Mothra",
        "description": "Defeat Mothron",
        "boost": 0.025  # 2.5%
    },
    "plantera_crusher": {
        "name": "The Great Southern Plantkill",
        "description": "Defeat Plantera",
        "boost": 0.025  # 2.5%
    },
    "wither_slayer": {
        "name": "Witherfall",
        "description": "Defeat The Wither",
        "boost": 0.025  # 2.5%
    },
    "ender_dragon_slayer": {
        "name": "The End?",
        "description": "Defeat the Ender Dragon",
        "boost": 0.025  # 2.5%
    },
    "retinazer_retired": {
        "name": "Ophthalmologist",
        "description": "Defeat Retinazer (The Twins)",
        "boost": 0.025  # 2.5%
    },
    "spazmatism_silenced": {
        "name": "Mechanical Menace",
        "description": "Defeat Spazmatism (The Twins)",
        "boost": 0.025  # 2.5%
    },
    "flower_plower": {
        "name": "Flower Plower",
        "description": "Fill out the Flower section of the /almanac",
        "boost": 0.20  # 20%
    },
    "parfaits": {
        "name": "Anyone Wanna Go For Parfaits?",
        "description": "Fill out the Fruit section of the /almanac",
        "boost": 0.20  # 20%
    },
    "eat_greens": {
        "name": "Yeah, I Eat My Greens",
        "description": "Fill out the Vegetable section of the /almanac",
        "boost": 0.20  # 20%
    },
    "hornet_hunter": {
        "name": "Hornet Hunter",
        "description": "Defeat a Hornet (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "bee_swarm_squasher": {
        "name": "Hive Mind",
        "description": "Defeat a Bee Swarm (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "angry_trapper_crusher": {
        "name": "Trapped",
        "description": "Defeat an Angry Trapper (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "man_eater_muncher": {
        "name": "Man Eaten",
        "description": "Defeat a Man Eater (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "giant_tortoise_tamer": {
        "name": "Shell Shock",
        "description": "Defeat a Giant Tortoise (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "arapaima_slayer": {
        "name": "Easy as Arapai",
        "description": "Defeat an Arapaima (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "piranha_predator": {
        "name": "I Prefer Tilapia",
        "description": "Defeat a Piranha (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "moth_masher": {
        "name": "Into The Light",
        "description": "Defeat the Moth (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "spiked_slime_squasher": {
        "name": "Slimed Out",
        "description": "Defeat a Spiked Jungle Slime (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "jungle_bat_basher": {
        "name": "Battered",
        "description": "Defeat a Jungle Bat (underground jungle)",
        "boost": 0.025  # 2.5%
    },
    "queen_bee_slayer": {
        "name": "Sting Operation",
        "description": "Defeat the Queen Bee",
        "boost": 0.025  # 2.5%
    },
    "the_roaring_knight_repelled": {
        "name": "Dess, Is That You?",
        "description": "Defeat The Roaring Knight",
        "boost": 0.05  # 5%
    },
    "nergigante_slayer": {
        "name": "Respect Your Elders",
        "description": "Defeat Nergigante",
        "boost": 0.05  # 5% (5+ Boost)
    },
    "rathalos_slayer": {
        "name": "King of the Skies",
        "description": "Defeat Rathalos",
        "boost": 0.05  # 5% (5+ Boost)
    },
    "sans_slayer": {
        "name": "Gaster Blasted",
        "description": "Defeat Sans",
        "boost": 0.05  # 5%
    },
    "pve_master": {
        "name": "Master Mode",
        "description": "Defeat every type of animal and boss at least once",
        "boost": 0.25  # 25%
    },
    "one_in_a_mikellion": {
        "name": "One in a Mikellion",
        "description": "Gather or harvest a plant with Mikellion rarity",
        "boost": 1.0  # 100%
    },
    "slots_three_in_a_row": {
        "name": "777",
        "description": "Win /slots 3 times in a row",
        "boost": 0.20  # 20%
    },
    "just_like_tf2": {
        "name": "Just Like TF2",
        "description": "Get a critical /gather",
        "boost": 0.05  # 5%
    },
    "moist": {
        "name": "Moist",
        "description": "Get 10 critical /gathers",
        "boost": 0.05  # 5%
    },
    "no_honor": {
        "name": "\"No Honor\"",
        "description": "Steal a critical /gather",
        "boost": 0.10  # 10%
    }
}

# Total number of hidden achievements (must match len(HIDDEN_ACHIEVEMENTS))
TOTAL_HIDDEN_ACHIEVEMENTS = len(HIDDEN_ACHIEVEMENTS)


def check_maxed_out_achievement(user_id: int, dossier: dict = None) -> bool:
    """Check if user has all upgrades maxed (all GPUs, all gardeners, all gear upgrades, all orchard upgrades).
    Returns True if achievement was newly unlocked, False otherwise.
    When dossier is provided (e.g. from get_user_dossier), uses it to avoid extra DB calls."""
    if dossier is not None:
        user_gpus = dossier.get("gpus", [])
        user_gardeners = dossier.get("gardeners", [])
        basket_upgrades = dossier.get("basket_upgrades", {})
        harvest_upgrades = dossier.get("harvest_upgrades", {})
    else:
        user_gpus = get_user_gpus(user_id)
        user_gardeners = get_user_gardeners(user_id)
        basket_upgrades = get_user_basket_upgrades(user_id)
        harvest_upgrades = get_user_harvest_upgrades(user_id)

    all_gpu_names = [gpu["name"] for gpu in GPU_SHOP]
    has_all_gpus = all(gpu_name in user_gpus for gpu_name in all_gpu_names)

    has_all_gardeners = len(user_gardeners) >= 5
    if has_all_gardeners:
        for gardener in user_gardeners:
            if not gardener.get("has_tool", False):
                has_all_gardeners = False
                break

    all_gear_maxed = (
        basket_upgrades.get("basket", 0) >= 10 and
        basket_upgrades.get("shoes", 0) >= 10 and
        basket_upgrades.get("gloves", 0) >= 10 and
        basket_upgrades.get("soil", 0) >= 10
    )

    all_orchard_maxed = (
        harvest_upgrades.get("car", 0) >= 10 and
        harvest_upgrades.get("chain", 0) >= 10 and
        harvest_upgrades.get("fertilizer", 0) >= 10 and
        harvest_upgrades.get("cooldown", 0) >= 10
    )

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
    9 = PLANTER IX (10000-14999 items) - achievement "Treehugger"
    10 = PLANTER X (15000+ items) - achievement "John Deere Himself"
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
    elif total_items < 15000:
        return 9  # PLANTER IX role -> achievement level 9
    else:
        return 10  # PLANTER X role -> achievement level 10


def get_achievement_multiplier(user_id: int, full_data=None) -> float:
    """
    Calculate the total achievement multiplier based on all achievement levels.
    Returns a multiplier (e.g., 1.005 for 0.5% boost).
    All achievement boosts stack additively.

    When *full_data* (from ``get_user_gather_full_data`` / ``get_user_harvest_full_data``)
    is provided, all values are read from the dict and **no** extra DB queries are made.
    """
    total_boost = 0.0

    if full_data is not None:
        achievements = full_data.get("achievements", {})
        hidden_achievements = achievements.get("hidden_achievements", {})

        for achievement_name, achievement_def in ACHIEVEMENTS.items():
            level = int(achievements.get(achievement_name, 0))
            if level > 0:
                levels = achievement_def["levels"]
                if level < len(levels):
                    total_boost += levels[level]["boost"]

        for achievement_key, achievement_data in HIDDEN_ACHIEVEMENTS.items():
            if hidden_achievements.get(achievement_key, False):
                total_boost += achievement_data["boost"]

        return 1.0 + total_boost

    # Fallback: individual DB queries (used by callers that don't pre-fetch)
    for achievement_name, achievement_def in ACHIEVEMENTS.items():
        level = get_user_achievement_level(user_id, achievement_name)
        if level > 0:
            levels = achievement_def["levels"]
            if level < len(levels):
                total_boost += levels[level]["boost"]

    for achievement_key, achievement_data in HIDDEN_ACHIEVEMENTS.items():
        if has_hidden_achievement(user_id, achievement_key):
            total_boost += achievement_data["boost"]

    return 1.0 + total_boost


_RANK_LEVELS = {
    "PINE I": 0, "PINE II": 1, "PINE III": 2,
    "CEDAR I": 3, "CEDAR II": 4, "CEDAR III": 5,
    "BIRCH I": 6, "BIRCH II": 7, "BIRCH III": 8,
    "MAPLE I": 9, "MAPLE II": 10, "MAPLE III": 11,
    "OAK I": 12, "OAK II": 13, "OAK III": 14,
    "FIR I": 15, "FIR II": 16, "FIR III": 17,
    "REDWOOD": 18,
}


def _bloom_count_to_rank(bloom_count: int) -> str:
    """Pure helper: derive bloom rank string from bloom_count (no DB)."""
    if bloom_count >= 18:
        return "REDWOOD"
    ranks = [
        "PINE I", "PINE II", "PINE III",
        "CEDAR I", "CEDAR II", "CEDAR III",
        "BIRCH I", "BIRCH II", "BIRCH III",
        "MAPLE I", "MAPLE II", "MAPLE III",
        "OAK I", "OAK II", "OAK III",
        "FIR I", "FIR II", "FIR III",
        "REDWOOD",
    ]
    return ranks[min(bloom_count, 18)]


def get_rank_perma_buff_multiplier(user_id, full_data=None):
    """
    Calculate the rank perma buff multiplier based on bloom rank.
    Each rank-up permanently multiplies ALL money earned by 1.2x (compounding).

    When *full_data* is provided, bloom_count is read from it (no DB query).
    """
    if full_data is not None:
        bloom_rank = _bloom_count_to_rank(full_data.get("bloom_count", 0))
    else:
        bloom_rank = get_bloom_rank(user_id)

    level = _RANK_LEVELS.get(bloom_rank, 0)
    return 1.2 ** level


# Bloom-scaled upgrade prices (Option B: compound per bloom)
BLOOM_PRICE_MULT = 1.12

# Bloom (prestige) cost scales 1.08x per bloom
BASE_BLOOM_COST = 500_000_000
BLOOM_PRESTIGE_MULT = 1.08
BLOOM_PLANTS_REQUIRED = 15000  # PLANTER X = 15000+ plants this cycle


def bloom_scaled_price(user_id: int, base_price: float) -> float:
    """Return base_price scaled by bloom count: base_price * (BLOOM_PRICE_MULT ** bloom_count)."""
    bloom_count = get_user_bloom_count(user_id)
    if bloom_count <= 0:
        return base_price
    return base_price * (BLOOM_PRICE_MULT ** bloom_count)


def bloom_prestige_cost(user_id: int) -> float:
    """Return the cost to bloom (prestige), scaled by 1.08x per bloom: BASE_BLOOM_COST * (BLOOM_PRESTIGE_MULT ** bloom_count)."""
    bloom_count = get_user_bloom_count(user_id)
    return BASE_BLOOM_COST * (BLOOM_PRESTIGE_MULT ** bloom_count)


BETA_TESTER_ROLE_NAME = "BETA TESTER"
BETA_TESTER_MONEY_MULTIPLIER = 1.3


def sync_beta_tester_from_member(member: discord.Member) -> None:
    """Sync the user's BETA TESTER role from Discord to the database cache."""
    if member is None:
        return
    has_role = discord.utils.get(member.roles, name=BETA_TESTER_ROLE_NAME) is not None
    set_user_beta_tester(member.id, has_role)


def get_beta_tester_money_multiplier(user_id: int) -> float:
    """Return 1.3 if user has BETA TESTER role (cached in DB), else 1.0."""
    return BETA_TESTER_MONEY_MULTIPLIER if get_user_beta_tester(user_id) else 1.0


SERVER_BOOSTER_MONEY_MULTIPLIER = 1.50
SERVER_BOOSTER_EMOJI = "<:ServerBoost:1477896878338347160>"


def sync_server_booster_from_member(member: discord.Member) -> None:
    """Sync the user's server booster status from Discord to the database cache (member.premium_since)."""
    if member is None:
        return
    is_booster = member.premium_since is not None
    set_user_server_booster(member.id, is_booster)


def get_server_booster_money_multiplier(user_id: int) -> float:
    """Return 1.50 if user is a server booster (cached in DB), else 1.0."""
    return SERVER_BOOSTER_MONEY_MULTIPLIER if get_user_server_booster(user_id) else 1.0


# Premium tiers (Discord roles): Seed $2, Sprout $5, Sapling $10, Evergreen $15. Exclusive: higher tier = better benefits, no stacking.
PREMIUM_ROLE_NAMES = ["Seed", "Sprout", "Sapling", "Evergreen"]
# Permanent money boost (\"Permaboost\") for each premium rank
# Seed=1.1x, Sprout=1.5x, Sapling=2.0x, Evergreen=3.0x
PREMIUM_MONEY_MULTIPLIERS = {0: 1.0, 1: 1.1, 2: 1.5, 3: 2.0, 4: 3.0}
# Command cooldown reductions for each premium rank (seconds)
# SEED:   -2s /gather, -1m /harvest, -2m /mine
# SPROUT: -3s /gather, -2m /harvest, -4m /mine
# SAPLING:-5s /gather, -4m /harvest, -8m /mine
# EVERGREEN: -7s /gather, -7m /harvest, -15m /mine
PREMIUM_COOLDOWN_REDUCTIONS = {
    0: {"gather_reduction": 0, "harvest_reduction": 0, "mine_reduction": 0},
    1: {"gather_reduction": 2, "harvest_reduction": 60, "mine_reduction": 120},
    2: {"gather_reduction": 3, "harvest_reduction": 120, "mine_reduction": 240},
    3: {"gather_reduction": 5, "harvest_reduction": 240, "mine_reduction": 480},
    4: {"gather_reduction": 7, "harvest_reduction": 420, "mine_reduction": 900},
}
# Display names must match Discord role names exactly (e.g. "🪴SAPLING ($10)" with no space after emoji)
PREMIUM_DISPLAY = {
    0: "",
    1: "🌰 SEED ($2)",
    2: "🌱 SPROUT ($5)",
    3: "🪴SAPLING ($10)",
    4: "🌲 EVERGREEN ($15)",
}
# Base daily /water amount per streak day for each premium rank
# 0 (none): 5,000; Seed: 15,000; Sprout: 50,000; Sapling: 100,000; Evergreen: 500,000
PREMIUM_WATER_BASE_AMOUNTS = {
    0: 5000.0,
    1: 15000.0,
    2: 50000.0,
    3: 100000.0,
    4: 500000.0,
}
# Premium gardener IDs (virtual): 6=Seed, 7=Sprout, 8=Sapling, 9=Evergreen.
# Auto-gather chances (per minute): Seed=22.5%, Sprout=35%, Sapling=50%, Evergreen=75%.
PREMIUM_GARDENER_CHANCES = {
    6: 0.225,  # Seed
    7: 0.35,   # Sprout
    8: 0.50,   # Sapling
    9: 0.75,   # Evergreen
}
# Auto-harvest chances (conditional on triggering an action): Seed=10%, Sprout=15%, Sapling=20%, Evergreen=35%.
PREMIUM_GARDENER_HARVEST_CHANCES = {
    6: 0.10,  # Seed
    7: 0.15,  # Sprout
    8: 0.20,  # Sapling
    9: 0.35,  # Evergreen
}

# Premium gardener embed colors
# Auto-gather: 4 shades of blue (lighter -> darker for cheaper -> more expensive)
PREMIUM_GARDENER_GATHER_COLORS = {
    6: discord.Color(0xA5D8FF),  # Seed - light blue
    7: discord.Color(0x74C0FC),  # Sprout - medium-light blue
    8: discord.Color(0x339AF0),  # Sapling - medium blue
    9: discord.Color(0x1864AB),  # Evergreen - dark blue
}

# Auto-harvest: 4 shades of orange (lighter -> darker for cheaper -> more expensive)
PREMIUM_GARDENER_HARVEST_COLORS = {
    6: discord.Color(0xFFE8CC),  # Seed - light orange
    7: discord.Color(0xFFC078),  # Sprout - medium-light orange
    8: discord.Color(0xFF922B),  # Sapling - medium orange
    9: discord.Color(0xE8590C),  # Evergreen - dark orange
}

# Emoji for each premium gardener in embeds (Seed=chestnut, Sprout=sprout, Sapling=potted plant, Evergreen=evergreen).
# Use Unicode below, or replace with custom Discord emoji <:name:id> from your server.
PREMIUM_GARDENER_EMOJI = {
    6: "🌰",   # Seed - chestnut
    7: "🌱",   # Sprout - sprout
    8: "🪴",   # Sapling - potted plant
    9: "🌲",   # Evergreen - evergreen
}

# Map Discord SKU IDs (from Developer Portal → Monetization → SKUs) to premium tier 1–4.
PREMIUM_SKU_TO_TIER: dict[str, int] = {
    "1479298502399098962": 1,   # SEED RANK ($2) 🌰
    "1479312006095048724": 2,   # SPROUT RANK ($5) 🌱
    "1479313565864165603": 3,   # SAPLING RANK ($10) 🪴
    "1479315594183446691": 4,   # EVERGREEN RANK ($15) 🌲
}

# Role names that match the SKU product names (create these exact roles in your server).
PREMIUM_ROLE_NAMES_SKU: dict[int, str] = {
    1: "SEED RANK ($2) 🌰",
    2: "SPROUT RANK ($5) 🌱",
    3: "SAPLING RANK ($10) 🪴",
    4: "EVERGREEN RANK ($15) 🌲",
}


async def assign_premium_role_for_entitlement(
    guild: discord.Guild, user_id: int, sku_id: str
) -> bool:
    """
    If sku_id is in PREMIUM_SKU_TO_TIER, assign the corresponding premium role to the user in the guild.
    Returns True if a role was assigned, False otherwise.
    """
    tier = PREMIUM_SKU_TO_TIER.get(str(sku_id))
    if tier is None or tier < 1 or tier > 4:
        return False
    member = guild.get_member(user_id)
    if member is None:
        return False
    # Try SKU role name first (e.g. "SEED RANK ($2) 🌰"), then PREMIUM_DISPLAY, then simple name
    role_name = PREMIUM_ROLE_NAMES_SKU.get(tier) or PREMIUM_DISPLAY.get(tier, "").strip() or PREMIUM_ROLE_NAMES[tier - 1]
    role = discord.utils.get(guild.roles, name=role_name)
    if role is None:
        role = discord.utils.get(guild.roles, name=PREMIUM_DISPLAY.get(tier, "").strip())
    if role is None:
        role = discord.utils.get(guild.roles, name=PREMIUM_ROLE_NAMES[tier - 1])
    if role is None:
        return False
    if role in member.roles:
        return False
    try:
        await member.add_roles(role, reason="Premium purchase (SKU)")
        set_user_premium_tier(user_id, tier)
        return True
    except Exception as e:
        print(f"[Premium] Failed to assign role {role_name} to {user_id}: {e}")
        return False


def get_premium_tier_from_member(member: discord.Member | None) -> int:
    """
    Return premium tier 0-4 by looking only at the member's Discord roles.

    Supports both legacy simple role names (Seed/Sprout/Sapling/Evergreen) and
    the display-style names used in PREMIUM_DISPLAY (e.g. \"🌰 SEED ($2)\").
    Highest matching tier wins. No DB reads.
    """
    if member is None:
        return 0

    # Collect role names once for efficient matching
    member_role_names = {r.name for r in member.roles}

    # Legacy simple names (case-insensitive)
    legacy_names = ["Seed", "Sprout", "Sapling", "Evergreen"]

    tier = 0
    # Check against SKU role names first (e.g. "SEED RANK ($2) 🌰")
    for i in range(1, 5):
        sku_name = PREMIUM_ROLE_NAMES_SKU.get(i)
        if sku_name and sku_name in member_role_names:
            tier = max(tier, i)
    # Check against PREMIUM_DISPLAY names (if configured)
    for i in range(1, 5):
        display_name = PREMIUM_DISPLAY.get(i, "")
        if display_name and display_name in member_role_names:
            tier = max(tier, i)

    # Fallback: role name contains tier name (e.g. "🪴SAPLING ($10)" contains "Sapling"), case-insensitive
    for i, legacy in enumerate(legacy_names, start=1):
        if any(legacy.lower() in name.lower() for name in member_role_names):
            tier = max(tier, i)

    return tier


def sync_premium_tier_from_member(member: discord.Member | None) -> None:
    """Sync premium tier from Discord roles only. Benefits are applied by looking at the user's premium role; DB is just a cache for code paths that only have user_id."""
    if member is None:
        return
    tier = get_premium_tier_from_member(member)
    set_user_premium_tier(member.id, tier)


def get_premium_tier_money_multiplier(user_id: int) -> float:
    """Return 1.1, 1.5, 2.0, or 3.0 for Seed/Sprout/Sapling/Evergreen; 1.0 for none."""
    return PREMIUM_MONEY_MULTIPLIERS.get(get_user_premium_tier(user_id), 1.0)


def get_premium_cooldown_reductions(user_id: int) -> dict:
    """Return gather_reduction, harvest_reduction, mine_reduction (seconds) for premium tier."""
    return PREMIUM_COOLDOWN_REDUCTIONS.get(get_user_premium_tier(user_id), PREMIUM_COOLDOWN_REDUCTIONS[0]).copy()


def get_effective_daily_shop_max_purchases(user_id: int) -> int:
    """Base 3 + premium tier (Seed=4, Sprout=5, Sapling=6, Evergreen=7)."""
    return 3 + get_user_premium_tier(user_id)


def get_premium_virtual_gardeners(user_id: int) -> list:
    """Return list of virtual gardener dicts for premium tier (id 6-9). Tier 1=Seed, 2=+Sprout, 3=+Sapling, 4=+Evergreen."""
    tier = get_user_premium_tier(user_id)
    return [{"id": 5 + t} for t in range(1, tier + 1)]


NETHER_STAR_MONEY_MULTIPLIER = 1.15
BLACK_SHARD_MONEY_MULTIPLIER = 1.2


def get_nether_star_money_multiplier(user_id: int) -> float:
    """Return multiplier based on Nether Star (shop item) and dayboosts. Additive stacking."""
    base_mult = NETHER_STAR_MONEY_MULTIPLIER if has_shop_item(user_id, "nether_star") else 1.0
    dayboost_count = get_dayboost_count(user_id, "nether_star")
    if dayboost_count > 0:
        # Additive: each dayboost adds (multiplier - 1.0) to the base
        # If no permanent, base is 1.0, so we add (1.15 - 1.0) * count = 0.15 * count
        # If permanent exists, base is 1.15, so we add 0.15 * count more
        boost_per_dayboost = NETHER_STAR_MONEY_MULTIPLIER - 1.0  # 0.15
        return base_mult + (boost_per_dayboost * dayboost_count)
    return base_mult


SHADOW_CRYSTAL_MONEY_MULTIPLIER = 1.05


def get_shadow_crystal_money_multiplier(user_id: int) -> float:
    """Return 1.05 if user has Shadow Crystal (shop item), else 1.0."""
    return SHADOW_CRYSTAL_MONEY_MULTIPLIER if has_shop_item(user_id, "shadow_crystal") else 1.0


def get_black_shard_money_multiplier(user_id: int) -> float:
    """Return multiplier based on Black Shard (shop item) and dayboosts. Additive stacking."""
    base_mult = BLACK_SHARD_MONEY_MULTIPLIER if has_shop_item(user_id, "black_shard") else 1.0
    dayboost_count = get_dayboost_count(user_id, "black_shard")
    if dayboost_count > 0:
        # Additive: each dayboost adds (multiplier - 1.0) to the base
        # If no permanent, base is 1.0, so we add (1.2 - 1.0) * count = 0.2 * count
        # If permanent exists, base is 1.2, so we add 0.2 * count more
        boost_per_dayboost = BLACK_SHARD_MONEY_MULTIPLIER - 1.0  # 0.2
        return base_mult + (boost_per_dayboost * dayboost_count)
    return base_mult


PALACE_TREASURE_MONEY_MULTIPLIER = 1.5
PALACE_TREASURE_STEAL_CHANCE_MULTIPLIER = 5.0


def get_palace_treasure_money_multiplier(user_id: int) -> float:
    """Return 1.5 if user has Palace Treasure (shop item), else 1.0."""
    return PALACE_TREASURE_MONEY_MULTIPLIER if has_shop_item(user_id, "palace_treasure") else 1.0


def get_edward_splash_money_multiplier(user_id: int) -> float:
    """Return cumulative 1% money multiplier per item. Edward and Splash Potion of Luck each add 1% (stack multiplicatively)."""
    mult = 1.0
    if has_shop_item(user_id, "edward"):
        mult *= 1.01
    if has_shop_item(user_id, "splash_potion_of_luck"):
        mult *= 1.01
    return mult


def get_eclipse_glasses_money_multiplier(user_id: int) -> float:
    """Return 1.15 if Solar Eclipse is active and user has Eclipse Glasses, else 1.0."""
    if get_active_event_by_type("solar_eclipse") is None:
        return 1.0
    return 1.15 if has_shop_item(user_id, "eclipse_glasses") else 1.0


def get_steal_chance_multiplier(user_id: int) -> float:
    """Return multiplier for victim's stealable chance. Palace Treasure 5x, Bodyguard 0.5x, Presidential Motorcade 0 (cannot be stolen)."""
    if has_shop_item(user_id, "presidential_motorcade"):
        return 0.0
    mult = 1.0
    if has_shop_item(user_id, "palace_treasure"):
        mult *= PALACE_TREASURE_STEAL_CHANCE_MULTIPLIER
    if has_shop_item(user_id, "bodyguard"):
        mult *= 0.5
    return mult


def get_pve_damage_multiplier(user_id: int, is_boss: bool = False) -> int:
    """Return total damage per hit for PvE (wild animals and optionally bosses). Base 1, weapons stack additively.
    King's Blade +3, Zenith +2, Death Note +2, Black Knife +2, Split Soul Katana +1, Inverted Spear +1 (enemies and bosses),
    Diamond Sword +1, Ancient Staff +1, Reaver Karambit +1, Nail +1, Evoker +1, Thorfinn's Dagger +1."""
    damage = 1
    if has_shop_item(user_id, "kings_blade"):
        damage += 3  # +3 for enemies
    if has_shop_item(user_id, "zenith"):
        damage += 2  # 1 -> 3
    if has_shop_item(user_id, "death_note"):
        damage += 2  # +2 for enemies
    if has_shop_item(user_id, "black_knife"):
        damage += 2  # +2 for enemies
    if has_shop_item(user_id, "split_soul_katana"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "inverted_spear_of_heaven"):
        damage += 1  # +1 for enemies and bosses
    if has_shop_item(user_id, "diamond_sword"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "ancient_staff"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "reaver_karambit"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "nail"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "evoker"):
        damage += 1  # +1 for enemies
    if has_shop_item(user_id, "thorfinns_dagger"):
        damage += 1  # +1 for enemies
    return damage


def can_gather(user_id, user_data=None, active_events=None, full_data=None):
    """
    Check if user can gather. Returns (can_gather: bool, time_left: int, is_roulette_cooldown: bool).
    
    Args:
        user_id: User ID
        user_data: Optional pre-fetched user data dict (from get_user_gather_data)
        active_events: Optional pre-fetched active events list
        full_data: Optional pre-fetched full data dict (from get_user_gather_full_data).
                   When provided, avoids extra DB calls for imbuement / invite reductions.
    """
    # Check Russian Roulette elimination cooldown first
    if full_data is not None:
        roulette_time = full_data.get("last_roulette_elimination_time", 0)
    else:
        roulette_time = get_user_last_roulette_elimination_time(user_id)

    if roulette_time > 0:
        current_time = time.time()
        cooldown_sec = get_roulette_elimination_cooldown_seconds(user_id)
        cooldown_end = roulette_time + cooldown_sec
        if current_time < cooldown_end:
            return False, int(cooldown_end - current_time), True
    
    # Fetch data if not provided
    if user_data is None:
        if full_data is not None:
            user_data = full_data
        else:
            user_data = get_user_gather_data(user_id)
    
    if active_events is None:
        active_events = get_active_events_cached()
    
    last_gather_time = user_data["last_gather_time"]
    current_time = time.time()
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
            cooldown_reduction += 8
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_day":
            cooldown_reduction += 5
    
    # Apply hoe imbuement cooldown reduction (Renewal)
    if full_data is not None:
        hoe_enchant = full_data.get("hoe_enchantment")
    else:
        hoe_enchant = get_user_hoe_attunement(user_id)
    if hoe_enchant:
        enchant_cd = hoe_enchant.get("cooldown_reduction", 0)
        cooldown_reduction += enchant_cd
    
    # Apply invite reward cooldown reduction (tier 13: -10 seconds)
    if full_data is not None:
        claimed = full_data.get("invite_claimed_rewards", [])
        cooldown_reduction += 10 if 13 in claimed else 0
    else:
        invite_reductions = get_invite_cooldown_reductions(user_id)
        cooldown_reduction += invite_reductions.get("gather_reduction", 0)
    premium_cd = get_premium_cooldown_reductions(user_id)
    cooldown_reduction += premium_cd.get("gather_reduction", 0)
    
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

def _perform_gather_for_user_sync(user_id: int, apply_cooldown: bool = True, 
                                  user_data=None, active_events=None,
                                  apply_orchard_fertilizer: bool = False,
                                  area_multiplier: float = 1.0,
                                  full_data=None,
                                  increment_command_count: bool = False,
                                  apply_update: bool = True) -> dict:
    """
    Synchronous implementation of gather logic. Runs in a thread via asyncio.to_thread()
    to avoid blocking the event loop with pymongo calls.

    When *full_data* (from ``get_user_gather_full_data``) is supplied, all
    multiplier look-ups read from it instead of issuing individual DB queries.
    """
    # Fetch data if not provided
    if user_data is None:
        if full_data is not None:
            user_data = full_data
        else:
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
    
    # Apply area multiplier (e.g. grove = 1.2x, marsh = 2x, etc.)
    base_value *= area_multiplier
    
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
    if full_data and full_data.get("shop_inventory", {}).get("mutagenic_serum", 0) >= 1:
        gmo_chance += 0.07
    elif not full_data and has_shop_item(user_id, "mutagenic_serum"):
        gmo_chance += 0.07
    
    # Apply event GMO chance modifications
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "radiation_leak":
            # GMO chance +20%
            gmo_chance += 0.20
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "gmo_surge":
            # GMO chance +10%
            gmo_chance += 0.10
    
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
            value_multiplier *= 1.5  # All earnings x1.5
        elif event_id == "harvest_festival":
            value_multiplier *= 1.5  # All item values +50%
    
    final_value *= basket_multiplier * value_multiplier

    # Apply seasonal month bonus
    month_index = random.randint(0, 11)
    month_name = MONTHS[month_index]
    seasonal_multiplier, seasonal_label = get_seasonal_multiplier(month_index, item["category"])
    final_value *= seasonal_multiplier

    # Apply orchard (harvest) fertilizer when e.g. gardener auto-gather
    if apply_orchard_fertilizer:
        harvest_upgrades = get_user_harvest_upgrades(user_id)
        fertilizer_tier = harvest_upgrades["fertilizer"]
        if fertilizer_tier > 0:
            fertilizer_multiplier = 1.0 + HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"]
            final_value *= fertilizer_multiplier

    # Additive boosts from base value, then rank multiplies the subtotal
    if full_data is not None:
        bloom_multiplier = 1.0 + (full_data.get("tree_rings", 0) * 0.005)
        water_base = 1.0 + (full_data.get("water_count", 0) * 0.01)
        if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1:
            water_multiplier = 1.0 + (water_base - 1.0) * 2  # Golden Watering Can: double water streak boost
        else:
            water_multiplier = water_base
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id, full_data=full_data)
        achievement_multiplier = get_achievement_multiplier(user_id, full_data=full_data)
        daily_rate = 0.04 if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1 else 0.02
        daily_bonus_multiplier = 1.0 + (full_data.get("consecutive_water_days", 0) * daily_rate)
    else:
        bloom_multiplier = get_bloom_multiplier(user_id)
        water_multiplier = get_water_multiplier(user_id)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
        achievement_multiplier = get_achievement_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
    base_final_value = final_value  # Base value after orchard/gear upgrades

    # Hoe imbuement: Prosperity (money bonus) and critical chance
    if full_data is not None:
        hoe_enchant = full_data.get("hoe_enchantment")
    else:
        hoe_enchant = get_user_hoe_attunement(user_id)
    enchant_money_bonus = 0.0
    is_critical_gather = False
    if hoe_enchant:
        money_bonus = hoe_enchant.get("money_bonus", 0)
        if money_bonus != 0:
            enchant_money_bonus = base_final_value * money_bonus

        # Check for critical gather (Abundance) - only for player gathers, not gardeners. Frontier Justice: +10% chance.
        crit_chance = hoe_enchant.get("critical_chance", 0)
        if has_shop_item(user_id, "frontier_justice"):
            crit_chance += 0.10
        if crit_chance > 0 and apply_cooldown:  # apply_cooldown=True means player, not gardener
            is_critical_gather = random.random() < crit_chance

    # Single base for all % buffs: (base + enchant) * rank * critical * scarecrow * bloomstone
    # This ensures displayed amounts match percentages (e.g. 55% always shows more $ than 50%).
    base_before_rank = base_final_value + enchant_money_bonus
    after_rank = base_before_rank * rank_perma_buff_multiplier
    after_critical = after_rank * (2.0 if is_critical_gather else 1.0)
    scarecrow_mult = 1.10 if (full_data is not None and full_data.get("shop_inventory", {}).get("scarecrow", 0) >= 1) or (full_data is None and has_shop_item(user_id, "scarecrow")) else 1.0
    after_scarecrow = after_critical * scarecrow_mult
    bloomstone_mult = 3.0 if (item.get("category") == "Flower" and ((full_data is not None and full_data.get("shop_inventory", {}).get("bloomstone", 0) >= 1) or (full_data is None and has_shop_item(user_id, "bloomstone")))) else 1.0
    after_bloomstone = after_scarecrow * bloomstone_mult
    base_for_buffs = float(after_bloomstone)

    # Rank boost display: dollar amount that rank added (for embed only)
    extra_money_from_rank = base_for_buffs * (rank_perma_buff_multiplier - 1.0) / rank_perma_buff_multiplier if rank_perma_buff_multiplier > 1.0 else 0.0

    # All additive % buffs use the SAME base (base_for_buffs) so higher % always = higher $
    extra_money_from_bloom = base_for_buffs * (bloom_multiplier - 1.0)
    extra_money_from_water = base_for_buffs * (water_multiplier - 1.0)
    extra_money_from_achievement = base_for_buffs * (achievement_multiplier - 1.0)
    beta_tester_mult = get_beta_tester_money_multiplier(user_id)
    server_booster_mult = get_server_booster_money_multiplier(user_id)
    premium_mult = get_premium_tier_money_multiplier(user_id)
    nether_star_mult = get_nether_star_money_multiplier(user_id)
    black_shard_mult = get_black_shard_money_multiplier(user_id)
    shadow_crystal_mult = get_shadow_crystal_money_multiplier(user_id)
    palace_mult = get_palace_treasure_money_multiplier(user_id)
    edward_mult = get_edward_splash_money_multiplier(user_id)
    eclipse_mult = get_eclipse_glasses_money_multiplier(user_id)
    extra_money_from_daily = base_for_buffs * (daily_bonus_multiplier - 1.0) if daily_bonus_multiplier > 1.0 else 0.0
    extra_money_from_beta_tester = base_for_buffs * (beta_tester_mult - 1.0) if beta_tester_mult > 1.0 else 0.0
    extra_money_from_server_booster = base_for_buffs * (server_booster_mult - 1.0) if server_booster_mult > 1.0 else 0.0
    extra_money_from_premium = base_for_buffs * (premium_mult - 1.0) if premium_mult > 1.0 else 0.0
    extra_money_from_nether_star = base_for_buffs * (nether_star_mult - 1.0) if nether_star_mult > 1.0 else 0.0
    extra_money_from_black_shard = base_for_buffs * (black_shard_mult - 1.0) if black_shard_mult > 1.0 else 0.0
    extra_money_from_shadow_crystal = base_for_buffs * (shadow_crystal_mult - 1.0) if shadow_crystal_mult > 1.0 else 0.0
    extra_palace = base_for_buffs * (palace_mult - 1.0) if palace_mult > 1.0 else 0.0
    extra_edward = base_for_buffs * (edward_mult - 1.0) if edward_mult > 1.0 else 0.0
    extra_eclipse = base_for_buffs * (eclipse_mult - 1.0) if eclipse_mult > 1.0 else 0.0
    work_lunch_extra = base_for_buffs * 0.10 if (not apply_cooldown and has_shop_item(user_id, "work_lunch")) else 0.0
    overtime_extra = base_for_buffs * 1.0 if (not apply_cooldown and has_shop_item(user_id, "overtime_approval") and random.random() < 0.10) else 0.0
    alchemist_extra = 0.0
    if full_data is not None and full_data.get("shop_inventory", {}).get("alchemists_pocketwatch", 0) >= 1:
        alchemist_extra = base_for_buffs * 0.05
    elif full_data is None and has_shop_item(user_id, "alchemists_pocketwatch"):
        alchemist_extra = base_for_buffs * 0.05
    final_value = base_for_buffs + extra_money_from_bloom + extra_money_from_water + extra_money_from_achievement + extra_money_from_daily + extra_money_from_beta_tester + extra_money_from_server_booster + extra_money_from_premium + extra_money_from_nether_star + extra_money_from_black_shard + extra_money_from_shadow_crystal + extra_palace + extra_edward + extra_eclipse + work_lunch_extra + overtime_extra + alchemist_extra

    # Calculate new balance from pre-fetched data
    current_balance = user_data["balance"]
    new_balance = current_balance + final_value

    if apply_update:
        tree_ring_awarded = perform_gather_update(
            user_id=user_id,
            balance_increment=final_value,
            item_name=name,
            ripeness_name=ripeness["name"],
            category=item["category"],
            apply_cooldown=apply_cooldown,
            increment_command_count=increment_command_count,
        )
    else:
        tree_ring_awarded = False

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
        "new_balance": new_balance,
        "enchant_money_bonus": enchant_money_bonus,
        "is_critical_gather": is_critical_gather,
        "hoe_enchant": hoe_enchant,
        "month_name": month_name,
        "beta_tester_multiplier": beta_tester_mult,
        "extra_money_from_beta_tester": extra_money_from_beta_tester,
        "server_booster_multiplier": server_booster_mult,
        "extra_money_from_server_booster": extra_money_from_server_booster,
        "premium_tier_multiplier": premium_mult,
        "extra_money_from_premium": extra_money_from_premium,
        "nether_star_multiplier": nether_star_mult,
        "extra_money_from_nether_star": extra_money_from_nether_star,
        "black_shard_multiplier": black_shard_mult,
        "extra_money_from_black_shard": extra_money_from_black_shard,
        "shadow_crystal_multiplier": shadow_crystal_mult,
        "extra_money_from_shadow_crystal": extra_money_from_shadow_crystal,
        "seasonal_multiplier": seasonal_multiplier,
        "seasonal_label": seasonal_label,
        "tree_ring_awarded": tree_ring_awarded,
    }

async def perform_gather_for_user(user_id: int, apply_cooldown: bool = True, 
                                  user_data=None, active_events=None,
                                  apply_orchard_fertilizer: bool = False,
                                  area_multiplier: float = 1.0,
                                  full_data=None,
                                  increment_command_count: bool = False) -> dict:
    """
    Perform a gather action for a user. Returns dict with gathered item info.
    Runs the synchronous DB-heavy logic in a thread to avoid blocking the event loop.
    """
    return await asyncio.to_thread(
        _perform_gather_for_user_sync,
        user_id, apply_cooldown, user_data, active_events,
        apply_orchard_fertilizer, area_multiplier, full_data,
        increment_command_count
    )

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
    {"category": "Flower", "name": "Hyacinth 🪻", "base_value": 3},
    {"category": "Flower", "name": "Wilted Rose 🥀", "base_value": 0.5},
    {"category": "Flower", "name": "Fortnite Battle Pass 🌸", "base_value": 5.2},
    {"category": "Flower", "name": "Rainbow Eucalyptus 🌈", "base_value": 3.5},


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
    {"category": "Fruit","name": "Rice Plant 🌾", "base_value": 0.7},
    {"category": "Fruit","name": "Lime 🍋", "base_value": 5.5},
    {"category": "Fruit","name": "Chestnut 🌰", "base_value": 0.35},
    {"category": "Fruit","name": "Golden Apple", "base_value": 15},
    {"category": "Fruit","name": "Enchanted Golden Apple", "base_value": 25},
    {"category": "Fruit","name": "Fruit of the Spirit 🍇", "base_value": 6.9},
    {"category": "Fruit","name": "Glowberry", "base_value": 4.5},

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
    {"category": "Vegetable","name": "Sweet Potato 🍠", "base_value": 13.13},
]

# Almanac: undiscovered (plant, ripeness) shown as 3x this emoji (custom :HIDDEN:)
ALMANAC_HIDDEN_EMOJI = "<:HIDDEN:1478915430390304788>"

# Custom Discord emojis for items that don't have a Unicode emoji in the name (CDN IDs from server)
# Use lowercase emoji names so :flowey: and :raspberry: display correctly in #rares and elsewhere
CUSTOM_ITEM_EMOJIS = {
    "Flowey": "<:flowey:1473550098716819682>",
    "Raspberry": "<:raspberry:1473550163711627399>",
    "Golden Apple": "<:goldenapple:1476645453415055657>",
    "Enchanted Golden Apple": "<a:enchantedgoldapple:1476645603931984014>",
    "Glowberry": "<:glowberry:1479154254576091136>",
}

def get_item_display_emoji(item_name: str) -> str:
    """Return emoji-only string for harvest display (avoids 1024 embed field limit).
    Uses CUSTOM_ITEM_EMOJIS for Flowey/Raspberry; otherwise extracts emoji from name (e.g. 'Rose 🌹' -> '🌹')."""
    if item_name in CUSTOM_ITEM_EMOJIS:
        return CUSTOM_ITEM_EMOJIS[item_name]
    if " " in item_name:
        return item_name.split()[-1]
    return item_name[-1] if item_name else ""

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
    "Hyacinth 🪻": "Fragrant spring bloom with clustered bell-shaped flowers!",
    "Wilted Rose 🥀": "What's a gather?",
    "Strawberry 🍓": "Sweet and juicy, nature's candy!",
    "Blueberry 🫐": "Tiny but packed with flavor!",
    "Raspberry": "Tart and tangy, perfect for desserts!",
    "Cherry 🍒": "Small and sweet, a summer treat!",
    "Apple 🍎": "One a day keeps the doctor away!",
    "Pear 🍐": "Sweet and crisp!",
    "Orange 🍊": "Yeah, we're from Florida. Hey Apple!",
    "Grape 🍇": "Gabo!",
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
    "Rice Plant 🌾": "The staple grain that feeds the world!",
    "Lime 🍋": "Tart citrus burst, perfect for cocktails and pies!",
    "Chestnut 🌰": "Roast 'em on an open fire, or eat 'em any way!",
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
    "Sweet Potato 🍠": "Naturally sweet and nutritious root vegetable!",
     "Golden Apple": "Half-a-heartedly comes in clutch! Commonly referred to as a Gapple.",
     "Enchanted Golden Apple": "A ambrosiac creation from the hands of Notch himself!",
     "Fruit of the Spirit 🍇": "A holy harvest of love, joy, and peace.",
     "Glowberry": "Found in a lush cave, this berry can produce light!",
     "Fortnite Battle Pass 🌸": "You just WHAT out of your WHAT?",
     "Rainbow Eucalyptus 🌈": "A tree trunk painted with every color of the rainbow!",
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
    {"name": "Legendary", "multiplier": 20, "chance": 1},
    {"name": "Netherite", "multiplier": 25, "chance": 0.5},
    {"name": "Luminite", "multiplier": 50, "chance": 0.1},
    {"name": "Celestial", "multiplier": 100, "chance": 0.05},
    {"name": "Mikellion", "multiplier": 200, "chance": 0.000101},
]

#level of ripeness - VEGETABLES
LEVEL_OF_RIPENESS_VEGETABLES = [
    {"name": "Sproutling", "multiplier": 1, "chance": 25},
    {"name": "Raw", "multiplier": 1.3, "chance": 15},
    {"name": "Slightly Ripe", "multiplier": 1.5, "chance": 25},
    {"name": "Perfectly Ripe", "multiplier": 2.5, "chance": 20},
    {"name": "Overripe", "multiplier": 1.6, "chance": 10},
    {"name": "Spoiled", "multiplier": 0.9, "chance": 4.99999},
    {"name": "Legendary", "multiplier": 20, "chance": 1},
    {"name": "Netherite", "multiplier": 25, "chance": 0.5},
    {"name": "Luminite", "multiplier": 50, "chance": 0.1},
    {"name": "Celestial", "multiplier": 100, "chance": 0.05},
    {"name": "Mikellion", "multiplier": 200, "chance": 0.000101},
]

#level of ripeness - FLOWERS
LEVEL_OF_RIPENESS_FLOWERS = [
    {"name": "Budded", "multiplier": 0.75, "chance": 30},
    {"name": "Blooming", "multiplier": 1, "chance": 45},
    {"name": "Full Bloom", "multiplier": 1.5, "chance": 20},
    {"name": "Wilted", "multiplier": 0.6, "chance": 4.99999},
    {"name": "Legendary", "multiplier": 20, "chance": 1},
    {"name": "Netherite", "multiplier": 25, "chance": 0.5},
    {"name": "Luminite", "multiplier": 50, "chance": 0.1},
    {"name": "Celestial", "multiplier": 100, "chance": 0.05},
    {"name": "Mikellion", "multiplier": 200, "chance": 0.000101},
]

# Almanac key separator (must match database.ALMANAC_KEY_SEP when checking entries)
_ALMANAC_KEY_SEP = "||"


def _almanac_slots_by_category_build():
    """Build (category_name, list of (item_name, ripeness_name)) excluding Mikellion. Called once at module load."""
    out = {}
    for item in GATHERABLE_ITEMS:
        cat = item["category"]
        name = item["name"]
        if cat == "Flower":
            ripeness_list = [r["name"] for r in LEVEL_OF_RIPENESS_FLOWERS if r["name"] != "Mikellion"]
        elif cat == "Fruit":
            ripeness_list = [r["name"] for r in LEVEL_OF_RIPENESS_FRUITS if r["name"] != "Mikellion"]
        elif cat == "Vegetable":
            ripeness_list = [r["name"] for r in LEVEL_OF_RIPENESS_VEGETABLES if r["name"] != "Mikellion"]
        else:
            ripeness_list = []
        if cat not in out:
            out[cat] = []
        for r in ripeness_list:
            out[cat].append((name, r))
    return out


# Cached at module load for lightning-fast /almanac (no repeated iteration over GATHERABLE_ITEMS).
_ALMANAC_SLOTS_BY_CATEGORY = _almanac_slots_by_category_build()
_ALMANAC_TOTAL_SLOTS = sum(len(s) for s in _ALMANAC_SLOTS_BY_CATEGORY.values())
_ALMANAC_COUNTABLE_KEYS = frozenset(
    f"{item}{_ALMANAC_KEY_SEP}{rip}"
    for slots in _ALMANAC_SLOTS_BY_CATEGORY.values()
    for (item, rip) in slots
)


def _almanac_slots_by_category():
    """Return cached (category_name -> list of (item_name, ripeness_name)) excluding Mikellion."""
    return _ALMANAC_SLOTS_BY_CATEGORY


def _almanac_total_slots_excluding_mikellion():
    """Total number of (plant, ripeness) slots that count toward almanac completion (no Mikellion)."""
    return _ALMANAC_TOTAL_SLOTS


def _almanac_filled_count(almanac_entries: dict) -> int:
    """Count how many of the countable (plant, ripeness) slots the user has (excluding Mikellion)."""
    return sum(1 for k in almanac_entries if k in _ALMANAC_COUNTABLE_KEYS)


def _almanac_section_filled(almanac_entries: dict, category: str) -> bool:
    """True if every (plant, ripeness) slot for this category is filled (excluding Mikellion)."""
    slots = _almanac_slots_by_category().get(category, [])
    for (item_name, rip) in slots:
        key = f"{item_name}{_ALMANAC_KEY_SEP}{rip}"
        if key not in almanac_entries:
            return False
    return True


async def check_almanac_achievements_async(user_id: int, channel_or_interaction, user_mention: str):
    """After gather/harvest (or gardener): update almanac achievement level and section hidden achievements; notify if new.
    Achievements are DM'd to the user (can't send ephemeral after response already sent)."""
    try:
        almanac_entries = get_user_almanac_entries(user_id)
        total_slots = _almanac_total_slots_excluding_mikellion()
        filled = _almanac_filled_count(almanac_entries)
        pct = (100.0 * filled / total_slots) if total_slots else 0
        new_level = get_achievement_level_for_stat("almanac", int(round(pct)))
        current_level = get_user_achievement_level(user_id, "almanac")
        if new_level > current_level:
            set_user_achievement_level(user_id, "almanac", new_level)
            await send_achievement_notification_dm(user_id, "almanac", new_level)
        for (hidden_key, cat) in [("flower_plower", "Flower"), ("parfaits", "Fruit"), ("eat_greens", "Vegetable")]:
            if _almanac_section_filled(almanac_entries, cat) and not has_hidden_achievement(user_id, hidden_key):
                unlock_hidden_achievement(user_id, hidden_key)
                await send_hidden_achievement_notification_dm(user_id, hidden_key)
    except Exception as e:
        print(f"Error in check_almanac_achievements_async: {e}")


MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

# Seasonal month bonuses - certain months boost certain item categories
SEASONAL_BONUSES = {
    3: {"category": "Flower", "multiplier": 1.75, "label": "🌸 Spring Flower Season"},      # April
    4: {"category": "Flower", "multiplier": 1.75, "label": "🌸 Spring Flower Season"},      # May
    5: {"category": "Vegetable", "multiplier": 1.5, "label": "🥬 Vegetable Season"},        # June
    6: {"category": "Fruit", "multiplier": 1.33, "label": "🍎 Summer Fruit Season"},        # July
    7: {"category": "Fruit", "multiplier": 1.33, "label": "🍎 Summer Fruit Season"},        # August
    8: {"category": "Fruit", "multiplier": 1.33, "label": "🍎 Summer Fruit Season"},        # September
    9: {"category": "Vegetable", "multiplier": 1.5, "label": "🥬 Vegetable Season"},        # October
}

def get_seasonal_multiplier(month_index: int, category: str) -> tuple:
    """Returns (multiplier, label) for a given month and item category."""
    bonus = SEASONAL_BONUSES.get(month_index)
    if bonus and bonus["category"] == category:
        return bonus["multiplier"], bonus["label"]
    return 1.0, None

# Event definitions
HOURLY_EVENTS = [
    {
        "id": "radiation_leak",
        "name": "Radiation Leak!",
        "emoji": "☢️",
        "description": "Radiation has leaked into the forest! GMO mutations are more common!",
        "effect": "GMO chance +20%"
    },
    {
        "id": "may_flowers",
        "name": "May Flowers!",
        "emoji": "🌸",
        "description": "Flowers are blooming everywhere!",
        "effect": "Flower gather chance +60%, flower prices +200%"
    },
    {
        "id": "bumper_crop",
        "name": "Nature's Blessing!",
        "emoji": "🌾",
        "description": "The spirits favor you! All items are worth double!",
        "effect": "All item values +100%"
    },
    {
        "id": "speed_harvest",
        "name": "Adrenaline Boost!",
        "emoji": "⚡",
        "description": "The forest pulses with urgency! Cooldowns are decreased!",
        "effect": "Gather cooldown -15s, Harvest cooldown -3 min"
    },
    {
        "id": "perfect_ripeness",
        "name": "High Noon!",
        "emoji": "⭐",
        "description": "The sun is aligned perfectly above! The plants are more ripe!",
        "effect": "All ripeness multipliers +50%"
    },
    {
        "id": "fruit_festival",
        "name": "Sweet Surge!",
        "emoji": "🍎",
        "description": "The fruits are flourishing! Fruits are now more common!",
        "effect": "Fruit gather chance +50%, fruit prices +100%"
    },
    {
        "id": "vegetable_boom",
        "name": "Earth's Bounty!",
        "emoji": "🥕",
        "description": "Vegetables are thriving in the earth! Vegetables are more common!",
        "effect": "Vegetable gather chance +50%, vegetable prices +100%"
    },
    {
        "id": "chain_reaction",
        "name": "Chain Reaction!",
        "emoji": "🔗",
        "description": "Chain chances are more common!",
        "effect": "Chain chance +10%"
    },
    {
        "id": "basket_boost",
        "name": "Carrier's Blessing!",
        "emoji": "🧺",
        "description": "Your basket is shining! The basket multiplier increases!",
        "effect": "Basket multiplier +50%"
    },
    {
        "id": "lucky_strike",
        "name": "Fortune Frenzy!",
        "emoji": "🍀",
        "description": "Luck is on your side! All multipliers increase!",
        "effect": "All earnings +25%"
    }
]

DAILY_EVENTS = [
    {
        "id": "double_money",
        "name": "Gold Rush!",
        "emoji": "💰",
        "description": "The California sun calls you! All money +50% for this day!",
        "effect": "All earnings +50% for 24 hours"
    },
    {
        "id": "speed_day",
        "name": "Overdrive!",
        "emoji": "🏃",
        "description": "Your equipment's going into high gear! Cooldowns are reduced!",
        "effect": "Gather cooldown -5s, Harvest cooldown -1 min for 24 hours"
    },
    {
        "id": "gmo_surge",
        "name": "Mutagenic Acceleration!",
        "emoji": "✨",
        "description": "The plants are evolving!! Increased GMO chance all day!",
        "effect": "GMO chance +10% for 24 hours"
    },
    {
        "id": "harvest_festival",
        "name": "Plenty o' Prosperity!",
        "emoji": "🎉",
        "description": "All items are worth more today!",
        "effect": "All item values +50% for 24 hours"
    },
    {
        "id": "ripeness_rush",
        "name": "Perfect Timing!",
        "emoji": "🌿",
        "description": "Everything's in season! All plants have a higher chance to be perfectly ripe!",
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
        self.players = {host_id: {"name": host_name, "alive": True, "rounds_survived": 0, "current_stake": bet_amount, "cashed_out": False}}
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
            "current_stake": self.bet_amount,
            "cashed_out": False
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
        embed.add_field(name="💰 Total Pot", value=format_money(game.pot), inline=True)
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
        embed.add_field(name="💰 Current Winnings", value=format_money(game.pot + winner['current_stake']), inline=True)
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
    embed.add_field(name="💰 Current Stake", value=format_money(current_player['current_stake']), inline=True)
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
    # Blanks: 20% chance for bullet to be a blank when fired on a user who has the item (they survive)
    is_blank = shot_fired and has_shop_item(current_player_id, "blanks") and random.random() < 0.20
    survived = not shot_fired or is_blank

    if shot_fired and not is_blank:
        # Player eliminated
        game.eliminate(current_player_id)
        game.bullets -= 1

        embed = discord.Embed(
            title="💥 BANG! 💥",
            description=f"**{current_player['name']}** has been eliminated!",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="💀 Status", value="ELIMINATED", inline=True)
        embed.add_field(name="💸 Lost", value=format_money(current_player['current_stake']), inline=True)
        embed.add_field(name="💰 Pot Now", value=format_money(game.pot), inline=True)
        embed.add_field(name="🔫 Bullets Left", value=f"{game.bullets}/6", inline=True)
        embed.add_field(name="👥 Players Alive", value=f"{len(game.get_alive_players())}", inline=True)

        await msg.edit(embed=embed)

        # remove player from active games
        if current_player_id in user_active_games:
            del user_active_games[current_player_id]

        # Check russian roulette achievement (player died = game completed)
        await check_russian_roulette_achievement(current_player_id)

        # check if anyone is left
        await asyncio.sleep(2)
        if len(game.get_alive_players()) == 0:
            await end_roulette_game(channel, game_id)
            return

        # continue to next player - give them option to cash out (except first turn)
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
            embed.add_field(name="💰 Potential Winnings", value=format_money(potential_winnings), inline=True)
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

    if survived:
        # Player survived (click or BLANK!)
        game.player_survived_round(current_player_id)
        if is_blank:
            embed = discord.Embed(
                title="💥 BLANK! 💥",
                description=f"**{current_player['name']}** survived — it was a blank!",
                color=discord.Color.green()
            )
        else:
            embed = discord.Embed(
                title="*click*",
                description=f"**{current_player['name']}** survived!",
                color=discord.Color.green()
            )
        new_multiplier = game.calculate_total_multiplier(current_player['rounds_survived'])
        embed.add_field(name="✅ Status", value="ALIVE", inline=True)
        embed.add_field(name="💰 Current Stake", value=format_money(current_player['current_stake']), inline=True)
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
        embed.add_field(name="💰 Total Pot", value=format_money(game.pot), inline=True)
        
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
    
    embed.add_field(name="💰 Potential Winnings", value=format_money(potential_winnings), inline=True)
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

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, emoji="🔫")
    async def join_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
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

    @discord.ui.button(label="Start", style=discord.ButtonStyle.blurple, emoji="❗")
    async def start_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
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
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, emoji="❌")
    async def cancel_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
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
            embed.add_field(
                name="💰 Refunded",
                value=f"{format_money(game.bet_amount)} to {refunded_count} player(s)",
                inline=True,
            )
            
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
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.game_id not in active_roulette_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            
            game = active_roulette_games[self.game_id]
            current_player_id = game.get_current_player()
            
            if interaction.user.id != current_player_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
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
            if not await safe_defer(interaction, ephemeral=False):
                return
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
            game.players[current_player_id]['cashed_out'] = True
            
            embed = discord.Embed(
                title="💰 CASHED OUT! 💰",
                description=f"**{player['name']}** decided to walk away!",
                color=discord.Color.gold()
            )
            embed.add_field(name="💵 Winnings", value=format_money(winnings), inline=True)
            embed.add_field(
                name="💸 Profit",
                value=format_money(normalize_money(winnings - normalize_money(game.bet_amount))),
                inline=True,
            )
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
            
            # Check russian roulette achievement (cashout = game completed)
            await check_russian_roulette_achievement(current_player_id, interaction=interaction)
            
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
        game.players[current_player_id]['cashed_out'] = True
        
        embed = discord.Embed(
            title="💰 AUTO CASHED OUT! 💰",
            description=f"**{player['name']}** timed out and was automatically cashed out!",
            color=discord.Color.orange()
        )
        embed.add_field(name="💵 Winnings", value=format_money(winnings), inline=True)
        embed.add_field(
            name="💸 Profit",
            value=format_money(normalize_money(winnings - normalize_money(game.bet_amount))),
            inline=True,
        )
        embed.add_field(name="📈 Multiplier Achieved", value=f"{game.calculate_total_multiplier(player['rounds_survived']):.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{player['rounds_survived']}", inline=True)
        
        await channel.send(embed=embed)
        
        # Check russian roulette achievement (auto-cashout = game completed)
        await check_russian_roulette_achievement(current_player_id)
        
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
        embed.add_field(name="💰 Total Winnings", value=format_money(total_winnings), inline=True)
        embed.add_field(name="💸 Net Profit", value=format_money(profit), inline=True)
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
        
        # Check russian roulette achievement (winner = game completed)
        await check_russian_roulette_achievement(winner_id)
        
    elif len(alive_players) == 0:
        # Check if everyone cashed out vs everyone died vs mixed (some cashed out, some died)
        everyone_cashed_out = all(data.get("cashed_out", False) for data in game.players.values())
        everyone_died = not any(data.get("cashed_out", False) for data in game.players.values())
        
        if everyone_cashed_out:
            # Results screen: everyone left with their winnings
            embed = discord.Embed(
                title="💰 EVERYONE CASHED OUT! 💰",
                description="Everyone walked away with their winnings. Here's how each player did:",
                color=discord.Color.gold()
            )
            for player_id, data in game.players.items():
                winnings = normalize_money(data["current_stake"])
                profit = normalize_money(winnings - normalize_money(game.bet_amount))
                mult = game.calculate_total_multiplier(data["rounds_survived"])
                embed.add_field(
                    name=f"**{data['name']}**",
                    value=(
                        f"💵 {format_money(winnings)} winnings | "
                        f"💸 {format_signed_money(profit)} profit\n"
                        f"📈 {mult:.2f}x multiplier | 🎯 {data['rounds_survived']} rounds"
                    ),
                    inline=False
                )
            await channel.send(embed=embed)
        elif everyone_died:
            # Everyone eliminated (all died, nobody cashed out)
            embed = discord.Embed(
                title="☠️ EVERYONE ELIMINATED ☠️",
                description="Nobody survived... The pot is lost to the void.",
                color=discord.Color.dark_red()
            )
            embed.add_field(name="💰 Lost Pot", value=format_money(game.pot), inline=True)
            await channel.send(embed=embed)
        else:
            # Mixed: some cashed out, some died — show results for each player
            embed = discord.Embed(
                title="/russian RESULTS",
                description="Here's how each player did:",
                color=discord.Color.blue()
            )
            for player_id, data in game.players.items():
                if data.get("cashed_out", False):
                    winnings = normalize_money(data["current_stake"])
                    profit = normalize_money(winnings - normalize_money(game.bet_amount))
                    mult = game.calculate_total_multiplier(data["rounds_survived"])
                    embed.add_field(
                        name=f"**{data['name']}** — Cashed out",
                        value=(
                            f"💵 {format_money(winnings)} winnings | "
                            f"💸 {format_signed_money(profit)} profit\n"
                            f"📈 {mult:.2f}x multiplier | 🎯 {data['rounds_survived']} rounds"
                        ),
                        inline=False
                    )
                else:
                    lost = normalize_money(data["current_stake"])
                    embed.add_field(
                        name=f"**{data['name']}** — Eliminated",
                        value=f"💀 Lost {format_money(lost)}",
                        inline=False
                    )
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


# --- GATHEMON (Turn-based Pokémon battle for plants) ---
# Move: power (int or None), raise_atk_self, raise_def_self, lower_atk_enemy, lower_def_enemy (0-2 each). Coil = atk+def self.
def _move(name: str, power: int | None = None, raise_atk_self: int = 0, raise_def_self: int = 0, lower_atk_enemy: int = 0, lower_def_enemy: int = 0):
    m = {"name": name, "raise_atk_self": raise_atk_self, "raise_def_self": raise_def_self, "lower_atk_enemy": lower_atk_enemy, "lower_def_enemy": lower_def_enemy}
    if power is not None:
        m["power"] = power
    return m

GATHEMON_FIRST = [
    {"name": "Sewaddle", "hp": 16, "atk": 3, "def": 2, "moves": [_move("Tackle", 3), _move("Razor Leaf", 4), _move("String Shot", lower_def_enemy=1), _move("Growth", raise_atk_self=1)]},
    {"name": "Bulbasaur", "hp": 18, "atk": 3, "def": 3, "moves": [_move("Tackle", 3), _move("Vine Whip", 4), _move("Growl", lower_atk_enemy=1), _move("Defense Curl", raise_def_self=1)]},
    {"name": "Snivy", "hp": 17, "atk": 3, "def": 2, "moves": [_move("Tackle", 3), _move("Leaf Blade", 4), _move("Leer", lower_def_enemy=1), _move("Growth", raise_atk_self=1)]},
    {"name": "Eevee", "hp": 17, "atk": 3, "def": 3, "moves": [_move("Tackle", 3), _move("Quick Attack", 4), _move("Tail Whip", lower_def_enemy=1), _move("Baby-Doll Eyes", lower_atk_enemy=1)]},
    {"name": "Oddish", "hp": 17, "atk": 3, "def": 3, "moves": [_move("Absorb", 3), _move("Acid", 4), _move("Growth", raise_atk_self=1), _move("Sweet Scent", lower_def_enemy=1)]},
    {"name": "Bellsprout", "hp": 16, "atk": 4, "def": 2, "moves": [_move("Vine Whip", 4), _move("Wrap", 3), _move("Growth", raise_atk_self=1), _move("Leer", lower_def_enemy=1)]},
    {"name": "Chikorita", "hp": 18, "atk": 2, "def": 4, "moves": [_move("Tackle", 3), _move("Razor Leaf", 4), _move("Growl", lower_atk_enemy=1), _move("Reflect", raise_def_self=1)]},
    {"name": "Treecko", "hp": 16, "atk": 4, "def": 2, "moves": [_move("Pound", 3), _move("Leaf Blade", 4), _move("Leer", lower_def_enemy=1), _move("Growth", raise_atk_self=1)]},
    {"name": "Turtwig", "hp": 18, "atk": 3, "def": 4, "moves": [_move("Tackle", 3), _move("Razor Leaf", 4), _move("Withdraw", raise_def_self=1), _move("Growl", lower_atk_enemy=1)]},
    {"name": "Chespin", "hp": 17, "atk": 4, "def": 3, "moves": [_move("Tackle", 3), _move("Vine Whip", 4), _move("Leer", lower_def_enemy=1), _move("Defense Curl", raise_def_self=1)]},
    {"name": "Rowlet", "hp": 16, "atk": 3, "def": 3, "moves": [_move("Peck", 3), _move("Razor Leaf", 4), _move("Growl", lower_atk_enemy=1), _move("Feather Dance", lower_atk_enemy=1)]},
    {"name": "Grookey", "hp": 17, "atk": 4, "def": 2, "moves": [_move("Scratch", 3), _move("Branch Poke", 4), _move("Leer", lower_def_enemy=1), _move("Growth", raise_atk_self=1)]},
    {"name": "Sprigatito", "hp": 16, "atk": 4, "def": 2, "moves": [_move("Scratch", 3), _move("Leafage", 4), _move("Tail Whip", lower_def_enemy=1), _move("Hone Claws", raise_atk_self=1)]},
]
GATHEMON_SECOND = [
    {"name": "Ivysaur", "hp": 24, "atk": 5, "def": 4, "moves": [_move("Razor Leaf", 6), _move("Seed Bomb", 7), _move("Growl", lower_atk_enemy=1), _move("Iron Defense", raise_def_self=2)]},
    {"name": "Swadloon", "hp": 26, "atk": 4, "def": 6, "moves": [_move("Razor Leaf", 6), _move("Bug Bite", 6), _move("Iron Defense", raise_def_self=2), _move("String Shot", lower_def_enemy=1)]},
    {"name": "Servine", "hp": 23, "atk": 5, "def": 4, "moves": [_move("Leaf Blade", 6), _move("Slam", 7), _move("Growth", raise_atk_self=2), _move("Leer", lower_def_enemy=1)]},
    {"name": "Gloom", "hp": 25, "atk": 5, "def": 4, "moves": [_move("Razor Leaf", 6), _move("Acid", 6), _move("Growth", raise_atk_self=2), _move("Sweet Scent", lower_def_enemy=1)]},
    {"name": "Weepinbell", "hp": 24, "atk": 6, "def": 3, "moves": [_move("Vine Whip", 6), _move("Slam", 7), _move("Growth", raise_atk_self=2), _move("Leer", lower_def_enemy=1)]},
    {"name": "Bayleef", "hp": 27, "atk": 4, "def": 6, "moves": [_move("Razor Leaf", 6), _move("Body Slam", 7), _move("Growl", lower_atk_enemy=1), _move("Reflect", raise_def_self=2)]},
    {"name": "Grovyle", "hp": 23, "atk": 6, "def": 4, "moves": [_move("Leaf Blade", 7), _move("Fury Cutter", 6), _move("Leer", lower_def_enemy=1), _move("Swords Dance", raise_atk_self=2)]},
    {"name": "Grotle", "hp": 26, "atk": 5, "def": 6, "moves": [_move("Razor Leaf", 6), _move("Bite", 6), _move("Withdraw", raise_def_self=2), _move("Growl", lower_atk_enemy=1)]},
    {"name": "Quilladin", "hp": 27, "atk": 5, "def": 6, "moves": [_move("Vine Whip", 6), _move("Body Slam", 7), _move("Defense Curl", raise_def_self=2), _move("Leer", lower_def_enemy=1)]},
    {"name": "Dartrix", "hp": 23, "atk": 5, "def": 4, "moves": [_move("Razor Leaf", 6), _move("Pluck", 6), _move("Growl", lower_atk_enemy=1), _move("Swords Dance", raise_atk_self=2)]},
    {"name": "Thwackey", "hp": 24, "atk": 6, "def": 4, "moves": [_move("Branch Poke", 6), _move("Slam", 7), _move("Screech", lower_def_enemy=2), _move("Growth", raise_atk_self=2)]},
    {"name": "Floragato", "hp": 23, "atk": 6, "def": 4, "moves": [_move("Leafage", 6), _move("Slash", 7), _move("Hone Claws", raise_atk_self=2), _move("Tail Whip", lower_def_enemy=1)]},
]
GATHEMON_THIRD = [
    {"name": "Leavanny", "hp": 34, "atk": 8, "def": 6, "moves": [_move("Leaf Blade", 9), _move("X-Scissor", 8), _move("Swords Dance", raise_atk_self=2), _move("Screech", lower_def_enemy=2)]},
    {"name": "Venusaur", "hp": 36, "atk": 7, "def": 8, "moves": [_move("Petal Blizzard", 9), _move("Seed Bomb", 8), _move("Growl", lower_atk_enemy=2), _move("Iron Defense", raise_def_self=2)]},
    {"name": "Serperior", "hp": 35, "atk": 8, "def": 7, "moves": [_move("Leaf Blade", 9), _move("Slam", 8), _move("Coil", raise_atk_self=1, raise_def_self=1), _move("Leer", lower_def_enemy=2)]},
    {"name": "Leafeon", "hp": 34, "atk": 9, "def": 7, "moves": [_move("Leaf Blade", 10), _move("Quick Attack", 8), _move("Swords Dance", raise_atk_self=2), _move("Tail Whip", lower_def_enemy=2)]},
    {"name": "Vileplume", "hp": 36, "atk": 7, "def": 8, "moves": [_move("Petal Dance", 10), _move("Sludge Bomb", 9), _move("Growth", raise_atk_self=2), _move("Sweet Scent", lower_def_enemy=2)]},
    {"name": "Victreebel", "hp": 34, "atk": 9, "def": 6, "moves": [_move("Leaf Blade", 9), _move("Power Whip", 10), _move("Swords Dance", raise_atk_self=2), _move("Screech", lower_def_enemy=2)]},
    {"name": "Meganium", "hp": 38, "atk": 6, "def": 9, "moves": [_move("Petal Blizzard", 9), _move("Body Slam", 8), _move("Reflect", raise_def_self=2), _move("Growl", lower_atk_enemy=2)]},
    {"name": "Sceptile", "hp": 33, "atk": 10, "def": 6, "moves": [_move("Leaf Blade", 11), _move("X-Scissor", 9), _move("Swords Dance", raise_atk_self=2), _move("Leer", lower_def_enemy=2)]},
    {"name": "Torterra", "hp": 38, "atk": 9, "def": 9, "moves": [_move("Wood Hammer", 11), _move("Earthquake", 10), _move("Withdraw", raise_def_self=2), _move("Growl", lower_atk_enemy=2)]},
    {"name": "Chesnaught", "hp": 37, "atk": 9, "def": 8, "moves": [_move("Seed Bomb", 9), _move("Hammer Arm", 10), _move("Bulk Up", raise_atk_self=1, raise_def_self=1), _move("Screech", lower_def_enemy=2)]},
    {"name": "Decidueye", "hp": 35, "atk": 10, "def": 6, "moves": [_move("Leaf Blade", 11), _move("Spirit Shackle", 9), _move("Swords Dance", raise_atk_self=2), _move("Feather Dance", lower_atk_enemy=2)]},
    {"name": "Rillaboom", "hp": 36, "atk": 11, "def": 7, "moves": [_move("Drum Beating", 11), _move("Wood Hammer", 10), _move("Swords Dance", raise_atk_self=2), _move("Screech", lower_def_enemy=2)]},
    {"name": "Meowscarada", "hp": 34, "atk": 11, "def": 6, "moves": [_move("Flower Trick", 11), _move("Night Slash", 9), _move("Hone Claws", raise_atk_self=2), _move("Leer", lower_def_enemy=2)]},
]

# GathéMon type emojis (custom Discord); Pokéball for rewards/start embed
GATHEMON_TYPE_EMOJI = {
    "Normal": "<:NORMAL:1475497322665218149>",
    "Flying": "<:FLYING:1475496973288341720>",
    "Dark": "<:DARK:1475497015763800296>",
    "Fighting": "<:FIGHT:1475496879155318937>",
    "Bug": "<:BUG:1475496917814349824>",
    "Ghost": "<:GHOST:1475497105220042844>",
    "Grass": "<:GRASS:1475497139261014047>",
    "Ground": "<:GROUND:1475497183741743300>",
    "Poison": "<:POISON:1475497230394855475>",
}
GATHEMON_POKEBALL_EMOJI = "<:pokeball:1475494042195333220>"

# Pokemon name -> list of type keys (for display)
GATHEMON_POKEMON_TYPES = {
    "Sewaddle": ["Bug", "Grass"], "Bulbasaur": ["Grass", "Poison"], "Snivy": ["Grass"], "Eevee": ["Normal"],
    "Oddish": ["Grass", "Poison"], "Bellsprout": ["Grass", "Poison"], "Chikorita": ["Grass"], "Treecko": ["Grass"],
    "Turtwig": ["Grass"], "Chespin": ["Grass"], "Rowlet": ["Grass", "Flying"], "Grookey": ["Grass"], "Sprigatito": ["Grass"],
    "Ivysaur": ["Grass", "Poison"], "Swadloon": ["Bug", "Grass"], "Servine": ["Grass"], "Gloom": ["Grass", "Poison"],
    "Weepinbell": ["Grass", "Poison"], "Bayleef": ["Grass"], "Grovyle": ["Grass"], "Grotle": ["Grass"],
    "Quilladin": ["Grass"], "Dartrix": ["Grass", "Flying"], "Thwackey": ["Grass"], "Floragato": ["Grass"],
    "Leavanny": ["Bug", "Grass"], "Venusaur": ["Grass", "Poison"], "Serperior": ["Grass"], "Leafeon": ["Grass"],
    "Vileplume": ["Grass", "Poison"], "Victreebel": ["Grass", "Poison"], "Meganium": ["Grass"], "Sceptile": ["Grass"],
    "Torterra": ["Grass", "Ground"], "Chesnaught": ["Grass", "Fighting"], "Decidueye": ["Grass", "Ghost"],
    "Rillaboom": ["Grass"], "Meowscarada": ["Grass", "Dark"],
}

# Per-Pokemon level options (pick one of the 3 at random)
GATHEMON_POKEMON_LEVELS = {
    "Sewaddle": [15, 16, 17], "Bulbasaur": [11, 12, 13], "Snivy": [12, 13, 14], "Eevee": [15, 16, 17],
    "Oddish": [16, 17, 18], "Bellsprout": [16, 17, 18], "Chikorita": [11, 12, 13], "Treecko": [11, 12, 13],
    "Turtwig": [13, 14, 15], "Chespin": [11, 12, 13], "Rowlet": [12, 13, 14], "Grookey": [11, 12, 13],
    "Sprigatito": [11, 12, 13],
    "Ivysaur": [27, 28, 29], "Swadloon": [21, 22, 23], "Servine": [31, 32, 33], "Gloom": [27, 28, 29],
    "Weepinbell": [27, 28, 29], "Bayleef": [27, 28, 29], "Grovyle": [31, 32, 33], "Grotle": [27, 28, 29],
    "Quilladin": [31, 32, 33], "Dartrix": [29, 30, 31], "Thwackey": [30, 31, 32], "Floragato": [31, 32, 33],
    "Leavanny": [30, 31, 32], "Venusaur": [33, 34, 35], "Serperior": [37, 38, 39], "Leafeon": [31, 32, 33],
    "Vileplume": [33, 34, 35], "Victreebel": [33, 34, 35], "Meganium": [33, 34, 35], "Sceptile": [37, 38, 39],
    "Torterra": [33, 34, 35], "Chesnaught": [37, 38, 39], "Decidueye": [35, 36, 37], "Rillaboom": [36, 37, 38],
    "Meowscarada": [37, 38, 39],
}
GATHEMON_GENDER_MALE = "\u2642"   # ♂
GATHEMON_GENDER_FEMALE = "\u2640" # ♀

def _gathemon_tier_for_plants(plants: int) -> int:
    """Return 1 (first), 2 (second), or 3 (third) evolution tier for bet amount 1-10."""
    if plants <= 3:
        return 1
    if plants <= 7:
        return 2
    return 3

def _gathemon_random_pokemon(plants: int) -> dict:
    """Return a random Pokémon dict (copy with mutable state) for the given plant bet tier."""
    tier = _gathemon_tier_for_plants(plants)
    if tier == 1:
        pool = GATHEMON_FIRST
    elif tier == 2:
        pool = GATHEMON_SECOND
    else:
        pool = GATHEMON_THIRD
    base = random.choice(pool)
    level_opts = GATHEMON_POKEMON_LEVELS.get(base["name"], [15, 16, 17])
    level = random.choice(level_opts)
    gender = random.choice([GATHEMON_GENDER_MALE, GATHEMON_GENDER_FEMALE])
    types = GATHEMON_POKEMON_TYPES.get(base["name"], ["Grass"])
    return {
        "name": base["name"],
        "hp": base["hp"],
        "max_hp": base["hp"],
        "atk": base["atk"],
        "def": base["def"],
        "moves": base["moves"],
        "modifiers": [],  # {"stat": "atk"|"def", "amount": int, "turns_left": int}
        "types": types,
        "level": level,
        "gender": gender,
    }


def _gathemon_pokemon_display_name(p: dict) -> str:
    """Format: Name TypeEmojis Gender Lv.XX (types before gender, gender before level)."""
    types_str = " ".join(GATHEMON_TYPE_EMOJI.get(t, "") for t in p.get("types", []))
    level = p.get("level", 50)
    gender = p.get("gender", GATHEMON_GENDER_MALE)
    parts = [p["name"]]
    if types_str:
        parts.append(types_str)
    parts.append(gender)
    parts.append(f"Lv.{level}")
    return " ".join(parts)

def _gathemon_effective_stat(base: int, modifiers: list, stat: str) -> int:
    total = sum(m["amount"] for m in modifiers if m["stat"] == stat)
    return max(1, base + total)  # stat floor 1 for display/calc

def _gathemon_damage(power: int, atk_eff: int, def_eff: int) -> int:
    d = power + atk_eff - def_eff
    return max(1, d)

def _gathemon_tick_modifiers(modifiers: list) -> list:
    """Decrement turns_left and remove expired. Returns new list."""
    out = []
    for m in modifiers:
        m["turns_left"] -= 1
        if m["turns_left"] > 0:
            out.append(m)
    return out

def _gathemon_apply_move(move: dict, attacker: dict, defender: dict) -> tuple[int, str]:
    """Apply move: stat changes, then damage if power. Returns (damage_dealt, log_line)."""
    log_parts = []
    # Stat changes (attacker's self buffs, defender's debuffs)
    for stat_key, amount, target in [
        ("atk", move.get("raise_atk_self", 0), attacker),
        ("def", move.get("raise_def_self", 0), attacker),
        ("atk", -move.get("lower_atk_enemy", 0), defender),
        ("def", -move.get("lower_def_enemy", 0), defender),
    ]:
        if amount == 0:
            continue
        current = sum(m["amount"] for m in target["modifiers"] if m["stat"] == stat_key)
        new_total = current + amount
        new_total = max(-2, min(2, new_total))
        add = new_total - current
        if add != 0:
            target["modifiers"].append({"stat": stat_key, "amount": add, "turns_left": 3})
            log_parts.append(f"{target['name']} {stat_key.upper()}: {'+' if add > 0 else ''}{add} (3 turns)")
    # Damage
    damage = 0
    if move.get("power") is not None:
        atk_eff = _gathemon_effective_stat(attacker["atk"], attacker["modifiers"], "atk")
        def_eff = _gathemon_effective_stat(defender["def"], defender["modifiers"], "def")
        damage = _gathemon_damage(move["power"], atk_eff, def_eff)
        defender["hp"] = max(0, defender["hp"] - damage)
        log_parts.append(f"{move['name']} hit for {damage} damage.")
    raw = " ".join(log_parts) if log_parts else move["name"] + "."
    return damage, f"***{raw}***"


def _gathemon_pick_random_move(moves: list, last_used_move_index: int | None = None) -> int:
    """Pick a move index: 70% attack (power), 30% stat (boost/lower ATK/DEF). Cannot pick the same stat move twice in a row."""
    attack_indices = [i for i, m in enumerate(moves) if m.get("power") is not None]
    # Exclude last-used move from stat pool if it was a stat move (so same ATK/DEF move can't repeat)
    stat_indices = [i for i, m in enumerate(moves) if m.get("power") is None]
    if last_used_move_index is not None and 0 <= last_used_move_index < len(moves) and moves[last_used_move_index].get("power") is None:
        stat_indices = [i for i in stat_indices if i != last_used_move_index]
    use_attack = random.random() < 0.70
    if use_attack and attack_indices:
        return random.choice(attack_indices)
    if not use_attack and stat_indices:
        return random.choice(stat_indices)
    # Fallback: use whichever pool has moves
    if attack_indices:
        return random.choice(attack_indices)
    if stat_indices:
        return random.choice(stat_indices)
    return 0


active_gathemon_challenges = {}  # challenge_id -> { challenger_id, opponent_id, bet }
active_gathemon_battles = {}     # game_id -> GathemonBattle
user_active_gathemon = {}        # user_id -> game_id


class GathemonBattle:
    def __init__(self, game_id: str, player1_id: int, player1_name: str, player2_id: int, player2_name: str, bet: int, channel_id: int):
        self.game_id = game_id
        self.player1_id = player1_id
        self.player1_name = player1_name
        self.player2_id = player2_id
        self.player2_name = player2_name
        self.bet = bet
        self.channel_id = channel_id
        self.pokemon1 = _gathemon_random_pokemon(bet)
        self.pokemon2 = _gathemon_random_pokemon(bet)
        self.current_turn_id = player1_id  # player1 goes first
        self.message = None  # single in-channel battle message
        self.last_log = "***Battle started!***"
        self.last_mover_id = None  # user id who made the move that produced last_log
        self.last_move_index_p1 = None  # last move index used by player1 (so same stat move can't repeat)
        self.last_move_index_p2 = None  # last move index used by player2

    def get_pokemon(self, is_player1: bool):
        return self.pokemon1 if is_player1 else self.pokemon2

    def get_opponent_pokemon(self, is_player1: bool):
        return self.pokemon2 if is_player1 else self.pokemon1

    def is_player1_turn(self):
        return self.current_turn_id == self.player1_id

    def tick_both_modifiers(self):
        self.pokemon1["modifiers"] = _gathemon_tick_modifiers(self.pokemon1["modifiers"])
        self.pokemon2["modifiers"] = _gathemon_tick_modifiers(self.pokemon2["modifiers"])


def _gathemon_battle_embed(battle: GathemonBattle, for_player1: bool) -> discord.Embed:
    """Build the battle embed for one player (own Pokémon + enemy HP/name, buffs)."""
    mine = battle.get_pokemon(for_player1)
    other = battle.get_opponent_pokemon(for_player1)
    atk_eff = _gathemon_effective_stat(mine["atk"], mine["modifiers"], "atk")
    def_eff = _gathemon_effective_stat(mine["def"], mine["modifiers"], "def")
    title = f"{GATHEMON_POKEBALL_EMOJI} {mine['name']} vs. {other['name']} {GATHEMON_POKEBALL_EMOJI}"
    desc = f"**Your {_gathemon_pokemon_display_name(mine)}**\nHP: **{mine['hp']}/{mine['max_hp']}** | ATK: **{atk_eff}** | DEF: **{def_eff}**"
    if mine["modifiers"]:
        buf = ", ".join(f"{m['stat']} {m['amount']:+d} ({m['turns_left']}t)" for m in mine["modifiers"])
        desc += f"\n*Buffs/Debuffs: {buf}*"
    desc += f"\n\n**Enemy {_gathemon_pokemon_display_name(other)}** — HP: **{other['hp']}/{other['max_hp']}**"
    if other["modifiers"]:
        buf = ", ".join(f"{m['stat']} {m['amount']:+d} ({m['turns_left']}t)" for m in other["modifiers"])
        desc += f"\n*Buffs/Debuffs: {buf}*"
    desc += f"\n\n{battle.last_log}"
    turn_id = battle.current_turn_id
    is_my_turn = (turn_id == battle.player1_id) == for_player1
    if is_my_turn:
        desc += "\n\n**▶ Your turn!** Press the button to use a random move (70% attack, 30% buff/debuff). (2 min)"
    else:
        desc += "\n\n*Waiting for opponent...*"
    embed = discord.Embed(title=title, description=desc, color=discord.Color.green())
    embed.add_field(name=f"{GATHEMON_POKEBALL_EMOJI} Bet", value=f"{battle.bet} plants", inline=True)
    return embed


def _gathemon_highest_area_mult_from_data(full_data: dict) -> float:
    """Return the highest unlocked area multiplier from pre-fetched full_data."""
    unlocked = full_data.get("unlocked_areas", {})
    best = 1.0
    for area_key, area_config in GATHERING_AREAS.items():
        if area_key == "forest" or unlocked.get(area_key, False):
            best = max(best, area_config.get("multiplier", 1.0))
    return best


def _gathemon_winner_gathers_sync(user_id: int, num_plants: int) -> tuple[list, float]:
    """Compute N gathers using winner's highest unlocked area and their boosts; one batch DB update. Returns (results, total_value)."""
    if num_plants <= 0:
        return [], 0.0
    full_data = get_user_gather_full_data(user_id)
    active_events = get_active_events_cached()
    area_mult = _gathemon_highest_area_mult_from_data(full_data)

    def do_one_gather(_: int) -> dict:
        return _perform_gather_for_user_sync(
            user_id,
            apply_cooldown=False,
            user_data=full_data,
            active_events=active_events,
            apply_orchard_fertilizer=False,
            area_multiplier=area_mult,
            full_data=full_data,
            increment_command_count=False,
            apply_update=False,
        )

    with ThreadPoolExecutor(max_workers=min(num_plants, 32)) as executor:
        raw_results = list(executor.map(do_one_gather, range(num_plants)))

    batch_payload = [
        {"name": r["name"], "value": r["value"], "ripeness": r["ripeness"], "category": r["category"]}
        for r in raw_results
    ]
    perform_batch_gather_update(user_id, batch_payload, apply_cooldown=False, increment_command_count=False)

    results = [{"name": r["name"], "value": r["value"]} for r in raw_results]
    total_value = sum(r["value"] for r in raw_results)
    return results, total_value


async def _gathemon_award_winner_gathers(winner_id: int, loser_id: int, num_plants: int, channel: discord.abc.Messageable):
    """Award plants instantly (run gathers in thread), then send a single reward embed. Uses winner's highest area + boosts."""
    if num_plants <= 0 or channel is None:
        return
    # Run all gathers first so plants, statistics, and money are awarded instantly
    results, total_value = await asyncio.to_thread(_gathemon_winner_gathers_sync, winner_id, num_plants)
    plant_emojis = [get_item_display_emoji(r["name"]) for r in results]
    emoji_display = " ".join(plant_emojis)
    header = f"<@{winner_id}> beat <@{loser_id}> in a **GathéMon** battle for **{num_plants}** plant{'s' if num_plants != 1 else ''}!\n\n"
    max_emoji_len = 4000 - len(header)
    if len(emoji_display) > max_emoji_len:
        emoji_display = emoji_display[: max_emoji_len - 5] + " …"
    reward_embed = discord.Embed(
        title=f"{GATHEMON_POKEBALL_EMOJI} YOU WON!",
        description=f"{header}{emoji_display}",
        color=discord.Color.gold(),
    )
    reward_embed.add_field(name="💰 Total Earned", value=f"**{format_money(total_value)}**", inline=True)
    try:
        await channel.send(f"<@{winner_id}>", embed=reward_embed)
    except Exception as e:
        print(f"Gathemon victory reward send failed: {e}")


def _gathemon_hp_bar(current: int, max_hp: int, segments: int = 10) -> str:
    """HP bar: 100-50% green 🟩, 49-25% yellow 🟨, 24-0% red 🟥. Empty = ⬛."""
    if max_hp <= 0:
        return "⬛" * segments
    pct = current / max_hp
    filled = max(0, min(segments, round((current / max_hp) * segments)))
    empty = segments - filled
    if pct > 0.49:
        square = "🟩"
    elif pct > 0.24:
        square = "🟨"
    else:
        square = "🟥"
    return f"{square * filled}{'⬛' * empty}"


def _gathemon_battle_embed_public(battle: GathemonBattle) -> discord.Embed:
    """Build the single in-channel battle embed (both Pokémon, HP bars, who did what, whose turn)."""
    p1 = battle.pokemon1
    p2 = battle.pokemon2
    atk1 = _gathemon_effective_stat(p1["atk"], p1["modifiers"], "atk")
    def1 = _gathemon_effective_stat(p1["def"], p1["modifiers"], "def")
    atk2 = _gathemon_effective_stat(p2["atk"], p2["modifiers"], "atk")
    def2 = _gathemon_effective_stat(p2["def"], p2["modifiers"], "def")
    bar1 = _gathemon_hp_bar(p1["hp"], p1["max_hp"])
    bar2 = _gathemon_hp_bar(p2["hp"], p2["max_hp"])
    # Capitalize ATK/DEF in modifier display
    def _mod_str(modifiers):
        return ", ".join(f"{m['stat'].upper()} {m['amount']:+d} ({m['turns_left']}t)" for m in modifiers)
    d1 = _gathemon_pokemon_display_name(p1)
    d2 = _gathemon_pokemon_display_name(p2)
    title = f"{GATHEMON_POKEBALL_EMOJI} {p1['name']} vs. {p2['name']} {GATHEMON_POKEBALL_EMOJI}"
    desc = f"**<@{battle.player1_id}> — {d1}**\nATK: **{atk1}** | DEF: **{def1}**\n{bar1} **{p1['hp']}/{p1['max_hp']}**"
    if p1["modifiers"]:
        desc += f"\n*{_mod_str(p1['modifiers'])}*"
    if battle.last_mover_id == battle.player1_id:
        desc += f"\n**Your move:** {battle.last_log}"
    elif battle.last_mover_id == battle.player2_id:
        desc += f"\n**Opponent's move:** {battle.last_log}"
    battle_over = p1["hp"] <= 0 or p2["hp"] <= 0
    if battle_over:
        # Winner (top) section: foe fainted + Exp.; loser (bottom) section: your Pokemon fainted.
        if p1["hp"] > 0:  # p1 won
            desc += f"\n**The foe's {p2['name']} fainted!**\n**{p1['name']} gained {getattr(battle, 'exp_gained', 0)} Exp. Points!**"
        else:  # p1 lost
            desc += f"\n**Your {p1['name']} fainted!**"
    desc += f"\n\n**<@{battle.player2_id}> — {d2}**\nATK: **{atk2}** | DEF: **{def2}**\n{bar2} **{p2['hp']}/{p2['max_hp']}**"
    if p2["modifiers"]:
        desc += f"\n*{_mod_str(p2['modifiers'])}*"
    if battle.last_mover_id == battle.player2_id:
        desc += f"\n**Your move:** {battle.last_log}"
    elif battle.last_mover_id == battle.player1_id:
        desc += f"\n**Opponent's move:** {battle.last_log}"
    if battle_over:
        if p2["hp"] > 0:  # p2 won
            desc += f"\n**The foe's {p1['name']} fainted!**\n**{p2['name']} gained {getattr(battle, 'exp_gained', 0)} Exp. Points!**"
        else:  # p2 lost
            desc += f"\n**Your {p2['name']} fainted!**"
        desc += "\n\n🏆 **Battle over!**"
    elif battle.current_turn_id == battle.player1_id:
        desc += f"\n\n**▶ It's <@{battle.player1_id}>'s turn!** Press the button to use a random move (2 min)."
    else:
        desc += f"\n\n**▶ It's <@{battle.player2_id}>'s turn!** Press the button to use a random move (2 min)."
    embed = discord.Embed(title=title, description=desc, color=discord.Color.green())
    return embed


class GathemonLobbyView(discord.ui.View):
    def __init__(self, challenge_id: str, challenger_id: int, opponent_id: int, bet: int, timeout=300):
        super().__init__(timeout=timeout)
        self.challenge_id = challenge_id
        self.challenger_id = challenger_id
        self.opponent_id = opponent_id
        self.bet = bet

    async def on_timeout(self):
        if self.challenge_id not in active_gathemon_challenges:
            return
        del active_gathemon_challenges[self.challenge_id]
        try:
            await self.message.edit(content="⏰ Challenge timed out.", view=None)
        except Exception:
            pass

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.green, emoji="✅")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if interaction.user.id != self.opponent_id:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ This challenge is not for you!", ephemeral=False)
            return
        if self.challenge_id not in active_gathemon_challenges:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Challenge no longer exists!", ephemeral=False)
            return
        chall = active_gathemon_challenges[self.challenge_id]
        if self.challenger_id in user_active_gathemon or self.opponent_id in user_active_gathemon:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ You or the challenger is already in a GathéMon battle!", ephemeral=False)
            return
        bet = self.bet
        if get_user_bloom_cycle_plants(self.challenger_id) < bet or get_user_bloom_cycle_plants(self.opponent_id) < bet:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ One of you doesn't have enough plants for this bet!", ephemeral=False)
            return
        if not deduct_user_bloom_cycle_plants(self.challenger_id, bet):
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Challenger doesn't have enough plants!", ephemeral=False)
            return
        if not deduct_user_bloom_cycle_plants(self.opponent_id, bet):
            add_user_bloom_cycle_plants(self.challenger_id, bet)
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ You don't have enough plants!", ephemeral=False)
            return
        del active_gathemon_challenges[self.challenge_id]
        game_id = str(uuid.uuid4())[:8]
        channel_id = interaction.channel_id
        member = interaction.guild.get_member(self.challenger_id) if interaction.guild else None
        challenger_name = member.name if member else (interaction.client.get_user(self.challenger_id).name if interaction.client.get_user(self.challenger_id) else "Challenger")
        battle = GathemonBattle(game_id, self.challenger_id, challenger_name, self.opponent_id, interaction.user.name, bet, channel_id)
        active_gathemon_battles[game_id] = battle
        user_active_gathemon[self.challenger_id] = game_id
        user_active_gathemon[self.opponent_id] = game_id

        view = GathemonBattleView(game_id, battle.current_turn_id)
        embed = _gathemon_battle_embed_public(battle)
        battle.message = await interaction.channel.send(embed=embed, view=view)
        try:
            await interaction.message.edit(content=f"{GATHEMON_POKEBALL_EMOJI} **Battle started!**", view=None)
        except Exception:
            pass

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.red, emoji="❌")
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if interaction.user.id != self.opponent_id:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ This challenge is not for you!", ephemeral=False)
            return
        if self.challenge_id in active_gathemon_challenges:
            del active_gathemon_challenges[self.challenge_id]
        await safe_interaction_response(interaction, interaction.response.edit_message, content="❌ Challenge declined.", view=None)


GATHEMON_TURN_TIMEOUT_SEC = 120  # 2 minutes per turn (like Mayflower)

class GathemonBattleView(discord.ui.View):
    def __init__(self, game_id: str, current_turn_id: int, timeout: int = GATHEMON_TURN_TIMEOUT_SEC):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.current_turn_id = current_turn_id  # whose turn this view is for; on timeout they forfeit
        self._add_move_buttons()

    def _add_move_buttons(self):
        if self.game_id not in active_gathemon_battles:
            return
        self.add_item(GathemonUseMoveButton(self.game_id))

    async def on_timeout(self):
        if self.game_id not in active_gathemon_battles:
            return
        battle = active_gathemon_battles[self.game_id]
        # Only forfeit if it's still this player's turn (same as Mayflower turn_sequence check)
        if battle.current_turn_id != self.current_turn_id:
            return
        forfeiter_id = self.current_turn_id
        winner_id = battle.player2_id if forfeiter_id == battle.player1_id else battle.player1_id
        for uid in (battle.player1_id, battle.player2_id):
            if uid in user_active_gathemon:
                del user_active_gathemon[uid]
        del active_gathemon_battles[self.game_id]
        try:
            if battle.message:
                await battle.message.edit(
                    content=f"⏰ <@{forfeiter_id}> ran out of time and forfeits! <@{winner_id}> wins!",
                    embed=None,
                    view=None,
                )
        except Exception:
            pass
        channel = bot.get_channel(battle.channel_id) if battle.channel_id else None
        if winner_id and channel:
            await _gathemon_award_winner_gathers(winner_id, forfeiter_id, battle.bet * 2, channel)
        else:
            add_user_bloom_cycle_plants(battle.player1_id, battle.bet)
            add_user_bloom_cycle_plants(battle.player2_id, battle.bet)


class GathemonUseMoveButton(discord.ui.Button):
    """Single button: on press, a move is chosen at random (70% attack, 30% buff/debuff) and used."""

    def __init__(self, game_id: str):
        super().__init__(style=discord.ButtonStyle.primary, label="Use move!", emoji="🎲", custom_id=f"gathemon_{game_id}_go")
        self.game_id = game_id

    async def callback(self, interaction: discord.Interaction):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
        except Exception:
            return
        if self.game_id not in active_gathemon_battles:
            try:
                await interaction.followup.send("❌ Battle no longer exists!", ephemeral=False)
            except Exception:
                pass
            return
        battle = active_gathemon_battles[self.game_id]
        is_p1 = (interaction.user.id == battle.player1_id)
        if interaction.user.id not in (battle.player1_id, battle.player2_id):
            try:
                await interaction.followup.send("❌ You're not in this battle!", ephemeral=False)
            except Exception:
                pass
            return
        if (battle.current_turn_id == battle.player1_id) != is_p1:
            try:
                await interaction.followup.send("❌ It's not your turn!", ephemeral=False)
            except Exception:
                pass
            return
        attacker = battle.get_pokemon(is_p1)
        defender = battle.get_opponent_pokemon(is_p1)
        if attacker["hp"] <= 0 or defender["hp"] <= 0:
            try:
                await interaction.followup.send("❌ Battle already ended!", ephemeral=False)
            except Exception:
                pass
            return
        last_used = battle.last_move_index_p1 if is_p1 else battle.last_move_index_p2
        move_index = _gathemon_pick_random_move(attacker["moves"], last_used_move_index=last_used)
        move = attacker["moves"][move_index]
        damage, log_line = _gathemon_apply_move(move, attacker, defender)
        battle.last_log = log_line
        battle.last_mover_id = battle.player1_id if is_p1 else battle.player2_id
        if is_p1:
            battle.last_move_index_p1 = move_index
        else:
            battle.last_move_index_p2 = move_index
        battle.tick_both_modifiers()
        winner_id = None
        num_plants_won = 0
        channel = None
        if defender["hp"] <= 0:
            winner_id = battle.player1_id if is_p1 else battle.player2_id
            loser_id = battle.player2_id if is_p1 else battle.player1_id
            num_plants_won = battle.bet * 2
            if has_shop_item(winner_id, "rare_candy"):
                num_plants_won += 2
            channel = interaction.channel
            battle.exp_gained = random.randint(23, 53)  # for embed: winner's Pokemon gained X Exp.
            for uid in (battle.player1_id, battle.player2_id):
                if uid in user_active_gathemon:
                    del user_active_gathemon[uid]
            del active_gathemon_battles[self.game_id]
            view = None
        else:
            battle.current_turn_id = battle.player2_id if battle.current_turn_id == battle.player1_id else battle.player1_id
            view = GathemonBattleView(self.game_id, battle.current_turn_id)
        embed = _gathemon_battle_embed_public(battle)
        try:
            if battle.message:
                await battle.message.edit(embed=embed, view=view)
        except Exception as e:
            print(f"Gathemon edit messages: {e}")
        if winner_id is not None and num_plants_won and channel:
            loser_id = battle.player2_id if winner_id == battle.player1_id else battle.player1_id
            await _gathemon_award_winner_gathers(winner_id, loser_id, num_plants_won, channel)


# --- MAYFLOWER (PVP Battleship-style game) ---
GATHERSHIP_GRID_SIZE = 5
SEA_EMOJI = "🌊"
SHIP_EMOJI = "🚢"
HIT_EMOJI = "🔥"
MISS_EMOJI = "❌"
CURSOR_EMOJI = "📍"

active_gathership_games = {}
user_active_gathership = {}  # user_id -> game_id
channel_gathership = {}  # channel_id -> game_id


class GathershipGame:
    def __init__(self, game_id: str, host_id: int, host_name: str, opponent_id: int, opponent_name: str, bet: float, num_ships: int, channel_id: int):
        self.game_id = game_id
        self.host_id = host_id
        self.host_name = host_name
        self.opponent_id = opponent_id
        self.opponent_name = opponent_name
        self.bet = bet
        self.num_ships = num_ships
        self.channel_id = channel_id
        self.phase = "lobby"  # lobby | setup | battle | ended
        self.host_ships = set()  # (r, c)
        self.opponent_ships = set()
        self.host_shot_at = set()  # (r, c) that opponent has fired at on host's board
        self.opponent_shot_at = set()  # (r, c) that host has fired at on opponent's board
        self.host_cursor = (2, 2)
        self.opponent_cursor = (2, 2)
        self.fire_cursor = (2, 2)
        self.current_turn_id = host_id  # host goes first
        self.turn_sequence = 0  # incremented when turn ends; prevents old turn views from forfeiting new player
        self.winner_id = None

    def get_ships(self, is_host: bool):
        return self.host_ships if is_host else self.opponent_ships

    def get_shot_at(self, is_host: bool):
        """Cells that have been fired at on this player's board (by the enemy)."""
        return self.host_shot_at if is_host else self.opponent_shot_at

    def get_cursor(self, is_host: bool):
        return self.host_cursor if is_host else self.opponent_cursor

    def set_cursor(self, is_host: bool, r: int, c: int):
        r = max(0, min(GATHERSHIP_GRID_SIZE - 1, r))
        c = max(0, min(GATHERSHIP_GRID_SIZE - 1, c))
        if is_host:
            self.host_cursor = (r, c)
        else:
            self.opponent_cursor = (r, c)

    def add_ship(self, is_host: bool, r: int, c: int) -> bool:
        ships = self.get_ships(is_host)
        if (r, c) in ships or len(ships) >= self.num_ships:
            return False
        ships.add((r, c))
        return True

    def is_setup_done(self, is_host: bool) -> bool:
        return len(self.get_ships(is_host)) >= self.num_ships

    def record_shot(self, at_host_board: bool, r: int, c: int) -> bool:
        """Record a shot. at_host_board=True means opponent fired at host. Returns True if hit a ship."""
        if at_host_board:
            self.host_shot_at.add((r, c))
            return (r, c) in self.host_ships
        else:
            self.opponent_shot_at.add((r, c))
            return (r, c) in self.opponent_ships

    def all_ships_sunk(self, is_host: bool) -> bool:
        ships = self.get_ships(is_host)
        shot_at = self.get_shot_at(is_host)
        return len(ships) > 0 and ships.issubset(shot_at)

    def get_current_turn_name(self) -> str:
        return self.host_name if self.current_turn_id == self.host_id else self.opponent_name


def _gathership_grid_display(ships: set, cursor: tuple, show_ships: bool, shot_at: set = None) -> str:
    """Build 5x5 grid string. show_ships=True for own board (setup), False for enemy (hit/miss only). shot_at = set of (r,c) fired at."""
    lines = []
    for r in range(GATHERSHIP_GRID_SIZE):
        row = []
        for c in range(GATHERSHIP_GRID_SIZE):
            pos = (r, c)
            if pos == cursor:
                row.append(CURSOR_EMOJI)
            elif show_ships:
                row.append(SHIP_EMOJI if pos in ships else SEA_EMOJI)
            else:
                if shot_at and pos in shot_at:
                    row.append(HIT_EMOJI if pos in ships else MISS_EMOJI)
                else:
                    row.append(SEA_EMOJI)
        lines.append("".join(row))
    return "\n".join(lines)


def _gathership_dual_board_display(
    my_ships: set, my_shot_at: set, enemy_ships: set, enemy_shot_at: set,
    fire_cursor: tuple, enemy_mention: str
) -> str:
    """Build side-by-side: Your Board (left), Enemy's Board (right). No cursor on your board."""
    your_lines = _gathership_grid_display(my_ships, (-1, -1), show_ships=True, shot_at=my_shot_at).split("\n")
    enemy_lines = _gathership_grid_display(enemy_ships, fire_cursor, show_ships=False, shot_at=enemy_shot_at).split("\n")
    spacer = "\t\t\t\t\t"  # a few tabs between the two boards for clearer separation
    header_left = "**Your Board**"
    header_right = f"**{enemy_mention}'s Board**"
    # Fixed-width left column so both boards align (padding accounts for spacer width)
    header = header_left + " " * (33 - len(header_left)) + header_right
    rows = [header]
    for i in range(len(your_lines)):
        rows.append(your_lines[i] + spacer + enemy_lines[i])
    return "\n".join(rows)


async def _gathership_refund_and_cleanup(game_id: str, channel=None):
    if game_id not in active_gathership_games:
        return
    game = active_gathership_games[game_id]
    bet = normalize_money(game.bet)
    for uid in (game.host_id, game.opponent_id):
        try:
            bal = get_user_balance(uid)
            new_bal = normalize_money(bal + bet)
            update_user_balance(uid, new_bal)
        except Exception as e:
            print(f"Gathership refund error for {uid}: {e}")
        if uid in user_active_gathership:
            del user_active_gathership[uid]
    for ch_id, gid in list(channel_gathership.items()):
        if gid == game_id:
            del channel_gathership[ch_id]
            break
    del active_gathership_games[game_id]
    if channel:
        try:
            await channel.send("❌ Mayflower game cancelled. Bets have been refunded.")
        except Exception:
            pass


async def end_gathership_game(channel, game_id: str, winner_id: int, loser_id: int):
    if game_id not in active_gathership_games:
        return
    game = active_gathership_games[game_id]
    total_pot = normalize_money(game.bet * 2)
    current = get_user_balance(winner_id)
    update_user_balance(winner_id, normalize_money(current + total_pot))
    for uid in (game.host_id, game.opponent_id):
        if uid in user_active_gathership:
            del user_active_gathership[uid]
    for ch_id, gid in list(channel_gathership.items()):
        if gid == game_id:
            del channel_gathership[ch_id]
            break
    del active_gathership_games[game_id]
    winner_mention = f"<@{winner_id}>"
    loser_mention = f"<@{loser_id}>"
    embed = discord.Embed(
        title="🏆 MAYFLOWER — GAME OVER 🏆",
        description=f"{winner_mention} sank all of {loser_mention}'s ships and wins **{format_money(total_pot)}**!",
        color=discord.Color.gold()
    )
    # embed.add_field(name="💰 Winner takes", value=format_money(total_pot), inline=True)
    await channel.send(embed=embed)


class GathershipLobbyView(discord.ui.View):
    def __init__(self, game_id: str, host_id: int, opponent_id: int, timeout=300):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.host_id = host_id
        self.opponent_id = opponent_id

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, emoji="🚢")
    async def join_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != self.opponent_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This challenge is not for you!", ephemeral=True)
                return
            if game.phase != "lobby":
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game already started!", ephemeral=True)
                return
            if interaction.user.id in user_active_gathership:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You're already in a Mayflower game!", ephemeral=True)
                return
            bal = get_user_balance(interaction.user.id)
            if not can_afford_rounded(normalize_money(bal), game.bet):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You don't have enough balance to join!", ephemeral=True)
                return
            new_bal = normalize_money(bal - game.bet)
            update_user_balance(interaction.user.id, new_bal)
            user_active_gathership[interaction.user.id] = self.game_id
            embed = interaction.message.embeds[0]
            host_mention = f"<@{game.host_id}>"
            opponent_mention = f"<@{game.opponent_id}>"
            embed.description = f"{host_mention} is challenging {opponent_mention} to **MAYFLOWER**!\n\n✅ {opponent_mention} has joined! Host can start."
            embed.set_field_at(0, name="💰 Bet", value=format_money(game.bet), inline=True)
            # Reset 5-minute timeout when opponent joins (backend only; no UI mention)
            fresh_view = GathershipLobbyView(self.game_id, self.host_id, self.opponent_id, timeout=300)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=fresh_view)
        except Exception as e:
            print(f"Gathership join_game: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    @discord.ui.button(label="Start", style=discord.ButtonStyle.blurple, emoji="❗")
    async def start_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if interaction.user.id != self.host_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Only the host can start!", ephemeral=True)
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if game.phase != "lobby":
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game already started!", ephemeral=True)
                return
            if self.opponent_id not in user_active_gathership or user_active_gathership.get(self.opponent_id) != self.game_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Wait for your opponent to join first!", ephemeral=True)
                return
            game.phase = "setup"
            host_mention = f"<@{game.host_id}>"
            opponent_mention = f"<@{game.opponent_id}>"
            await safe_interaction_response(interaction, interaction.response.edit_message, content=f"{host_mention} {opponent_mention} ⚓ **Place your ships!**", view=GathershipOpenSetupView(self.game_id, timeout=300))
            channel = bot.get_channel(game.channel_id)
            if channel:
                await channel.send(f"{host_mention} {opponent_mention} ⚓ **Ship Placement** — Click the button above to open your grid and place your ships!")
        except Exception as e:
            print(f"Gathership start_game: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.red, emoji="❌")
    async def cancel_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if interaction.user.id != self.host_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Only the host can cancel!", ephemeral=True)
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if game.phase != "lobby":
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Cannot cancel after game has started!", ephemeral=True)
                return
            channel = bot.get_channel(game.channel_id)
            await _gathership_refund_and_cleanup(self.game_id, channel)
            await safe_interaction_response(interaction, interaction.response.edit_message, content="❌ Mayflower challenge cancelled. Bets refunded.", embed=None, view=None)
        except Exception as e:
            print(f"Gathership cancel: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    async def on_timeout(self):
        if self.game_id not in active_gathership_games:
            return
        game = active_gathership_games[self.game_id]
        if game.phase != "lobby":
            return
        channel = bot.get_channel(game.channel_id)
        await _gathership_refund_and_cleanup(self.game_id, channel)
        try:
            msg = self.message
            if msg:
                await msg.edit(content="⏰ Mayflower challenge timed out. Bets have been refunded.", embed=None, view=None)
        except Exception:
            pass


class GathershipOpenSetupView(discord.ui.View):
    """Single button: open your ephemeral ship placement."""
    def __init__(self, game_id: str, timeout=300):
        super().__init__(timeout=timeout)
        self.game_id = game_id

    @discord.ui.button(label="Place Ships", style=discord.ButtonStyle.green, emoji="🚢")
    async def open_setup(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if game.phase != "setup":
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Setup phase is over!", ephemeral=True)
                return
            if interaction.user.id not in (game.host_id, game.opponent_id):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You're not in this game!", ephemeral=True)
                return
            is_host = interaction.user.id == game.host_id
            ships = game.get_ships(is_host)
            cursor = game.get_cursor(is_host)
            grid = _gathership_grid_display(ships, cursor, show_ships=True)
            embed = discord.Embed(
                title="⚓ Your fleet",
                description=f"Place **{game.num_ships}** ship(s). Use arrows to move, then click **Place Ship**!\n\n{grid}",
                color=discord.Color.blue()
            )
            embed.add_field(name="Ships placed", value=f"{len(ships)}/{game.num_ships}", inline=True)
            view = GathershipSetupView(self.game_id, is_host, timeout=300)
            await safe_interaction_response(interaction, interaction.response.send_message, embed=embed, view=view, ephemeral=True)
        except Exception as e:
            print(f"Gathership open_setup: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    async def on_timeout(self):
        if self.game_id not in active_gathership_games:
            return
        game = active_gathership_games[self.game_id]
        if game.phase != "setup":
            return
        channel = bot.get_channel(game.channel_id)
        await _gathership_refund_and_cleanup(self.game_id, channel)
        try:
            if self.message:
                await self.message.edit(content="⏰ Ship placement timed out. Bets refunded.", view=None)
        except Exception:
            pass


class GathershipSetupView(discord.ui.View):
    def __init__(self, game_id: str, is_host: bool, timeout=300):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.is_host = is_host

    def _build_embed(self, game: GathershipGame) -> discord.Embed:
        ships = game.get_ships(self.is_host)
        cursor = game.get_cursor(self.is_host)
        grid = _gathership_grid_display(ships, cursor, show_ships=True)
        title = "⚓ Your fleet"
        desc = f"Place **{game.num_ships}** ship(s). Use arrows to move, then click **Place Ship**!\n\n{grid}"
        embed = discord.Embed(title=title, description=desc, color=discord.Color.blue())
        embed.add_field(name="Ships placed", value=f"{len(ships)}/{game.num_ships}", inline=True)
        return embed

    @discord.ui.button(label="⬅️", style=discord.ButtonStyle.secondary, row=0)
    async def left(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 0, -1)

    @discord.ui.button(label="➡️", style=discord.ButtonStyle.secondary, row=0)
    async def right(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 0, 1)

    @discord.ui.button(label="⬆️", style=discord.ButtonStyle.secondary, row=1)
    async def up(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, -1, 0)

    @discord.ui.button(label="⬇️", style=discord.ButtonStyle.secondary, row=1)
    async def down(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 1, 0)

    async def _move(self, interaction: discord.Interaction, dr: int, dc: int):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != (game.host_id if self.is_host else game.opponent_id):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your grid!", ephemeral=True)
                return
            r, c = game.get_cursor(self.is_host)
            game.set_cursor(self.is_host, r + dr, c + dc)
            embed = self._build_embed(game)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Gathership setup move: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    @discord.ui.button(label="Place Ship", style=discord.ButtonStyle.green, row=2)
    async def place_ship(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != (game.host_id if self.is_host else game.opponent_id):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your grid!", ephemeral=True)
                return
            r, c = game.get_cursor(self.is_host)
            if not game.add_ship(self.is_host, r, c):
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Can't place here or you've placed all ships!", ephemeral=True)
                return
            embed = self._build_embed(game)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
            if game.is_setup_done(True) and game.is_setup_done(False):
                # Prefer game channel; fallback to interaction.channel (get_channel can be None if not cached)
                channel = bot.get_channel(game.channel_id) or interaction.channel
                game.phase = "battle"
                await channel.send("🔥 **All ships placed! MAYFLOWER STARTS!**")
                await _gathership_send_turn_message(channel, self.game_id)
        except Exception as e:
            print(f"Gathership place_ship: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)


async def _gathership_send_turn_message(channel, game_id: str):
    if game_id not in active_gathership_games:
        return
    game = active_gathership_games[game_id]
    # Reset fire cursor to center (2,2) so each player starts their turn from a consistent position
    game.fire_cursor = (2, 2)
    current_name = game.get_current_turn_name()
    view = GathershipTurnView(game_id, game.turn_sequence, timeout=120)
    # Always post in the game channel (mayflower-1 / mayflower-2); never DM
    await channel.send(f"🎯 **{current_name}**'s turn! Click **Take Shot** below (2 min).", view=view)


class GathershipTurnView(discord.ui.View):
    def __init__(self, game_id: str, turn_sequence: int, timeout=120):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.turn_sequence = turn_sequence  # only forfeit if game still on this turn (prevents old views from forfeiting new player)

    @discord.ui.button(label="Take Shot", style=discord.ButtonStyle.danger, emoji="🔥")
    async def take_shot(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != game.current_turn_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
                return
            # Attacker sees both boards: their own (left) and enemy's (right)
            at_host_board = game.current_turn_id == game.opponent_id
            my_ships = game.host_ships if game.current_turn_id == game.host_id else game.opponent_ships
            my_shot_at = game.host_shot_at if game.current_turn_id == game.host_id else game.opponent_shot_at
            enemy_ships = game.host_ships if at_host_board else game.opponent_ships
            enemy_shot_at = game.host_shot_at if at_host_board else game.opponent_shot_at
            enemy_id = game.opponent_id if game.current_turn_id == game.host_id else game.host_id
            enemy_mention = f"<@{enemy_id}>"
            dual = _gathership_dual_board_display(my_ships, my_shot_at, enemy_ships, enemy_shot_at, game.fire_cursor, enemy_mention)
            embed = discord.Embed(
                title="🔥 Fire at Enemy Fleet",
                description=f"Move cursor on the right board, then press **Fire!**\n\n{dual}",
                color=discord.Color.dark_red()
            )
            view = GathershipFireView(self.game_id, timeout=120)
            await safe_interaction_response(interaction, interaction.response.send_message, embed=embed, view=view, ephemeral=True)
        except Exception as e:
            print(f"Gathership take_shot: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    async def on_timeout(self):
        if self.game_id not in active_gathership_games:
            return
        game = active_gathership_games[self.game_id]
        # Only forfeit if this view is for the CURRENT turn; otherwise the turn already ended (old view timing out)
        if game.turn_sequence != self.turn_sequence:
            return
        channel = bot.get_channel(game.channel_id)
        if not channel:
            return
        loser_id = game.current_turn_id
        winner_id = game.host_id if loser_id == game.opponent_id else game.opponent_id
        await channel.send(f"⏰ **{game.get_current_turn_name()}** ran out of time and forfeits!")
        await end_gathership_game(channel, self.game_id, winner_id, loser_id)


class GathershipFireView(discord.ui.View):
    def __init__(self, game_id: str, timeout=120):
        super().__init__(timeout=timeout)
        self.game_id = game_id

    def _build_embed(self, game: GathershipGame) -> discord.Embed:
        at_host_board = game.current_turn_id == game.opponent_id
        my_ships = game.host_ships if game.current_turn_id == game.host_id else game.opponent_ships
        my_shot_at = game.host_shot_at if game.current_turn_id == game.host_id else game.opponent_shot_at
        enemy_ships = game.host_ships if at_host_board else game.opponent_ships
        enemy_shot_at = game.host_shot_at if at_host_board else game.opponent_shot_at
        enemy_id = game.opponent_id if game.current_turn_id == game.host_id else game.host_id
        enemy_mention = f"<@{enemy_id}>"
        dual = _gathership_dual_board_display(my_ships, my_shot_at, enemy_ships, enemy_shot_at, game.fire_cursor, enemy_mention)
        return discord.Embed(
            title="🔥 Fire at Enemy Fleet",
            description=f"Move cursor on the right board, then press **Fire!**\n\n{dual}",
            color=discord.Color.dark_red()
        )

    @discord.ui.button(label="⬅️", style=discord.ButtonStyle.secondary, row=0)
    async def left(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 0, -1)

    @discord.ui.button(label="➡️", style=discord.ButtonStyle.secondary, row=0)
    async def right(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 0, 1)

    @discord.ui.button(label="⬆️", style=discord.ButtonStyle.secondary, row=1)
    async def up(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, -1, 0)

    @discord.ui.button(label="⬇️", style=discord.ButtonStyle.secondary, row=1)
    async def down(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._move(interaction, 1, 0)

    async def _move(self, interaction: discord.Interaction, dr: int, dc: int):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != game.current_turn_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
                return
            r, c = game.fire_cursor
            game.fire_cursor = (max(0, min(GATHERSHIP_GRID_SIZE - 1, r + dr)), max(0, min(GATHERSHIP_GRID_SIZE - 1, c + dc)))
            embed = self._build_embed(game)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Gathership fire move: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)

    @discord.ui.button(label="Fire!", style=discord.ButtonStyle.danger, emoji="🔥", row=2)
    async def fire(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=True):
                return
            if self.game_id not in active_gathership_games:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Game no longer exists!", ephemeral=True)
                return
            game = active_gathership_games[self.game_id]
            if interaction.user.id != game.current_turn_id:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ It's not your turn!", ephemeral=True)
                return
            r, c = game.fire_cursor
            at_host_board = game.current_turn_id == game.opponent_id
            already_shot = (at_host_board and (r, c) in game.host_shot_at) or (not at_host_board and (r, c) in game.opponent_shot_at)
            if already_shot:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ You already shot here! Pick a different square.", ephemeral=True)
                return
            hit = game.record_shot(at_host_board, r, c)
            channel = bot.get_channel(game.channel_id)
            if not channel:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Channel not found.", ephemeral=True)
                return
            hit_text = "🔥 **Hit!**" if hit else "❌ **Miss!**"
            await channel.send(f"**{game.get_current_turn_name()}** fired at ({r+1},{c+1}). {hit_text}")
            loser_id = None
            if at_host_board and game.all_ships_sunk(True):
                loser_id = game.host_id
            elif not at_host_board and game.all_ships_sunk(False):
                loser_id = game.opponent_id
            if loser_id is not None:
                winner_id = game.host_id if loser_id == game.opponent_id else game.opponent_id
                await end_gathership_game(channel, self.game_id, winner_id, loser_id)
                return
            # Hit = same player shoots again (Battleship rules); miss = switch turn
            if not hit:
                game.current_turn_id = game.opponent_id if game.current_turn_id == game.host_id else game.host_id
                game.turn_sequence += 1  # so old turn view's on_timeout won't forfeit the new player
            await _gathership_send_turn_message(channel, self.game_id)
        except Exception as e:
            print(f"Gathership fire: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ Something went wrong.", ephemeral=True)


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

        # 10-second cooldown after losing a coinflip — reply via followup (we already deferred)
        last_loss_time = get_user_last_coinflip_loss_time(user_id)
        if last_loss_time > 0:
            now = time.time()
            elapsed = now - last_loss_time
            if elapsed < COINFLIP_LOSS_COOLDOWN:
                wait_secs = int(COINFLIP_LOSS_COOLDOWN - elapsed)
                await safe_interaction_response(interaction, interaction.response.send_message, f"⏳ Wait **{wait_secs}** second{'s' if wait_secs != 1 else ''} before flipping again.", ephemeral=True)
                return

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
            await safe_interaction_response(
                interaction,
                interaction.followup.send,
                f"You do not have enough balance to bet **{format_money(bet)}**, {interaction.user.name}.",
                ephemeral=False,
            )
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
            # They lose - reset streak to 0 and set 10-second cooldown
            set_user_coinflip_win_streak(user_id, 0)
            update_user_last_coinflip_loss_time(user_id, time.time())

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
            message = (
                f"You placed **{format_money(bet)}** on **{choice}**!\n"
                f"The coin landed on **{coin_result}**! You doubled your bet!!\n"
                f"Your new balance is **{format_money(new_balance)}**."
            )
        else:
            # They lose - bet was already deducted
            new_balance = new_balance_after_bet
            # Get the opposite choice for display (lowercase)
            opposite = "tails" if choice.lower() == "heads" else "heads"
            message = (
                f"You placed **{format_money(bet)}** on **{choice}**!\n"
                f"Ouch {interaction.user.name}, the coin landed on **{opposite}**. You lost **{format_money(bet)}**.\n"
                f"Your new balance is **{format_money(new_balance)}**."
            )
        
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


# Slots game implementation — 5x5 grid, bet bias, all-line wins
# Symbol odds tuned so matching 5 is much more likely (fewer symbols, even spread)
SLOT_EMOJI_ODDS = [
    ("💎", 5),   # rarest
    ("7️⃣", 11),
    ("⭐", 18),
    ("💰", 22),
    ("🍋", 22),
    ("🍒", 22),
]
SLOT_EMOJIS = [e for e, _ in SLOT_EMOJI_ODDS]


def _slot_emoji_from_roll(roll):
    """Return which emoji corresponds to roll in [0, 100). Uses SLOT_EMOJI_ODDS cumulative."""
    cumul = 0
    for emoji, pct in SLOT_EMOJI_ODDS:
        cumul += pct
        if roll < cumul:
            return emoji
    return SLOT_EMOJI_ODDS[-1][0]


def generate_slot_emoji():
    """Generate a random slot emoji from the configured odds (more even = easier to match lines)."""
    return _slot_emoji_from_roll(random.random() * 100)




# V-shape win lines (like real slots): top V = (0,0)->(2,2)->(0,4), bottom V = (4,0)->(2,2)->(4,4)
V_TOP_CELLS = [(0, 0), (1, 1), (2, 2), (1, 3), (0, 4)]
V_BOTTOM_CELLS = [(4, 0), (3, 1), (2, 2), (3, 3), (4, 4)]


def _fill_line_5x5(grid, line_type, line_idx, emoji):
    """Fill one line of a 5x5 grid with the same emoji. line_type: 'row','col','diag_main','diag_anti','v_top','v_bottom'."""
    if line_type == "row":
        for c in range(5):
            grid[line_idx][c] = emoji
    elif line_type == "col":
        for r in range(5):
            grid[r][line_idx] = emoji
    elif line_type == "diag_main":  # (0,0) to (4,4)
        for i in range(5):
            grid[i][i] = emoji
    elif line_type == "diag_anti":  # (0,4) to (4,0)
        for i in range(5):
            grid[i][4 - i] = emoji
    elif line_type == "v_top":
        for r, c in V_TOP_CELLS:
            grid[r][c] = emoji
    else:  # v_bottom
        for r, c in V_BOTTOM_CELLS:
            grid[r][c] = emoji


def generate_slot_grid(bet: float = 0, balance: float = 1, middle_only: bool = False):
    """Generate a 5x5 grid. Higher bet/balance ratio increases win chance (biased RNG)."""
    grid = [[generate_slot_emoji() for _ in range(5)] for _ in range(5)]
    if balance <= 0 or bet <= 0:
        return grid
    ratio = min(1.0, bet / balance)
    # 1% bet → 10% forced-win chance; 0.1% bet → 1%. When forced, always one line (lower-payout).
    WIN_BIAS_MULTIPLIER = 10  # 1% bet → 10% forced win; 0.1% bet → 1%
    win_bias = ratio * WIN_BIAS_MULTIPLIER
    if random.random() >= win_bias:
        return grid
    emoji = random.choice(SLOT_EMOJIS)
    if middle_only:
        _fill_line_5x5(grid, "row", 2, emoji)
        return grid
    # One line only when forced (lower-payout combo more often than multi-line)
    line_types = ["row", "col", "diag_main", "diag_anti", "v_top", "v_bottom"]
    line_type = random.choice(line_types)
    line_idx = random.randint(0, 4) if line_type in ("row", "col") else 0
    _fill_line_5x5(grid, line_type, line_idx, emoji)
    return grid


def format_slot_grid(grid, locked_columns=None, highlight_middle_row=False):
    """Format the 5x5 grid with top/bottom lines, row separators, and left/right borders (rectangular frame)."""
    if locked_columns is None:
        locked_columns = set()
    row_sep = "│─────────────│"
    lines = [row_sep]
    for r in range(5):
        row_str = " ".join(grid[r][c] for c in range(5))
        if highlight_middle_row and r == 2:
            row_str += " ⬅️"
        lines.append(f"│{row_str}│")
        if r < 4:
            lines.append(row_sep)
    lines.append(row_sep)
    return "\n".join(lines)


def _line_same_5x5(grid, line_type, line_idx):
    """Return True if the given line has all same emoji."""
    if line_type == "row":
        row = grid[line_idx]
        return len(set(row)) == 1
    if line_type == "col":
        col = [grid[r][line_idx] for r in range(5)]
        return len(set(col)) == 1
    if line_type == "diag_main":
        vals = [grid[i][i] for i in range(5)]
        return len(set(vals)) == 1
    if line_type == "diag_anti":
        vals = [grid[i][4 - i] for i in range(5)]
        return len(set(vals)) == 1
    if line_type == "v_top":
        vals = [grid[r][c] for r, c in V_TOP_CELLS]
        return len(set(vals)) == 1
    if line_type == "v_bottom":
        vals = [grid[r][c] for r, c in V_BOTTOM_CELLS]
        return len(set(vals)) == 1
    return False


def check_win_5x5(grid, middle_only: bool):
    """
    Check wins on 5x5 grid. middle_only: only middle row (index 2).
    Otherwise: all 5 rows, 5 cols, both diagonals (X), and both V shapes. Returns (won: bool, line_count: int).
    """
    if middle_only:
        if _line_same_5x5(grid, "row", 2):
            return True, 1
        return False, 0
    count = 0
    for r in range(5):
        if _line_same_5x5(grid, "row", r):
            count += 1
    for c in range(5):
        if _line_same_5x5(grid, "col", c):
            count += 1
    if _line_same_5x5(grid, "diag_main", 0):
        count += 1
    if _line_same_5x5(grid, "diag_anti", 0):
        count += 1
    if _line_same_5x5(grid, "v_top", 0):
        count += 1
    if _line_same_5x5(grid, "v_bottom", 0):
        count += 1
    return count > 0, count


class SlotsView(discord.ui.View):
    def __init__(self, user_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.bet_type = None  # "0.1%" or "1%"
        self.bet = 0.0
        self.grid = generate_slot_grid()
        self.spinning = False
        self.spun = False
        self.locked_columns = set()
        self.final_grid = None
        self.session_start_balance = normalize_money(get_user_balance(self.user_id))
        self.spins_this_session = 0

    def _middle_only(self):
        return self.bet_type == "0.1%"

    def _update_spin_button(self):
        # Third button is SPIN (0.1%, 1%, SPIN)
        if len(self.children) >= 3:
            self.children[2].disabled = self.spinning or self.bet_type is None

    def _session_status_line(self, current_balance: float) -> str:
        """Return UP/DOWN/EVEN/BROKE EVEN line for display beneath bet. current_balance should be normalized."""
        net = round(current_balance - self.session_start_balance, 2)
        if self.spins_this_session == 0:
            return "**EVEN:** $0"
        if net > 0:
            return f"**UP:** {format_money(net)}"
        if net < 0:
            return f"**DOWN:** {format_money(abs(net))}"
        return "**BROKE EVEN:** $0"

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ This is not your slots game!", ephemeral=True)
            return False
        return True

    def update_embed(self, is_spinning=False, status_text=""):
        title = "🎰 SLOTS - SPINNING... 🎰" if is_spinning else "🎰 SLOTS 🎰"
        balance = get_user_balance(self.user_id)
        balance = normalize_money(balance)
        desc_parts = [f"Balance: **{format_money(balance)}**"]
        # Show when Slot Token grants a free spin today (daily shop item)
        date_est = _get_date_est()
        if has_shop_item(self.user_id, "slot_token") and get_slot_token_free_spin_used_date_est(self.user_id) != date_est:
            desc_parts.append("🎫 **You have a free spin today!** (Slot Token)")
        if self.bet_type:
            pct = 0.001 if self.bet_type == "0.1%" else 0.01
            bet_amt = normalize_money(balance * pct)
            desc_parts.append(f"**BET:** **{self.bet_type}** ({format_money(bet_amt)})")
            desc_parts.append(self._session_status_line(balance))
            if self.bet_type == "0.1%":
                desc_parts.append("Win: middle row only.")
            else:
                desc_parts.append("Win: any row, column, diagonal (X), or V.")
        else:
            desc_parts.append(self._session_status_line(balance))
        desc_parts.append("")
        desc_parts.append(format_slot_grid(self.grid, self.locked_columns, highlight_middle_row=self._middle_only()))
        embed = discord.Embed(
            title=title,
            description="\n".join(desc_parts),
            color=discord.Color.gold() if not is_spinning else discord.Color.orange(),
        )
        if not self.spun and not is_spinning:
            embed.set_footer(text="Pick 0.1% or 1%, then click SPIN! You can respin anytime.")
        elif is_spinning:
            embed.set_footer(text=status_text or "🎰 Spinning... 🎰")
        else:
            embed.set_footer(text="Click SPIN to play again!")
        return embed

    async def animate_spin(self, interaction: discord.Interaction):
        for item in self.children:
            item.disabled = True
        balance = get_user_balance(self.user_id)
        balance = normalize_money(balance)
        date_est = _get_date_est()
        use_free_spin = has_shop_item(self.user_id, "slot_token") and get_slot_token_free_spin_used_date_est(self.user_id) != date_est
        if use_free_spin:
            self.bet = 0
            effective_bet = max(0.01, normalize_money(balance * 0.001))
        else:
            pct = 0.001 if self.bet_type == "0.1%" else 0.01
            self.bet = normalize_money(balance * pct)
            effective_bet = self.bet
            if self.bet <= 0 or not can_afford_rounded(balance, self.bet):
                embed = self.update_embed()
                embed.set_footer(text="❌ Not enough balance to spin.")
                self.spinning = False
                self._update_spin_button()
                for c in self.children:
                    c.disabled = False
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
                return
            new_balance = normalize_money(balance - self.bet)
            update_user_balance(self.user_id, new_balance)
        middle_only = self._middle_only()
        self.final_grid = generate_slot_grid(bet=effective_bet, balance=balance, middle_only=middle_only)
        embed = self.update_embed(is_spinning=True, status_text="🎰 All columns spinning... 🎰")
        await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        spin_frames = 4
        frame_interval = 0.07
        spin_start = time.monotonic()
        for frame in range(spin_frames):
            for r in range(5):
                for c in range(5):
                    self.grid[r][c] = generate_slot_emoji()
            embed = self.update_embed(is_spinning=True, status_text="🎰 Spinning... 🎰")
            await interaction.message.edit(embed=embed, view=self)
            if frame < spin_frames - 1:
                next_at = spin_start + (frame + 1) * frame_interval
                wait = next_at - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
        self.grid = [row[:] for row in self.final_grid]
        self.locked_columns = set(range(5))
        won, line_count = check_win_5x5(self.grid, middle_only)
        self.spinning = False
        self.spun = True
        if use_free_spin:
            set_slot_token_free_spin_used_date_est(self.user_id, date_est)
        payout_mult = 3
        winnings = effective_bet * payout_mult * line_count if won else 0
        if won:
            cap_mult = 15
            if line_count > cap_mult:
                winnings = effective_bet * payout_mult * cap_mult
            curr = get_user_balance(self.user_id)
            curr = normalize_money(curr)
            new_bal = normalize_money(curr + winnings)
            update_user_balance(self.user_id, new_bal)
        curr_balance = get_user_balance(self.user_id)
        curr_balance = normalize_money(curr_balance)
        self.spins_this_session += 1

        # Slots achievements: spin count + win streak (hidden 777 = 3 wins in a row)
        increment_user_slots_spin_count(self.user_id)
        achievements_unlocked = []
        if won:
            current_streak = get_user_slots_win_streak(self.user_id)
            new_streak = current_streak + 1
            set_user_slots_win_streak(self.user_id, new_streak)
            if new_streak >= 3 and unlock_hidden_achievement(self.user_id, "slots_three_in_a_row"):
                achievements_unlocked.append(("hidden", "slots_three_in_a_row"))
        else:
            set_user_slots_win_streak(self.user_id, 0)
        spin_count = get_user_slots_spin_count(self.user_id)
        new_slots_level = get_achievement_level_for_stat("slots", spin_count)
        current_slots_level = get_user_achievement_level(self.user_id, "slots")
        if new_slots_level > current_slots_level:
            set_user_achievement_level(self.user_id, "slots", new_slots_level)
            achievements_unlocked.append(("slots", new_slots_level))

        title = "🎰 SLOTS - RESULT 🎰"
        bet_text = "**FREE SPIN** (Slot Token)" if use_free_spin else f"**{format_money(self.bet)}** ({self.bet_type})"
        session_line = self._session_status_line(curr_balance)
        result_embed = discord.Embed(
            title=title,
            description=f"**BET:** {bet_text}\n{session_line}\n\n{format_slot_grid(self.grid, self.locked_columns, highlight_middle_row=middle_only)}",
            color=discord.Color.green() if won else discord.Color.red(),
        )
        if won:
            result_embed.add_field(
                name="🎉 YOU WON! 🎉",
                value=(
                    f"**{line_count}** line(s)! You won **{format_money(winnings)}**!\n"
                    f"Balance: **{format_money(curr_balance)}**."
                ),
                inline=False,
            )
        else:
            lost_text = (
                "Free spin used — no winnings this time."
                if use_free_spin
                else f"You lost **{format_money(self.bet)}**."
            )
            result_embed.add_field(
                name="❌ You Lost",
                value=f"{lost_text}\nBalance: **{format_money(curr_balance)}**.",
                inline=False,
            )
        result_embed.set_footer(text="Click SPIN to play again!")
        self.spun = False
        self.locked_columns = set()
        self.grid = generate_slot_grid()
        self._update_spin_button()
        for c in self.children:
            if getattr(c, "custom_id", "") != "slots_spin":
                c.disabled = False
        await interaction.message.edit(embed=result_embed, view=self)

        for item in achievements_unlocked:
            if item[0] == "hidden":
                await send_hidden_achievement_notification(interaction, item[1])
            else:
                await send_achievement_notification(interaction, item[0], item[1])
            await asyncio.sleep(0.5)

    @discord.ui.button(label="0.1%", style=discord.ButtonStyle.secondary, custom_id="slots_01pct", row=0)
    async def bet_01pct(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.spinning:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Spin in progress!", ephemeral=True)
                return
            self.bet_type = "0.1%"
            self._update_spin_button()
            embed = self.update_embed()
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in bet_01pct: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred.", ephemeral=True)

    @discord.ui.button(label="1%", style=discord.ButtonStyle.secondary, custom_id="slots_1pct", row=0)
    async def bet_1pct(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.spinning:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Spin in progress!", ephemeral=True)
                return
            self.bet_type = "1%"
            self._update_spin_button()
            embed = self.update_embed()
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in bet_1pct: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred.", ephemeral=True)

    @discord.ui.button(label="SPIN", style=discord.ButtonStyle.success, emoji="🎲", custom_id="slots_spin", row=0)
    async def spin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.spinning:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Spin in progress!", ephemeral=True)
                return
            if self.bet_type is None:
                await safe_interaction_response(interaction, interaction.response.send_message, "❌ Pick 0.1% or 1% first!", ephemeral=True)
                return
            self.spinning = True
            await self.animate_spin(interaction)
        except Exception as e:
            print(f"Error in spin_button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


VALID_SLOTS_CHANNELS = ("slots-1", "slots-2", "slots-3", "slots-4", "slots-5")
VALID_MAYFLOWER_CHANNELS = ("mayflower-1", "mayflower-2", "mayflower-3", "mayflower-4", "mayflower-5")
VALID_GATHEMON_CHANNELS = ("gathemon-1", "gathemon-2", "gathemon-3", "gathemon-4", "gathemon-5")


@bot.tree.command(name="slots", description="Play slots! 5x5 grid — bet 0.1% (middle row) or 1% (all lines).")
async def slots(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        channel_name = (interaction.channel.name or "").lower().replace(" ", "-")
        if channel_name not in VALID_SLOTS_CHANNELS:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ Slots can only be played in **#slots-1**, **#slots-2**, **#slots-3**, **#slots-4**, or **#slots-5**!", ephemeral=True)
            return
        user_id = interaction.user.id
        is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
        if is_roulette_cooldown:
            minutes_left = roulette_time_left // 60
            await safe_interaction_response(interaction, interaction.followup.send,
                f"Sorry, {interaction.user.name}, you're dead. You cannot play slots for {minutes_left} minute(s)", ephemeral=True)
            return
        view = SlotsView(user_id)
        view._update_spin_button()
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
            name="running /gather on V1.0.1"
        )
    )
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands globally")
        cmd_names = [c.name for c in synced]
        if "give" in cmd_names and "bot_pay" in cmd_names and "giveaway" in cmd_names:
            print("Commands verified: /give, /bot_pay, /giveaway are registered.")
        # In development, also sync to each guild so the dev server sees new/renamed commands immediately (global sync can take up to 1 hour to propagate)
        env = os.getenv("ENVIRONMENT", "").lower()
        if env == "development" and bot.guilds:
            for guild in bot.guilds:
                try:
                    await bot.tree.sync(guild=guild)
                    print(f"Synced commands to guild: {guild.name} ({guild.id})")
                except Exception as eg:
                    print(f"Guild sync failed for {guild.name}: {eg}")
    except Exception as e:
        print(f"Error syncing commands: {e}")
        import traceback
        traceback.print_exc()

    # If bot restarted during an event, send end embeds for any expired events so #events channel stays consistent
    try:
        await _send_end_embeds_for_expired_events()
        print("Event recovery at startup completed")
    except Exception as e:
        print(f"Error during event recovery at startup: {e}")
        import traceback
        traceback.print_exc()

    # Start the leaderboard update task
    bot.loop.create_task(update_all_leaderboards())
    print("Started automatic leaderboard updates")
    
    
    # Start the marketboard update task
    bot.loop.create_task(update_all_marketboards())
    print("Started automatic marketboard updates")
    
    # Start the market news task
    bot.loop.create_task(send_market_news_loop())
    print("Started automatic market news alerts")
    
    # Start the fernbase update task
    bot.loop.create_task(update_all_coinbase())
    print("Started automatic fernbase updates")
    
    # Start the gardener background task
    bot.loop.create_task(gardener_background_task())
    print("Started automatic gardener gathering")
    
    # Start the secret gardener background task
    bot.loop.create_task(secret_gardener_background_task())
    print("Started automatic secret gardener gathering")
    
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
    bot.loop.create_task(celestial_event_check())
    print("Started celestial event check (Solar Eclipse / Blood Moon)")
    bot.loop.create_task(irrigation_auto_water_task())
    print("Started irrigation auto-water task")
    
    # Cache invites for invite tracking (needs "Manage Server" permission)
    global _invite_cache
    _invite_cache = {}
    for guild in bot.guilds:
        try:
            invites = await guild.invites()
            _invite_cache[guild.id] = {inv.code: inv.uses for inv in invites}
        except discord.Forbidden:
            print(f"[Invites] No permission to read invites in {guild.name} — enable 'Manage Server' for invite tracking.")
        except Exception as e:
            print(f"[Invites] Error caching invites for {guild.name}: {e}")


@bot.event
async def on_member_join(member):
    global _invite_cache
    guild = member.guild
    inviter = None

    try:
        current_invites = await guild.invites()
    except discord.Forbidden:
        print(f"[Invites] Missing permission (Manage Server) in {guild.name} — cannot track who invited whom.")
        current_invites = []
    except Exception as e:
        print(f"[Invites] Error fetching invites for {guild.name}: {e}")
        current_invites = []

    cached = _invite_cache.get(guild.id, {})
    if not cached and current_invites:
        # First join after bot start or cache miss: store current state for next time
        _invite_cache[guild.id] = {inv.code: inv.uses for inv in current_invites}
    else:
        # Find the invite whose uses increased by exactly 1 (the one used for this join)
        for invite in current_invites:
            old_uses = cached.get(invite.code, 0)
            if invite.uses == old_uses + 1 and invite.inviter and invite.inviter.id != member.id:
                inviter = invite.inviter
                break
        _invite_cache[guild.id] = {inv.code: inv.uses for inv in current_invites}

    # Increment inviter's invite count (even if we can't send welcome message)
    if inviter:
        try:
            await asyncio.to_thread(increment_invite_joins_count_only, inviter.id, member.id)
        except Exception as e:
            print(f"[Invites] Error incrementing invite count for {inviter.id}: {e}")

    # Send welcome message in #welcome channel
    welcome_channel = discord.utils.get(guild.text_channels, name="welcome")
    if welcome_channel:
        try:
            if inviter:
                await welcome_channel.send(
                    f"\U0001f333 Welcome {member.mention} to **/GATHER**, thank you to {inviter.mention} for inviting them! \U0001f338"
                )
            else:
                await welcome_channel.send(f"\U0001f333 Welcome {member.mention} to **/GATHER**! \U0001f338")
        except discord.Forbidden:
            print(f"[Invites] No permission to send in #welcome in {guild.name}")
        except Exception as e:
            print(f"[Invites] Error sending welcome message in {guild.name}: {e}")
    
    # Assign gatherer role (PLANTER I for new users) and Bloom rank role (PINE I for new users)
    try:
        await assign_gatherer_role(member, guild)
    except Exception as e:
        print(f"Error assigning gatherer role to user {member.id}: {e}")
    try:
        await assign_bloom_rank_role(member, guild)
    except Exception as e:
        print(f"Error assigning bloom rank role to user {member.id}: {e}")


@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    """Sync server booster and premium tier when a member boosts or their roles change."""
    # Sync server booster flag when Nitro boosting starts/stops
    if before.premium_since != after.premium_since:
        try:
            set_user_server_booster(after.id, after.premium_since is not None)
        except Exception as e:
            print(f"[Server Booster] Error syncing booster status for {after.id}: {e}")

    # Sync premium tier cache whenever roles change so premium gardeners unlock from roles alone
    before_role_ids = {r.id for r in before.roles}
    after_role_ids = {r.id for r in after.roles}
    if before_role_ids != after_role_ids:
        try:
            await asyncio.to_thread(sync_premium_tier_from_member, after)
        except Exception as e:
            print(f"[Premium] Error syncing premium tier for {after.id}: {e}")


@bot.event
async def on_entitlement_create(entitlement):
    """
    When a user purchases a premium SKU, Discord may send this gateway event.
    If your discord.py version does not dispatch it, use /syncpremium after purchasing.
    """
    if not PREMIUM_SKU_TO_TIER:
        return
    data = entitlement if isinstance(entitlement, dict) else getattr(entitlement, "__dict__", {})
    if not data:
        return
    sku_id = data.get("sku_id")
    user_id = data.get("user_id")
    guild_id = data.get("guild_id")
    if not sku_id or not user_id or not guild_id:
        return
    user_id = int(user_id)
    guild_id = int(guild_id)
    guild = bot.get_guild(guild_id)
    if guild is None:
        return
    try:
        assigned = await assign_premium_role_for_entitlement(guild, user_id, str(sku_id))
        if assigned:
            print(f"[Premium] Assigned role for SKU {sku_id} to user {user_id} in guild {guild_id}")
    except Exception as e:
        print(f"[Premium] Error handling entitlement_create: {e}")


@bot.event
async def on_invite_create(invite):
    """Keep invite cache updated when new invites are created."""
    global _invite_cache
    guild_id = invite.guild.id
    if guild_id not in _invite_cache:
        _invite_cache[guild_id] = {}
    _invite_cache[guild_id][invite.code] = invite.uses


@bot.event
async def on_invite_delete(invite):
    """Keep invite cache updated when invites are deleted."""
    global _invite_cache
    guild_id = invite.guild.id
    if guild_id in _invite_cache and invite.code in _invite_cache[guild_id]:
        del _invite_cache[guild_id][invite.code]


# ----- Auto-log rare occurrences to #rares -----
# Only these plant ripenesses get posted to #rares (NOT Perfectly Ripe or Full Bloom — those are too common).
# Also posted to #rares: netherite+ imbue rolls (_post_rares_imbue), Nether Star claims (_post_rares_nether_star_claim), Black Shard claims (_post_rares_black_shard_claim).
_PLANT_RARES: dict[str, tuple[str, str]] = {
    "Legendary": ("LEGENDARY", "LEGENDARY"),
    "Netherite": ("NETHERITE", "NETHERITE"),
    "Luminite": ("LUMINITE", "LUMINITE"),
    "Celestial": ("CELESTIAL", "CELESTIAL"),
    "Mikellion": ("MIKELLION", "SECRET"),
}


def _plant_rare_label(ripeness: str, is_gmo: bool) -> tuple[str | None, str | None]:
    """Return (display_label, imbue_tier) for a plant rare (Legendary/Netherite/Luminite/Celestial/Mikellion), or (None, None) if not rare."""
    r = (ripeness or "").strip()
    entry = _PLANT_RARES.get(r)
    if not entry:
        return None, None
    suffix, tier = entry
    label = f"GMO {suffix}" if is_gmo else suffix
    return label, tier


def _rares_rarity_emoji(rarity: str) -> str:
    """Return the IMBUE emoji for #rares display. Uses RARITY_EMOJI; fallback to [RARITY] for new tiers so they still display."""
    if not (rarity or "").strip():
        return ""
    r = (rarity or "").strip().upper()
    return RARITY_EMOJI.get(r, f"[{r}]")


async def _post_to_rares_channel(guild: discord.Guild, content: str) -> None:
    """Send a message to the #rares channel if it exists. Swallows errors."""
    if not guild:
        return
    try:
        rares_ch = discord.utils.get(guild.text_channels, name=RARES_CHANNEL_NAME)
        if rares_ch:
            await rares_ch.send(content)
    except Exception as e:
        print(f"Error posting to #rares: {e}")


async def _post_rares_plant(guild: discord.Guild, user: discord.Member, source: str,
                            item_name: str, category: str, value: float,
                            ripeness: str, is_gmo: bool, area_tag: str) -> None:
    """Post a plant rare (Legendary/Netherite/Luminite/Celestial/Mikellion) to #rares with its IMBUE emoji."""
    label, tier = _plant_rare_label(ripeness, is_gmo)
    if not label:
        return
    rarity_emoji = _rares_rarity_emoji(tier)
    # Use get_item_display_emoji so Flowey/Raspberry (custom server emojis) display correctly in #rares
    item_emoji = get_item_display_emoji(item_name)
    # Avoid double emoji when item name already includes it (e.g. "Daffodil 🌼")
    item_display = f"{item_name} {item_emoji}" if item_emoji and item_emoji not in item_name else item_name
    # e.g. ":IMBUE_L: @User caught a LEGENDARY *Strawberry 🍓* in a GATHER worth $2,216,775.69! | **[FOREST]**"
    msg = (
        f"{rarity_emoji} {user.mention} caught a **{label}** *{item_display}* "
        f"in a **{source}** worth **{format_money(value)}**! | **{area_tag}**"
    )
    await _post_to_rares_channel(guild, msg)


IMBUE_RARES_RARITIES = {"NETHERITE", "LUMINITE", "CELESTIAL", "SECRET"}


async def _post_rares_imbue(guild: discord.Guild, user: discord.Member,
                            enchant: dict, tool_type: str) -> None:
    """Post a rolled netherite+ imbue to #rares."""
    if not guild or not enchant:
        return
    rarity = (enchant.get("rarity") or "").upper()
    if rarity not in IMBUE_RARES_RARITIES:
        return
    rarity_emoji = _rares_rarity_emoji(rarity)
    tool_text = "**hoe imbue**" if tool_type == "hoe" else "**tractor imbue**"
    name = enchant.get("name", "Unknown")
    # Leading rarity emoji, rarity in caps, tool type next, imbue name bold+italic, sparkle at end
    # e.g. ":IMBUE_CE: @User rolled a CELESTIAL hoe imbue: CULTISCYTHE OF THE LIGHTBRINGER! ✨"
    msg = f"{rarity_emoji} {user.mention} rolled a **{rarity}** {tool_text}: **_{name}_**! ✨"
    await _post_to_rares_channel(guild, msg)


async def _post_rares_nether_star_claim(guild: discord.Guild, user: discord.Member) -> None:
    """Post a Nether Star claim (from Wither defeat) to #rares."""
    if not guild or not user:
        return
    msg = f"{NETHER_STAR_EMOJI} {user.mention} claimed the **Nether Star** from the Wither!"
    await _post_to_rares_channel(guild, msg)


async def _post_rares_black_shard_claim(guild: discord.Guild, user: discord.Member) -> None:
    """Post a Black Shard claim (from Roaring Knight defeat) to #rares."""
    if not guild or not user:
        return
    msg = f"{BLACK_SHARD_EMOJI} {user.mention} claimed the **Black Shard** from The Roaring Knight!"
    await _post_to_rares_channel(guild, msg)


def _gather_critical_path(member, user_id: int, channel_name: str, area: dict) -> dict:
    """All DB work for /gather in ONE sync call (runs in a single thread).

    Syncs beta/booster/premium from Discord, then 1 read (full_data) + 1 write (gather + command-count + cooldown).
    Area check, cooldown check and chain roll are pure computation.
    Returns a dict describing the outcome so the caller can build the embed
    and send Discord messages without touching the database again.
    """
    # Sync Discord role state to DB in-thread so the main event loop is not blocked
    sync_beta_tester_from_member(member)
    sync_server_booster_from_member(member)
    sync_premium_tier_from_member(member)
    user_planter_level = get_user_planter_level(member)

    area_mult = area.get("multiplier", 1.0)
    if has_shop_item(user_id, "atlas"):
        area_mult *= 2.0
    area_mult *= area.get("gather_boost", 1.0)

    full_data = get_user_gather_full_data(user_id)
    active_events = get_active_events_cached()

    # --- area access check (no DB call, uses pre-fetched unlocked_areas) ---
    # Use effective planter level: max of passed-in (Discord role) and DB level from bloom_cycle_plants
    bloom_cycle = full_data.get("bloom_cycle_plants", 0)
    effective_planter_level = max(user_planter_level, get_planter_level_from_total_items(bloom_cycle))
    if not area.get("unlocked_by_default", False):
        unlocked = full_data.get("unlocked_areas", {})
        prev = area.get("previous_area")
        if prev and not unlocked.get(prev, False):
            prev_display = GATHERING_AREAS[prev]["display_name"]
            return {"area_error": f"❌ You must unlock **{prev_display}** before accessing **{area['display_name']}**!"}
        if not unlocked.get(channel_name, False):
            return {
                "area_error": (
                    f"❌ You haven't unlocked **{area['display_name']}** yet! "
                    f"Use `/unlock {channel_name}` to unlock it for **{format_money(area['unlock_cost'])}**."
                )
            }
        if effective_planter_level < area.get("required_planter_level", 0):
            return {"area_error": f"❌ You must be **{area['required_planter_rank']}** or above to gather in **{area['display_name']}**! You need to gather more plants to rank up."}

    # --- cooldown check (pure computation on pre-fetched data) ---
    can_user, time_left, is_roulette = can_gather(
        user_id, user_data=full_data, active_events=active_events, full_data=full_data)
    if not can_user:
        # Hidden achievement check for time_left == 0 (rare, safe to do in-thread)
        almost_unlocked = False
        if time_left == 0:
            almost_unlocked = unlock_hidden_achievement(user_id, "almost_got_it")
        return {"on_cooldown": True, "time_left": time_left,
                "is_roulette": is_roulette, "almost_unlocked": almost_unlocked}

    # --- perform gather + cooldown + command-count in ONE write ---
    gather_result = _perform_gather_for_user_sync(
        user_id, apply_cooldown=True, user_data=full_data,
        active_events=active_events, area_multiplier=area_mult,
        full_data=full_data, increment_command_count=True)

    # --- chain roll (pure computation) ---
    user_upgrades = full_data["basket_upgrades"]
    gloves_tier = user_upgrades["gloves"]
    chain_chance = GLOVES_UPGRADES[gloves_tier - 1]["chain_chance"] if gloves_tier > 0 else 0.0
    hoe_enc = gather_result.get("hoe_enchant")
    if hoe_enc:
        chain_chance += hoe_enc.get("chain_chance", 0)
    chain_chance = max(0.0, chain_chance)
    if chain_chance > 0:
        hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
        if hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "chain_reaction":
            chain_chance += 0.10
    chain_triggered = chain_chance > 0 and random.random() < chain_chance
    if chain_triggered:
        update_user_last_gather_time(user_id, 0)

    # --- stealable roll: 4% for gather (× Palace Treasure mult for victim); crits can be stealable; stealable = no PvE ---
    is_crit = gather_result.get("is_critical_gather", False)
    steal_chance = STEAL_CHANCE_GATHER * get_steal_chance_multiplier(user_id)
    stealable = random.random() < steal_chance
    steal_payload = None
    if stealable:
        steal_payload = {
            "value": gather_result["value"],
            "item_name": gather_result["name"],
            "ripeness_name": gather_result["ripeness"],
            "category": gather_result["category"],
            "is_critical_gather": is_crit,
        }

    return {
        "gather_result": gather_result,
        "full_data": full_data,
        "chain_triggered": chain_triggered,
        "stealable": stealable,
        "victim_planter_level": user_planter_level,
        "steal_payload": steal_payload,
        "area_mult": area_mult,
    }


async def _gather_post_response(interaction: discord.Interaction, user_id: int,
                                full_data: dict, gather_result: dict):
    """Background task: role assignment, achievement checks, tree-ring notice.

    Runs AFTER the main embed is already sent so the user sees the response
    instantly.  All blocking DB calls are wrapped in ``asyncio.to_thread``.
    """
    try:
        # Tree Ring notice
        if gather_result.get("tree_ring_awarded"):
            await safe_interaction_response(interaction, interaction.followup.send,
                f"<:TreeRing:1474244868288282817> {interaction.user.mention} You've been awarded **1 Tree Ring**!",
                ephemeral=True)

        # Role assignment (async Discord API) — per-user lock so gather+harvest don't both send rank-up
        old_role = new_role = None
        if user_id not in _planter_role_locks:
            _planter_role_locks[user_id] = asyncio.Lock()
        async with _planter_role_locks[user_id]:
            try:
                old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
            except Exception as e:
                print(f"Error assigning gatherer role to user {user_id}: {e}")

            if new_role:
                if new_role == "PLANTER I" and old_role is None:
                    try:
                        await assign_bloom_rank_role(interaction.user, interaction.guild)
                    except Exception as e:
                        print(f"Error assigning bloom rank role to user {user_id}: {e}")
                    rankup_embed = discord.Embed(
                        title="🌱 Rank Up!",
                        description=f"{interaction.user.mention} advanced to **PLANTER I** and is ranked **PINE I**!",
                        color=discord.Color.gold())
                else:
                    rankup_embed = discord.Embed(
                        title="🌱 Rank Up!",
                        description=f"{interaction.user.mention} advanced from **{old_role or 'PLANTER I'}** to **{new_role}**!",
                        color=discord.Color.gold())
                await safe_interaction_response(interaction, interaction.followup.send, embed=rankup_embed)

        # Achievement checks (run all DB calls in a single thread)
        def _check_achievements():
            total_items = get_user_total_items(user_id)
            planter_lvl = get_planter_level_from_total_items(total_items)
            cur_planter = get_user_achievement_level(user_id, "planter")
            planter_up = None
            if planter_lvl > cur_planter:
                set_user_achievement_level(user_id, "planter", planter_lvl)
                planter_up = planter_lvl

            cmd_count = full_data.get("gather_command_count", 0) + 1
            gatherer_lvl = get_achievement_level_for_stat("gatherer", cmd_count)
            cur_gatherer = get_user_achievement_level(user_id, "gatherer")
            gatherer_up = None
            if gatherer_lvl > cur_gatherer:
                set_user_achievement_level(user_id, "gatherer", gatherer_lvl)
                gatherer_up = gatherer_lvl
            return planter_up, gatherer_up

        planter_up, gatherer_up = await asyncio.to_thread(_check_achievements)
        # Planter achievement: only send when we just showed the rank-up (new_role), so rank-up always displays first
        if new_role and new_role in PLANTER_RANK_ORDER:
            level = PLANTER_RANK_ORDER[new_role]
            cur_planter_ach = planter_up if planter_up else (await asyncio.to_thread(get_user_achievement_level, user_id, "planter"))
            if cur_planter_ach >= level:
                await send_achievement_notification(interaction, "planter", level)
        if gatherer_up:
            await send_achievement_notification(interaction, "gatherer", gatherer_up)

        # Hidden achievement: One in a Mikellion (gather a plant with Mikellion rarity)
        if gather_result.get("ripeness") == "Mikellion" and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
            await send_hidden_achievement_notification(interaction, "one_in_a_mikellion")

        # Hidden achievements: Just Like TF2 (first crit), Moist (10 crits)
        if gather_result.get("is_critical_gather"):
            if unlock_hidden_achievement(user_id, "just_like_tf2"):
                await send_hidden_achievement_notification(interaction, "just_like_tf2")
            await asyncio.to_thread(increment_critical_gathers_count, user_id)
            crit_count = await asyncio.to_thread(get_user_critical_gathers_count, user_id)
            if crit_count >= 10 and unlock_hidden_achievement(user_id, "moist"):
                await send_hidden_achievement_notification(interaction, "moist")

        # Almanac achievements (level + section hidden)
        await check_almanac_achievements_async(user_id, interaction, interaction.user.mention)
    except Exception as e:
        print(f"Error in gather post-response: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Steal View — STEAL button for stealable gather/harvest (time window, rank check)
# ═══════════════════════════════════════════════════════════════════════════════

class StealView(discord.ui.View):
    """Red STEAL button on stealable gather/harvest. Valid for 5s (gather) or 1s (harvest)."""

    def __init__(self, victim_id: int, victim_planter_level: int, steal_type: str, steal_payload: dict, window_sec: float):
        super().__init__(timeout=window_sec + 1.0)  # slightly longer so we can disable on_timeout
        self.victim_id = victim_id
        self.victim_planter_level = victim_planter_level
        self.steal_type = steal_type
        self.steal_payload = steal_payload
        self._window_sec = window_sec
        self._created_at = time.time()
        self._stolen = False
        self._message = None  # set by caller after send so on_timeout can edit

    def _expired(self) -> bool:
        return (time.time() - self._created_at) > self._window_sec

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if getattr(self, "_message", None) is not None:
            try:
                await self._message.edit(view=self)
            except Exception:
                pass

    @discord.ui.button(label="STEAL", style=discord.ButtonStyle.danger, custom_id="steal_btn")
    async def steal_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if interaction.user.id == self.victim_id:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ You can't steal from yourself!", ephemeral=True)
            return
        if self._stolen:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ This has already been stolen!", ephemeral=True)
            return
        if self._expired():
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ Too late! The steal window has closed!", ephemeral=True)
            return

        stealer_level = get_user_planter_level(interaction.user)
        if stealer_level == 0:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ You need a Planter rank to steal! Use /gather to rank up.", ephemeral=True)
            return
        stealer_id = interaction.user.id
        if not has_shop_item(stealer_id, "bandana") and abs(stealer_level - self.victim_planter_level) > 2:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ You can only steal from someone within 2 Planter ranks above or below you!", ephemeral=True)
            return

        self._stolen = True
        victim_id = self.victim_id
        payload = self.steal_payload

        def _do_steal():
            # Van der Linde's Plan: +15% money from gather/harvests you steal
            steal_value_mult = 1.15 if has_shop_item(stealer_id, "van_der_lindes_plan") else 1.0
            stolen_value = payload["value"] if self.steal_type == "gather" else payload["total_value"]
            if self.steal_type == "gather":
                gather_value = payload["value"] * steal_value_mult
                steal_revert_gather(
                    victim_id, payload["value"], payload["item_name"],
                    payload["ripeness_name"], payload["category"])
                steal_apply_gather(
                    stealer_id, gather_value, payload["item_name"],
                    payload["ripeness_name"], payload["category"])
            else:
                harvest_value = payload["total_value"] * steal_value_mult
                steal_revert_harvest(
                    victim_id, payload["items_inc"], payload["ripeness_inc"],
                    payload["total_value"], payload["num_items"])
                steal_apply_harvest(
                    stealer_id, payload["items_inc"], payload["ripeness_inc"],
                    harvest_value, payload["num_items"])
            # Decoys: victim gets 15% of stolen value back
            if has_shop_item(victim_id, "decoys") and stolen_value > 0:
                decoy_refund = round(stolen_value * 0.15, 2)
                victim_bal = get_user_balance(victim_id)
                update_user_balance(victim_id, victim_bal + decoy_refund)

        await asyncio.to_thread(_do_steal)

        # Check stealing achievement (total steals just incremented in DB)
        def _check_stealing_achievement():
            total_steals = get_user_total_steals(stealer_id)
            new_level = get_achievement_level_for_stat("stealing", total_steals)
            cur_level = get_user_achievement_level(stealer_id, "stealing")
            if new_level > cur_level:
                set_user_achievement_level(stealer_id, "stealing", new_level)
                return new_level
            return None

        stealing_level_up = await asyncio.to_thread(_check_stealing_achievement)

        # Hidden achievement: No Honor (steal a critical gather)
        no_honor_unlocked = False
        if self.steal_type == "gather" and payload.get("is_critical_gather") and unlock_hidden_achievement(stealer_id, "no_honor"):
            no_honor_unlocked = True

        for child in self.children:
            child.disabled = True

        stealer_name = interaction.user.display_name
        # Decoys: show victim's 15% back in embed if applicable
        stolen_val = payload["value"] if self.steal_type == "gather" else payload["total_value"]
        decoy_refund = round(stolen_val * 0.15, 2) if has_shop_item(self.victim_id, "decoys") and stolen_val > 0 else 0
        try:
            old_embed = interaction.message.embeds[0] if interaction.message.embeds else None
            if self.steal_type == "gather" and old_embed:
                # Rebuild as dark-red "STOLEN GATHER!" embed with clean formatting
                DARK_RED = 0x8B0000
                new_embed = discord.Embed(
                    title="🔴 STOLEN GATHER!",
                    description=f"**{stealer_name}** stole this gather!",
                    color=DARK_RED,
                )
                for f in old_embed.fields:
                    new_embed.add_field(name=f.name, value=f.value, inline=f.inline)
                if decoy_refund > 0:
                    new_embed.add_field(name="🪤 Decoys", value=f"Victim got **15%** back: **${decoy_refund:.2f}**", inline=False)
                embed = new_embed
            elif self.steal_type == "harvest" and old_embed:
                # Harvest: keep embed, add footer for who stole it
                old_embed.set_footer(text=f"🔴 Stolen by **{stealer_name}**")
                embed = old_embed
            elif old_embed:
                old_embed.set_footer(text=f"🔴 Stolen by **{stealer_name}**")
                embed = old_embed
            else:
                embed = discord.Embed(
                    description=f"🔴 Stolen by **{stealer_name}**!",
                    color=0x8B0000,
                )
            await safe_interaction_response(
                interaction, interaction.response.edit_message, embed=embed, view=self)
            if stealing_level_up is not None:
                await send_achievement_notification(interaction, "stealing", stealing_level_up)
            if no_honor_unlocked:
                await send_hidden_achievement_notification(interaction, "no_honor")
        except Exception:
            if stealing_level_up is not None:
                try:
                    await send_achievement_notification(interaction, "stealing", stealing_level_up)
                except Exception:
                    pass
            if no_honor_unlocked:
                try:
                    await send_hidden_achievement_notification(interaction, "no_honor")
                except Exception:
                    pass
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                f"🔴 Stolen by **{stealer_name}**!", ephemeral=False)
        self.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# PvE Wild Animal Event — View, trigger, and reward logic
# ═══════════════════════════════════════════════════════════════════════════════

class WildAnimalView(discord.ui.View):
    """Interactive button view for the PvE wild animal event.
    
    Each button press drains 1 HP and records the attacker.
    When HP reaches 0, rewards are distributed and the channel is unlocked.
    """

    def __init__(self, animal: dict, hp: int, channel_id: int, area_multiplier: float):
        super().__init__(timeout=None)
        self.animal = animal
        self.max_hp = hp
        self.hp = hp
        self.channel_id = channel_id
        self.area_multiplier = area_multiplier
        self.attackers: dict[int, int] = {}  # user_id -> hit count
        self.defeated = False
        self._lock = asyncio.Lock()

    def _hp_bar(self) -> str:
        filled = max(0, round((self.hp / self.max_hp) * 20))
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    @discord.ui.button(label="⚔️", style=discord.ButtonStyle.danger, custom_id="pve_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                return  # Already dead; send nothing to avoid flooding chat when spam-clicking

            damage = get_pve_damage_multiplier(interaction.user.id)
            self.hp -= damage
            self.attackers[interaction.user.id] = self.attackers.get(interaction.user.id, 0) + damage

            if self.hp <= 0:
                self.defeated = True
                button.disabled = True
                button.label = "☠️ Defeated!"
                button.style = discord.ButtonStyle.secondary

                victory_embed = discord.Embed(
                    title=f"☠️ {self.animal['emoji']} {self.animal['name']} Defeated! ☠️",
                    description=self.animal["defeat_msg"],
                    color=discord.Color.gold())
                victory_embed.add_field(
                    name="HP", value=f"**0** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)

                # Cap displayed damage by the animal's max HP so overkill isn't shown
                display_attackers = _pve_cap_damage_by_hp(self.attackers, self.max_hp)
                participants_lines = []
                sorted_attackers = sorted(display_attackers.items(), key=lambda x: -x[1])
                for uid, dmg in sorted_attackers:
                    member = interaction.guild.get_member(uid)
                    name = member.display_name if member else f"User {uid}"
                    participants_lines.append(f"**{name}** — {dmg} damage")
                participants_text = "\n".join(participants_lines) if participants_lines else "No participants"
                if len(participants_text) > 1024:
                    participants_text = participants_text[:1020] + " …"
                victory_embed.add_field(name="🏆 Contributors", value=participants_text, inline=False)
                victory_embed.set_footer(text="Rewards are being distributed…")

                await safe_interaction_response(
                    interaction, interaction.response.edit_message, embed=victory_embed, view=self)

                # Unlock the channel IMMEDIATELY so commands aren't stuck
                active_pve_events.pop(self.channel_id, None)

                asyncio.create_task(
                    _pve_distribute_rewards(interaction, self.animal, dict(self.attackers), self.channel_id, self.area_multiplier, max_hp=self.max_hp))
                return

            progress_embed = discord.Embed(
                title=f"🚨 {self.animal['emoji']} Wild {self.animal['name']} Appeared! 🚨",
                description=self.animal["description"],
                color=self.animal["color"])
            progress_embed.add_field(
                name="HP", value=f"**{self.hp}** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
            progress_embed.add_field(
                name="⚔️ Last Hit", value=f"**{interaction.user.display_name}**!", inline=False)
            progress_embed.set_footer(text="All commands are blocked until it's defeated")

            await safe_interaction_response(
                interaction, interaction.response.edit_message, embed=progress_embed, view=self)


# Reward display for Bullet Ant Swarm (used when swarm is fully defeated)
BULLET_ANT_SWARM_REWARD_ANIMAL = {
    "id": "bullet_ant",
    "name": "Bullet Ant Swarm",
    "emoji": "🐜",
    "description": BULLET_ANT_SWARM_INTRO,
    "defeat_msg": BULLET_ANT_SWARM_DEFEAT,
    "color": 0x2C1810,
}


class BulletAntView(discord.ui.View):
    """Single ant in a Bullet Ant Swarm. On defeat, decrements swarm ants_remaining; when 0, edits intro to defeat and distributes rewards."""

    def __init__(self, ant_hp: int, channel_id: int, swarm_state: dict):
        super().__init__(timeout=None)
        self.max_hp = ant_hp
        self.hp = ant_hp
        self.channel_id = channel_id
        self.swarm_state = swarm_state
        self.defeated = False
        self._lock = asyncio.Lock()

    def _hp_bar(self) -> str:
        filled = max(0, round((self.hp / self.max_hp) * 20))
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    @discord.ui.button(label="⚔️", style=discord.ButtonStyle.danger, custom_id="pve_ant_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                await safe_interaction_response(
                    interaction, interaction.response.send_message,
                    "This ant is already squashed!", ephemeral=True)
                return

            damage = get_pve_damage_multiplier(interaction.user.id)
            self.hp -= damage
            self.swarm_state["attackers"][interaction.user.id] = self.swarm_state["attackers"].get(interaction.user.id, 0) + damage

            if self.hp <= 0:
                self.defeated = True
                button.disabled = True
                button.label = "☠️"
                button.style = discord.ButtonStyle.secondary

                await safe_interaction_response(
                    interaction, interaction.response.edit_message,
                    embed=discord.Embed(
                        title=f"☠️ {BULLET_ANT_ANIMAL['emoji']} Bullet Ant squashed!",
                        description=BULLET_ANT_ANIMAL["defeat_msg"],
                        color=discord.Color.gold()),
                    view=self)

                self.swarm_state["ants_remaining"] -= 1
                if self.swarm_state["ants_remaining"] <= 0:
                    channel = interaction.guild.get_channel(self.channel_id)
                    intro_id = self.swarm_state.get("intro_message_id")
                    active_pve_events.pop(self.channel_id, None)
                    if channel and intro_id:
                        try:
                            intro_msg = await channel.fetch_message(intro_id)
                            defeat_embed = discord.Embed(
                                title=f"☠️ 🐜 Bullet Ant Swarm Defeated! ☠️",
                                description=self.swarm_state["defeat_msg"],
                                color=discord.Color.gold())
                            total_hp = self.swarm_state.get("total_hp")
                            display_attackers = _pve_cap_damage_by_hp(dict(self.swarm_state["attackers"]), total_hp) if total_hp else dict(self.swarm_state["attackers"])
                            participants_lines = []
                            for uid, dmg in sorted(display_attackers.items(), key=lambda x: -x[1]):
                                member = interaction.guild.get_member(uid)
                                name = member.display_name if member else f"User {uid}"
                                participants_lines.append(f"**{name}** — {dmg} damage")
                            participants_text = "\n".join(participants_lines) if participants_lines else "No participants"
                            if len(participants_text) > 1024:
                                participants_text = participants_text[:1020] + " …"
                            defeat_embed.add_field(name="🏆 Contributors", value=participants_text, inline=False)
                            await intro_msg.edit(embed=defeat_embed)
                        except Exception as e:
                            print(f"Bullet Ant Swarm intro edit failed: {e}")
                    asyncio.create_task(
                        _pve_distribute_rewards(
                            interaction, BULLET_ANT_SWARM_REWARD_ANIMAL,
                            dict(self.swarm_state["attackers"]), self.channel_id,
                            self.swarm_state["area_multiplier"],
                            achievements_ephemeral=True,
                            max_hp=self.swarm_state.get("total_hp")))
                return

            progress_embed = discord.Embed(
                title=f"🚨 {BULLET_ANT_ANIMAL['emoji']} Bullet Ant 🚨",
                description=BULLET_ANT_ANIMAL["description"],
                color=BULLET_ANT_ANIMAL["color"])
            progress_embed.add_field(
                name="HP", value=f"**{self.hp}** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
            progress_embed.add_field(name="⚔️ Last Hit", value=f"**{interaction.user.display_name}**!", inline=False)
            await safe_interaction_response(
                interaction, interaction.response.edit_message, embed=progress_embed, view=self)


# Bee Swarm (underground-jungle only): reward display and Larva chance after defeat
BEE_SWARM_REWARD_ANIMAL = {
    "id": "bee",
    "name": "Bee Swarm",
    "emoji": "🐝",
    "description": BEE_SWARM_INTRO,
    "defeat_msg": BEE_SWARM_DEFEAT,
    "color": 0xFFC800,
}


class LarvaView(discord.ui.View):
    """Break or Ignore the Larva. Break summons Queen Bee. Timeout 10s = auto-ignore."""

    def __init__(self, channel_id: int, guild_id: int, area_multiplier: float):
        super().__init__(timeout=LARVA_TIMEOUT_SEC)
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.area_multiplier = area_multiplier
        self.resolved = False

    async def on_timeout(self):
        if self.resolved:
            return
        self.resolved = True
        try:
            for item in self.children:
                item.disabled = True
            if self.message:
                await self.message.edit(
                    content="The Larva is left alone.",
                    view=self)
        except Exception as e:
            print(f"Larva timeout edit failed: {e}")

    @discord.ui.button(label="Break", style=discord.ButtonStyle.danger, custom_id="larva_break")
    async def break_larva(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if self.resolved:
            await safe_interaction_response(interaction, interaction.response.send_message, "Someone already chose!", ephemeral=True)
            return
        self.resolved = True
        self.stop()
        queen_bee = next(b for b in PVE_BOSSES if b["id"] == "queen_bee")
        await safe_interaction_response(interaction, interaction.response.edit_message,
            content="Queen Bee has awoken!",
            view=None)
        channel = interaction.guild.get_channel(self.channel_id) or interaction.channel
        try:
            await trigger_boss_event(channel, queen_bee, self.area_multiplier)
        except Exception as e:
            print(f"Queen Bee spawn after Larva break failed: {e}")

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.secondary, custom_id="larva_ignore")
    async def ignore_larva(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if self.resolved:
            await safe_interaction_response(interaction, interaction.response.send_message, "Someone already chose!", ephemeral=True)
            return
        self.resolved = True
        self.stop()
        await safe_interaction_response(interaction, interaction.response.edit_message,
            content="The Larva is left alone.",
            view=None)


class BeeView(discord.ui.View):
    """Single bee in a Bee Swarm. On defeat, decrements bees_remaining; when 0, edits intro, distributes rewards, then 40%% chance to spawn Larva (Break/Ignore → Queen Bee or ignore)."""

    def __init__(self, bee_hp: int, channel_id: int, swarm_state: dict):
        super().__init__(timeout=None)
        self.max_hp = bee_hp
        self.hp = bee_hp
        self.channel_id = channel_id
        self.swarm_state = swarm_state
        self.defeated = False
        self._lock = asyncio.Lock()

    def _hp_bar(self) -> str:
        filled = max(0, round((self.hp / self.max_hp) * 20))
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    @discord.ui.button(label="⚔️", style=discord.ButtonStyle.danger, custom_id="pve_bee_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                await safe_interaction_response(
                    interaction, interaction.response.send_message,
                    "This bee is already swatted!", ephemeral=True)
                return

            damage = get_pve_damage_multiplier(interaction.user.id)
            self.hp -= damage
            self.swarm_state["attackers"][interaction.user.id] = self.swarm_state["attackers"].get(interaction.user.id, 0) + damage

            if self.hp <= 0:
                self.defeated = True
                button.disabled = True
                button.label = "☠️"
                button.style = discord.ButtonStyle.secondary

                await safe_interaction_response(
                    interaction, interaction.response.edit_message,
                    embed=discord.Embed(
                        title=f"☠️ {BEE_ANIMAL['emoji']} Bee swatted!",
                        description=BEE_ANIMAL["defeat_msg"],
                        color=discord.Color.gold()),
                    view=self)

                self.swarm_state["bees_remaining"] -= 1
                if self.swarm_state["bees_remaining"] <= 0:
                    channel = interaction.guild.get_channel(self.channel_id)
                    intro_id = self.swarm_state.get("intro_message_id")
                    active_pve_events.pop(self.channel_id, None)
                    guild_id = interaction.guild.id if interaction.guild else 0
                    area_mult = self.swarm_state.get("area_multiplier", 1.0)
                    if channel and intro_id:
                        try:
                            intro_msg = await channel.fetch_message(intro_id)
                            defeat_embed = discord.Embed(
                                title=f"☠️ 🐝 Bee Swarm Defeated! ☠️",
                                description=self.swarm_state["defeat_msg"],
                                color=discord.Color.gold())
                            total_hp = self.swarm_state.get("total_hp")
                            display_attackers = _pve_cap_damage_by_hp(dict(self.swarm_state["attackers"]), total_hp) if total_hp else dict(self.swarm_state["attackers"])
                            participants_lines = []
                            for uid, dmg in sorted(display_attackers.items(), key=lambda x: -x[1]):
                                member = interaction.guild.get_member(uid)
                                name = member.display_name if member else f"User {uid}"
                                participants_lines.append(f"**{name}** — {dmg} damage")
                            participants_text = "\n".join(participants_lines) if participants_lines else "No participants"
                            if len(participants_text) > 1024:
                                participants_text = participants_text[:1020] + " …"
                            defeat_embed.add_field(name="🏆 Contributors", value=participants_text, inline=False)
                            await intro_msg.edit(embed=defeat_embed)
                        except Exception as e:
                            print(f"Bee Swarm intro edit failed: {e}")
                    asyncio.create_task(
                        _pve_distribute_rewards(
                            interaction, BEE_SWARM_REWARD_ANIMAL,
                            dict(self.swarm_state["attackers"]), self.channel_id,
                            area_mult,
                            achievements_ephemeral=True,
                            max_hp=self.swarm_state.get("total_hp")))
                    # 40% chance: spawn Larva (Break → Queen Bee, Ignore or 10s → nothing)
                    if channel and guild_id and random.random() < BEE_SWARM_LARVA_CHANCE:
                        larva_embed = discord.Embed(
                            title="🐝 A Larva has been revealed!",
                            description="Should we break it. . . ?",
                            color=0xFFD700)
                        larva_embed.set_footer(text="(Auto-ignored after 10 sec)")
                        view_larva = LarvaView(channel_id=self.channel_id, guild_id=guild_id, area_multiplier=area_mult)
                        larva_msg = await channel.send(embed=larva_embed, view=view_larva)
                        view_larva.message = larva_msg
                return

            progress_embed = discord.Embed(
                title=f"🚨 {BEE_ANIMAL['emoji']} Bee 🚨",
                description=BEE_ANIMAL["description"],
                color=BEE_ANIMAL["color"])
            progress_embed.add_field(
                name="HP", value=f"**{self.hp}** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
            progress_embed.add_field(name="⚔️ Last Hit", value=f"**{interaction.user.display_name}**!", inline=False)
            await safe_interaction_response(
                interaction, interaction.response.edit_message, embed=progress_embed, view=self)


def _get_guild_gather_channels(guild: discord.Guild) -> list[discord.TextChannel]:
    """Return list of text channels in guild whose name is a valid gathering channel (forest, grove, marsh, bog, mire, underground-jungle)."""
    out = []
    for ch in guild.text_channels:
        name = ch.name.lower().replace(" ", "-")
        if name in VALID_GATHERING_CHANNELS:
            out.append(ch)
    return out


# Nether Star (Wither drop): custom animated emoji for claim button and shop
NETHER_STAR_EMOJI = "<a:netherstar:1476237557221163210>"
NETHER_STAR_EMOJI_PARTIAL = discord.PartialEmoji(name="netherstar", id=1476237557221163210, animated=True)
# Black Shard (Roaring Knight drop): custom emoji for claim button and shop
BLACK_SHARD_EMOJI = "<:blackshard:1478988887110975519>"
BLACK_SHARD_EMOJI_PARTIAL = discord.PartialEmoji(name="blackshard", id=1478988887110975519, animated=False)
# End Crystal (Ender Dragon Obsidian Towers): custom emoji for embeds and buttons
END_CRYSTAL_EMOJI = "<:endcrystal:1479156246081835050>"
END_CRYSTAL_EMOJI_PARTIAL = discord.PartialEmoji(name="endcrystal", id=1479156246081835050, animated=False)


def _format_item_boost_source(name: str, count: int = 1) -> str:
    """Format an item boost source for embed display; adds Nether Star / Black Shard emoji after name."""
    if name == "Nether Star":
        count_display = f" (x{count})" if count > 1 else ""
        return f"**NETHER STAR**{count_display} {NETHER_STAR_EMOJI}"
    if name == "Black Shard":
        count_display = f" (x{count})" if count > 1 else ""
        return f"**BLACK SHARD**{count_display} {BLACK_SHARD_EMOJI}"
    return f"**{name.upper()}**"


class NetherStarClaimView(discord.ui.View):
    """One-time claim button after Wither defeat. First user without a Nether Star gets it; button locks after claim."""

    def __init__(self, claimed_ref: list):
        super().__init__(timeout=None)
        self.claimed_ref = claimed_ref  # [None] or [user_id] after claim

    @discord.ui.button(label="Claim Nether Star!", style=discord.ButtonStyle.success, emoji=NETHER_STAR_EMOJI_PARTIAL, custom_id="wither_nether_star_claim")
    async def claim_nether_star(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        user_id = interaction.user.id
        if self.claimed_ref[0] is not None:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ Someone else already claimed the Nether Star!", ephemeral=True)
            return
        # Add dayboost (24-hour temporary boost) instead of permanent shop item
        add_dayboost(user_id, "nether_star", 24.0)
        dayboost_count = get_dayboost_count(user_id, "nether_star")
        self.claimed_ref[0] = user_id
        button.disabled = True
        button.label = "Claimed!"
        embed = interaction.message.embeds[0].copy() if interaction.message.embeds else discord.Embed(title="Wither Defeated", color=discord.Color.gold())
        count_display = f" (x{dayboost_count})" if dayboost_count > 1 else ""
        embed.add_field(name=f"{NETHER_STAR_EMOJI} Nether Star{count_display}", value=f"**Claimed by {interaction.user.mention}!**", inline=False)
        embed.set_footer(text=f"+15% all Money dayboost active for 24 hours!{count_display}")
        await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        if interaction.guild:
            asyncio.create_task(_post_rares_nether_star_claim(interaction.guild, interaction.user))
        await interaction.followup.send(
            f"{NETHER_STAR_EMOJI} You claimed the **Nether Star**! You now have a **+15% all Money dayboost for 24 hours**!{count_display}",
            ephemeral=True)


class BlackShardClaimView(discord.ui.View):
    """One-time claim button after Roaring Knight defeat. First user without a Black Shard gets it; button locks after claim."""

    def __init__(self, claimed_ref: list):
        super().__init__(timeout=None)
        self.claimed_ref = claimed_ref  # [None] or [user_id] after claim

    @discord.ui.button(label="Claim Black Shard!", style=discord.ButtonStyle.success, emoji=BLACK_SHARD_EMOJI_PARTIAL, custom_id="roaring_knight_black_shard_claim")
    async def claim_black_shard(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        user_id = interaction.user.id
        if self.claimed_ref[0] is not None:
            await safe_interaction_response(
                interaction, interaction.response.send_message,
                "❌ Someone else already claimed the Black Shard!", ephemeral=True)
            return
        # Add dayboost (24-hour temporary boost) instead of permanent shop item
        add_dayboost(user_id, "black_shard", 24.0)
        dayboost_count = get_dayboost_count(user_id, "black_shard")
        self.claimed_ref[0] = user_id
        button.disabled = True
        button.label = "Claimed!"
        embed = interaction.message.embeds[0].copy() if interaction.message.embeds else discord.Embed(title="The Roaring Knight Defeated", color=discord.Color.gold())
        count_display = f" (x{dayboost_count})" if dayboost_count > 1 else ""
        embed.add_field(name=f"{BLACK_SHARD_EMOJI} Black Shard{count_display}", value=f"**Claimed by {interaction.user.mention}!**", inline=False)
        embed.set_footer(text=f"+20% all Money dayboost active for 24 hours!{count_display}")
        await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        if interaction.guild:
            asyncio.create_task(_post_rares_black_shard_claim(interaction.guild, interaction.user))
        await interaction.followup.send(
            f"{BLACK_SHARD_EMOJI} You claimed the **Black Shard**! You now have a **+20% all Money dayboost for 24 hours**!{count_display}",
            ephemeral=True)


class BossView(discord.ui.View):
    """Boss PvE: same as wild animal but removes from active_boss_events on defeat; marks guild eligible for Plantera bulb."""

    def __init__(self, boss: dict, hp: int, channel_id: int, guild_id: int, area_multiplier: float, boss_state_ref: dict):
        super().__init__(timeout=None)
        self.boss = boss
        self.max_hp = hp
        self.hp = hp
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.area_multiplier = area_multiplier
        self.boss_state_ref = boss_state_ref  # ref into active_boss_events[guild_id] entry for this boss
        self.attackers: dict[int, int] = {}
        self.defeated = False
        self._lock = asyncio.Lock()

    def _hp_bar(self) -> str:
        filled = max(0, round((self.hp / self.max_hp) * 20))
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    @discord.ui.button(label="⚔️", style=discord.ButtonStyle.danger, custom_id="pve_boss_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                return  # Already dead; send nothing to avoid flooding chat when spam-clicking

            damage = get_pve_damage_multiplier(interaction.user.id)
            self.hp -= damage
            self.attackers[interaction.user.id] = self.attackers.get(interaction.user.id, 0) + damage

            if self.hp <= 0:
                self.defeated = True
                button.disabled = True
                button.label = "☠️ Defeated!"
                button.style = discord.ButtonStyle.secondary

                victory_embed = discord.Embed(
                    title=f"☠️ {self.boss['emoji']} {self.boss['name']} Defeated! ☠️",
                    description=self.boss["defeat_msg"],
                    color=discord.Color.gold())
                victory_embed.add_field(name="HP", value=f"**0** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
                # Cap displayed damage by the boss's max HP so overkill isn't shown
                display_attackers = _pve_cap_damage_by_hp(self.attackers, self.max_hp)
                participants_lines = []
                for uid, dmg in sorted(display_attackers.items(), key=lambda x: -x[1]):
                    member = interaction.guild.get_member(uid)
                    name = member.display_name if member else f"User {uid}"
                    participants_lines.append(f"**{name}** — {dmg} damage")
                victory_embed.add_field(name="🏆 Contributors", value="\n".join(participants_lines) or "No participants", inline=False)
                victory_embed.set_footer(text="Rewards are being distributed…")
                if self.boss["id"] == "wither":
                    nether_claimed_ref = [None]
                    claim_view = NetherStarClaimView(claimed_ref=nether_claimed_ref)
                    await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=claim_view)
                elif self.boss["id"] == "the_roaring_knight":
                    black_shard_claimed_ref = [None]
                    claim_view = BlackShardClaimView(claimed_ref=black_shard_claimed_ref)
                    await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=claim_view)
                else:
                    await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=self)

                # Accumulate this boss's defeat for deferred rewards (single boss: 1 entry; Twins: 2 entries)
                _pve_boss_defeated_pending.setdefault(self.guild_id, []).append((self.boss, dict(self.attackers), self.max_hp))
                boss_list = active_boss_events.get(self.guild_id, [])
                if self.boss_state_ref in boss_list:
                    boss_list = [e for e in boss_list if e is not self.boss_state_ref]
                    if not boss_list:
                        active_boss_events.pop(self.guild_id, None)
                    else:
                        active_boss_events[self.guild_id] = boss_list

                # Mark guild eligible for Plantera bulb (any boss defeat)
                plantera_bulb_eligible_guilds[self.guild_id] = time.time()

                # When all bosses in this event are defeated: broadcast to all channels and distribute rewards
                if self.guild_id not in active_boss_events:
                    pending = _pve_boss_defeated_pending.pop(self.guild_id, [])
                    guild = interaction.guild
                    channels = _get_guild_gather_channels(guild)
                    if len(pending) == 1:
                        boss = pending[0][0]
                        broadcast_desc = boss.get("server_defeat_msg", boss["defeat_msg"])
                        broadcast_embed = discord.Embed(
                            title=f"☠️ {boss['emoji']} {boss['name']} Defeated! ☠️",
                            description=broadcast_desc,
                            color=discord.Color.gold())
                    else:
                        broadcast_embed = discord.Embed(
                            title="☠️ The Twins Defeated! ☠️",
                            description="The Twins have been defeated! Conditions return to normal.",
                            color=discord.Color.gold())
                    broadcast_embed.set_footer(text="gathering channels are now unblocked")
                    for ch in channels:
                        try:
                            if ch.permissions_for(guild.me).send_messages:
                                await ch.send(embed=broadcast_embed)
                        except Exception as e:
                            print(f"Boss defeat broadcast failed in {ch.name}: {e}")
                    multi_boss = len(pending) > 1
                    for item in pending:
                        boss = item[0]
                        attackers = item[1]
                        max_hp = item[2] if len(item) > 2 else None
                        asyncio.create_task(_pve_distribute_rewards(
                            interaction, boss, attackers, self.channel_id, self.area_multiplier,
                            achievements_ephemeral=multi_boss, max_hp=max_hp))
                return

            progress_embed = discord.Embed(
                title=f"🚨 {self.boss['emoji']} {self.boss['name']} 🚨",
                description=self.boss["description"],
                color=self.boss["color"])
            progress_embed.add_field(name="HP", value=f"**{self.hp}** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
            progress_embed.add_field(name="⚔️ Last Hit", value=f"**{interaction.user.display_name}**!", inline=False)
            progress_embed.set_footer(text="All gathering channels are blocked until this boss is defeated!")
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=progress_embed, view=self)


# Sans death timer: user_id -> death_end_time (timestamp when death cooldown ends)
sans_death_timers: dict[int, float] = {}

# SOUL emoji for Sans
SOUL_EMOJI = "<:SOUL:1479165808147042374>"
SOUL_EMOJI_PARTIAL = discord.PartialEmoji(name="SOUL", id=1479165808147042374, animated=False)

# Sans emoji
SANS_EMOJI = "<:sans:1479162217525154007>"
SANS_EMOJI_PARTIAL = discord.PartialEmoji(name="sans", id=1479162217525154007, animated=False)


class SansView(discord.ui.View):
    """Special boss view for Sans with dodge mechanics and Undertale-style buttons."""

    def __init__(self, boss: dict, hp: int, channel_id: int, guild_id: int, area_multiplier: float, boss_state_ref: dict):
        super().__init__(timeout=None)
        self.boss = boss
        self.max_hp = hp
        self.hp = hp  # Always 1, but we track it
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.area_multiplier = area_multiplier
        self.boss_state_ref = boss_state_ref
        self.attackers: dict[int, int] = {}  # Tracks attempted damage (for rewards)
        self.attack_attempts: dict[int, int] = {}  # Tracks number of attempts per user
        self.total_attempts = 0  # Total attack attempts across all users
        self.defeated = False
        self.mercy_users: set[int] = set()  # Track users who have used MERCY
        self._lock = asyncio.Lock()
        # Sans is defeated after 50 total attack attempts
        self.DEFEAT_THRESHOLD = 50

    def _hp_bar(self) -> str:
        # Start empty and fill from left to right as attempts increase
        filled = round((self.total_attempts / self.DEFEAT_THRESHOLD) * 20)
        filled = min(20, max(0, filled))  # Clamp between 0 and 20
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    @discord.ui.button(label="FIGHT", emoji=SOUL_EMOJI_PARTIAL, style=discord.ButtonStyle.danger, custom_id="sans_fight", row=0)
    async def fight(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            async with self._lock:
                if self.defeated:
                    return

                damage = get_pve_damage_multiplier(interaction.user.id)
                self.total_attempts += 1
                self.attack_attempts[interaction.user.id] = self.attack_attempts.get(interaction.user.id, 0) + 1
                # Track attempted damage for rewards (even though it "dodges")
                self.attackers[interaction.user.id] = self.attackers.get(interaction.user.id, 0) + damage

                # Check if defeated (after enough attempts)
                if self.total_attempts >= self.DEFEAT_THRESHOLD:
                    self.defeated = True
                    button.disabled = True
                    button.label = "☠️ Defeated!"
                    button.style = discord.ButtonStyle.secondary
                    # Disable other buttons too
                    for child in self.children:
                        if hasattr(child, 'disabled'):
                            child.disabled = True

                    victory_embed = discord.Embed(
                        title=f"☠️ {self.boss['emoji']} {self.boss['name']} Defeated! ☠️",
                        description=self.boss["defeat_msg"],
                        color=discord.Color.gold())
                    victory_embed.add_field(name="HP", value=f"**0** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
                    # For Sans, show attempts instead of damage (since he dodges everything)
                    participants_lines = []
                    for uid, attempts in sorted(self.attack_attempts.items(), key=lambda x: -x[1]):
                        member = interaction.guild.get_member(uid)
                        name = member.display_name if member else f"User {uid}"
                        participants_lines.append(f"**{name}** — {attempts} attack{'s' if attempts != 1 else ''}")
                    victory_embed.add_field(name="🏆 Contributors", value="\n".join(participants_lines) or "No participants", inline=False)
                    victory_embed.set_footer(text="Rewards are being distributed…")
                    await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=self)

                    # Accumulate for deferred rewards
                    # For Sans, pass attack_attempts instead of attackers (damage), and None as max_hp so rewards aren't capped
                    _pve_boss_defeated_pending.setdefault(self.guild_id, []).append((self.boss, dict(self.attack_attempts), None))
                    boss_list = active_boss_events.get(self.guild_id, [])
                    if self.boss_state_ref in boss_list:
                        boss_list = [e for e in boss_list if e is not self.boss_state_ref]
                        if not boss_list:
                            active_boss_events.pop(self.guild_id, None)
                        else:
                            active_boss_events[self.guild_id] = boss_list

                    plantera_bulb_eligible_guilds[self.guild_id] = time.time()

                    # Broadcast defeat to all channels
                    if self.guild_id not in active_boss_events:
                        pending = _pve_boss_defeated_pending.pop(self.guild_id, [])
                        guild = interaction.guild
                        channels = _get_guild_gather_channels(guild)
                        if len(pending) == 1:
                            boss = pending[0][0]
                            broadcast_desc = boss.get("server_defeat_msg", boss["defeat_msg"])
                            broadcast_embed = discord.Embed(
                                title=f"☠️ {boss['emoji']} {boss['name']} Defeated! ☠️",
                                description=broadcast_desc,
                                color=discord.Color.gold())
                            broadcast_embed.set_footer(text="gathering channels are now unblocked")
                            for ch in channels:
                                try:
                                    if ch.permissions_for(guild.me).send_messages:
                                        await ch.send(embed=broadcast_embed)
                                except Exception as e:
                                    print(f"Boss defeat broadcast failed in {ch.name}: {e}")
                            # For Sans, pass attack_attempts instead of attackers (damage), and None as max_hp so rewards aren't capped
                            asyncio.create_task(_pve_distribute_rewards(
                                interaction, boss, dict(self.attack_attempts), self.channel_id, self.area_multiplier,
                                achievements_ephemeral=False, max_hp=None))
                    return

                # Attack "dodged" - show MISS message
                # Rewards will be distributed on defeat based on all attempts (not capped)
                progress_embed = discord.Embed(
                    title=f"🚨 {self.boss['emoji']} {self.boss['name']} 🚨",
                    description=self.boss["description"],
                    color=self.boss["color"])
                progress_embed.add_field(name="HP", value=f"**{self.hp}** / **{self.max_hp}**\n{self._hp_bar()}", inline=False)
                progress_embed.add_field(name="⚔️ Last Action", value=f"**{interaction.user.display_name}** attacked! *MISS!*", inline=False)
                progress_embed.set_footer(text=f"All gathering channels are blocked! ({self.total_attempts}/{self.DEFEAT_THRESHOLD} attempts)")
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=progress_embed, view=self)
        except Exception as e:
            print(f"Error in Sans FIGHT button: {e}")
            try:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ An error occurred. Please try again.", ephemeral=True)
            except:
                pass

    @discord.ui.button(label="ACT", style=discord.ButtonStyle.danger, custom_id="sans_act", row=0)
    async def act(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_interaction_response(interaction, interaction.response.send_message,
            "This isn't UNDERTALE! Just attack!", ephemeral=True)

    @discord.ui.button(label="ITEM", style=discord.ButtonStyle.danger, custom_id="sans_item", row=1)
    async def item(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_interaction_response(interaction, interaction.response.send_message,
            "This isn't UNDERTALE! Just attack!", ephemeral=True)

    @discord.ui.button(label="MERCY", style=discord.ButtonStyle.danger, custom_id="sans_mercy", row=1)
    async def mercy(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            async with self._lock:
                if self.defeated:
                    return

                user_id = interaction.user.id
                
                # Check if user already used MERCY
                if user_id in self.mercy_users:
                    await safe_interaction_response(interaction, interaction.followup.send,
                        "❌ You already tried to SPARE Sans!", ephemeral=True)
                    return

                # Mark this user as having used MERCY
                self.mercy_users.add(user_id)
                
                # Set death timer: 30 minutes = 1800 seconds
                death_end_time = time.time() + 1800
                sans_death_timers[user_id] = death_end_time

                # Create GAME OVER embed with SOUL emoji in title (public)
                game_over_embed = discord.Embed(
                    title=f"{SOUL_EMOJI} GAME OVER",
                    description=f"{interaction.user.mention} tried to SPARE Sans and got dunked on!",
                    color=discord.Color.red())
                game_over_embed.set_footer(text="Stay determined...")

                # Send the embed publicly using followup (since we already deferred)
                try:
                    await interaction.followup.send(embed=game_over_embed)
                except Exception as e:
                    print(f"Error sending GAME OVER embed: {e}")
                    # Fallback: try to send as regular message
                    try:
                        channel = interaction.channel
                        if channel:
                            await channel.send(embed=game_over_embed)
                    except Exception as e2:
                        print(f"Error sending GAME OVER embed as fallback: {e2}")

                # Send death timer message as ephemeral
                time_left = 1800
                minutes = time_left // 60
                seconds = time_left % 60
                time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"{SOUL_EMOJI} **GAME OVER** - You're dead for **{time_str}**.", ephemeral=True)
        except Exception as e:
            print(f"Error in Sans MERCY button: {e}")
            try:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ An error occurred. Please try again.", ephemeral=True)
            except:
                pass


async def _send_boss_warning_embed(guild: discord.Guild, boss: dict, spawn_channel: discord.TextChannel):
    """Send 1-min warning embed to ALL gathering channels; only @here in the channel where the boss will spawn. No footer."""
    channels = _get_guild_gather_channels(guild)
    text = boss.get("pre_spawn_text", "Something terrible is approaching...")
    embed = discord.Embed(
        title="Something's coming...",
        description=text,
        color=discord.Color.dark_red())
    for ch in channels:
        try:
            if ch.permissions_for(guild.me).send_messages:
                if ch.id == spawn_channel.id:
                    await ch.send("@here", embed=embed)
                else:
                    await ch.send(embed=embed)
        except Exception as e:
            print(f"Boss warning embed failed in {ch.name}: {e}")


async def trigger_boss_event(channel: discord.TextChannel, boss: dict, area_multiplier: float):
    """Spawn a boss in the given channel. Blocks all gather channels in the guild until defeated."""
    guild_id = channel.guild.id
    if guild_id in active_boss_events:
        return  # already a boss active
    hp = random.randint(*boss["hp_range"])
    entry = {"channel_id": channel.id, "boss": boss, "hp": hp, "max_hp": hp}
    active_boss_events[guild_id] = [entry]

    embed = discord.Embed(
        title=f"🚨 {boss['emoji']} {boss['name']} 🚨",
        description=boss["description"],
        color=boss["color"])
    embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{'🟥' * 20}", inline=False)
    
    # Special handling for Sans
    if boss["id"] == "sans":
        # Sans starts with empty HP bar (will fill as attempts increase)
        embed.set_field_at(0, name="HP", value=f"**1** / **{hp}**\n{'⬛' * 20}", inline=False)
        embed.add_field(name="⚔️ How to Fight", value="Press **FIGHT**! (Sans will dodge, but keep attacking!)", inline=False)
        view = SansView(boss=boss, hp=hp, channel_id=channel.id, guild_id=guild_id, area_multiplier=area_multiplier, boss_state_ref=entry)
    else:
        embed.add_field(name="⚔️ How to Fight", value="Press **Attack**!", inline=False)
        view = BossView(boss=boss, hp=hp, channel_id=channel.id, guild_id=guild_id, area_multiplier=area_multiplier, boss_state_ref=entry)
    
    embed.set_footer(text="All gathering channels are BLOCKED until this boss is defeated!")
    await channel.send(embed=embed, view=view)


async def trigger_twins_boss_event(channel: discord.TextChannel, area_multiplier: float):
    """Spawn both Twins (Retinazer and Spazmatism) in the same channel. Both must be defeated to unblock."""
    guild_id = channel.guild.id
    if guild_id in active_boss_events:
        return
    retinazer = next(b for b in PVE_BOSSES if b["id"] == "retinazer")
    spazmatism = next(b for b in PVE_BOSSES if b["id"] == "spazmatism")
    hp_r = random.randint(*retinazer["hp_range"])
    hp_s = random.randint(*spazmatism["hp_range"])
    entry_r = {"channel_id": channel.id, "boss": retinazer, "hp": hp_r, "max_hp": hp_r}
    entry_s = {"channel_id": channel.id, "boss": spazmatism, "hp": hp_s, "max_hp": hp_s}
    active_boss_events[guild_id] = [entry_r, entry_s]

    for boss, hp, entry in [(retinazer, hp_r, entry_r), (spazmatism, hp_s, entry_s)]:
        embed = discord.Embed(
            title=f"🚨 {boss['emoji']} {boss['name']} 🚨",
            description=boss["description"],
            color=boss["color"])
        embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{'🟥' * 20}", inline=False)
        embed.add_field(name="⚔️ How to Fight", value="Press **Attack**! Both Twins must fall to free the channels!", inline=False)
        embed.set_footer(text="All gathering channels are BLOCKED until BOTH Twins are defeated!")
        view = BossView(boss=boss, hp=hp, channel_id=channel.id, guild_id=guild_id, area_multiplier=area_multiplier, boss_state_ref=entry)
        await channel.send(embed=embed, view=view)


# --- Ender Dragon: dragon in trigger channel, Obsidian Towers (End Crystals) in other gather channels ---

def _tower_hp_bar(hp: int, max_hp: int = ENDER_DRAGON_TOWER_HP) -> str:
    filled = max(0, round((hp / max_hp) * 20))
    empty = 20 - filled
    return f"{'🟥' * filled}{'⬛' * empty}"


class ObsidianTowerView(discord.ui.View):
    """Obsidian Tower (End Crystal) in non-trigger channels during Ender Dragon. 20 HP; when 0, Break End Crystal to defeat."""

    def __init__(self, channel_id: int, guild_id: int):
        super().__init__(timeout=None)
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.tower_hp = ENDER_DRAGON_TOWER_HP
        self.defeated = False
        self._lock = asyncio.Lock()

    def _embed(self, last_hit: str | None = None) -> discord.Embed:
        if self.defeated:
            embed = discord.Embed(
                title="🖤 Obsidian Tower 🖤",
                description=f"This tower's End Crystal {END_CRYSTAL_EMOJI} has been broken.",
                color=0x1a1a2e)
            embed.add_field(name="HP", value=f"**0** / **{ENDER_DRAGON_TOWER_HP}**\n{_tower_hp_bar(0)}", inline=False)
            embed.add_field(name="Status", value=f"**End Crystal** {END_CRYSTAL_EMOJI} **Broken!**", inline=False)
            embed.set_footer(text="The tower has no effect!")
        else:
            embed = discord.Embed(
                title="🖤 Obsidian Tower 🖤",
                description=f"An **End Crystal** {END_CRYSTAL_EMOJI} powers this tower. Damage the tower, then **Break End Crystal** to stop the Ender Dragon's regeneration!",
                color=0x1a1a2e)
            embed.add_field(name="HP", value=f"**{self.tower_hp}** / **{ENDER_DRAGON_TOWER_HP}**\n{_tower_hp_bar(self.tower_hp)}", inline=False)
            if last_hit:
                embed.add_field(name="⚔️ Last Hit", value=f"**{last_hit}**!", inline=False)
            embed.set_footer(text=f"Break all End Crystals {END_CRYSTAL_EMOJI} in every channel, then defeat the Ender Dragon!")
        return embed

    @discord.ui.button(label="⚔️ Attack", style=discord.ButtonStyle.danger, custom_id="obsidian_tower_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"This tower's End Crystal {END_CRYSTAL_EMOJI} has already been broken!", ephemeral=True)
                return
            damage = get_pve_damage_multiplier(interaction.user.id)
            self.tower_hp = max(0, self.tower_hp - damage)
            # Track tower damage for plant rewards when Ender Dragon is defeated (no achievement / enemy defeat)
            ender_dragon_tower_attackers.setdefault(self.guild_id, {})[interaction.user.id] = (
                ender_dragon_tower_attackers[self.guild_id].get(interaction.user.id, 0) + damage)
            if self.tower_hp <= 0:
                self.children[1].disabled = False
                self.children[1].style = discord.ButtonStyle.primary
            progress_embed = self._embed(interaction.user.display_name)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=progress_embed, view=self)

    @discord.ui.button(label="Break End Crystal", style=discord.ButtonStyle.secondary, emoji=END_CRYSTAL_EMOJI_PARTIAL, custom_id="obsidian_tower_break_crystal", disabled=True)
    async def break_crystal(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    f"This End Crystal {END_CRYSTAL_EMOJI} has already been broken!", ephemeral=True)
                return
            if self.tower_hp > 0:
                await safe_interaction_response(interaction, interaction.response.send_message,
                    "Reduce the tower's HP to 0 first!", ephemeral=True)
                return
            self.defeated = True
            button.disabled = True
            button.label = "<:endcrystal:1479156246081835050> Crystal Broken!"
            if self.guild_id in ender_dragon_towers:
                ender_dragon_towers[self.guild_id].discard(self.channel_id)
            victory_embed = self._embed()
            victory_embed.set_footer(text="The tower has no effect!")
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=self)


class EnderDragonView(discord.ui.View):
    """Ender Dragon boss: HP stored in entry for regen task. Regen 10 HP/sec while any Obsidian Tower stands."""

    def __init__(self, boss: dict, entry: dict, channel_id: int, guild_id: int, area_multiplier: float):
        super().__init__(timeout=None)
        self.boss = boss
        self.entry = entry  # shared ref: hp, max_hp, message_id
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.area_multiplier = area_multiplier
        self.attackers: dict[int, int] = {}
        self.defeated = False
        self._lock = asyncio.Lock()

    def _hp_bar(self) -> str:
        hp, max_hp = self.entry["hp"], self.entry["max_hp"]
        filled = max(0, round((hp / max_hp) * 20))
        empty = 20 - filled
        return f"{'🟥' * filled}{'⬛' * empty}"

    def _embed(self, last_hit: str | None = None) -> discord.Embed:
        hp, max_hp = self.entry["hp"], self.entry["max_hp"]
        towers_up = len(ender_dragon_towers.get(self.guild_id, set()))
        embed = discord.Embed(
            title=f"🚨 {self.boss['emoji']} {self.boss['name']} 🚨",
            description=self.boss["description"],
            color=self.boss["color"])
        embed.add_field(name="HP", value=f"**{hp}** / **{max_hp}**\n{self._hp_bar()}", inline=False)
        if towers_up > 0:
            embed.add_field(name="❤️ Regeneration", value=f"**{towers_up}** End Crystal(s) {END_CRYSTAL_EMOJI} still power the dragon. It recovers **{towers_up * ENDER_DRAGON_REGEN_PER_CRYSTAL}** HP/sec!", inline=False)
        if last_hit:
            embed.add_field(name="⚔️ Last Hit", value=f"**{last_hit}**!", inline=False)
        embed.set_footer(text="All gathering channels are BLOCKED until the Ender Dragon is defeated!")
        return embed

    @discord.ui.button(label="⚔️", style=discord.ButtonStyle.danger, custom_id="pve_ender_dragon_attack")
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        async with self._lock:
            if self.defeated:
                return  # Already dead; send nothing to avoid flooding chat when spam-clicking
            damage = get_pve_damage_multiplier(interaction.user.id)
            self.entry["hp"] = max(0, self.entry["hp"] - damage)
            self.attackers[interaction.user.id] = self.attackers.get(interaction.user.id, 0) + damage

            if self.entry["hp"] <= 0:
                self.defeated = True
                button.disabled = True
                button.label = "☠️ Defeated!"
                button.style = discord.ButtonStyle.secondary
                victory_embed = discord.Embed(
                    title=f"☠️ {self.boss['emoji']} {self.boss['name']} Defeated! ☠️",
                    description=self.boss["defeat_msg"],
                    color=discord.Color.gold())
                victory_embed.add_field(name="HP", value=f"**0** / **{self.entry['max_hp']}**\n{self._hp_bar()}", inline=False)
                participants_lines = []
                for uid, dmg in sorted(self.attackers.items(), key=lambda x: -x[1]):
                    member = interaction.guild.get_member(uid)
                    name = member.display_name if member else f"User {uid}"
                    participants_lines.append(f"**{name}** — {dmg} damage")
                victory_embed.add_field(name="🏆 Contributors", value="\n".join(participants_lines) or "No participants", inline=False)
                victory_embed.set_footer(text="Rewards are being distributed…")
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=victory_embed, view=self)

                _pve_boss_defeated_pending.setdefault(self.guild_id, []).append((self.boss, dict(self.attackers)))
                boss_list = active_boss_events.get(self.guild_id, [])
                if self.entry in boss_list:
                    boss_list = [e for e in boss_list if e is not self.entry]
                    if not boss_list:
                        active_boss_events.pop(self.guild_id, None)
                    else:
                        active_boss_events[self.guild_id] = boss_list
                ender_dragon_towers.pop(self.guild_id, None)
                task = ender_dragon_regen_tasks.pop(self.guild_id, None)
                if task and not task.done():
                    task.cancel()
                plantera_bulb_eligible_guilds[self.guild_id] = time.time()

                if self.guild_id not in active_boss_events:
                    pending = _pve_boss_defeated_pending.pop(self.guild_id, [])
                    guild = interaction.guild
                    channels = _get_guild_gather_channels(guild)
                    boss, _ = pending[0]
                    broadcast_desc = boss.get("server_defeat_msg", boss["defeat_msg"])
                    broadcast_embed = discord.Embed(
                        title=f"☠️ {boss['emoji']} {boss['name']} Defeated! ☠️",
                        description=broadcast_desc,
                        color=discord.Color.gold())
                    broadcast_embed.set_footer(text="Gathering channels are now unblocked.")
                    for ch in channels:
                        try:
                            if ch.permissions_for(guild.me).send_messages:
                                await ch.send(embed=broadcast_embed)
                        except Exception as e:
                            print(f"Boss defeat broadcast failed in {ch.name}: {e}")
                    # Merge dragon + tower hits so users get plants for both (one reward, Ender Dragon only counts for achievement)
                    tower_attackers = ender_dragon_tower_attackers.pop(self.guild_id, {})
                    all_user_ids = set(self.attackers.keys()) | set(tower_attackers.keys())
                    combined_attackers = {uid: self.attackers.get(uid, 0) + tower_attackers.get(uid, 0) for uid in all_user_ids}
                    dragon_max_hp = self.entry["max_hp"]
                    asyncio.create_task(_pve_distribute_rewards(
                        interaction, self.boss, combined_attackers, self.channel_id, self.area_multiplier, max_hp=dragon_max_hp))
                return

            progress_embed = self._embed(interaction.user.display_name)
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=progress_embed, view=self)


def _ender_dragon_embed_from_entry(entry: dict, boss: dict, guild_id: int) -> discord.Embed:
    """Build the Ender Dragon embed from entry state (for regen loop; no view ref)."""
    hp, max_hp = entry["hp"], entry["max_hp"]
    filled = max(0, round((hp / max_hp) * 20))
    empty = 20 - filled
    bar = f"{'🟥' * filled}{'⬛' * empty}"
    towers_up = len(ender_dragon_towers.get(guild_id, set()))
    embed = discord.Embed(
        title=f"🚨 {boss['emoji']} {boss['name']} 🚨",
        description=boss["description"],
        color=boss["color"])
    embed.add_field(name="HP", value=f"**{hp}** / **{max_hp}**\n{bar}", inline=False)
    if towers_up > 0:
        embed.add_field(name="❤️ Regeneration", value=f"**{towers_up}** End Crystal(s) {END_CRYSTAL_EMOJI} still power the dragon. It recovers **{towers_up * ENDER_DRAGON_REGEN_PER_CRYSTAL}** HP/sec!", inline=False)
    embed.set_footer(text="All gathering channels are BLOCKED until the Ender Dragon is defeated!")
    return embed


async def _ender_dragon_regen_loop(guild_id: int):
    """Every second, for each standing Obsidian Tower (End Crystal), heal the Ender Dragon by ENDER_DRAGON_REGEN_PER_CRYSTAL (capped at max_hp)."""
    while True:
        try:
            await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            break
        if guild_id not in active_boss_events:
            break
        events = active_boss_events[guild_id]
        if not events or events[0]["boss"]["id"] != ENDER_DRAGON_ID:
            break
        towers_up = len(ender_dragon_towers.get(guild_id, set()))
        if towers_up == 0:
            continue
        entry = events[0]
        regen = towers_up * ENDER_DRAGON_REGEN_PER_CRYSTAL
        entry["hp"] = min(entry["max_hp"], entry["hp"] + regen)
        message_id = entry.get("message_id")
        if not message_id:
            continue
        try:
            guild = bot.get_guild(guild_id)
            if not guild:
                continue
            channel = guild.get_channel(entry["channel_id"])
            if not channel or not isinstance(channel, discord.TextChannel):
                continue
            message = await channel.fetch_message(message_id)
            boss = entry["boss"]
            embed = _ender_dragon_embed_from_entry(entry, boss, guild_id)
            await message.edit(embed=embed)
        except Exception as e:
            print(f"Ender Dragon regen embed update failed: {e}")


async def trigger_ender_dragon_event(channel: discord.TextChannel, area_multiplier: float):
    """Spawn Ender Dragon in the trigger channel and Obsidian Towers in all other gather channels."""
    guild = channel.guild
    guild_id = guild.id
    if guild_id in active_boss_events:
        return
    boss = next(b for b in PVE_BOSSES if b["id"] == ENDER_DRAGON_ID)
    hp = boss["hp_range"][0]
    entry = {"channel_id": channel.id, "boss": boss, "hp": hp, "max_hp": hp}
    active_boss_events[guild_id] = [entry]

    channels = _get_guild_gather_channels(guild)
    other_channels = [ch for ch in channels if ch.id != channel.id]
    ender_dragon_towers[guild_id] = {ch.id for ch in other_channels}
    ender_dragon_tower_attackers[guild_id] = {}

    embed = discord.Embed(
        title=f"🚨 {boss['emoji']} {boss['name']} 🚨",
        description=boss["description"],
        color=boss["color"])
    embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{'🟥' * 20}", inline=False)
    embed.add_field(name="❤️ Regeneration", value=f"Destroy the **End Crystals** {END_CRYSTAL_EMOJI} on Obsidian Towers in other channels to stop the dragon's regeneration!", inline=False)
    embed.add_field(name="⚔️ How to Fight", value="Press **Attack**!", inline=False)
    embed.set_footer(text="All gathering channels are BLOCKED until the Ender Dragon is defeated!")
    view = EnderDragonView(boss=boss, entry=entry, channel_id=channel.id, guild_id=guild_id, area_multiplier=area_multiplier)
    msg = await channel.send(embed=embed, view=view)
    entry["message_id"] = msg.id

    for ch in other_channels:
        try:
            if ch.permissions_for(guild.me).send_messages:
                tower_view = ObsidianTowerView(channel_id=ch.id, guild_id=guild_id)
                tower_embed = tower_view._embed()
                await ch.send(embed=tower_embed, view=tower_view)
        except Exception as e:
            print(f"Obsidian Tower spawn failed in {ch.name}: {e}")
            ender_dragon_towers[guild_id].discard(ch.id)

    task = asyncio.create_task(_ender_dragon_regen_loop(guild_id))
    ender_dragon_regen_tasks[guild_id] = task


async def _delayed_boss_spawn(guild: discord.Guild, channel: discord.TextChannel, boss: dict, area_multiplier: float, delay: float = 60.0):
    try:
        await asyncio.sleep(delay)
        if guild.id not in active_boss_events:
            try:
                if boss["id"] in PVE_BOSSES_TWINS_IDS:
                    await trigger_twins_boss_event(channel, area_multiplier)
                elif boss["id"] == ENDER_DRAGON_ID:
                    await trigger_ender_dragon_event(channel, area_multiplier)
                else:
                    await trigger_boss_event(channel, boss, area_multiplier)
            except Exception as e:
                print(f"Delayed boss spawn failed: {e}")
    finally:
        pending_boss_spawn_guild_ids.discard(guild.id)


# Bosses that can spawn randomly: excludes Plantera (bulb only); Mothron only when Solar Eclipse; Queen Bee only via Larva.
def _get_random_spawn_bosses():
    is_solar_eclipse = get_active_event_by_type("solar_eclipse") is not None
    return [b for b in PVE_BOSSES if b["id"] != "plantera" and b["id"] != "queen_bee"
            and (b["id"] != "mothron" or is_solar_eclipse)]


class PlanteraBulbView(discord.ui.View):
    """Break or Ignore the Plantera Bulb. Break summons Plantera in this channel."""

    def __init__(self, channel_id: int, guild_id: int, area_multiplier: float):
        super().__init__(timeout=120.0)
        self.channel_id = channel_id
        self.guild_id = guild_id
        self.area_multiplier = area_multiplier
        self.resolved = False

    @discord.ui.button(label="Break", style=discord.ButtonStyle.danger, custom_id="plantera_break")
    async def break_bulb(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if self.resolved:
            await safe_interaction_response(interaction, interaction.response.send_message, "Someone already chose!", ephemeral=True)
            return
        self.resolved = True
        self.stop()
        plantera = next(b for b in PVE_BOSSES if b["id"] == "plantera")
        plantera_bulb_eligible_guilds.pop(self.guild_id, None)
        await safe_interaction_response(interaction, interaction.response.edit_message,
            content="The bulb breaks! **Plantera** has been summoned!",
            view=None)
        channel = interaction.guild.get_channel(self.channel_id) or interaction.channel
        try:
            await trigger_boss_event(channel, plantera, self.area_multiplier)
        except Exception as e:
            print(f"Plantera spawn after bulb break failed: {e}")

    @discord.ui.button(label="Ignore", style=discord.ButtonStyle.secondary, custom_id="plantera_ignore")
    async def ignore_bulb(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=False):
            return
        if self.resolved:
            await safe_interaction_response(interaction, interaction.response.send_message, "Someone already chose!", ephemeral=True)
            return
        self.resolved = True
        self.stop()
        await safe_interaction_response(interaction, interaction.response.edit_message,
            content="The bulb is left alone. The jungle settles back into silence.",
            view=None)


def _pve_roll_items_and_batch_write(user_id: int, num_items: int, area_multiplier: float, enemy_id: str | None = None):
    """Roll N gather items using pure math, then write everything to DB in ONE operation.

    Returns (display_results, total_value).
    Only 1 DB read + 1–2 DB writes. If enemy_id is set, also records PvE defeat in the same thread.
    """
    full_data = get_user_gather_full_data(user_id)
    active_events = get_active_events_cached()

    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)

    # Pre-compute event-adjusted item weights once
    item_weights = None
    hourly_eid = hourly_event.get("effects", {}).get("event_id", "") if hourly_event else ""
    daily_eid = daily_event.get("effects", {}).get("event_id", "") if daily_event else ""

    if hourly_eid == "may_flowers":
        item_weights = [1.6 if i["category"] == "Flower" else 1.0 for i in GATHERABLE_ITEMS]
    elif hourly_eid == "fruit_festival":
        item_weights = [1.5 if i["category"] == "Fruit" else 1.0 for i in GATHERABLE_ITEMS]
    elif hourly_eid == "vegetable_boom":
        item_weights = [1.5 if i["category"] == "Vegetable" else 1.0 for i in GATHERABLE_ITEMS]

    # Pre-compute user multipliers once (all from full_data, zero extra DB calls)
    user_upgrades = full_data.get("basket_upgrades", {})
    basket_tier = user_upgrades.get("basket", 0)
    basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"] if basket_tier > 0 else 1.0
    soil_tier = user_upgrades.get("soil", 0)
    base_gmo_chance = 0.05 + (SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0)
    if full_data.get("shop_inventory", {}).get("mutagenic_serum", 0) >= 1:
        base_gmo_chance += 0.07

    bloom_mult = 1.0 + (full_data.get("tree_rings", 0) * 0.005)
    water_base = 1.0 + (full_data.get("water_count", 0) * 0.01)
    water_mult = (1.0 + (water_base - 1.0) * 2) if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1 else water_base
    ach_mult = get_achievement_multiplier(user_id, full_data=full_data)
    daily_rate = 0.04 if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1 else 0.02
    daily_mult = 1.0 + (full_data.get("consecutive_water_days", 0) * daily_rate)
    rank_mult = get_rank_perma_buff_multiplier(user_id, full_data=full_data)
    hoe_enchant = full_data.get("hoe_enchantment")
    enchant_pct = hoe_enchant.get("money_bonus", 0) if hoe_enchant else 0
    has_scarecrow = full_data.get("shop_inventory", {}).get("scarecrow", 0) >= 1
    has_bloomstone = full_data.get("shop_inventory", {}).get("bloomstone", 0) >= 1

    if hourly_eid == "basket_boost":
        basket_multiplier *= 1.5
    value_multiplier = 1.0
    if hourly_eid == "bumper_crop":
        value_multiplier *= 2.0
    elif hourly_eid == "lucky_strike":
        value_multiplier *= 1.25
    if daily_eid == "double_money":
        value_multiplier *= 1.5
    elif daily_eid == "harvest_festival":
        value_multiplier *= 1.5

    gmo_chance = min(base_gmo_chance + (0.20 if hourly_eid == "radiation_leak" else 0)
                     + (0.10 if daily_eid == "gmo_surge" else 0), 1.0)

    # Additive boost factor (computed once)
    additive_boost = (bloom_mult - 1.0) + (water_mult - 1.0) + (ach_mult - 1.0) + (daily_mult - 1.0) + enchant_pct

    # Pre-compute ripeness weights per category
    def _ripe_cfg(rlist):
        base_w = [r["chance"] for r in rlist]
        if hourly_eid == "perfect_ripeness":
            return rlist, base_w, True
        if daily_eid == "ripeness_rush":
            return rlist, [r["chance"] * 2 if "Perfect" in r["name"] else r["chance"] for r in rlist], False
        return rlist, base_w, False

    fruit_cfg = _ripe_cfg(LEVEL_OF_RIPENESS_FRUITS)
    veg_cfg = _ripe_cfg(LEVEL_OF_RIPENESS_VEGETABLES)
    flower_cfg = _ripe_cfg(LEVEL_OF_RIPENESS_FLOWERS)

    # Roll all items (pure CPU, zero DB)
    items_inc: dict[str, int] = {}
    ripeness_inc: dict[str, int] = {}
    almanac_pairs: list = []
    total_balance = 0.0
    display_results = []

    for _ in range(num_items):
        item = random.choices(GATHERABLE_ITEMS, weights=item_weights, k=1)[0] if item_weights else random.choice(GATHERABLE_ITEMS)
        bv = item["base_value"] * area_multiplier
        cat = item["category"]

        if hourly_eid == "may_flowers" and cat == "Flower":
            bv *= 3
        elif hourly_eid == "fruit_festival" and cat == "Fruit":
            bv *= 2
        elif hourly_eid == "vegetable_boom" and cat == "Vegetable":
            bv *= 2

        if cat == "Fruit":
            rlist, rw, pb = fruit_cfg
        elif cat == "Vegetable":
            rlist, rw, pb = veg_cfg
        elif cat == "Flower":
            rlist, rw, pb = flower_cfg
        else:
            rlist, rw, pb = [], [], False

        if rlist:
            rip = random.choices(rlist, weights=rw, k=1)[0]
            rm = rip["multiplier"] * 1.5 if pb else rip["multiplier"]
            fv = bv * rm
        else:
            rip = {"name": "Normal"}
            fv = bv

        if random.random() < gmo_chance:
            fv *= 2

        fv *= basket_multiplier * value_multiplier
        fv *= get_seasonal_multiplier(random.randint(0, 11), cat)[0]

        base_fv = fv
        fv = (base_fv + base_fv * additive_boost) * rank_mult

        if has_scarecrow:
            fv *= 1.10
        if has_bloomstone and cat == "Flower":
            fv *= 3.0

        # All money buffs apply to the SAME base value (additive stacking) per item
        base_for_buffs = float(fv)
        beta_mult = get_beta_tester_money_multiplier(user_id)
        sb_mult = get_server_booster_money_multiplier(user_id)
        prem_mult = get_premium_tier_money_multiplier(user_id)
        nether_mult = get_nether_star_money_multiplier(user_id)
        shadow_crystal_mult = get_shadow_crystal_money_multiplier(user_id)
        palace_mult = get_palace_treasure_money_multiplier(user_id)
        edward_mult = get_edward_splash_money_multiplier(user_id)
        eclipse_mult = get_eclipse_glasses_money_multiplier(user_id)
        fv = base_for_buffs * (1.0 + (beta_mult - 1.0) + (sb_mult - 1.0) + (prem_mult - 1.0) + (nether_mult - 1.0) + (shadow_crystal_mult - 1.0) + (palace_mult - 1.0) + (edward_mult - 1.0) + (eclipse_mult - 1.0))
        total_balance += fv
        name = item["name"]
        items_inc[name] = items_inc.get(name, 0) + 1
        ripeness_inc[rip["name"]] = ripeness_inc.get(rip["name"], 0) + 1
        almanac_pairs.append((name, rip["name"]))
        display_results.append({"name": name, "value": fv})

    # Paladin's Shield: 10% more money from PvE (wild animal/boss)
    if has_shop_item(user_id, "paladins_shield"):
        total_balance *= 1.10
    # Single batched DB write
    pre_total = full_data.get("gather_stats_total_items", 0) or full_data.get("total_forage_count", 0)
    pre_bloom_cycle = full_data.get("bloom_cycle_plants", 0)
    perform_harvest_batch_update(
        user_id=user_id,
        items_inc=items_inc,
        ripeness_inc=ripeness_inc,
        balance_increment=total_balance,
        num_items=num_items,
        pre_total_items=pre_total,
        pre_bloom_cycle=pre_bloom_cycle,
        set_cooldown=False,
        increment_command_count=False,
        almanac_pairs=almanac_pairs,
    )

    if enemy_id:
        add_pve_defeat(user_id, enemy_id)

    return display_results, total_balance


def _pve_cap_damage_by_hp(attackers: dict[int, int], max_hp: int) -> dict[int, int]:
    """Cap total reward damage to the entity's max HP; distribute proportionally so overkill doesn't over-reward."""
    total = sum(attackers.values())
    if total <= 0 or max_hp <= 0:
        return dict(attackers)
    cap = min(total, max_hp)
    if cap >= total:
        return dict(attackers)
    # Proportional split; use largest-remainder so sum of ints equals cap
    fracs = [(uid, (dmg / total) * cap) for uid, dmg in attackers.items()]
    int_parts = [(uid, int(f)) for uid, f in fracs]
    remainder = cap - sum(p for _, p in int_parts)
    # Sort by fractional part descending, give +1 to first 'remainder' users
    with_frac = [(uid, int(p), (fracs[i][1] - int(p))) for i, (uid, p) in enumerate(int_parts)]
    with_frac.sort(key=lambda x: -x[2])
    result = {}
    for i, (uid, base) in enumerate((x[0], x[1]) for x in with_frac):
        result[uid] = base + (1 if i < remainder else 0)
    return result


async def _pve_reward_followup(member: discord.Member, guild: discord.Guild, user_id: int, enemy_id: str | None) -> None:
    """Background: assign PLANTER role and send achievement DMs so the main reward path stays fast."""
    try:
        await assign_gatherer_role(member, guild)
    except Exception as e:
        print(f"Error assigning gatherer role after PvE for user {user_id}: {e}")
    try:
        if enemy_id:
            ach_key = PVE_ENEMY_TO_HIDDEN_ACHIEVEMENT.get(enemy_id)
            if ach_key and unlock_hidden_achievement(user_id, ach_key):
                await send_hidden_achievement_notification_dm(user_id, ach_key)
        total_defeats = get_user_total_pve_defeats(user_id)
        new_slayer_level = get_achievement_level_for_stat("slayer", total_defeats)
        cur_slayer = get_user_achievement_level(user_id, "slayer")
        if new_slayer_level > cur_slayer:
            set_user_achievement_level(user_id, "slayer", new_slayer_level)
            await send_achievement_notification_dm(user_id, "slayer", new_slayer_level)
        if PVE_MASTER_REQUIRED_IDS.issubset(set(get_user_pve_defeated(user_id))):
            if unlock_hidden_achievement(user_id, "pve_master"):
                await send_hidden_achievement_notification_dm(user_id, "pve_master")
    except Exception as e:
        print(f"PvE reward followup failed for user {user_id}: {e}")


async def _pve_distribute_rewards(interaction: discord.Interaction, animal: dict,
                                   attackers: dict[int, int], channel_id: int,
                                   area_multiplier: float, *, achievements_ephemeral: bool = False,
                                   max_hp: int | None = None):
    """Award plants to every participant. Record defeats, update PLANTER role, unlock achievements.
    Reward damage is capped by the entity's max_hp so overkill (e.g. 12 damage on 1 HP left) doesn't over-reward.
    Items + defeat are applied in one thread; reward DM is sent first; role + achievement DMs run in background."""
    channel = interaction.guild.get_channel(channel_id)
    enemy_id = animal.get("id")
    guild = interaction.guild

    if max_hp is not None:
        attackers = _pve_cap_damage_by_hp(attackers, max_hp)

    # Block bloom until rewards are distributed (so they can't bloom before getting PvE rewards)
    for uid in attackers:
        users_pending_pve_rewards.add(uid)

    async def reward_one_user(user_id: int, total_damage: int) -> None:
        try:
            member = guild.get_member(user_id)
            if not member:
                return

            # Single thread: roll items, batch write, and record PvE defeat
            results, total_value = await asyncio.to_thread(
                _pve_roll_items_and_batch_write, user_id, total_damage, area_multiplier, enemy_id)

            plant_emojis = [get_item_display_emoji(r["name"]) for r in results]
            emoji_display = " ".join(plant_emojis)
            is_sans = enemy_id == "sans"
            if is_sans:
                header = (
                    f"You made **{total_damage}** attack{'s' if total_damage != 1 else ''} "
                    f"and gathered **{total_damage}** plant{'s' if total_damage != 1 else ''}!\n\n")
            else:
                header = (
                    f"You dealt **{total_damage}** damage "
                    f"and gathered **{total_damage}** plant{'s' if total_damage != 1 else ''}!\n\n")
            max_emoji_len = 4000 - len(header)
            if len(emoji_display) > max_emoji_len:
                emoji_display = emoji_display[:max_emoji_len - 5] + " …"

            reward_embed = discord.Embed(
                title=f"🎁 PvE Rewards — {animal['emoji']} {animal['name']}",
                description=f"{header}{emoji_display}",
                color=discord.Color.green())
            reward_embed.add_field(
                name="💰 Total Earned", value=f"**{format_money(total_value)}**", inline=True)
            reward_embed.set_footer(text="Thanks for defending the gathering grounds!")

            # Send reward DM first so user sees rewards quickly
            try:
                await member.send(embed=reward_embed)
            except Exception:
                pass
            # Role + achievement DMs in background so they don't delay the reward
            asyncio.create_task(_pve_reward_followup(member, guild, user_id, enemy_id))
        except Exception as e:
            print(f"PvE reward failed for user {user_id}: {e}")
        finally:
            users_pending_pve_rewards.discard(user_id)

    try:
        tasks = [reward_one_user(user_id, total_damage) for user_id, total_damage in attackers.items()]
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"Error distributing PvE rewards: {e}")
    finally:
        for uid in attackers:
            users_pending_pve_rewards.discard(uid)


async def trigger_bullet_ant_swarm_event(channel: discord.TextChannel, area_multiplier: float):
    """Spawn a Bullet Ant Swarm: one informational intro embed, then 4–9 individual Bullet Ant embeds with attack buttons."""
    if channel.id in active_pve_events:
        return
    n_ants = random.randint(*BULLET_ANT_SWARM_COUNT_RANGE)
    ant_hps = [random.randint(*BULLET_ANT_ANIMAL["hp_range"]) for _ in range(n_ants)]
    swarm_state = {
        "intro_message_id": None,
        "ants_remaining": n_ants,
        "attackers": {},
        "defeat_msg": BULLET_ANT_SWARM_DEFEAT,
        "area_multiplier": area_multiplier,
        "channel_id": channel.id,
        "total_hp": sum(ant_hps),
    }
    active_pve_events[channel.id] = {"swarm": True, "swarm_type": "bullet_ant", "swarm_state": swarm_state, "start_time": time.time()}

    try:
        intro_embed = discord.Embed(
            title="🚨 🐜 Bullet Ant Swarm has appeared! 🚨",
            description=BULLET_ANT_SWARM_INTRO,
            color=0x2C1810)
        intro_embed.set_footer(text="Defeat each ant below! All gathering commands are BLOCKED until the swarm is defeated.")
        intro_msg = await channel.send(embed=intro_embed)
        swarm_state["intro_message_id"] = intro_msg.id

        for ant_hp in ant_hps:
            ant_embed = discord.Embed(
                title=f"🚨 {BULLET_ANT_ANIMAL['emoji']} Bullet Ant 🚨",
                description=BULLET_ANT_ANIMAL["description"],
                color=BULLET_ANT_ANIMAL["color"])
            filled = max(0, round((ant_hp / ant_hp) * 20))
            ant_embed.add_field(name="HP", value=f"**{ant_hp}** / **{ant_hp}**\n{'🟥' * 20}", inline=False)
            view = BulletAntView(ant_hp=ant_hp, channel_id=channel.id, swarm_state=swarm_state)
            await channel.send(embed=ant_embed, view=view)
    except Exception:
        active_pve_events.pop(channel.id, None)
        raise


async def trigger_bee_swarm_event(channel: discord.TextChannel, area_multiplier: float):
    """Spawn a Bee Swarm (underground-jungle only): intro embed then 4–6 Bees (1–3 HP each). Defeat can spawn Larva → Queen Bee."""
    if channel.id in active_pve_events:
        return
    n_bees = random.randint(*BEE_SWARM_COUNT_RANGE)
    bee_hps = [random.randint(*BEE_ANIMAL["hp_range"]) for _ in range(n_bees)]
    swarm_state = {
        "intro_message_id": None,
        "bees_remaining": n_bees,
        "attackers": {},
        "defeat_msg": BEE_SWARM_DEFEAT,
        "area_multiplier": area_multiplier,
        "channel_id": channel.id,
        "total_hp": sum(bee_hps),
    }
    active_pve_events[channel.id] = {"swarm": True, "swarm_type": "bee", "swarm_state": swarm_state, "start_time": time.time()}

    try:
        intro_embed = discord.Embed(
            title="🚨 🐝 Bee Swarm has appeared! 🚨",
            description=BEE_SWARM_INTRO,
            color=0xFFC800)
        intro_embed.set_footer(text="Defeat each bee below! All gathering commands are BLOCKED until the swarm is defeated.")
        intro_msg = await channel.send(embed=intro_embed)
        swarm_state["intro_message_id"] = intro_msg.id

        for bee_hp in bee_hps:
            bee_embed = discord.Embed(
                title=f"🚨 {BEE_ANIMAL['emoji']} Bee 🚨",
                description=BEE_ANIMAL["description"],
                color=BEE_ANIMAL["color"])
            bee_embed.add_field(name="HP", value=f"**{bee_hp}** / **{bee_hp}**\n{'🟥' * 20}", inline=False)
            view = BeeView(bee_hp=bee_hp, channel_id=channel.id, swarm_state=swarm_state)
            await channel.send(embed=bee_embed, view=view)
    except Exception:
        active_pve_events.pop(channel.id, None)
        raise


def _weighted_choice_jungle():
    """Pick one underground-jungle animal by spawn_weight (Moth rarer)."""
    weights = [a.get("spawn_weight", 1.0) for a in PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE_SINGLE]
    return random.choices(PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE_SINGLE, weights=weights, k=1)[0]


async def trigger_pve_event(channel: discord.TextChannel, area_multiplier: float):
    """Spawn a wild animal PvE event in the given channel and lock it from commands.
    In #underground-jungle only: use jungle-only animals and Bee Swarm. Other channels: normal animals and Bullet Ant Swarm."""
    if channel.id in active_pve_events:
        return

    channel_name = (channel.name or "").lower().replace(" ", "-")
    is_underground_jungle = channel_name == "underground-jungle"

    if is_underground_jungle:
        if random.random() < PVE_SWARM_TRIGGER_CHANCE:
            await trigger_bee_swarm_event(channel, area_multiplier)
            return
        # Underground jungle: spawn either jungle-specific or regular animals
        if random.random() < 0.5:
            animal = _weighted_choice_jungle()
        else:
            animal = random.choice(PVE_WILD_ANIMALS_SINGLE)
    else:
        if random.random() < PVE_SWARM_TRIGGER_CHANCE:
            await trigger_bullet_ant_swarm_event(channel, area_multiplier)
            return
        animal = random.choice(PVE_WILD_ANIMALS_SINGLE)

    hp = random.randint(*animal["hp_range"])

    active_pve_events[channel.id] = {"animal": animal, "hp": hp, "start_time": time.time()}

    try:
        embed = discord.Embed(
            title=f"🚨 {animal['emoji']} Wild {animal['name']} Appeared! 🚨",
            description=animal["description"],
            color=animal["color"])
        hp_bar_filled = "🟥" * 20
        embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{hp_bar_filled}", inline=False)
        embed.add_field(
            name="⚔️ How to Fight",
            value="Press the **Attack** button below!",
            inline=False)
        embed.set_footer(text="All gathering commands are BLOCKED until the wild animal is defeated")

        view = WildAnimalView(animal=animal, hp=hp, channel_id=channel.id, area_multiplier=area_multiplier)
        await channel.send(embed=embed, view=view)
    except Exception:
        active_pve_events.pop(channel.id, None)
        raise


@bot.tree.command(name="gather", description="Gather a random item from nature!")
async def gather(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        channel_name = interaction.channel.name.lower() if hasattr(interaction.channel, 'name') else ""
        user_id = interaction.user.id

        # Quick validation (no DB call)
        if channel_name not in VALID_GATHERING_CHANNELS:
            channels_list = ", ".join(f"**{GATHERING_AREAS[a]['display_name']}**" for a in GATHERING_AREAS)
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You can only use this command in gathering channels: {channels_list}", ephemeral=True)
            return

        # Check if user is on Sans death timer
        if user_id in sans_death_timers:
            death_end_time = sans_death_timers[user_id]
            current_time = time.time()
            if current_time < death_end_time:
                time_left = int(death_end_time - current_time)
                minutes = time_left // 60
                seconds = time_left % 60
                time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"{SOUL_EMOJI} **GAME OVER** - You're dead for **{time_str}**.", ephemeral=True)
                return
            else:
                # Timer expired, remove from dict
                sans_death_timers.pop(user_id, None)

        # Block commands while a PvE wild animal event is active in this channel
        # Block if a boss is active in this guild (all gather channels blocked until boss defeated)
        guild_id = interaction.guild.id if interaction.guild else 0
        if guild_id and guild_id in active_boss_events:
            boss_list = active_boss_events[guild_id]
            ch_id = boss_list[0]["channel_id"] if boss_list else 0
            boss_ch = interaction.guild.get_channel(ch_id) if ch_id else None
            where = f" in {boss_ch.mention}" if boss_ch else ""
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🚨 A **boss** is terrorizing the gathering grounds! Defeat it{where} before you can gather again!", ephemeral=True)
            return

        if interaction.channel.id in active_pve_events:
            pve_info = active_pve_events[interaction.channel.id]
            if pve_info.get("swarm"):
                animal_name, animal_emoji = ("Bee Swarm", "🐝") if pve_info.get("swarm_type") == "bee" else ("Bullet Ant Swarm", "🐜")
            else:
                animal_name = pve_info["animal"]["name"]
                animal_emoji = pve_info["animal"]["emoji"]
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🚨 {animal_emoji} A wild **{animal_name}** is terrorizing this channel! "
                f"Defeat it before you can gather again!", ephemeral=True)
            return

        area = GATHERING_AREAS[channel_name]

        # === ONE thread call: sync roles to DB + data fetch + area check + cooldown + gather + chain roll ===
        result = await asyncio.to_thread(
            _gather_critical_path, interaction.user, user_id, channel_name, area)

        # --- handle early-exit cases ---
        if result.get("area_error"):
            await safe_interaction_response(interaction, interaction.followup.send,
                result["area_error"], ephemeral=True)
            return

        if result.get("on_cooldown"):
            time_left = result["time_left"]
            if result["is_roulette"]:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot /gather for {time_left // 60} minute(s)", ephemeral=True)
            elif result.get("almost_unlocked"):
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"You must wait {time_left} seconds before gathering again, {interaction.user.name}.", ephemeral=True)
                await send_hidden_achievement_notification(interaction, "almost_got_it")
            else:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"You must wait {time_left} seconds before gathering again, {interaction.user.name}.", ephemeral=True)
            return

        gather_result = result["gather_result"]
        full_data = result["full_data"]
        chain_triggered = result["chain_triggered"]
        area_mult = result.get("area_mult", 1.0)

        # --- build embed (pure computation, no DB) ---
        is_crit = gather_result.get('is_critical_gather', False)
        item_name = gather_result.get("name", "—")
        item_emoji = get_item_display_emoji(item_name)
        item_display = f"{item_name} {item_emoji}" if item_emoji and item_emoji not in item_name else item_name
        rip_emoji = get_ripeness_imbue_emoji(gather_result.get("ripeness", ""))

        # Daily shop / item boosts that affected this gather
        item_boost_sources = []
        shop_inv = full_data.get("shop_inventory", {}) if full_data else {}
        category = gather_result.get("category")

        if shop_inv.get("scarecrow", 0) >= 1:
            item_boost_sources.append(("Scarecrow", 1))
        if category == "Flower" and shop_inv.get("bloomstone", 0) >= 1:
            item_boost_sources.append(("Bloomstone", 1))
        if shop_inv.get("alchemists_pocketwatch", 0) >= 1:
            item_boost_sources.append(("Alchemist's Pocketwatch", 1))
        if gather_result.get("extra_money_from_nether_star", 0) > 0:
            # Check for permanent shop item and dayboosts
            has_permanent = has_shop_item(user_id, "nether_star")
            dayboost_count = get_dayboost_count(user_id, "nether_star")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Nether Star", total_count))
        if gather_result.get("extra_money_from_black_shard", 0) > 0:
            # Check for permanent shop item and dayboosts
            has_permanent = has_shop_item(user_id, "black_shard")
            dayboost_count = get_dayboost_count(user_id, "black_shard")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Black Shard", total_count))

        if is_crit:
            hoe_enc = gather_result.get('hoe_enchant')
            hoe_name = hoe_enc.get("name", "Unknown") if hoe_enc else "Unknown"
            hoe_rarity = hoe_enc.get("rarity", "COMMON") if hoe_enc else "COMMON"
            hoe_rarity_display = RARITY_EMOJI.get(hoe_rarity, f"[{hoe_rarity}]")
            pre_crit_value = gather_result['value'] / 2
            desc_prefix = f"{rip_emoji} " if rip_emoji else ""
            embed = discord.Embed(
                title="\U0001f4a5 CRITICAL HIT! \U0001f4a5",
                description=(
                    f"{desc_prefix}**{interaction.user.name}** foraged for a(n) **{item_display}** "
                    f"and got a **CRIT**!\n\n"
                    f"\U0001f4a5 **2X MONEY** \U0001f4a5"),
                color=discord.Color.orange())
            embed.add_field(name="Value", value=f"**{format_money(gather_result['base_value'])}**", inline=True)
            embed.add_field(name="Ripeness", value=f"{rip_emoji} {gather_result['ripeness']}".strip(), inline=True)
            embed.add_field(name="GMO?", value=f"{'Yes ✨' if gather_result['is_gmo'] else 'No'}", inline=False)

            bloom_count = full_data.get("bloom_count", 0)
            if bloom_count > 0 and gather_result.get('extra_money_from_bloom', 0) > 0:
                multiplier_percent = (gather_result['bloom_multiplier'] - 1.0) * 100
                embed.add_field(name="<:TreeRing:1474244868288282817> Tree Ring Boost",
                    value=f"+{multiplier_percent:.1f}% - **+{format_money(gather_result['extra_money_from_bloom'])}**", inline=False)
            bloom_rank = _bloom_count_to_rank(bloom_count)
            if bloom_rank != "PINE I" and gather_result.get('extra_money_from_rank', 0) > 0:
                rank_percent = (gather_result['rank_perma_buff_multiplier'] - 1.0) * 100
                embed.add_field(name="⭐ Rank Boost",
                    value=f"+{rank_percent:.1f}% - **+${gather_result['extra_money_from_rank']:.2f}**", inline=False)
            if gather_result.get('extra_money_from_achievement', 0) > 0:
                achievement_percent = (gather_result['achievement_multiplier'] - 1.0) * 100
                embed.add_field(name="🏆 Achievement Boost",
                    value=f"+{achievement_percent:.1f}% - **+{format_money(gather_result['extra_money_from_achievement'])}**", inline=False)
            if gather_result.get('extra_money_from_daily', 0) > 0:
                daily_bonus_percent = (gather_result['daily_bonus_multiplier'] - 1.0) * 100
                embed.add_field(name="💧 Water Streak Boost",
                    value=f"+{daily_bonus_percent:.1f}% - **+{format_money(gather_result['extra_money_from_daily'])}**", inline=False)

            embed.add_field(name="\u2728 Imbuement", value=f"**{hoe_name}** {hoe_rarity_display}", inline=False)
            embed.add_field(name="\U0001f4a5 Critical Multiplier",
                value=f"{format_money(pre_crit_value)} \u2192 **{format_money(gather_result['value'])}**", inline=False)
            if gather_result.get('extra_money_from_beta_tester', 0) > 0:
                beta_percent = (gather_result['beta_tester_multiplier'] - 1.0) * 100
                embed.add_field(name="🧪 Beta Tester!",
                    value=f"+{beta_percent:.1f}% - **+{format_money(gather_result['extra_money_from_beta_tester'])}**", inline=False)
            if gather_result.get('extra_money_from_server_booster', 0) > 0:
                booster_percent = (gather_result['server_booster_multiplier'] - 1.0) * 100
                embed.add_field(name=f"{SERVER_BOOSTER_EMOJI} Server Booster!",
                    value=f"+{booster_percent:.1f}% - **+{format_money(gather_result['extra_money_from_server_booster'])}**", inline=False)
            if gather_result.get('extra_money_from_premium', 0) > 0:
                tier = get_user_premium_tier(user_id)
                premium_percent = (gather_result['premium_tier_multiplier'] - 1.0) * 100
                embed.add_field(name=PREMIUM_DISPLAY.get(tier, "Premium"),
                    value=f"+{premium_percent:.1f}% - **+{format_money(gather_result['extra_money_from_premium'])}**", inline=False)
            if item_boost_sources:
                extra_ns = gather_result.get("extra_money_from_nether_star", 0)
                extra_bs = gather_result.get("extra_money_from_black_shard", 0)
                total_extra = extra_ns + extra_bs
                value_parts = [f"**+{format_money(total_extra)}**"] if total_extra > 0 else []
                value_parts.append("\n".join(_format_item_boost_source(name, count) for name, count in item_boost_sources))
                embed.add_field(
                    name="📦 Item Boost",
                    value="\n".join(value_parts),
                    inline=False
                )
            month_name = gather_result.get("month_name", "—")
            embed.add_field(name="\u200b", value=f"**~**\n{interaction.user.name} in {month_name}", inline=False)
            embed.add_field(name="\U0001f4b0 Total Earned", value=f"**{format_money(gather_result['value'])}**", inline=True)
            embed.add_field(name="\U0001f4b5 New Balance", value=f"**{format_money(gather_result['new_balance'])}**", inline=True)
        else:
            desc_prefix = f"{rip_emoji} " if rip_emoji else ""
            embed = discord.Embed(
                title="You Gathered!",
                description=f"{desc_prefix}You foraged for a(n) **{item_display}**!",
                color=discord.Color.green())
            embed.add_field(name="Value", value=f"**{format_money(gather_result['base_value'])}**", inline=True)
            embed.add_field(name="Ripeness", value=f"{rip_emoji} {gather_result['ripeness']}".strip(), inline=True)
            embed.add_field(name="GMO?", value=f"{'Yes ✨' if gather_result['is_gmo'] else 'No'}", inline=False)

            bloom_count = full_data.get("bloom_count", 0)
            if bloom_count > 0 and gather_result.get('extra_money_from_bloom', 0) > 0:
                multiplier_percent = (gather_result['bloom_multiplier'] - 1.0) * 100
                embed.add_field(name="<:TreeRing:1474244868288282817> Tree Ring Boost",
                    value=f"+{multiplier_percent:.1f}% - **+{format_money(gather_result['extra_money_from_bloom'])}**", inline=False)

            bloom_rank = _bloom_count_to_rank(bloom_count)
            if bloom_rank != "PINE I" and gather_result.get('extra_money_from_rank', 0) > 0:
                rank_percent = (gather_result['rank_perma_buff_multiplier'] - 1.0) * 100
                embed.add_field(name="⭐ Rank Boost",
                    value=f"+{rank_percent:.1f}% - **+{format_money(gather_result['extra_money_from_rank'])}**", inline=False)

            if gather_result.get('extra_money_from_achievement', 0) > 0:
                achievement_percent = (gather_result['achievement_multiplier'] - 1.0) * 100
                embed.add_field(name="🏆 Achievement Boost",
                    value=f"+{achievement_percent:.1f}% - **+{format_money(gather_result['extra_money_from_achievement'])}**", inline=False)

            if gather_result.get('extra_money_from_daily', 0) > 0:
                daily_bonus_percent = (gather_result['daily_bonus_multiplier'] - 1.0) * 100
                embed.add_field(name="💧 Water Streak Boost",
                    value=f"+{daily_bonus_percent:.1f}% - **+{format_money(gather_result['extra_money_from_daily'])}**", inline=False)

            if gather_result.get('extra_money_from_beta_tester', 0) > 0:
                beta_percent = (gather_result['beta_tester_multiplier'] - 1.0) * 100
                embed.add_field(name="🧪 Beta Tester!",
                    value=f"+{beta_percent:.1f}% - **+{format_money(gather_result['extra_money_from_beta_tester'])}**", inline=False)
            if gather_result.get('extra_money_from_server_booster', 0) > 0:
                booster_percent = (gather_result['server_booster_multiplier'] - 1.0) * 100
                embed.add_field(name=f"{SERVER_BOOSTER_EMOJI} Server Booster!",
                    value=f"+{booster_percent:.1f}% - **+{format_money(gather_result['extra_money_from_server_booster'])}**", inline=False)
            if gather_result.get('extra_money_from_premium', 0) > 0:
                tier = get_user_premium_tier(user_id)
                premium_percent = (gather_result['premium_tier_multiplier'] - 1.0) * 100
                embed.add_field(name=PREMIUM_DISPLAY.get(tier, "Premium"),
                    value=f"+{premium_percent:.1f}% - **+{format_money(gather_result['extra_money_from_premium'])}**", inline=False)
            if item_boost_sources:
                extra_ns = gather_result.get("extra_money_from_nether_star", 0)
                extra_bs = gather_result.get("extra_money_from_black_shard", 0)
                total_extra = extra_ns + extra_bs
                value_parts = [f"**+{format_money(total_extra)}**"] if total_extra > 0 else []
                value_parts.append("\n".join(_format_item_boost_source(name, count) for name, count in item_boost_sources))
                embed.add_field(
                    name="📦 Item Boost",
                    value="\n".join(value_parts),
                    inline=False
                )

            hoe_enc = gather_result.get('hoe_enchant')
            if hoe_enc:
                hoe_name = hoe_enc.get("name", "Unknown")
                hoe_rarity = hoe_enc.get("rarity", "COMMON")
                hoe_rarity_display = RARITY_EMOJI.get(hoe_rarity, f"[{hoe_rarity}]")
                embed.add_field(name="\u2728 Imbuement", value=f"**{hoe_name}** {hoe_rarity_display}", inline=False)

            month_name = gather_result.get("month_name", "—")
            embed.add_field(name="\u200b", value=f"**~**\n{interaction.user.name} in {month_name}", inline=False)
            embed.add_field(name="\U0001f4b0 Total Earned", value=f"**{format_money(gather_result['value'])}**", inline=True)
            embed.add_field(name="\U0001f4b5 New Balance", value=f"**{format_money(gather_result['new_balance'])}**", inline=True)

        # === Send the response ASAP (with optional STEAL button) ===
        view = None
        if result.get("stealable") and result.get("steal_payload"):
            view = StealView(
                victim_id=user_id,
                victim_planter_level=result.get("victim_planter_level", 1),
                steal_type="gather",
                steal_payload=result["steal_payload"],
                window_sec=STEAL_WINDOW_GATHER_SEC,
            )
        if view:
            msg = await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
            if msg:
                view._message = msg
        else:
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed)

        # Chain message (must be after the main embed)
        if chain_triggered:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🔗🔗 **CHAIN!** Your cooldown has been reset! Gather again! 🔗🔗")

        # Auto-log to #rares for Legendary/Netherite/Luminite/Celestial/Mikellion (GMO or not)
        rare_label, _ = _plant_rare_label(gather_result.get("ripeness", ""), gather_result.get("is_gmo", False))
        if rare_label:
            area_tag = "[" + channel_name.upper().replace("-", " ") + "]"
            asyncio.create_task(_post_rares_plant(
                interaction.guild, interaction.user, "GATHER",
                gather_result["name"], gather_result.get("category", "Item"),
                gather_result["value"], gather_result["ripeness"], gather_result.get("is_gmo", False),
                area_tag))

        # === Background: role assignment + achievements (user already has the response) ===
        asyncio.create_task(_gather_post_response(interaction, user_id, full_data, gather_result))

        # === PvE spawn: try Plantera (jungle only) -> boss -> wild animal, in order; at most one spawn per gather ===
        pve_spawned = False
        # 1. Plantera bulb (only in #underground-jungle after a recent boss defeat)
        if (not pve_spawned and channel_name == "underground-jungle" and guild_id
                and guild_id in plantera_bulb_eligible_guilds
                and (time.time() - plantera_bulb_eligible_guilds[guild_id]) <= PLANTERA_BULB_MAX_AGE_SEC
                and interaction.channel.id not in active_pve_events
                and guild_id not in active_boss_events
                and random.random() < PLANTERA_BULB_CHANCE):
            plantera_bulb_eligible_guilds.pop(guild_id, None)
            bulb_embed = discord.Embed(
                title="🌸 The jungle grows restless...",
                description="A **Plantera Bulb** spawned!",
                color=0x2d5a1a)
            view = PlanteraBulbView(interaction.channel.id, guild_id, area_mult)
            asyncio.create_task(interaction.channel.send(embed=bulb_embed, view=view))
            pve_spawned = True
        # 2. Boss spawn (rarer): 1-min warning then boss in this channel (only if no boss active, no enemy in any gather channel, no boss already scheduled)
        if (not pve_spawned and channel_name in VALID_GATHERING_CHANNELS and guild_id and guild_id not in active_boss_events
                and guild_id not in pending_boss_spawn_guild_ids
                and interaction.channel.id not in active_pve_events
                and not _guild_has_active_pve(interaction.guild)
                and not result.get("stealable")):
            _, boss_mult = _pve_spawn_multiplier()
            effective_boss_chance = min(1.0, PVE_BOSS_TRIGGER_CHANCE * boss_mult)
            if channel_name == "underground-jungle":
                effective_boss_chance = min(1.0, effective_boss_chance * UNDERGROUND_JUNGLE_BOSS_SPAWN_MULT)
            bosses_candidates = _get_random_spawn_bosses()
            if random.random() < effective_boss_chance and bosses_candidates:
                boss = random.choice(bosses_candidates)
                pending_boss_spawn_guild_ids.add(guild_id)
                await _send_boss_warning_embed(interaction.guild, boss, interaction.channel)
                asyncio.create_task(_delayed_boss_spawn(interaction.guild, interaction.channel, boss, area_mult, 60.0))
                pve_spawned = True
        # 3. PvE wild animal: only if no boss active and this channel has no PvE event
        if (not pve_spawned and channel_name in VALID_GATHERING_CHANNELS and interaction.channel.id not in active_pve_events
                and guild_id not in active_boss_events
                and not result.get("stealable")):
            animal_mult, _ = _pve_spawn_multiplier()
            effective_chance = min(1.0, PVE_TRIGGER_CHANCE_GATHER * animal_mult)
            if channel_name == "underground-jungle":
                effective_chance = min(1.0, effective_chance * UNDERGROUND_JUNGLE_ANIMAL_SPAWN_MULT)
            if random.random() < effective_chance:
                asyncio.create_task(trigger_pve_event(interaction.channel, area_mult))
    except Exception as e:
        traceback.print_exc()
        print(f"Error in gather command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


def _water_critical_path(user_id: int) -> dict:
    """Run in thread: all DB reads + logic + writes for /water. Returns dict for response and notifications."""
    EST_OFFSET = datetime.timedelta(hours=-5)
    current_time = time.time()
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_est = now_utc + EST_OFFSET
    current_date = now_est.date()
    current_hour = now_est.hour

    last_water_time = get_user_last_water_time(user_id)
    invite_reductions = get_invite_cooldown_reductions(user_id)
    has_double_water = invite_reductions.get("water_double", False)

    if has_double_water and current_hour < 12:
        next_reset_est = now_est.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        next_reset_est = (now_est + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    time_until_reset = (next_reset_est - now_est).total_seconds()

    already_watered = False
    if last_water_time > 0:
        last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
        last_water_est = last_water_utc + EST_OFFSET
        last_water_date = last_water_est.date()
        last_water_hour = last_water_est.hour
        if last_water_date == current_date:
            if has_double_water:
                in_afternoon_window = current_hour >= 12
                last_in_afternoon_window = last_water_hour >= 12
                if in_afternoon_window and last_in_afternoon_window:
                    already_watered = True
                elif not in_afternoon_window and not last_in_afternoon_window:
                    already_watered = True
            else:
                already_watered = True

    if already_watered:
        time_left = int(time_until_reset)
        if time_left < 60:
            time_msg = f"{time_left} second{'s' if time_left != 1 else ''}"
        elif time_left < 3600:
            minutes_left = time_left // 60
            seconds_left = time_left % 60
            time_msg = f"{minutes_left} minute{'s' if minutes_left != 1 else ''} and {seconds_left} second{'s' if seconds_left != 1 else ''}"
        else:
            hours_left = time_left // 3600
            minutes_left = (time_left % 3600) // 60
            seconds_left = time_left % 60
            time_msg = f"{hours_left} hour{'s' if hours_left != 1 else ''}, {minutes_left} minute{'s' if minutes_left != 1 else ''}, and {seconds_left} second{'s' if seconds_left != 1 else ''}"
        return {"already_watered": True, "time_msg": time_msg}

    consecutive_days = get_user_consecutive_water_days(user_id)
    is_first_water_today = True
    if last_water_time > 0:
        last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
        last_water_est = last_water_utc + EST_OFFSET
        last_water_date = last_water_est.date()
        if last_water_date == current_date:
            is_first_water_today = False
        else:
            yesterday_date = (now_est - datetime.timedelta(days=1)).date()
            if last_water_date != yesterday_date and last_water_date != current_date:
                consecutive_days = 0
    else:
        consecutive_days = 0

    if is_first_water_today:
        consecutive_days += 1
        set_user_consecutive_water_days(user_id, consecutive_days)

    update_user_last_water_time(user_id, current_time)
    increment_user_water_count(user_id)

    # Base daily water reward scales with premium rank (Seed/Sprout/Sapling/Evergreen)
    tier = get_user_premium_tier(user_id)
    base_per_day = PREMIUM_WATER_BASE_AMOUNTS.get(tier, PREMIUM_WATER_BASE_AMOUNTS[0])
    money_reward = normalize_money(consecutive_days * base_per_day)
    current_balance = get_user_balance(user_id)
    new_balance = normalize_money(current_balance + money_reward)
    update_user_balance(user_id, new_balance)
    daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)

    tree_rings_awarded = 0
    if consecutive_days == 5 and is_first_water_today:
        increment_tree_rings(user_id, 10)
        tree_rings_awarded = 10

    water_label = "💧💧 **Double Water!** " if not is_first_water_today else ""
    daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
    message = (
        f"{water_label}You've been rewarded with **{format_money(money_reward)}**. "
        f"Your streak is **{consecutive_days}**! (+{daily_bonus_percent:.1f}% money)"
    )
    if tree_rings_awarded > 0:
        message += f" You've been awarded **{tree_rings_awarded} Tree Rings**!"

    water_streak_level_up = None
    leap_year_unlocked = False
    if is_first_water_today:
        new_water_streak_level = get_achievement_level_for_stat("water_streak", consecutive_days)
        current_water_streak_level = get_user_achievement_level(user_id, "water_streak")
        if new_water_streak_level > current_water_streak_level:
            set_user_achievement_level(user_id, "water_streak", new_water_streak_level)
            water_streak_level_up = new_water_streak_level
        if consecutive_days == 29:
            leap_year_unlocked = unlock_hidden_achievement(user_id, "leap_year")

    return {
        "already_watered": False,
        "message": message,
        "water_streak_level_up": water_streak_level_up,
        "leap_year_unlocked": leap_year_unlocked,
    }


#/water command - daily watering system
@bot.tree.command(name="water", description="Water your plants daily for bonus rewards! (Resets at midnight)")
async def water(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        # Ensure premium tier cache matches current roles so water rewards
        # use the latest Seed/Sprout/Sapling/Evergreen rank.
        await asyncio.to_thread(sync_premium_tier_from_member, interaction.user)

        user_id = interaction.user.id
        result = await asyncio.to_thread(_water_critical_path, user_id)

        if result["already_watered"]:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"💧 {interaction.user.mention}, you need to wait **{result['time_msg']}** before watering your plants again!", ephemeral=False)
            return

        message = f"💧 {interaction.user.mention}, " + result["message"]
        await safe_interaction_response(interaction, interaction.followup.send, message, ephemeral=False)
        if result.get("water_streak_level_up") is not None:
            await send_achievement_notification(interaction, "water_streak", result["water_streak_level_up"])
        if result.get("leap_year_unlocked"):
            await send_hidden_achievement_notification(interaction, "leap_year")
    except Exception as e:
        print(f"Error in water command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


def _format_cooldown_seconds(seconds: int) -> str:
    """Format seconds as human-readable time left (e.g. '2h 15m' or 'Ready')."""
    if seconds <= 0:
        return "Ready"
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s" if s else f"{m}m"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if m or s:
        return f"{h}h {m}m {s}s" if s else f"{h}h {m}m"
    return f"{h}h"


def _format_reduction_seconds(seconds: int) -> str:
    """Format reduction in seconds for display (e.g. '−15s', '−5m', '−1h 20m')."""
    if seconds <= 0:
        return "−0s"
    if seconds < 60:
        return f"−{seconds}s"
    if seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"−{m}m {s}s" if s else f"−{m}m"
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if m or s:
        return f"−{h}h {m}m {s}s" if s else f"−{h}h {m}m"
    return f"−{h}h"


def _cooldowns_data_sync(user_id: int) -> dict:
    """Gather all cooldown remaining times for /cooldowns. Runs in thread."""
    data = {}
    current_time = time.time()
    EST_OFFSET = datetime.timedelta(hours=-5)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_est = now_utc + EST_OFFSET

    # /gather
    can_g, time_g, _ = can_gather(user_id)
    data["gather"] = 0 if can_g else time_g

    # /harvest
    can_h, time_h, _ = can_harvest(user_id)
    data["harvest"] = 0 if can_h else time_h

    # /mine
    last_mine = get_user_last_mine_time(user_id)
    mine_cd = MINE_COOLDOWN
    mine_cd = max(0, mine_cd - get_invite_cooldown_reductions(user_id).get("mine_reduction", 0))
    mine_cd = max(0, mine_cd - get_premium_cooldown_reductions(user_id).get("mine_reduction", 0))
    
    # Apply event cooldown reductions
    active_events = get_active_events_cached()
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    
    if hourly_event:
        event_id = hourly_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_harvest":  # Adrenaline Boost
            mine_cd = max(0, mine_cd - 450)  # 7.5 minutes = 450 seconds
    
    if daily_event:
        event_id = daily_event.get("effects", {}).get("event_id", "")
        if event_id == "speed_day":  # Overdrive
            mine_cd = max(0, mine_cd - 300)  # 5 minutes = 300 seconds
    
    if last_mine <= 0:
        data["mine"] = 0
    else:
        end = last_mine + mine_cd
        data["mine"] = max(0, int(end - current_time))

    # /dailyshop — resets at 12 AM EST
    data["dailyshop"] = max(0, int(_seconds_until_midnight_est()))

    # /water — midnight EST, or 12 PM & 12 AM EST if water_double
    has_double = get_invite_cooldown_reductions(user_id).get("water_double", False)
    if has_double and now_est.hour < 12:
        next_water_est = now_est.replace(hour=12, minute=0, second=0, microsecond=0)
    else:
        next_water_est = (now_est + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    target_utc = next_water_est - EST_OFFSET
    data["water"] = max(0, int((target_utc - now_utc).total_seconds()))

    # Death timer (/russian) — also blocks /gather, /harvest, /mine
    _, death_left = check_roulette_elimination_cooldown(user_id)
    data["death"] = max(0, death_left)
    # When dead, show same cooldown for /mine as /gather and /harvest (death time)
    if data["death"] > 0:
        data["mine"] = max(data["mine"], data["death"])

    # Death penalty duration (5 min with Gambler's Revolver, else 30 min)
    data["death_penalty_seconds"] = get_roulette_elimination_cooldown_seconds(user_id)
    data["has_gamblers_revolver"] = has_shop_item(user_id, "gamblers_revolver")

    # Total cooldown reductions (for display)
    invite_cd = get_invite_cooldown_reductions(user_id)
    premium_cd = get_premium_cooldown_reductions(user_id)
    active_events = get_active_events_cached()
    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)

    # Gather: shoes + events + hoe imbuement + invite + premium
    user_upgrades = get_user_basket_upgrades(user_id)
    shoes_tier = user_upgrades.get("shoes", 0)
    gather_red = (SHOES_UPGRADES[shoes_tier - 1]["reduction"] if shoes_tier > 0 else 0)
    if hourly_event and hourly_event.get("effects", {}).get("event_id") == "speed_harvest":
        gather_red += 8
    if daily_event and daily_event.get("effects", {}).get("event_id") == "speed_day":
        gather_red += 5
    hoe_enchant = get_user_hoe_attunement(user_id)
    if hoe_enchant:
        gather_red += hoe_enchant.get("cooldown_reduction", 0)
    gather_red += invite_cd.get("gather_reduction", 0)
    gather_red += premium_cd.get("gather_reduction", 0)
    data["gather_reduction_total"] = gather_red

    # Harvest: upgrade tier + events + tractor imbuement + invite + premium
    harvest_upgrades = get_user_harvest_upgrades(user_id)
    cooldown_tier = harvest_upgrades.get("cooldown", 0)
    harvest_red = (HARVEST_COOLDOWN_UPGRADES[cooldown_tier - 1]["reduction"] if cooldown_tier > 0 else 0)
    if hourly_event and hourly_event.get("effects", {}).get("event_id") == "speed_harvest":
        harvest_red += 120
    if daily_event and daily_event.get("effects", {}).get("event_id") == "speed_day":
        harvest_red += 60
    tractor_enchant = get_user_tractor_attunement(user_id)
    if tractor_enchant:
        harvest_red += tractor_enchant.get("cooldown_reduction", 0)
    harvest_red += invite_cd.get("harvest_reduction", 0)
    harvest_red += premium_cd.get("harvest_reduction", 0)
    data["harvest_reduction_total"] = harvest_red

    # Mine: invite + premium only
    data["mine_reduction_total"] = invite_cd.get("mine_reduction", 0) + premium_cd.get("mine_reduction", 0)

    return data


@bot.tree.command(name="cooldowns", description="View time remaining on your cooldowns")
async def cooldowns(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        # Sync premium tier from Discord roles so reductions reflect current Seed/Sprout/Sapling/Evergreen rank
        await asyncio.to_thread(sync_premium_tier_from_member, interaction.user)
        user_id = interaction.user.id
        data = await asyncio.to_thread(_cooldowns_data_sync, user_id)
        death_seconds = data["death"]
        is_dead = death_seconds > 0
        dead_suffix = " (Dead)" if is_dead else ""
        has_revolver = data.get("has_gamblers_revolver", False)
        death_penalty_desc = "5 min (Gambler's Revolver)" if has_revolver else "30 min"
        russian_status = "Alive" if death_seconds <= 0 else f"Dead ({_format_cooldown_seconds(death_seconds)})"
        lines = [
            f"**/gather** - {_format_cooldown_seconds(data['gather'])}{dead_suffix}",
            f"**/harvest** - {_format_cooldown_seconds(data['harvest'])}{dead_suffix}",
            f"**/mine** - {_format_cooldown_seconds(data['mine'])}{dead_suffix}",
            f"**New /dailyshop** - {_format_cooldown_seconds(data['dailyshop'])}",
            f"**/water** - {_format_cooldown_seconds(data['water'])}",
            f"**/russian Status** - {russian_status}",
            f"**Death penalty if eliminated** - {death_penalty_desc}",
        ]
        # Total cooldown reductions for /gather, /harvest, /mine
        gather_red = data.get("gather_reduction_total", 0)
        harvest_red = data.get("harvest_reduction_total", 0)
        mine_red = data.get("mine_reduction_total", 0)
        reduction_line = f"**Reductions:** /gather {_format_reduction_seconds(gather_red)}, /harvest {_format_reduction_seconds(harvest_red)}, /mine {_format_reduction_seconds(mine_red)}"
        lines.append(reduction_line)
        embed = discord.Embed(
            title=f"⏱️ {interaction.user.name}'s Cooldowns",
            description="\n".join(lines),
            color=discord.Color.blue(),
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in cooldowns command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


def _is_admin(member: discord.Member | None) -> bool:
    """Return True if the member has Administrator permission in the guild."""
    if member is None:
        return False
    return member.guild_permissions.administrator


@bot.tree.command(
    name="syncpremium",
    description="[Admin] Sync premium role from Discord purchase (fallback if not auto-assigned).",
)
@app_commands.describe(
    user="The member to sync (leave empty to sync yourself). Must have purchased a premium SKU for this server.",
)
@app_commands.default_permissions(administrator=True)
async def syncpremium(interaction: discord.Interaction, user: discord.Member | None = None):
    """
    Admin-only. Fetches the target user's entitlements from Discord's API and assigns
    the matching premium role if they purchased a SKU for this server. Use when the
    automatic entitlement event didn't assign the role (e.g. bot was restarted or event not supported).
    """
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not _is_admin(interaction.user):
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ Only server administrators can use this command.",
                ephemeral=True,
            )
            return
        if not PREMIUM_SKU_TO_TIER:
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ Premium SKUs are not configured.",
                ephemeral=True,
            )
            return
        guild = interaction.guild
        if guild is None:
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ This command must be used in a server.",
                ephemeral=True,
            )
            return
        target_member = user if user is not None else interaction.user
        target_member = guild.get_member(target_member.id)
        if target_member is None:
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ That user is not in this server.",
                ephemeral=True,
            )
            return
        user_id = target_member.id
        bot_token = token
        if not bot_token:
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ Bot token not available.",
                ephemeral=True,
            )
            return
        url = (
            "https://discord.com/api/v10/applications/@me/entitlements"
            f"?guild_id={guild.id}&user_id={user_id}"
        )
        headers = {"Authorization": f"Bot {bot_token}"}
        assigned_any = False
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[syncpremium] Entitlements API returned {resp.status}: {body[:200]}")
                    await safe_interaction_response(
                        interaction, interaction.followup.send,
                        "❌ Could not fetch entitlements from Discord. Try again later or check bot logs.",
                        ephemeral=True,
                    )
                    return
                data = await resp.json()
        if not isinstance(data, list):
            await safe_interaction_response(
                interaction, interaction.followup.send,
                "❌ Unexpected response from Discord.",
                ephemeral=True,
            )
            return
        for ent in data:
            if ent.get("deleted") or ent.get("consumed"):
                continue
            sku_id = ent.get("sku_id")
            if sku_id is None:
                continue
            assigned = await assign_premium_role_for_entitlement(guild, user_id, str(sku_id))
            if assigned:
                assigned_any = True
        await asyncio.to_thread(sync_premium_tier_from_member, target_member)
        if assigned_any:
            who = "your" if target_member.id == interaction.user.id else f"**{target_member.display_name}**'s"
            await safe_interaction_response(
                interaction, interaction.followup.send,
                f"✅ {who.capitalize()} premium role has been assigned based on their purchase.",
                ephemeral=True,
            )
        else:
            who = "You don't" if target_member.id == interaction.user.id else f"**{target_member.display_name}** doesn't"
            await safe_interaction_response(
                interaction, interaction.followup.send,
                f"{who} have any active premium purchases for this server, or already have the role. If they just bought, wait a moment and try again.",
                ephemeral=True,
            )
    except Exception as e:
        print(f"Error in syncpremium command: {e}")
        traceback.print_exc()
        await safe_interaction_response(
            interaction, interaction.followup.send,
            "❌ An error occurred. Please try again.",
            ephemeral=True,
        )


def _perform_harvest_for_user_sync(user_id: int, allow_chain: bool = True,
                                   area_multiplier: float = 1.0,
                                   full_data=None,
                                   set_cooldown: bool = False,
                                   increment_command_count: bool = False) -> dict:
    """Synchronous implementation of harvest logic.  Runs in a thread via
    ``asyncio.to_thread()`` to avoid blocking the event loop.

    When *full_data* (from ``get_user_harvest_full_data``) is supplied, all
    look-ups read from it and per-item DB writes are batched into **one**
    ``perform_harvest_batch_update`` call at the end.
    """
    active_events = get_active_events_cached()

    # ----- read upgrades (from pre-fetched data or individual queries) -----
    if full_data is not None:
        user_upgrades = full_data.get("basket_upgrades", {})
        harvest_upgrades = full_data.get("harvest_upgrades", {})
        tractor_enchant = full_data.get("tractor_enchantment")
        # Ensure tractor imbuement is a dict (fallback if serialization/document quirk drops it)
        if tractor_enchant is None or not isinstance(tractor_enchant, dict):
            tractor_enchant = get_user_tractor_attunement(user_id)
        bloom_multiplier = 1.0 + (full_data.get("tree_rings", 0) * 0.005)
        water_base = 1.0 + (full_data.get("water_count", 0) * 0.01)
        if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1:
            water_multiplier = 1.0 + (water_base - 1.0) * 2
        else:
            water_multiplier = water_base
        plants_before_harvest = full_data.get("gather_stats_total_items", 0)
        current_balance = full_data.get("balance", 0)
        achievement_multiplier = get_achievement_multiplier(user_id, full_data=full_data)
        daily_rate = 0.04 if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1 else 0.02
        daily_bonus_multiplier = 1.0 + (full_data.get("consecutive_water_days", 0) * daily_rate)
        rank_perma_buff_mult = get_rank_perma_buff_multiplier(user_id, full_data=full_data)
        pre_bloom_cycle = full_data.get("bloom_cycle_plants", 0)
    else:
        user_upgrades = get_user_basket_upgrades(user_id)
        harvest_upgrades = get_user_harvest_upgrades(user_id)
        tractor_enchant = get_user_tractor_attunement(user_id)
        bloom_multiplier = get_bloom_multiplier(user_id)
        water_multiplier = get_water_multiplier(user_id)
        plants_before_harvest = get_user_total_items(user_id)
        current_balance = get_user_balance(user_id)
        achievement_multiplier = get_achievement_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)
        rank_perma_buff_mult = get_rank_perma_buff_multiplier(user_id)
        pre_bloom_cycle = get_user_bloom_cycle_plants(user_id)

    basket_tier = user_upgrades.get("basket", 0)
    soil_tier = user_upgrades.get("soil", 0)
    car_tier = harvest_upgrades.get("car", 0)
    chain_tier = harvest_upgrades.get("chain", 0)
    fertilizer_tier = harvest_upgrades.get("fertilizer", 0)

    # Orchard plants: base (10) + car upgrade extra. Imbue plants: from tractor Nature's Favor (additional_plants).
    # Always add both so imbue bonus is never overwritten by orchard count.
    base_items = 10
    extra_items = HARVEST_CAR_UPGRADES[car_tier - 1]["extra_items"] if car_tier > 0 else 0
    orchard_plant_count = base_items + extra_items

    enchant_extra_plants = 0
    enchant_money_bonus = 0.0
    if tractor_enchant and isinstance(tractor_enchant, dict):
        enchant_extra_plants = tractor_enchant.get("additional_plants", 0)
        # Nature's Favor: derive from levels if additional_plants missing/zero (legacy or serialization)
        if enchant_extra_plants == 0:
            levels = tractor_enchant.get("levels") or {}
            enchant_extra_plants = int(levels.get("natures_favor", 0) or 0)
        else:
            enchant_extra_plants = int(enchant_extra_plants)
        enchant_money_bonus = tractor_enchant.get("money_bonus", 0)

    total_items_to_harvest = orchard_plant_count + enchant_extra_plants
    basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"] if basket_tier > 0 else 1.0
    base_gmo_chance = 0.05
    soil_gmo_boost = SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0
    gmo_chance = base_gmo_chance + soil_gmo_boost
    if full_data and full_data.get("shop_inventory", {}).get("mutagenic_serum", 0) >= 1:
        gmo_chance += 0.07
    elif not full_data and has_shop_item(user_id, "mutagenic_serum"):
        gmo_chance += 0.07
    fertilizer_multiplier = 1.0
    if fertilizer_tier > 0:
        fertilizer_multiplier = 1.0 + HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"]
    chain_chance = HARVEST_CHAIN_UPGRADES[chain_tier - 1]["chain_chance"] if chain_tier > 0 else 0.0
    if tractor_enchant:
        chain_chance += tractor_enchant.get("chain_chance", 0)
    chain_chance = max(0, chain_chance)

    hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
    daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
    if allow_chain and hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "chain_reaction":
        chain_chance += 0.10
    if not allow_chain:
        chain_chance = 0.0
    if hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "radiation_leak":
        gmo_chance += 0.20
    if daily_event and daily_event.get("effects", {}).get("event_id", "") == "gmo_surge":
        gmo_chance += 0.10
    gmo_chance = min(gmo_chance, 1.0)
    if hourly_event and hourly_event.get("effects", {}).get("event_id", "") == "basket_boost":
        basket_multiplier *= 1.5

    # ----- harvest loop (pure math, no DB writes) -----
    gathered_items = []
    total_value = 0.0
    total_base_value = 0.0
    total_value_before_daily = 0.0
    items_inc: dict[str, int] = {}
    ripeness_inc: dict[str, int] = {}
    month_index = random.randint(0, 11)
    month_name = MONTHS[month_index]
    total_seasonal_bonus = 0.0
    seasonal_label = None

    for _ in range(total_items_to_harvest):
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
        base_value = item["base_value"] * area_multiplier
        if hourly_event:
            eid = hourly_event.get("effects", {}).get("event_id", "")
            if eid == "may_flowers" and item["category"] == "Flower":
                base_value *= 3
            elif eid == "fruit_festival" and item["category"] == "Fruit":
                base_value *= 2
            elif eid == "vegetable_boom" and item["category"] == "Vegetable":
                base_value *= 2
        if ripeness_list:
            weights = [r["chance"] for r in ripeness_list]
            h_eid = hourly_event.get("effects", {}).get("event_id", "") if hourly_event else ""
            d_eid = daily_event.get("effects", {}).get("event_id", "") if daily_event else ""
            if h_eid == "perfect_ripeness":
                ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
                ripeness_multiplier = ripeness["multiplier"] * 1.5
            elif d_eid == "ripeness_rush":
                rw = [r["chance"] * 2 if "Perfect" in r["name"] else r["chance"] for r in ripeness_list]
                ripeness = random.choices(ripeness_list, weights=rw, k=1)[0]
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
        final_value *= basket_multiplier * fertilizer_multiplier

        value_multiplier = 1.0
        if hourly_event:
            eid = hourly_event.get("effects", {}).get("event_id", "")
            if eid == "bumper_crop":
                value_multiplier *= 2.0
            elif eid == "lucky_strike":
                value_multiplier *= 1.25
        if daily_event:
            eid = daily_event.get("effects", {}).get("event_id", "")
            if eid == "double_money":
                value_multiplier *= 1.5
            elif eid == "harvest_festival":
                value_multiplier *= 1.5
        final_value *= value_multiplier

        item_seasonal_mult, item_seasonal_label = get_seasonal_multiplier(month_index, item["category"])
        if item_seasonal_mult > 1.0:
            total_seasonal_bonus += final_value * (item_seasonal_mult - 1.0)
            final_value *= item_seasonal_mult
            if item_seasonal_label:
                seasonal_label = item_seasonal_label

        base_value_before_boosts = final_value
        extra_bloom = base_value_before_boosts * (bloom_multiplier - 1.0)
        extra_water = base_value_before_boosts * (water_multiplier - 1.0)
        extra_achievement = base_value_before_boosts * (achievement_multiplier - 1.0)
        extra_daily = base_value_before_boosts * (daily_bonus_multiplier - 1.0)
        extra_enchant = base_value_before_boosts * enchant_money_bonus if enchant_money_bonus != 0 else 0.0
        subtotal = base_value_before_boosts + extra_bloom + extra_water + extra_achievement + extra_daily + extra_enchant
        final_value = subtotal * rank_perma_buff_mult

        # Daily shop: Bloomstone (flowers 3x)
        if item.get("category") == "Flower":
            shop_inv_harvest = full_data.get("shop_inventory", {}) if full_data else get_user_shop_inventory(user_id)
            if shop_inv_harvest.get("bloomstone", 0) >= 1:
                final_value *= 3.0

        current_balance += final_value
        total_value += final_value
        total_base_value += base_value_before_boosts

        # Accumulate for batch write instead of per-item DB calls
        items_inc[name] = items_inc.get(name, 0) + 1
        ripeness_inc[ripeness["name"]] = ripeness_inc.get(ripeness["name"], 0) + 1
        gathered_items.append({
            "name": name, "value": final_value,
            "base_value": base_value_before_boosts,
            "ripeness": ripeness["name"], "is_gmo": is_gmo,
        })

    # ----- Daily shop: Fuzzy Dice (+5% harvest money) -----
    shop_inv = full_data.get("shop_inventory", {}) if full_data else get_user_shop_inventory(user_id)
    if shop_inv.get("fuzzy_dice", 0) >= 1:
        total_value *= 1.05

    # All money buffs below apply to the SAME base value (additive stacking). No buff's result becomes the "new base".
    base_for_buffs = float(total_value)
    beta_tester_mult = get_beta_tester_money_multiplier(user_id)
    server_booster_mult = get_server_booster_money_multiplier(user_id)
    premium_mult = get_premium_tier_money_multiplier(user_id)
    nether_star_mult = get_nether_star_money_multiplier(user_id)
    black_shard_mult = get_black_shard_money_multiplier(user_id)
    shadow_crystal_mult = get_shadow_crystal_money_multiplier(user_id)
    palace_mult = get_palace_treasure_money_multiplier(user_id)
    edward_mult = get_edward_splash_money_multiplier(user_id)
    eclipse_mult = get_eclipse_glasses_money_multiplier(user_id)
    extra_money_from_beta_tester = base_for_buffs * (beta_tester_mult - 1.0) if beta_tester_mult > 1.0 else 0.0
    extra_money_from_server_booster = base_for_buffs * (server_booster_mult - 1.0) if server_booster_mult > 1.0 else 0.0
    extra_money_from_premium = base_for_buffs * (premium_mult - 1.0) if premium_mult > 1.0 else 0.0
    extra_money_from_nether_star = base_for_buffs * (nether_star_mult - 1.0) if nether_star_mult > 1.0 else 0.0
    extra_money_from_black_shard = base_for_buffs * (black_shard_mult - 1.0) if black_shard_mult > 1.0 else 0.0
    extra_money_from_shadow_crystal = base_for_buffs * (shadow_crystal_mult - 1.0) if shadow_crystal_mult > 1.0 else 0.0
    extra_palace = base_for_buffs * (palace_mult - 1.0) if palace_mult > 1.0 else 0.0
    extra_edward = base_for_buffs * (edward_mult - 1.0) if edward_mult > 1.0 else 0.0
    extra_eclipse = base_for_buffs * (eclipse_mult - 1.0) if eclipse_mult > 1.0 else 0.0
    work_lunch_extra = base_for_buffs * 0.10 if (not set_cooldown and has_shop_item(user_id, "work_lunch")) else 0.0
    overtime_extra = base_for_buffs * 1.0 if (not set_cooldown and has_shop_item(user_id, "overtime_approval") and random.random() < 0.10) else 0.0
    total_value = base_for_buffs + extra_money_from_beta_tester + extra_money_from_server_booster + extra_money_from_premium + extra_money_from_nether_star + extra_money_from_black_shard + extra_money_from_shadow_crystal + extra_palace + extra_edward + extra_eclipse + work_lunch_extra + overtime_extra

    # ----- single batch write: items + ripeness + balance + counts + tree rings + cooldown + almanac -----
    num_items = total_items_to_harvest
    almanac_pairs = [(g["name"], g["ripeness"]) for g in gathered_items]
    tree_rings_to_award = perform_harvest_batch_update(
        user_id=user_id,
        items_inc=items_inc,
        ripeness_inc=ripeness_inc,
        balance_increment=total_value,
        num_items=num_items,
        pre_total_items=plants_before_harvest,
        pre_bloom_cycle=pre_bloom_cycle,
        set_cooldown=set_cooldown,
        increment_command_count=increment_command_count,
        almanac_pairs=almanac_pairs,
    )

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
        "achievement_unlocked": None,
        "tractor_enchant": tractor_enchant,
        "enchant_extra_plants": enchant_extra_plants,
        "enchant_money_bonus": enchant_money_bonus,
        "month_name": month_name,
        "seasonal_label": seasonal_label,
        "total_seasonal_bonus": total_seasonal_bonus,
        "base_plants": base_items,
        "car_extra_plants": extra_items,
        "items_inc": items_inc,
        "ripeness_inc": ripeness_inc,
        "num_items": total_items_to_harvest,
        "beta_tester_multiplier": beta_tester_mult,
        "extra_money_from_beta_tester": extra_money_from_beta_tester,
        "server_booster_multiplier": server_booster_mult,
        "extra_money_from_server_booster": extra_money_from_server_booster,
        "premium_tier_multiplier": premium_mult,
        "extra_money_from_premium": extra_money_from_premium,
        "nether_star_multiplier": nether_star_mult,
        "extra_money_from_nether_star": extra_money_from_nether_star,
        "black_shard_multiplier": black_shard_mult,
        "extra_money_from_black_shard": extra_money_from_black_shard,
    }

async def perform_harvest_for_user(user_id: int, allow_chain: bool = True,
                                   area_multiplier: float = 1.0,
                                   full_data=None,
                                   set_cooldown: bool = False,
                                   increment_command_count: bool = False) -> dict:
    """Perform a full harvest for a user. Runs the synchronous DB-heavy logic in a thread
    to avoid blocking the event loop."""
    return await asyncio.to_thread(
        _perform_harvest_for_user_sync,
        user_id, allow_chain, area_multiplier, full_data,
        set_cooldown, increment_command_count
    )


#/harvest command, basically /castnet
def _harvest_critical_path(member, user_id: int, channel_name: str, area: dict) -> dict:
    """All DB work for /harvest in ONE sync call (runs in a single thread).

    Syncs beta/booster/premium from Discord, then 1 read (full_data) + 1 write (harvest batch + cooldown + command-count).
    Returns a dict so the caller can build the embed without touching the DB.
    """
    sync_beta_tester_from_member(member)
    sync_server_booster_from_member(member)
    sync_premium_tier_from_member(member)
    user_planter_level = get_user_planter_level(member)
    area_mult = area.get("multiplier", 1.0)
    if has_shop_item(user_id, "atlas"):
        area_mult *= 2.0
    area_mult *= area.get("gather_boost", 1.0)

    full_data = get_user_harvest_full_data(user_id)

    # --- area access check: use effective planter level (max of role and DB from bloom_cycle_plants) ---
    bloom_cycle = full_data.get("bloom_cycle_plants", 0)
    effective_planter_level = max(user_planter_level, get_planter_level_from_total_items(bloom_cycle))
    if not area.get("unlocked_by_default", False):
        unlocked = full_data.get("unlocked_areas", {})
        prev = area.get("previous_area")
        if prev and not unlocked.get(prev, False):
            prev_display = GATHERING_AREAS[prev]["display_name"]
            return {"area_error": f"❌ You must unlock **{prev_display}** before accessing **{area['display_name']}**!"}
        if not unlocked.get(channel_name, False):
            return {"area_error": f"❌ You haven't unlocked **{area['display_name']}** yet! Use `/unlock {channel_name}` to unlock it for **${area['unlock_cost']:,}**."}
        if effective_planter_level < area.get("required_planter_level", 0):
            return {"area_error": f"❌ You must be **{area['required_planter_rank']}** or above to gather in **{area['display_name']}**! You need to gather more plants to rank up."}

    # --- cooldown check (pure computation) ---
    can_user, time_left, is_roulette = can_harvest(user_id, full_data=full_data)
    if not can_user:
        almost_unlocked_too = False
        if time_left == 0 and not is_roulette:
            almost_unlocked_too = unlock_hidden_achievement(user_id, "almost_got_it_too")
        return {"on_cooldown": True, "time_left": time_left, "is_roulette": is_roulette, "almost_unlocked_too": almost_unlocked_too}

    # --- perform harvest + cooldown + command-count in ONE batch write ---
    result = _perform_harvest_for_user_sync(
        user_id, allow_chain=True, area_multiplier=area_mult,
        full_data=full_data, set_cooldown=True, increment_command_count=True)

    # --- chain roll (pure computation) ---
    chain_chance = result["chain_chance"]
    chain_triggered = chain_chance > 0 and random.random() < chain_chance
    if chain_triggered:
        update_user_last_harvest_time(user_id, 0)

    # --- stealable roll: 0.5% for harvest (× Palace Treasure mult for victim); stealable = no PvE ---
    steal_chance = STEAL_CHANCE_HARVEST * get_steal_chance_multiplier(user_id)
    stealable = random.random() < steal_chance
    steal_payload = None
    if stealable:
        steal_payload = {
            "total_value": result["total_value"],
            "items_inc": result["items_inc"],
            "ripeness_inc": result["ripeness_inc"],
            "num_items": result["num_items"],
        }

    return {
        "result": result,
        "full_data": full_data,
        "chain_triggered": chain_triggered,
        "stealable": stealable,
        "victim_planter_level": user_planter_level,
        "steal_payload": steal_payload,
        "area_mult": area_mult,
    }


async def _harvest_post_response(interaction: discord.Interaction, user_id: int,
                                 full_data: dict, result: dict):
    """Background task: role assignment + achievement checks after harvest.

    Runs AFTER the main embed is sent so the user sees the response instantly.
    """
    try:
        # Tree Ring notice
        tree_rings_to_award = result.get("tree_rings_to_award", 0)
        if tree_rings_to_award > 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"<:TreeRing:1474244868288282817> {interaction.user.mention} You've been awarded **{tree_rings_to_award} Tree Ring{'s' if tree_rings_to_award > 1 else ''}**!",
                ephemeral=True)

        # Role assignment (async Discord API) — per-user lock so gather+harvest don't both send rank-up
        old_role = new_role = None
        if user_id not in _planter_role_locks:
            _planter_role_locks[user_id] = asyncio.Lock()
        async with _planter_role_locks[user_id]:
            try:
                old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
            except Exception as e:
                print(f"Error assigning gatherer role to user {user_id}: {e}")

            if new_role:
                if new_role == "PLANTER I" and old_role is None:
                    try:
                        await assign_bloom_rank_role(interaction.user, interaction.guild)
                    except Exception as e:
                        print(f"Error assigning bloom rank role to user {user_id}: {e}")
                    rankup_embed = discord.Embed(
                        title="🌾 Rank Up!",
                        description=f"{interaction.user.mention} advanced to **PLANTER I** and is ranked **PINE I**!",
                        color=discord.Color.gold())
                else:
                    rankup_embed = discord.Embed(
                        title="🌾 Rank Up!",
                        description=f"{interaction.user.mention} advanced from **{old_role or 'PLANTER I'}** to **{new_role}**!",
                        color=discord.Color.gold())
                await safe_interaction_response(interaction, interaction.followup.send, embed=rankup_embed)

        # Achievement checks in a single thread
        def _check_achievements():
            total_items = get_user_total_items(user_id)
            planter_lvl = get_planter_level_from_total_items(total_items)
            cur_planter = get_user_achievement_level(user_id, "planter")
            planter_up = None
            if planter_lvl > cur_planter:
                set_user_achievement_level(user_id, "planter", planter_lvl)
                planter_up = planter_lvl

            cmd_count = full_data.get("harvest_command_count", 0) + 1
            harvesting_lvl = get_achievement_level_for_stat("harvesting", cmd_count)
            cur_harvesting = get_user_achievement_level(user_id, "harvesting")
            harvesting_up = None
            if harvesting_lvl > cur_harvesting:
                set_user_achievement_level(user_id, "harvesting", harvesting_lvl)
                harvesting_up = harvesting_lvl
            return planter_up, harvesting_up

        planter_up, harvesting_up = await asyncio.to_thread(_check_achievements)
        # Planter achievement: only send when we just showed the rank-up (new_role), so rank-up always displays first
        if new_role and new_role in PLANTER_RANK_ORDER:
            level = PLANTER_RANK_ORDER[new_role]
            cur_planter_ach = planter_up if planter_up else (await asyncio.to_thread(get_user_achievement_level, user_id, "planter"))
            if cur_planter_ach >= level:
                await send_achievement_notification(interaction, "planter", level)
        if harvesting_up:
            await send_achievement_notification(interaction, "harvesting", harvesting_up)

        # Hidden achievement: One in a Mikellion (harvest a plant with Mikellion rarity)
        has_mikellion = any(item.get("ripeness") == "Mikellion" for item in result.get("gathered_items", []))
        if has_mikellion and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
            await send_hidden_achievement_notification(interaction, "one_in_a_mikellion")

        if result.get('achievement_unlocked'):
            ach_name, ach_level = result['achievement_unlocked']
            await send_achievement_notification(interaction, ach_name, ach_level)

        # Almanac achievements (level + section hidden)
        await check_almanac_achievements_async(user_id, interaction, interaction.user.mention)
    except Exception as e:
        print(f"Error in harvest post-response: {e}")


@bot.tree.command(name="harvest", description="Harvest a bunch of plants at once!")
async def harvest(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        channel_name = interaction.channel.name.lower() if hasattr(interaction.channel, 'name') else ""
        user_id = interaction.user.id

        # Quick validation (no DB call)
        if channel_name not in VALID_GATHERING_CHANNELS:
            channels_list = ", ".join(f"**{GATHERING_AREAS[a]['display_name']}**" for a in GATHERING_AREAS)
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You can only use this command in gathering channels: {channels_list}", ephemeral=True)
            return

        # Check if user is on Sans death timer
        if user_id in sans_death_timers:
            death_end_time = sans_death_timers[user_id]
            current_time = time.time()
            if current_time < death_end_time:
                time_left = int(death_end_time - current_time)
                minutes = time_left // 60
                seconds = time_left % 60
                time_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"{SOUL_EMOJI} **GAME OVER** - You're dead for **{time_str}**.", ephemeral=True)
                return
            else:
                # Timer expired, remove from dict
                sans_death_timers.pop(user_id, None)

        guild_id = interaction.guild.id if interaction.guild else 0
        if guild_id and guild_id in active_boss_events:
            boss_list = active_boss_events[guild_id]
            ch_id = boss_list[0]["channel_id"] if boss_list else 0
            boss_ch = interaction.guild.get_channel(ch_id) if ch_id else None
            where = f" in {boss_ch.mention}" if boss_ch else ""
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🚨 A **boss** is terrorizing the gathering grounds! Defeat it{where} before you can harvest again!", ephemeral=True)
            return

        # Block commands while a PvE wild animal event is active in this channel
        if interaction.channel.id in active_pve_events:
            pve_info = active_pve_events[interaction.channel.id]
            if pve_info.get("swarm"):
                animal_name, animal_emoji = ("Bee Swarm", "🐝") if pve_info.get("swarm_type") == "bee" else ("Bullet Ant Swarm", "🐜")
            else:
                animal_name = pve_info["animal"]["name"]
                animal_emoji = pve_info["animal"]["emoji"]
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🚨 {animal_emoji} A wild **{animal_name}** is terrorizing this channel! "
                f"Defeat it before you can harvest again!", ephemeral=True)
            return

        area = GATHERING_AREAS[channel_name]

        # === ONE thread call: sync roles + data fetch + area check + cooldown + harvest + chain roll ===
        crit = await asyncio.to_thread(
            _harvest_critical_path, interaction.user, user_id, channel_name, area)

        if crit.get("area_error"):
            await safe_interaction_response(interaction, interaction.followup.send,
                crit["area_error"], ephemeral=True)
            return

        if crit.get("on_cooldown"):
            time_left = crit["time_left"]
            if crit["is_roulette"]:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot /harvest for {time_left // 60} minute(s)", ephemeral=True)
            elif crit.get("almost_unlocked_too"):
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"You must wait {time_left} seconds before harvesting again, {interaction.user.name}.", ephemeral=True)
                await send_hidden_achievement_notification(interaction, "almost_got_it_too")
            else:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"You must wait {time_left // 60} minutes and {time_left % 60} seconds before harvesting again, {interaction.user.name}.", ephemeral=True)
            return

        result = crit["result"]
        full_data = crit["full_data"]
        chain_triggered = crit["chain_triggered"]
        area_mult = crit.get("area_mult", 1.0)

        gathered_items = result["gathered_items"]
        total_value = result["total_value"]
        current_balance = result["current_balance"]
        total_base_value = result["total_base_value"]
        bloom_multiplier = result["bloom_multiplier"]
        water_multiplier = result["water_multiplier"]

        # --- build embed (pure computation, no DB) ---
        embed = discord.Embed(title="You Harvested!", color=discord.Color.green())

        # (obsolete) (~35–50 chars per line; 20–30 items stay under Discord’s 1024 limit)
        # One line per item: emoji (ripeness) GMO? — no plant name text to stay under 1024
        lines = []
        for item in gathered_items:
            rip_em = get_ripeness_imbue_emoji(item.get("ripeness", ""))
            emoji = get_item_display_emoji(item["name"])
            gmo = " GMO! ✨" if item["is_gmo"] else ""
            prefix = f"{rip_em} " if rip_em else ""
            lines.append(f"{prefix}{emoji} (**{item['ripeness']}**){gmo}")
        items_display = "\n".join(lines)
        if len(items_display) > 1024:
            max_content = 1024 - 20  # reserve space for " … and 99999 more"
            n, current_len = 0, 0
            for i, line in enumerate(lines):
                need = len(line) + (1 if n else 0)  # +1 for newline
                if current_len + need > max_content:
                    break
                current_len += need
                n += 1
            items_display = "\n".join(lines[:n]) + f" … and {len(lines) - n} more"
        embed.add_field(name="📦 Items Gathered", value=items_display or "No items", inline=False)

        bloom_count = full_data.get("bloom_count", 0)
        extra_money_from_bloom = total_base_value * (bloom_multiplier - 1.0)
        if bloom_count > 0 and extra_money_from_bloom > 0:
            multiplier_percent = (bloom_multiplier - 1.0) * 100
            embed.add_field(name="<:TreeRing:1474244868288282817> Tree Ring Boost",
                value=f"+{multiplier_percent:.1f}% - **+{format_money(extra_money_from_bloom)}**", inline=False)

        achievement_multiplier = get_achievement_multiplier(user_id, full_data=full_data)
        extra_money_from_achievement = total_base_value * (achievement_multiplier - 1.0)
        if extra_money_from_achievement > 0:
            achievement_percent = (achievement_multiplier - 1.0) * 100
            embed.add_field(name="🏆 Achievement Boost",
                value=f"+{achievement_percent:.1f}% - **+{format_money(extra_money_from_achievement)}**", inline=False)

        bloom_rank = _bloom_count_to_rank(bloom_count)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id, full_data=full_data)
        if bloom_rank != "PINE I" and rank_perma_buff_multiplier > 1.0:
            enchant_money_bonus = result.get("enchant_money_bonus", 0)
            extra_money_from_water = total_base_value * (water_multiplier - 1.0)
            enchant_total = total_base_value * enchant_money_bonus if enchant_money_bonus != 0 else 0.0
            # Calculate base for Water Streak display (same as Beta Tester base: after rank, fuzzy dice, etc.)
            # This approximates base_for_buffs by subtracting final buffs from total_value
            total_subtotal = total_base_value + extra_money_from_bloom + extra_money_from_water + extra_money_from_achievement + enchant_total
            extra_money_from_rank = total_subtotal * (rank_perma_buff_multiplier - 1.0)
            total_after_rank = total_subtotal + extra_money_from_rank
            # Apply fuzzy dice if present (1.05x)
            total_after_fuzzy = total_after_rank * 1.05 if (full_data.get("shop_inventory", {}).get("fuzzy_dice", 0) >= 1) else total_after_rank
            base_for_display = total_after_fuzzy
            rank_percent = (rank_perma_buff_multiplier - 1.0) * 100
            embed.add_field(name="⭐ Rank Boost",
                value=f"+{rank_percent:.1f}% - **+{format_money(extra_money_from_rank)}**", inline=False)
        else:
            # No rank boost, so base is just after fuzzy dice
            total_after_fuzzy = total_base_value * 1.05 if (full_data.get("shop_inventory", {}).get("fuzzy_dice", 0) >= 1) else total_base_value
            base_for_display = total_after_fuzzy

        daily_rate_display = 0.04 if full_data.get("shop_inventory", {}).get("golden_watering_can", 0) >= 1 else 0.02
        daily_bonus_multiplier = 1.0 + (full_data.get("consecutive_water_days", 0) * daily_rate_display)
        # Calculate Water Streak on same base as Beta Tester for consistent display
        extra_money_from_daily = base_for_display * (daily_bonus_multiplier - 1.0) if daily_bonus_multiplier > 1.0 else 0.0
        if extra_money_from_daily > 0:
            daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
            embed.add_field(name="💧 Water Streak Boost",
                value=f"+{daily_bonus_percent:.1f}% - **+{format_money(extra_money_from_daily)}**", inline=False)

        if result.get("extra_money_from_beta_tester", 0) > 0:
            beta_percent = (result['beta_tester_multiplier'] - 1.0) * 100
            embed.add_field(name="🧪 Beta Tester!",
                value=f"+{beta_percent:.1f}% - **+{format_money(result['extra_money_from_beta_tester'])}**", inline=False)
        if result.get("extra_money_from_server_booster", 0) > 0:
            booster_percent = (result['server_booster_multiplier'] - 1.0) * 100
            embed.add_field(name=f"{SERVER_BOOSTER_EMOJI} Server Booster!",
                value=f"+{booster_percent:.1f}% - **+{format_money(result['extra_money_from_server_booster'])}**", inline=False)
        if result.get("extra_money_from_premium", 0) > 0:
            tier = get_user_premium_tier(user_id)
            premium_percent = (result['premium_tier_multiplier'] - 1.0) * 100
            embed.add_field(name=PREMIUM_DISPLAY.get(tier, "Premium"),
                value=f"+{premium_percent:.1f}% - **+{format_money(result['extra_money_from_premium'])}**", inline=False)
        # Daily shop / item boosts that affected this harvest
        item_boost_sources = []
        shop_inv = full_data.get("shop_inventory", {}) if full_data else {}
        if shop_inv.get("bloomstone", 0) >= 1:
            item_boost_sources.append(("Bloomstone", 1))
        if shop_inv.get("fuzzy_dice", 0) >= 1:
            item_boost_sources.append(("Fuzzy Dice", 1))
        if result.get("extra_money_from_nether_star", 0) > 0:
            # Check for permanent shop item and dayboosts
            has_permanent = has_shop_item(user_id, "nether_star")
            dayboost_count = get_dayboost_count(user_id, "nether_star")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Nether Star", total_count))
        if result.get("extra_money_from_black_shard", 0) > 0:
            # Check for permanent shop item and dayboosts
            has_permanent = has_shop_item(user_id, "black_shard")
            dayboost_count = get_dayboost_count(user_id, "black_shard")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Black Shard", total_count))

        if item_boost_sources:
            extra_ns = result.get("extra_money_from_nether_star", 0)
            extra_bs = result.get("extra_money_from_black_shard", 0)
            total_extra = extra_ns + extra_bs
            value_parts = [f"**+{format_money(total_extra)}**"] if total_extra > 0 else []
            value_parts.append("\n".join(_format_item_boost_source(name, count) for name, count in item_boost_sources))
            embed.add_field(
                name="📦 Item Boost",
                value="\n".join(value_parts),
                inline=False
            )

        tractor_enc = result.get("tractor_enchant")
        if tractor_enc:
            tractor_name = tractor_enc.get("name", "Unknown")
            tractor_rarity = tractor_enc.get("rarity", "COMMON")
            tractor_rarity_display = RARITY_EMOJI.get(tractor_rarity, f"[{tractor_rarity}]")
            embed.add_field(name="\u2728 Imbuement",
                value=f"**{tractor_name}** {tractor_rarity_display}", inline=False)

        month_name = result.get("month_name", "")
        if month_name:
            embed.add_field(name="\u200b", value=f"**~**\n{interaction.user.name} in {month_name}", inline=False)
        embed.add_field(name="💰 Total Value", value=f"**{format_money(total_value)}**", inline=True)
        embed.add_field(name="💵 New Balance", value=f"**{format_money(current_balance)}**", inline=True)

        # === Send the response ASAP (with optional STEAL button) ===
        view = None
        if crit.get("stealable") and crit.get("steal_payload"):
            view = StealView(
                victim_id=user_id,
                victim_planter_level=crit.get("victim_planter_level", 1),
                steal_type="harvest",
                steal_payload=crit["steal_payload"],
                window_sec=STEAL_WINDOW_HARVEST_SEC,
            )
        if view:
            msg = await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
            if msg:
                view._message = msg
        else:
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed)

        # Chain message (must be after the main embed)
        if chain_triggered:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"🔗🔗 **CHAIN!** Your harvest cooldown has been reset! Harvest again! 🔗🔗")

        # Auto-log to #rares for any Legendary/Netherite/Luminite/Celestial/Mikellion (GMO or not)
        area_tag = "[" + channel_name.upper().replace("-", " ") + "]"
        for item in gathered_items:
            rare_label, _ = _plant_rare_label(item.get("ripeness", ""), item.get("is_gmo", False))
            if rare_label:
                cat = next((i["category"] for i in GATHERABLE_ITEMS if i["name"] == item["name"]), "Item")
                asyncio.create_task(_post_rares_plant(
                    interaction.guild, interaction.user, "HARVEST",
                    item["name"], cat, item["value"], item["ripeness"], item.get("is_gmo", False),
                    area_tag))

        # === Background: role assignment + achievements (user already has the response) ===
        asyncio.create_task(_harvest_post_response(interaction, user_id, full_data, result))

        # === PvE spawn: try Plantera (jungle only) -> boss -> wild animal, in order; at most one spawn per harvest ===
        pve_spawned = False
        # 1. Plantera bulb (only in #underground-jungle after a recent boss defeat)
        if (not pve_spawned and channel_name == "underground-jungle" and guild_id
                and guild_id in plantera_bulb_eligible_guilds
                and (time.time() - plantera_bulb_eligible_guilds[guild_id]) <= PLANTERA_BULB_MAX_AGE_SEC
                and interaction.channel.id not in active_pve_events
                and guild_id not in active_boss_events
                and random.random() < PLANTERA_BULB_CHANCE):
            plantera_bulb_eligible_guilds.pop(guild_id, None)
            bulb_embed = discord.Embed(
                title="🌸 The jungle grows restless...",
                description="A **Plantera Bulb** spawned!",
                color=0x2d5a1a)
            view = PlanteraBulbView(interaction.channel.id, guild_id, area_mult)
            asyncio.create_task(interaction.channel.send(embed=bulb_embed, view=view))
            pve_spawned = True
        # 2. Boss spawn (rarer) — only if no boss active, no enemy in any gather channel, no boss already scheduled
        if (not pve_spawned and channel_name in VALID_GATHERING_CHANNELS and guild_id and guild_id not in active_boss_events
                and guild_id not in pending_boss_spawn_guild_ids
                and interaction.channel.id not in active_pve_events
                and not _guild_has_active_pve(interaction.guild)
                and not crit.get("stealable")):
            _, boss_mult = _pve_spawn_multiplier()
            effective_boss_chance = min(1.0, PVE_BOSS_TRIGGER_CHANCE * boss_mult)
            if channel_name == "underground-jungle":
                effective_boss_chance = min(1.0, effective_boss_chance * UNDERGROUND_JUNGLE_BOSS_SPAWN_MULT)
            bosses_candidates = _get_random_spawn_bosses()
            if random.random() < effective_boss_chance and bosses_candidates:
                boss = random.choice(bosses_candidates)
                pending_boss_spawn_guild_ids.add(guild_id)
                await _send_boss_warning_embed(interaction.guild, boss, interaction.channel)
                asyncio.create_task(_delayed_boss_spawn(interaction.guild, interaction.channel, boss, area_mult, 60.0))
                pve_spawned = True
        # 3. PvE wild animal
        if (not pve_spawned and channel_name in VALID_GATHERING_CHANNELS and interaction.channel.id not in active_pve_events
                and guild_id not in active_boss_events
                and not crit.get("stealable")):
            animal_mult, _ = _pve_spawn_multiplier()
            effective_chance = min(1.0, PVE_TRIGGER_CHANCE_HARVEST * animal_mult)
            if channel_name == "underground-jungle":
                effective_chance = min(1.0, effective_chance * UNDERGROUND_JUNGLE_ANIMAL_SPAWN_MULT)
            if random.random() < effective_chance:
                asyncio.create_task(trigger_pve_event(interaction.channel, area_mult))
    except Exception as e:
        print(f"Error in harvest command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="achievements", description="View your achievements and progress!")
@app_commands.describe(hidden="Show hidden achievements (discovered show description; undiscovered show ???)")
async def achievements(interaction: discord.Interaction, hidden: bool = False):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        user_id = interaction.user.id
        # Single DB read for all achievement data (avoids 66+ round-trips); run in thread to not block event loop
        data = await asyncio.to_thread(get_user_achievements_display_data, user_id)
        if data is None:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Could not load your data. Please try again.", ephemeral=True)
            return

        achievements_data = data["achievements"]
        total_items = data["total_items"]
        hidden_achievements_map = achievements_data.get("hidden_achievements", {})
        hidden_achievements_count = int(achievements_data.get("hidden_achievements_discovered", 0))
        full_data = {"achievements": achievements_data}
        
        # If hidden=True, show only hidden achievements list (all from in-memory data)
        if hidden:
            hidden_fields = []
            for key, h_data in HIDDEN_ACHIEVEMENTS.items():
                name = h_data["name"]
                desc = h_data["description"] if hidden_achievements_map.get(key, False) else f"{ALMANAC_HIDDEN_EMOJI}{ALMANAC_HIDDEN_EMOJI}{ALMANAC_HIDDEN_EMOJI}"
                hidden_fields.append((name, desc))

            MAX_FIELDS_PER_EMBED = 25
            embeds = []
            for i in range(0, len(hidden_fields), MAX_FIELDS_PER_EMBED):
                chunk = hidden_fields[i:i + MAX_FIELDS_PER_EMBED]
                title = f"🔒 {interaction.user.name}'s Hidden Achievements" if i == 0 else f"🔒 {interaction.user.name}'s Hidden Achievements (Page {i // MAX_FIELDS_PER_EMBED + 1})"
                embed = discord.Embed(title=title, color=discord.Color.dark_gray())
                for name, desc in chunk:
                    embed.add_field(name=name, value=desc, inline=False)
                embed.set_footer(text=f"Discovered: {hidden_achievements_count}/{TOTAL_HIDDEN_ACHIEVEMENTS}")
                embeds.append(embed)

            if len(embeds) <= 10:
                await safe_interaction_response(interaction, interaction.followup.send, embeds=embeds)
            else:
                for i in range(0, len(embeds), 10):
                    await safe_interaction_response(interaction, interaction.followup.send, embeds=embeds[i:i + 10])
            return
        
        # Main achievements embed: use in-memory multiplier and levels (no extra DB calls)
        total_boost_multiplier = get_achievement_multiplier(user_id, full_data=full_data)
        total_boost_percent = (total_boost_multiplier - 1.0) * 100
        
        embed = discord.Embed(
            title=f"🏆 {interaction.user.name}'s Achievements",
            color=discord.Color.gold()
        )
        
        for achievement_name, achievement_data in ACHIEVEMENTS.items():
            current_level = int(achievements_data.get(achievement_name, 0))
            levels = achievement_data["levels"]
            current_level_data = levels[current_level] if current_level < len(levels) else levels[-1]
            max_earnable = len(levels) - 1
            num_green_squares = current_level
            progress_bar = "".join(PROGRESS_Y if i < num_green_squares else PROGRESS_N for i in range(max_earnable))
            boost_percent = current_level_data["boost"] * 100
            field_value = f"**{current_level_data['description']}**\n{progress_bar}\n"
            field_value += f"💰 Boost: **{boost_percent:.1f}%**" if current_level > 0 else "💰 Boost: **0%**"
            embed.add_field(
                name=current_level_data["name"],
                value=field_value,
                inline=False
            )
        
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━", value=f"💰 **Total Boost: {total_boost_percent:.1f}%**", inline=False)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━", value=f"**Hidden Achievements:** {hidden_achievements_count}/{TOTAL_HIDDEN_ACHIEVEMENTS}", inline=False)
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in achievements command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


class BloomConfirmView(discord.ui.View):
    """Confirmation view for /bloom: 'Bloom!' button performs the bloom and edits the message to success embed."""
    def __init__(self, user_id: int, timeout: float = 120.0):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.done = False

    @discord.ui.button(label="Bloom!", emoji="🌸", style=discord.ButtonStyle.success, custom_id="bloom_confirm")
    async def confirm_bloom(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await safe_defer(interaction, ephemeral=True):
            return
        if interaction.user.id != self.user_id:
            await safe_interaction_response(interaction, interaction.response.send_message, "Only the user who ran /bloom can confirm.", ephemeral=True)
            return
        if self.done:
            await safe_interaction_response(interaction, interaction.response.send_message, "You already bloomed!", ephemeral=True)
            return

        user_id = interaction.user.id
        # Re-check all bloom blocks in case user joined a game or defeated PvE after opening /bloom
        if user_id in user_active_games:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ You cannot bloom while in an active **/russian** game! Finish or cash out first.", ephemeral=True)
            return
        if user_id in user_active_gathership:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ You cannot bloom while in an active **Mayflower** game! Finish the game first.", ephemeral=True)
            return
        if user_id in user_active_gathemon:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ You cannot bloom while in an active **GathéMon** battle! Finish the battle first.", ephemeral=True)
            return
        if user_id in users_pending_pve_rewards:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ You cannot bloom yet! Your rewards from a Wild Animal or Boss are still being distributed. Wait a moment and try again.", ephemeral=True)
            return

        self.done = True
        self.stop()

        guild = interaction.guild
        if not guild:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ This command must be used in a server.", ephemeral=True)
            return
        old_rank = get_bloom_rank(user_id)
        tree_rings = get_user_tree_rings(user_id)
        perform_bloom(user_id)
        new_rank = get_bloom_rank(user_id)
        # CRITICAL: For ALL blooms, planter MUST reset to PLANTER I. DB bloom_cycle_plants is already 0 in perform_bloom.
        set_user_bloom_cycle_plants(user_id, 0)
        try:
            member = await guild.fetch_member(user_id)
        except Exception as e:
            print(f"Error fetching member for bloom planter reset: {e}")
            member = interaction.user
        try:
            await assign_gatherer_role(member, guild, force_planter_role="PLANTER I")
        except Exception as e:
            print(f"Error resetting planter role for user {user_id} after bloom: {e}")
        try:
            await assign_bloom_rank_role(member, guild)
        except Exception as e:
            print(f"Error assigning bloom rank role to user {user_id}: {e}")

        success_embed = discord.Embed(
            title="<:TreeRing:1474244868288282817> You Bloomed!",
            description=f"{interaction.user.mention} has advanced to **{new_rank}**!",
            color=discord.Color.gold()
        )
        success_embed.add_field(name="🌲 Bloom Rank", value=f"**{old_rank}** → **{new_rank}**", inline=False)
        success_embed.add_field(
            name="<:TreeRing:1474244868288282817> Tree Rings (kept)",
            value=f"**{tree_rings}** Tree Rings — you keep these through bloom!",
            inline=False,
        )
        if tree_rings > 0:
            multiplier = get_bloom_multiplier(user_id)
            success_embed.add_field(
                name="💰 Money Boost (from Tree Rings)",
                value=f"+{(multiplier - 1.0) * 100:.1f}% on all earnings *(from your kept Tree Rings)*",
                inline=False,
            )
        success_embed.add_field(
            name="🗺️ Reset, PLANTER X → PLANTER I",
            value="All unlocked areas have also been reset.",
            inline=False,
        )
        success_embed.set_footer(text="Go /gather in #forest!!")

        try:
            await interaction.message.edit(embed=success_embed, view=None)
        except Exception as e:
            print(f"Error editing bloom message: {e}")
        await interaction.followup.send("You bloomed! 🌸", ephemeral=True)

        bloom_count = get_user_bloom_count(user_id)
        new_blooming_level = get_achievement_level_for_stat("blooming", bloom_count)
        current_blooming_level = get_user_achievement_level(user_id, "blooming")
        if new_blooming_level > current_blooming_level:
            set_user_achievement_level(user_id, "blooming", new_blooming_level)
            await send_achievement_notification(interaction, "blooming", new_blooming_level)


@bot.tree.command(name="bloom", description="Bloom to advance your Bloom Rank! (Requires PLANTER X & cost scales per bloom)")
async def bloom(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        user_id = interaction.user.id
        if user_id in user_active_games:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You cannot bloom while in an active **/russian** game, {interaction.user.name}! Finish or cash out first.",
                ephemeral=True)
            return
        if user_id in user_active_gathership:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You cannot bloom while in an active **Mayflower** game, {interaction.user.name}! Finish the game first.",
                ephemeral=True)
            return
        if user_id in user_active_gathemon:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ You cannot bloom while in an active **GathéMon** battle, {interaction.user.name}! Finish the battle first.",
                ephemeral=True)
            return
        if user_id in users_pending_pve_rewards:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ You cannot bloom yet! Your rewards from a Wild Animal or Boss are still being distributed. Wait a moment and try again.",
                ephemeral=True)
            return

        user_roles = [role.name for role in interaction.user.roles]
        has_planter_x = "PLANTER X" in user_roles
        cycle_plants = get_user_bloom_cycle_plants(user_id)
        bloom_cost = bloom_prestige_cost(user_id)
        user_balance = get_user_balance(user_id)
        plants_needed = max(0, BLOOM_PLANTS_REQUIRED - cycle_plants)
        money_needed = max(0.0, bloom_cost - user_balance)

        what_bloom_does = (
            "This is a command that allows you to prestige and reset all your upgrades, money, and current plant count, "
            "but you keep lifetime progress, achievements, and Tree Rings! <:TreeRing:1474244868288282817> "
            "Each prestige makes your Bloom Rank increase (+20% permabuff per rank increase)."
        )

        can_bloom = has_planter_x and plants_needed == 0 and money_needed <= 0

        embed = discord.Embed(
            title=f"🌸 {interaction.user.display_name} wants to Bloom!",
            color=discord.Color.gold()
        )
        embed.add_field(name="📊 Plants Needed", value=f"**{plants_needed:,}**", inline=True)
        embed.add_field(name="💰 Money needed", value=f"**${money_needed:,.0f}**", inline=True)
        embed.add_field(name="ℹ️ What is /bloom?", value=what_bloom_does, inline=False)
        if can_bloom:
            embed.add_field(name="✅ Ready", value="You meet all requirements. Click **Bloom!** below to confirm.", inline=False)
        else:
            if not has_planter_x:
                embed.add_field(name="❌ Requirement", value="You must be **PLANTER X**", inline=False)
            elif plants_needed > 0:
                embed.add_field(name="❌ Requirement", value=f"Gather **{plants_needed:,}** more plants this cycle to reach PLANTER X.", inline=False)
            else:
                embed.add_field(name="❌ Requirement", value=f"Not enough money! Earn **${money_needed:,.2f}** more to /bloom!", inline=False)

        if can_bloom:
            view = BloomConfirmView(user_id=user_id)
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
        else:
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in bloom command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


def _unlock_critical_path(member, user_id: int, area_key: str) -> dict:
    """Run in thread: all DB reads + checks + writes for /unlock. Returns result dict for embed/error."""
    name = getattr(member, "name", "You")
    if area_key not in GATHERING_AREAS or GATHERING_AREAS[area_key]["unlocked_by_default"]:
        return {"success": False, "error": "❌ Invalid area!"}
    area_data = GATHERING_AREAS[area_key]
    unlocked_areas = get_user_unlocked_areas(user_id)
    if unlocked_areas.get(area_key, False):
        return {"success": False, "error": f"❌ You've already unlocked **{area_data['display_name']}**, {name}!"}
    if area_data["previous_area"] and not unlocked_areas.get(area_data["previous_area"], False):
        prev_display = GATHERING_AREAS[area_data["previous_area"]]["display_name"]
        return {"success": False, "error": f"❌ You must unlock **{prev_display}** before you can unlock **{area_data['display_name']}**, {name}!"}
    role_level = get_user_planter_level(member)
    cycle_plants = get_user_bloom_cycle_plants(user_id)
    effective_planter_level = max(role_level, get_planter_level_from_total_items(cycle_plants))
    if effective_planter_level < area_data["required_planter_level"]:
        return {"success": False, "error": f"❌ You must be **{area_data['required_planter_rank']}** or above to unlock **{area_data['display_name']}**, {name}! Keep gathering to rank up!"}
    unlock_cost = bloom_scaled_price(user_id, area_data["unlock_cost"])
    user_balance = get_user_balance(user_id)
    if user_balance < unlock_cost:
        money_needed = unlock_cost - user_balance
        return {
            "success": False,
            "error": (
                f"❌ You need **{format_money(money_needed)}** more to unlock **{area_data['display_name']}**, {name}! "
                f"(Cost: **{format_money(unlock_cost)}**)"
            ),
        }
    new_balance = normalize_money(user_balance - unlock_cost)
    update_user_balance(user_id, new_balance)
    unlock_user_area(user_id, area_key)
    updated_unlocked = get_user_unlocked_areas(user_id)
    unlocked_levels = [i + 1 for i, key in enumerate(AREA_ORDER_FOR_ACHIEVEMENT) if updated_unlocked.get(key)]
    areas_achievement_level = max(unlocked_levels) if unlocked_levels else 0
    current_areas_achievement = get_user_achievement_level(user_id, "areas_unlocked")
    should_notify_areas = areas_achievement_level > current_areas_achievement
    if should_notify_areas:
        set_user_achievement_level(user_id, "areas_unlocked", areas_achievement_level)
    return {
        "success": True,
        "new_balance": new_balance,
        "area_data": area_data,
        "areas_achievement_level": areas_achievement_level,
        "should_notify_areas": should_notify_areas,
    }


# /unlock command - unlock new gathering areas
@bot.tree.command(name="unlock", description="Unlock a new gathering area!")
@app_commands.describe(area="The area to unlock")
@app_commands.choices(area=[
    app_commands.Choice(name="Underground Jungle ($100,000 & PLANTER II)", value="underground-jungle"),
    app_commands.Choice(name="Grove ($250,000 & PLANTER III)", value="grove"),
    app_commands.Choice(name="Marsh ($5,000,000 & PLANTER V)", value="marsh"),
    app_commands.Choice(name="Bog ($75,000,000 & PLANTER VII)", value="bog"),
    app_commands.Choice(name="Mire ($300,000,000 & PLANTER IX)", value="mire"),
])
async def unlock(interaction: discord.Interaction, area: app_commands.Choice[str]):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        user_id = interaction.user.id
        area_key = area.value
        result = await asyncio.to_thread(_unlock_critical_path, interaction.user, user_id, area_key)

        if not result["success"]:
            await safe_interaction_response(interaction, interaction.followup.send,
                result["error"], ephemeral=True)
            return

        area_data = result["area_data"]
        new_balance = result["new_balance"]
        embed = discord.Embed(
            title=f"{area_data['emoji']} Area Unlocked!",
            description=f"{interaction.user.mention} has unlocked **{area_data['display_name']}**!",
            color=discord.Color.green()
        )
        embed.add_field(name="📈 Area Multiplier", value=f"**{area_data['multiplier']}x**", inline=False)
        embed.add_field(name="💵 New Balance", value=f"**${new_balance:,.2f}**", inline=False)
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
        if result.get("should_notify_areas"):
            await send_achievement_notification(interaction, "areas_unlocked", result["areas_achievement_level"])
    except Exception as e:
        print(f"Error in unlock command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# ─── Invite Awards System ───
# 20 tiers of invite rewards, claimed in order
INVITE_REWARDS = {
    1:  {"type": "money", "amount": 50_000, "description": "**$50,000**"},
    2:  {"type": "money", "amount": 200_000, "description": "**$200,000**"},
    3:  {"type": "tree_rings", "amount": 5, "description": "**5 Tree Rings**"},
    4:  {"type": "money", "amount": 1_000_000, "description": "**$1,000,000**"},
    5:  {"type": "tree_rings", "amount": 10, "description": "**10 Tree Rings**"},
    6:  {"type": "money", "amount": 5_000_000, "description": "**$5,000,000**"},
    7:  {"type": "money", "amount": 10_000_000, "description": "**$10,000,000**"},
    8:  {"type": "tree_rings", "amount": 20, "description": "**20 Tree Rings**"},
    9:  {"type": "money", "amount": 15_000_000, "description": "**$15,000,000**"},
    10: {"type": "secret_gardener", "amount": 0, "description": "**SECRET GARDENER**"},
    11: {"type": "tree_rings", "amount": 30, "description": "**30 Tree Rings**"},
    12: {"type": "secret_gardener_harvest", "amount": 0, "description": "**SECRET GARDENER UNLOCKS AUTO HARVEST!**"},
    13: {"type": "gather_cooldown", "amount": 10, "description": "**Permanent /gather cooldown reduction by 10 seconds**"},
    14: {"type": "harvest_cooldown", "amount": 300, "description": "**Permanent /harvest cooldown reduction by 5 minutes**"},
    15: {"type": "mine_cooldown", "amount": 1200, "description": "**Permanent /mine cooldown reduction by 20 minutes**"},
    16: {"type": "tree_rings", "amount": 100, "description": "**100 Tree Rings**"},
    17: {"type": "money", "amount": 100_000_000, "description": "**$100,000,000**"},
    18: {"type": "tree_rings", "amount": 300, "description": "**300 Tree Rings**"},
    19: {"type": "water_double", "amount": 0, "description": "**Permanent /water cooldown reduction! Water twice a day (12 PM & 12 AM EST)**"},
    20: {"type": "hidden_achievement", "amount": 0, "description": "**HIDDEN ACHIEVEMENT**"},
}


# ─── Daily Shop (Tree Rings currency) ───
DAILY_SHOP_ITEMS = {
    "fuzzy_dice": {
        "name": "Fuzzy Dice",
        "description": "A good trinket to have for your tractor!",
        "cost": 5,
        "effect": "5% permanent boost to money from /harvest!",
    },
    "mutagenic_serum": {
        "name": "Mutagenic Serum",
        "description": "Organic Schmorganic!",
        "cost": 50,
        "effect": "Permanent +7% GMO chance!",
    },
    "scarecrow": {
        "name": "Scarecrow",
        "description": "Scare those pesky crows away!",
        "cost": 50,
        "effect": "+10% money gain from /gather!",
    },
    "cryptobro_shadow": {
        "name": "Cryptobro's Shadow",
        "description": "You GOTTA buy this coin, man.",
        "cost": 75,
        "effect": "+50% money gain from /sell!",
    },
    "bloomstone": {
        "name": "Bloomstone",
        "description": "An ancient gem, shining with light.",
        "cost": 300,
        "effect": "All flowers triple in worth!",
    },
    "irrigation_system": {
        "name": "Irrigation System",
        "description": "It's got electrolytes!",
        "cost": 410,
        "effect": "Auto-/waters for you!",
    },
    "gamblers_revolver": {
        "name": "Gambler's Revolver",
        "description": "A flashy pistol, engraved with \"KH\".",
        "cost": 475,
        "effect": "Russian Roulette death penalty reduces to 5 minutes!",
    },
    "commoners_respite": {
        "name": "Commoner's Respite",
        "description": "Say goodbye to those common imbues!",
        "cost": 600,
        "effect": "You cannot roll a common imbue when doing /imbue!",
    },
    "atlas": {
        "name": "Atlas",
        "description": "Not to be confused with the browser.",
        "cost": 500,
        "effect": "Doubles the money gain from each area!",
    },
    "nether_star": {
        "name": "Nether Star",
        "description": "Let's make a Beacon!",
        "cost": 250,
        "effect": "+15% all Money",
        "emoji": NETHER_STAR_EMOJI,
    },
    "zenith": {
        "name": "Zenith",
        "description": "The culmination of a journey forged into the ultimate sword.",
        "cost": 275,
        "effect": "+2 ATK to Enemies",
    },
    "alchemists_pocketwatch": {
        "name": "Alchemist's Pocketwatch",
        "description": "Written on it is \"Don't forget\", dated October 3rd, year 11.",
        "cost": 25,
        "effect": "+5% money from /gather.",
    },
    "split_soul_katana": {
        "name": "Split Soul Katana",
        "description": "No cursed energy required!",
        "cost": 150,
        "effect": "+1 ATK to Enemies",
    },
    "inverted_spear_of_heaven": {
        "name": "Inverted Spear of Heaven",
        "description": "Your name's not Zenin? That makes me glad.",
        "cost": 200,
        "effect": "+1 ATK to Enemies",
    },
    "diamond_sword": {
        "name": "Diamond Sword",
        "description": "Do you like it?",
        "cost": 100,
        "effect": "+1 ATK to Enemies",
    },
    "kings_blade": {
        "name": "King's Blade",
        "description": "Imbued with the soul of a fierce Mimiga..",
        "cost": 300,
        "effect": "+3 ATK to Enemies",
    },
    "ancient_staff": {
        "name": "Ancient Staff",
        "description": "Said to be over 1000 years old, a ref staff with a gold, crescent-shaped tip, holding a red jewled orb. Ineffective against mimics!",
        "cost": 110,
        "effect": "+1 ATK to Enemies",
    },
    "reaver_karambit": {
        "name": "Reaver Karambit",
        "description": "Aces guaranteed!",
        "cost": 135,
        "effect": "+1 ATK to Enemies",
    },
    "nail": {
        "name": "Nail",
        "description": "A sharp blade wielded by a knight of hollow..",
        "cost": 150,
        "effect": "+1 ATK to Enemies",
    },
    "palace_treasure": {
        "name": "Palace Treasure",
        "description": "Looking cool, Joker!",
        "cost": 250,
        "effect": "+50% money gain, but your gathers and harvests are 5x more likely to be stealable!",
    },
    "bodyguard": {
        "name": "Bodyguard",
        "description": "Just here to protect you.",
        "cost": 200,
        "effect": "Halves your stealable chance",
    },
    "presidential_motorcade": {
        "name": "Presidential Motorcade",
        "description": "Frankly, I have the lowest chance to be stolen from. The lowest! It's effectively zero.",
        "cost": 300,
        "effect": "Cannot be stolen from",
    },
    "golden_watering_can": {
        "name": "Golden Watering Can",
        "description": "Perfect for the Zen Garden!",
        "cost": 200,
        "effect": "Double /water streak boost and increase the daily increase from 2% per day to 4%",
    },
    "van_der_lindes_plan": {
        "name": "Van der Linde's Plan",
        "description": "We just need more money.",
        "cost": 90,
        "effect": "+15% money from gather/harvests you steal",
    },
    "edward": {
        "name": "Edward",
        "description": "Just a little guy!",
        "cost": 30,
        "effect": "1% to all money",
    },
    "time_machine": {
        "name": "Future Gadget 204, 2nd Edition Ver. 2.31",
        "description": "El Psy Kongroo. The Phone Microwave (tentative)—gathering across world lines!",
        "cost": 700,
        "effect": "You get 1 Tree Ring every 50 plants instead of 100",
    },
    "shadow_crystal": {
        "name": "Shadow Crystal",
        "description": "An invisible crystal that still casts a shadow, obtained from a jester, puppet, a knight, and an old man...",
        "cost": 50,
        "effect": "+5% Money",
    },
    "splash_potion_of_luck": {
        "name": "Splash Potion of Luck",
        "description": "Straight from a blocky world.",
        "cost": 10,
        "effect": "1% to all money",
    },
    "evoker": {
        "name": "Evoker",
        "description": "This strange object resembles a pistol, branded with \"S.E.E.S\".",
        "cost": 70,
        "effect": "+1 ATK to Enemies",
    },
    "death_note": {
        "name": "Death Note",
        "description": "You must like potato chips, huh?",
        "cost": 167,
        "effect": "+2 ATK to Enemies",
    },
    "thorfinns_dagger": {
        "name": "Thorfinn's Dagger",
        "description": "If you're a true warrior, you shouldn't need this.",
        "cost": 95,
        "effect": "+1 ATK to Enemies",
    },
    "black_knife": {
        "name": "Black Knife",
        "description": "A long, shadowy & pixelated blade able to make enemies swoon!",
        "cost": 200,
        "effect": "+2 ATK to Enemies",
    },
    "black_shard": {
        "name": "Black Shard",
        "description": "A small chip of extremely hard glass. Oddly, it's nearly opaque.",
        "cost": 275,
        "effect": "+20% to Money",
        "emoji": BLACK_SHARD_EMOJI,
    },
    "decoys": {
        "name": "Decoys",
        "description": "Throw thieves off your trail!",
        "cost": 50,
        "effect": "15% back from the money stolen from you",
    },
    "bandana": {
        "name": "Bandana",
        "description": "No face, no case.",
        "cost": 400,
        "effect": "Steal from ANYONE — no rank restrictions",
    },
    "paladins_shield": {
        "name": "Paladin's Shield",
        "description": "A sacred shield, obtained from a fallen warrior born of the Dungeon. . .",
        "cost": 115,
        "effect": "10% more money from wild animal and boss fights",
    },
    "msi_afterburner": {
        "name": "MSI Afterburner",
        "description": "Overclock your GPUs!",
        "cost": 105,
        "effect": "20% increase to money from /sell",
    },
    "eclipse_glasses": {
        "name": "Eclipse Glasses",
        "description": "I love staring at the sun!",
        "cost": 67,
        "effect": "15% more money during a Solar Eclipse",
    },
    "work_lunch": {
        "name": "Work Lunch",
        "description": "Good for morale!",
        "cost": 160,
        "effect": "10% more money from each gardener's auto-gather and auto-harvest",
    },
    "overtime_approval": {
        "name": "Overtime Approval",
        "description": "Loyal to the farm!",
        "cost": 200,
        "effect": "10% chance for each gardener auto-gather to grant double money",
    },
    "slot_token": {
        "name": "Slot Token",
        "description": "Let's go gambling!",
        "cost": 195,
        "effect": "One free Slot Spin every day (resets midnight EST)",
    },
    "blanks": {
        "name": "Blanks",
        "description": "Is this Buckshot or Russian?",
        "cost": 200,
        "effect": "20% chance for a /russian bullet to be a blank when fired on you",
    },
    "rare_candy": {
        "name": "Rare Candy",
        "description": "+1 Level!",
        "cost": 40,
        "effect": "+2 plants from a /gathemon win",
    },
    "frontier_justice": {
        "name": "Frontier Justice",
        "description": "Need a Dispenser here!",
        "cost": 100,
        "effect": "+10% chance for a critical gather",
    },
    "best_buy_geek_squad": {
        "name": "Best Buy Geek Squad Membership",
        "description": "I LOVE GRAPHICAL PROCESSING UNITS!",
        "cost": 400,
        "effect": "Every click in /mine counts as two clicks",
    },
}
DAILY_SHOP_ITEM_IDS = list(DAILY_SHOP_ITEMS.keys())
MAX_DAILY_SHOP_PURCHASES = 3


def get_daily_shop_offerings(date_est: str, user_id: int = None) -> list:
    """
    Return item ids for the given EST date (YYYY-MM-DD).
    For a user: up to (3 + premium tier) items they don't own (Seed=4, Sprout=5, Sapling=6, Evergreen=7).
    Without user: up to 3 items.
    """
    seed = f"{date_est}_{user_id}" if user_id is not None else date_est
    rng = random.Random(seed)
    all_ids = list(DAILY_SHOP_ITEMS.keys())

    # No user context: just sample up to 3 items from the full list
    if user_id is None:
        k = min(3, len(all_ids))
        return rng.sample(all_ids, k)

    # For a user: deterministically shuffle all items, then take the first
    # (3 + premium tier) that the user does NOT already own.
    max_offerings = get_effective_daily_shop_max_purchases(user_id)
    shuffled = all_ids[:]
    rng.shuffle(shuffled)
    offerings_for_day: list[str] = []
    for item_id in shuffled:
        if not has_shop_item(user_id, item_id):
            offerings_for_day.append(item_id)
            if len(offerings_for_day) >= max_offerings:
                break
    return offerings_for_day


def _get_date_est() -> str:
    """Current date in EST as YYYY-MM-DD."""
    EST_OFFSET = datetime.timedelta(hours=-5)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_est = now_utc + EST_OFFSET
    return now_est.strftime("%Y-%m-%d")


def _seconds_until_midnight_est() -> float:
    """Seconds from now until next midnight EST."""
    EST_OFFSET = datetime.timedelta(hours=-5)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_est = now_utc + EST_OFFSET
    next_midnight = (now_est + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    target_utc = next_midnight - EST_OFFSET
    return (target_utc - now_utc).total_seconds()


def _format_refresh_countdown() -> str:
    """Human-readable countdown to next midnight EST (e.g. '19h 54m')."""
    secs = max(0, int(_seconds_until_midnight_est()))
    h, remainder = divmod(secs, 3600)
    m, _ = divmod(remainder, 60)
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _dailyshop_load_sync(member, user_id: int, date_est: str) -> dict:
    """Run in thread: sync premium tier + load shop inventory, offerings, tree rings. Returns dict for dailyshop command."""
    sync_premium_tier_from_member(member)
    inv = get_user_shop_inventory(user_id)
    offerings = get_daily_shop_offerings(date_est, user_id)
    tree_rings = get_user_tree_rings(user_id)
    return {"inv": inv, "offerings": offerings, "tree_rings": tree_rings}


async def _dailyshop_load_async(member, user_id: int, date_est: str) -> dict:
    """Sync premium first, then load inventory, offerings, and tree rings in parallel for minimal latency."""
    await asyncio.to_thread(sync_premium_tier_from_member, member)
    inv, tree_rings, offerings = await asyncio.gather(
        asyncio.to_thread(get_user_shop_inventory, user_id),
        asyncio.to_thread(get_user_tree_rings, user_id),
        asyncio.to_thread(get_daily_shop_offerings, date_est, user_id),
    )
    return {"inv": inv, "offerings": offerings, "tree_rings": tree_rings}


def _build_daily_shop_embed_and_view(offerings: list, date_est: str, user_id: int, tree_rings: int = None):
    """Build the Daily Shop embed and view for the given list of item ids (already filtered for user)."""
    tree_ring_emoji = "<:TreeRing:1474244868288282817>"
    if tree_rings is None:
        tree_rings = get_user_tree_rings(user_id)
    embed = discord.Embed(
        title="🛒 Daily Shop",
        description=f"{tree_ring_emoji} Your Tree Rings: **{tree_rings}**",
        color=discord.Color.green()
    )
    for item_id in offerings:
        info = DAILY_SHOP_ITEMS[item_id]
        item_emoji = info.get("emoji", tree_ring_emoji)
        embed.add_field(
            name=f"{item_emoji} {info['name']}",
            value=f"{info['description']}\n***{info['effect']}***\nPrice: **{info['cost']}** {tree_ring_emoji} Tree Rings",
            inline=False
        )
    embed.set_footer(text=f"Shop refreshes in {_format_refresh_countdown()}")
    view = DailyShopView(item_ids=offerings, date_est=date_est)
    return embed, view


@bot.tree.command(name="inviteawards", description="Check or claim your invite rewards!")
@app_commands.describe(action="Check your invite progress or claim rewards")
@app_commands.choices(action=[
    app_commands.Choice(name="Check", value="check"),
    app_commands.Choice(name="Claim", value="claim"),
])
async def inviteawards(interaction: discord.Interaction, action: app_commands.Choice[str]):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        user_id = interaction.user.id
        invite_stats = get_user_invite_stats(user_id)
        total_invites = invite_stats["total_joins"]
        claimed_rewards = invite_stats.get("claimed_rewards", [])
        
        if action.value == "check":
            # Show invite rewards progress
            embed = discord.Embed(
                title=f"🎁 {interaction.user.name}'s Invite Awards",
                color=discord.Color.gold()
            )
            embed.add_field(name="Invites", value=f"**{total_invites}**", inline=False)
            
            rewards_text = ""
            for tier in range(1, 21):
                reward = INVITE_REWARDS[tier]
                is_claimed = tier in claimed_rewards
                can_claim = total_invites >= tier and not is_claimed
                
                if is_claimed:
                    prefix = "✅"
                elif can_claim:
                    prefix = "🟡"
                else:
                    prefix = "⬜"
                
                rewards_text += f"{prefix} {tier} Invite{'s' if tier != 1 else ''}: {reward['description']}\n"
            
            embed.add_field(name="Rewards:", value=rewards_text, inline=False)
            embed.set_footer(text="Do /inviteawards claim to claim your invite awards!")
            
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        
        elif action.value == "claim":
            # Try to claim the next unclaimed reward
            next_tier = None
            for tier in range(1, 21):
                if tier not in claimed_rewards and total_invites >= tier:
                    next_tier = tier
                    break
            
            if next_tier is None:
                # Check if all are claimed or not enough invites
                all_claimed = all(t in claimed_rewards for t in range(1, 21))
                if all_claimed:
                    await safe_interaction_response(interaction, interaction.followup.send,
                        f"🎁 You've already claimed all invite rewards, {interaction.user.name}!", ephemeral=True)
                else:
                    # Find next unclaimed tier to show requirement
                    next_unclaimed = next((t for t in range(1, 21) if t not in claimed_rewards), None)
                    if next_unclaimed:
                        await safe_interaction_response(interaction, interaction.followup.send,
                            f"❌ You need **{next_unclaimed} invite{'s' if next_unclaimed != 1 else ''}** to claim the next reward! You have **{total_invites}**.",
                            ephemeral=True)
                    else:
                        await safe_interaction_response(interaction, interaction.followup.send,
                            f"❌ No rewards available to claim right now.", ephemeral=True)
                return
            
            # Claim the reward
            reward = INVITE_REWARDS[next_tier]
            success = claim_invite_reward(user_id, next_tier)
            if not success:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You've already claimed this reward!", ephemeral=True)
                return
            
            # Apply the reward
            reward_msg = ""
            if reward["type"] == "money":
                current_balance = get_user_balance(user_id)
                new_balance = normalize_money(current_balance + reward["amount"])
                update_user_balance(user_id, new_balance)
                reward_msg = f"You received {reward['description']}!"
            elif reward["type"] == "tree_rings":
                increment_tree_rings(user_id, reward["amount"])
                reward_msg = f"You received {reward['description']}!"
            elif reward["type"] == "secret_gardener":
                reward_msg = f"You unlocked the {reward['description']}! Check `/hire` page 6!"
            elif reward["type"] == "secret_gardener_harvest":
                reward_msg = f"You unlocked {reward['description']}"
            elif reward["type"] == "gather_cooldown":
                reward_msg = f"You unlocked {reward['description']}!"
            elif reward["type"] == "harvest_cooldown":
                reward_msg = f"You unlocked {reward['description']}!"
            elif reward["type"] == "mine_cooldown":
                reward_msg = f"You unlocked {reward['description']}!"
            elif reward["type"] == "water_double":
                reward_msg = f"You unlocked {reward['description']}!"
            elif reward["type"] == "hidden_achievement":
                # Award the social_butterfly hidden achievement
                newly_unlocked = unlock_hidden_achievement(user_id, "social_butterfly")
                reward_msg = f"You unlocked a {reward['description']}!"
                if newly_unlocked:
                    await send_hidden_achievement_notification(interaction, "social_butterfly")
            
            embed = discord.Embed(
                title="🎁 Invite Reward Claimed!",
                description=f"**Tier {next_tier}** — {reward_msg}",
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Invites: {total_invites} | Use /inviteawards check to see all rewards")
            
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in inviteawards command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


class DailyShopView(discord.ui.View):
    """View with Buy buttons for each of today's 3 items, plus Inventory. Stores date_est so buy validation uses the same day the shop was shown."""

    def __init__(self, item_ids: list, date_est: str = None, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.date_est = date_est or _get_date_est()
        for item_id in item_ids:
            info = DAILY_SHOP_ITEMS.get(item_id, {})
            label = f"Buy {info.get('name', item_id)}"
            if len(label) > 80:
                label = label[:77] + "..."
            self.add_item(DailyShopBuyButton(item_id=item_id, label=label))
        self.add_item(DailyShopInventoryButton())

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True


class DailyShopBuyButton(discord.ui.Button):
    def __init__(self, item_id: str, label: str):
        super().__init__(style=discord.ButtonStyle.primary, label=label, custom_id=f"dailyshop_buy_{item_id}")

    async def callback(self, interaction: discord.Interaction):
        item_id = self.custom_id.replace("dailyshop_buy_", "")
        if item_id not in DAILY_SHOP_ITEMS:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "❌ That item is no longer available.", ephemeral=True)
            return
        user_id = interaction.user.id
        # Use the date when this shop was opened (stored on the view), not "now" - so buying works even across midnight EST or from an open message
        date_est = getattr(self.view, "date_est", None) or _get_date_est()
        if has_shop_item(user_id, item_id):
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"❌ You already own **{DAILY_SHOP_ITEMS[item_id]['name']}**.", ephemeral=True)
            return
        offerings = get_daily_shop_offerings(date_est, user_id)
        if item_id not in offerings:
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"❌ **{DAILY_SHOP_ITEMS[item_id]['name']}** is not in today's shop.", ephemeral=True)
            return
        purchase_count, last_date = get_user_daily_shop_purchases(user_id)
        if last_date != date_est:
            purchase_count = 0
        max_purchases = get_effective_daily_shop_max_purchases(user_id)
        if purchase_count >= max_purchases:
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"❌ You've already bought **{max_purchases}** items today. Come back at midnight EST!", ephemeral=True)
            return
        info = DAILY_SHOP_ITEMS[item_id]
        cost = info["cost"]
        tree_rings = get_user_tree_rings(user_id)
        if tree_rings < cost:
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"❌ You need **{cost}** <:TreeRing:1474244868288282817> Tree Rings for **{info['name']}**, but you have **{tree_rings}**.", ephemeral=True)
            return
        success = purchase_daily_shop_item(user_id, item_id, cost, date_est)
        if not success:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "❌ Purchase failed (you may already own this item). Try again.", ephemeral=True)
            return
        await safe_interaction_response(interaction, interaction.response.defer, ephemeral=True)
        new_offerings = get_daily_shop_offerings(date_est, user_id)
        if new_offerings:
            new_embed, new_view = _build_daily_shop_embed_and_view(new_offerings, date_est, user_id)
            try:
                await interaction.message.edit(embed=new_embed, view=new_view)
            except Exception:
                pass
        else:
            tree_ring_emoji = "<:TreeRing:1474244868288282817>"
            tree_rings = get_user_tree_rings(user_id)
            done_embed = discord.Embed(
                title="🛒 Daily Shop",
                description=f"You've bought everything available for you today! You own all items currently on offer.\n\n{tree_ring_emoji} Your Tree Rings: **{tree_rings}**",
                color=discord.Color.gold()
            )
            done_embed.set_footer(text=f"Shop refreshes in {_format_refresh_countdown()}")
            try:
                await interaction.message.edit(embed=done_embed, view=None)
            except Exception:
                pass
        msg = f"✅ You bought **{info['name']}** for **{cost}** <:TreeRing:1474244868288282817> Tree Rings!"
        await safe_interaction_response(interaction, interaction.followup.send, msg, ephemeral=True)


# Discord embed field value limit
_EMBED_FIELD_VALUE_MAX = 1024


def _format_shop_inventory_field(inv: dict) -> list[str]:
    """
    Format shop inventory into one or more field-sized chunks (<= _EMBED_FIELD_VALUE_MAX chars each).
    Items are one-per-user (no quantity shown).
    """
    if not inv:
        return []

    # Build per-item blocks first
    blocks = []
    for i in sorted(inv.keys(), key=lambda k: (DAILY_SHOP_ITEMS.get(k, {}).get("name", k),)):
        info = DAILY_SHOP_ITEMS.get(i, {})
        name = info.get("name", i)
        desc = info.get("description", "")
        effect = info.get("effect", "")
        blocks.append(f"**{name}**\n*{desc}*\n**Buff:** {effect}")

    # Greedy pack blocks into chunks within the embed field limit
    chunks: list[str] = []
    current = ""
    for block in blocks:
        if not current:
            current = block
        elif len(current) + 2 + len(block) <= _EMBED_FIELD_VALUE_MAX:
            current = current + "\n\n" + block
        else:
            chunks.append(current)
            current = block
    if current:
        chunks.append(current)
    return chunks


class DailyShopInventoryButton(discord.ui.Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.secondary, label="Inventory", custom_id="dailyshop_inventory")

    async def callback(self, interaction: discord.Interaction):
        try:
            user_id = interaction.user.id
            inv = get_user_shop_inventory(user_id)
            # If no items, send a simple single embed
            if not inv:
                embed = discord.Embed(
                    title="🛒 Your Shop Inventory",
                    description="Items you've purchased from the Daily Shop.",
                    color=discord.Color.gold()
                )
                embed.add_field(
                    name="Items",
                    value="*No items yet. Use /dailyshop to open the shop and buy!*",
                    inline=False,
                )
                await safe_interaction_response(
                    interaction, interaction.response.send_message, embed=embed, ephemeral=True)
                return

            chunks = _format_shop_inventory_field(inv)
            embeds: list[discord.Embed] = []
            for idx, chunk in enumerate(chunks):
                title = "🛒 Your Shop Inventory" if idx == 0 else f"🛒 Your Shop Inventory (Page {idx + 1})"
                embed = discord.Embed(
                    title=title,
                    description="Items you've purchased from the Daily Shop.",
                    color=discord.Color.gold()
                )
                embed.add_field(name="Items", value=chunk, inline=False)
                embeds.append(embed)

            if len(embeds) == 1:
                await safe_interaction_response(
                    interaction, interaction.response.send_message, embed=embeds[0], ephemeral=True)
            else:
                await safe_interaction_response(
                    interaction, interaction.response.send_message, embeds=embeds, ephemeral=True)
        except Exception as e:
            traceback.print_exc()
            print(f"Error in dailyshop Inventory button: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="dailyshop", description="Open the Daily Shop or view your inventory")
@app_commands.describe(action="Open the shop or only view your inventory")
@app_commands.choices(action=[
    app_commands.Choice(name="Shop", value="shop"),
    app_commands.Choice(name="Inventory", value="inventory"),
])
async def dailyshop(interaction: discord.Interaction, action: app_commands.Choice[str] = None):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        user_id = interaction.user.id
        date_est = _get_date_est()
        data = await _dailyshop_load_async(interaction.user, user_id, date_est)
        if action is not None and action.value == "inventory":
            inv = data["inv"]
            # No items: single simple embed
            if not inv:
                embed = discord.Embed(
                    title="🛒 Daily Shop – Your Inventory",
                    description=f"{interaction.user.mention}'s purchased items (Tree Ring shop)",
                    color=discord.Color.gold()
                )
                embed.add_field(
                    name="Items",
                    value="*No items yet. Use /dailyshop to open the shop and buy!*",
                    inline=False,
                )
                embed.set_footer(text="Shop refreshes daily at midnight EST")
                await safe_interaction_response(
                    interaction, interaction.followup.send, embed=embed, ephemeral=True)
                return

            chunks = _format_shop_inventory_field(inv)
            embeds: list[discord.Embed] = []
            for idx, chunk in enumerate(chunks):
                title = "🛒 Daily Shop – Your Inventory" if idx == 0 else f"🛒 Daily Shop – Your Inventory (Page {idx + 1})"
                embed = discord.Embed(
                    title=title,
                    description=f"{interaction.user.mention}'s purchased items (Tree Ring shop)",
                    color=discord.Color.gold()
                )
                embed.add_field(name="Items", value=chunk, inline=False)
                embed.set_footer(text="Shop refreshes daily at midnight EST")
                embeds.append(embed)

            if len(embeds) == 1:
                await safe_interaction_response(
                    interaction, interaction.followup.send, embed=embeds[0], ephemeral=True)
            else:
                await safe_interaction_response(
                    interaction, interaction.followup.send, embeds=embeds, ephemeral=True)
            return
        offerings = data["offerings"]
        if not offerings:
            tree_ring_emoji = "<:TreeRing:1474244868288282817>"
            tree_rings = data["tree_rings"]
            embed = discord.Embed(
                title="🛒 Daily Shop",
                description=f"You already own **all** Daily Shop items! There's nothing new for you today. Check back after the next refresh.\n\n{tree_ring_emoji} Your Tree Rings: **{tree_rings}**",
                color=discord.Color.gold()
            )
            embed.set_footer(text=f"Shop refreshes in {_format_refresh_countdown()}")
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
            return
        embed, view = _build_daily_shop_embed_and_view(offerings, date_est, user_id, tree_rings=data["tree_rings"])
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view, ephemeral=True)
    except Exception as e:
        traceback.print_exc()
        print(f"Error in dailyshop command: {e}")
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


# userstats command (single DB read via get_user_dossier)
@bot.tree.command(name="userstats", description="View your statistics!")
async def userstats(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        user_id = interaction.user.id
        doc = await asyncio.to_thread(get_user_dossier, user_id)

        user_balance = doc["balance"]
        total_items = doc["gather_stats_total_items"]
        cycle_plants = doc["bloom_cycle_plants"]
        bloom_count = doc["bloom_count"]

        items_needed = None
        next_rank = None
        if cycle_plants < 50:
            items_needed = 50 - cycle_plants
            next_rank = "PLANTER II"
        elif cycle_plants < 150:
            items_needed = 150 - cycle_plants
            next_rank = "PLANTER III"
        elif cycle_plants < 300:
            items_needed = 300 - cycle_plants
            next_rank = "PLANTER IV"
        elif cycle_plants < 500:
            items_needed = 500 - cycle_plants
            next_rank = "PLANTER V"
        elif cycle_plants < 1000:
            items_needed = 1000 - cycle_plants
            next_rank = "PLANTER VI"
        elif cycle_plants < 2000:
            items_needed = 2000 - cycle_plants
            next_rank = "PLANTER VII"
        elif cycle_plants < 4000:
            items_needed = 4000 - cycle_plants
            next_rank = "PLANTER VIII"
        elif cycle_plants < 10000:
            items_needed = 10000 - cycle_plants
            next_rank = "PLANTER IX"
        elif cycle_plants < 15000:
            items_needed = 15000 - cycle_plants
            next_rank = "PLANTER X"
        else:
            items_needed = 0
            next_rank = "MAX RANK"

        bloom_rank = _bloom_count_to_rank(bloom_count)
        tree_rings = doc["tree_rings"]
        bloom_multiplier = 1.0 + (tree_rings * 0.005)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id, full_data={"bloom_count": bloom_count})
        water_streak = doc["consecutive_water_days"]
        daily_rate = 0.04 if doc["shop_inventory"].get("golden_watering_can", 0) >= 1 else 0.02
        daily_bonus_multiplier = 1.0 + (water_streak * daily_rate)
        ach_data = doc["achievements"]
        hoe_attunement = doc["hoe_enchantment"]
        tractor_attunement = doc["tractor_enchantment"]

        embed = discord.Embed(
            title=f"📊 {interaction.user.name}'s Stats",
            color=discord.Color.blue()
        )
        embed.add_field(name="💰 Balance", value=f"**${user_balance:.2f}**", inline=True)
        embed.add_field(name="🌱 Plants Gathered (Total)", value=f"**{total_items}** plants", inline=True)
        if bloom_count > 0:
            embed.add_field(name="🌿 Plants Gathered (This Bloom)", value=f"**{cycle_plants}** plants", inline=True)
        embed.add_field(name="🌲 Bloom Rank", value=f"**{bloom_rank}**", inline=True)
        tree_ring_boost_pct = (bloom_multiplier - 1.0) * 100
        embed.add_field(name="<:TreeRing:1474244868288282817> Tree Rings", value=f"**{tree_rings}** (+{tree_ring_boost_pct:.1f}% boost)", inline=True)
        if bloom_rank != "PINE I":
            embed.add_field(name="⭐ Rank Boost", value=f"**{rank_perma_buff_multiplier:.2f}x**", inline=True)
        day_text = "day" if water_streak == 1 else "days"
        water_streak_pct = (daily_bonus_multiplier - 1.0) * 100
        embed.add_field(name="💧 Water Streak", value=f"**{water_streak}** {day_text} (+{water_streak_pct:.1f}%)", inline=True)
        if hoe_attunement:
            hoe_name = hoe_attunement.get("name", "Unknown")
            hoe_rarity = hoe_attunement.get("rarity", "COMMON")
            embed.add_field(name="✨ Gather Imbuement", value=f"**{hoe_name}** {RARITY_EMOJI.get(hoe_rarity, f'[{hoe_rarity}]')}", inline=True)
        else:
            embed.add_field(name="✨ Gather Imbuement", value="**None**", inline=True)
        if tractor_attunement:
            tractor_name = tractor_attunement.get("name", "Unknown")
            tractor_rarity = tractor_attunement.get("rarity", "COMMON")
            embed.add_field(name="✨ Harvest Imbuement", value=f"**{tractor_name}** {RARITY_EMOJI.get(tractor_rarity, f'[{tractor_rarity}]')}", inline=True)
        else:
            embed.add_field(name="✨ Harvest Imbuement", value="**None**", inline=True)
        if items_needed == 0:
            embed.add_field(name="🏆 Rank Status", value=f"**{next_rank}** - You've reached **PLANTER X**!", inline=False)
        else:
            embed.add_field(name="📈 Next Rank", value=f"**{items_needed}** more plants until **{next_rank}**", inline=False)

        maxed_out_just_unlocked = check_maxed_out_achievement(user_id, dossier=doc)
        if maxed_out_just_unlocked:
            await send_hidden_achievement_notification(interaction, "maxed_out")

        total_achievement_categories = len(ACHIEVEMENTS)
        total_completion_slots = total_achievement_categories + TOTAL_HIDDEN_ACHIEVEMENTS
        completed_regular = 0
        for ach_name, ach_def in ACHIEVEMENTS.items():
            levels = ach_def.get("levels", [])
            if not levels:
                continue
            max_level = max(l["level"] for l in levels)
            if int(ach_data.get(ach_name, 0)) >= max_level:
                completed_regular += 1
        completed_hidden = int(ach_data.get("hidden_achievements_discovered", 0))
        completed_slots = completed_regular + completed_hidden
        # Weighted completion: prestige (bloom) has most weight so Pine II isn't ~70% done
        max_bloom_level = 18  # REDWOOD
        bloom_count = int(doc.get("bloom_count", 0))
        prestige_pct = (bloom_count / max_bloom_level) * 100 if max_bloom_level else 0
        ach_pct = (completed_slots / total_completion_slots) * 100 if total_completion_slots else 0
        completion_pct = round(0.7 * min(100, prestige_pct) + 0.3 * min(100, ach_pct))
        completion_pct = min(100, max(0, completion_pct))
        bar_length = 15
        filled = round((completion_pct / 100.0) * bar_length) if bar_length else 0
        filled = min(bar_length, max(0, filled))
        progress_bar = "\u2588" * filled + "\u2581" * (bar_length - filled)
        embed.add_field(name="GAME COMPLETION", value=f"{progress_bar} **{completion_pct}%**", inline=False)

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in userstats command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="treering", description="[ADMIN] Recalculate Tree Rings for everyone in this server (fix after wipe or desync)")
@app_commands.default_permissions(administrator=True)
async def treering(interaction: discord.Interaction):
    """[ADMIN] Reset Tree Rings for all users in this server to match their plants gathered."""
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        if not hasattr(interaction.channel, "name") or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ This command can only be used in the #hidden channel.", ephemeral=True)
            return
        guild = interaction.guild
        if not guild:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Could not get guild information.", ephemeral=True)
            return
        members = [m for m in guild.members if not m.bot]
        user_ids = [m.id for m in members]
        updated_count = await asyncio.to_thread(recalculate_guild_tree_rings, user_ids)
        await safe_interaction_response(
            interaction, interaction.followup.send,
            f"<:TreeRing:1474244868288282817> Tree Rings have been recalculated for **{updated_count}** user(s) in this server. Each user's Tree Rings now match their plants gathered.",
            ephemeral=True,
        )
        print(f"Admin {interaction.user.name} used /treering — recalculated for {updated_count} users")
    except Exception as e:
        print(f"Error in treering command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# almanac command: sections (flowers/fruits/vegetables), pagination, ??? = 2x HIDDEN, completion % (excluding Mikellion)
# 4 plants per page for all sections to avoid embed truncation
ALMANAC_PLANTS_PER_PAGE_BY_SECTION = {"Flower": 4, "Fruit": 4, "Vegetable": 4}


class AlmanacView(discord.ui.View):
    def __init__(self, user_id: int, section: str, almanac_entries: dict = None, timeout: float = 120):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.section = section  # "Flower" | "Fruit" | "Vegetable"
        self.page = 0
        self._plants_per_page = ALMANAC_PLANTS_PER_PAGE_BY_SECTION.get(section, 4)
        # Use pre-fetched entries so pagination never hits the DB
        self._almanac_entries = almanac_entries if almanac_entries is not None else get_user_almanac_entries(user_id)
        self._filled = _almanac_filled_count(self._almanac_entries)
        self._total_slots = _ALMANAC_TOTAL_SLOTS
        self._slots = _ALMANAC_SLOTS_BY_CATEGORY.get(section, [])
        self._by_plant: dict[str, list[str]] = {}
        for (item_name, rip) in self._slots:
            self._by_plant.setdefault(item_name, []).append(rip)
        self._plant_list = list(self._by_plant.keys())
        self._max_page = max(0, (len(self._plant_list) - 1) // self._plants_per_page)

    def _build_embed(self) -> discord.Embed:
        pct = (100.0 * self._filled / self._total_slots) if self._total_slots else 0
        start = self.page * self._plants_per_page
        end = min(start + self._plants_per_page, len(self._plant_list))
        plants_on_page = self._plant_list[start:end]
        lines = []
        entries = self._almanac_entries
        for item_name in plants_on_page:
            emoji = get_item_display_emoji(item_name)
            emoji_str = f"{emoji} " if emoji else ""
            desc = ITEM_DESCRIPTIONS.get(item_name, "A mysterious item from nature!")
            parts = []
            for rip in self._by_plant[item_name]:
                key = f"{item_name}{_ALMANAC_KEY_SEP}{rip}"
                if key in entries:
                    parts.append(f"**{rip.upper()}**")
                else:
                    parts.append(f"{ALMANAC_HIDDEN_EMOJI}{ALMANAC_HIDDEN_EMOJI}")
            line = f"{emoji_str}**{item_name.upper()}** — \"{desc}\"\n  " + " | ".join(parts)
            lines.append(line)
        body = "\n\n".join(lines) if lines else "*No plants in this section.*"
        title = f"📚 {self.section}s"
        embed = discord.Embed(title=title, description=body[:4000], color=discord.Color.green())
        embed.set_footer(text=f"Page {self.page + 1}/{self._max_page + 1} • Completion: {self._filled}/{self._total_slots} ({pct:.1f}%)")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, custom_id="almanac_prev")
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.defer(ephemeral=True)
            return
        self.page = max(0, self.page - 1)
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary, custom_id="almanac_next")
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.defer(ephemeral=True)
            return
        self.page = min(self._max_page, self.page + 1)
        await interaction.response.edit_message(embed=self._build_embed(), view=self)


@bot.tree.command(name="almanac", description="View your almanac: plants and ripeness you've gathered (by section)")
@app_commands.describe(section="Section to view: flowers, fruits, or vegetables")
@app_commands.choices(section=[
    app_commands.Choice(name="Flowers", value="flowers"),
    app_commands.Choice(name="Fruits", value="fruits"),
    app_commands.Choice(name="Vegetables", value="vegetables"),
])
async def almanac(interaction: discord.Interaction, section: app_commands.Choice[str] = None):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        user_id = interaction.user.id
        # Single DB read for almanac; view caches it so prev/next never hit DB
        almanac_entries = get_user_almanac_entries(user_id)
        section_map = {"flowers": "Flower", "fruits": "Fruit", "vegetables": "Vegetable"}
        choice = section.value if section else "flowers"
        section_cat = section_map.get(choice, "Flower")
        view = AlmanacView(user_id, section_cat, almanac_entries=almanac_entries)
        embed = view._build_embed()
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view, ephemeral=True)
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
        
        def _upgrade_bar(tier: int, max_tier: int = 10) -> str:
            return PROGRESS_Y * tier + PROGRESS_N * (max_tier - tier)

        # Path 1: Baskets (Money Multiplier)
        basket_tier = upgrades["basket"]
        current_basket = "No Basket" if basket_tier == 0 else BASKET_UPGRADES[basket_tier - 1]["name"]
        current_multiplier = 1.0 if basket_tier == 0 else BASKET_UPGRADES[basket_tier - 1]["multiplier"]
        bar_basket = _upgrade_bar(basket_tier)
        if basket_tier < 10:
            next_basket = BASKET_UPGRADES[basket_tier]["name"]
            next_multiplier = BASKET_UPGRADES[basket_tier]["multiplier"]
            next_cost = bloom_scaled_price(self.user_id, UPGRADE_PRICES[basket_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            basket_text = f"{bar_basket}\n**Current:** {current_basket} ({current_multiplier}x money)\n**Next:** {next_basket} ({next_multiplier}x money)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            basket_text = f"{bar_basket}\n**Current:** {current_basket} ({current_multiplier}x money)"
        
        embed.add_field(
            name="🧺 PATH 1: BASKETS",
            value=basket_text,
            inline=False
        )
        
        # Path 2: Shoes (Cooldown Reduction)
        shoes_tier = upgrades["shoes"]
        current_shoes = "Bare Feet" if shoes_tier == 0 else SHOES_UPGRADES[shoes_tier - 1]["name"]
        current_reduction = 0 if shoes_tier == 0 else SHOES_UPGRADES[shoes_tier - 1]["reduction"]
        bar_shoes = _upgrade_bar(shoes_tier)
        if shoes_tier < 10:
            next_shoes = SHOES_UPGRADES[shoes_tier]["name"]
            next_reduction = SHOES_UPGRADES[shoes_tier]["reduction"]
            next_cost = bloom_scaled_price(self.user_id, UPGRADE_PRICES[shoes_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            shoes_text = f"{bar_shoes}\n**Current:** {current_shoes} (-{current_reduction}s cooldown)\n**Next:** {next_shoes} (-{next_reduction}s cooldown)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            shoes_text = f"{bar_shoes}\n**Current:** {current_shoes} (-{current_reduction}s cooldown)"
        
        embed.add_field(
            name="👟 PATH 2: RUNNING SHOES",
            value=shoes_text,
            inline=False
        )
        
        # Path 3: Gloves (Chain Chance)
        gloves_tier = upgrades["gloves"]
        current_gloves = "Bare Hands" if gloves_tier == 0 else GLOVES_UPGRADES[gloves_tier - 1]["name"]
        current_chain = 0 if gloves_tier == 0 else GLOVES_UPGRADES[gloves_tier - 1]["chain_chance"] * 100
        bar_gloves = _upgrade_bar(gloves_tier)
        if gloves_tier < 10:
            next_gloves = GLOVES_UPGRADES[gloves_tier]["name"]
            next_chain = GLOVES_UPGRADES[gloves_tier]["chain_chance"] * 100
            next_cost = bloom_scaled_price(self.user_id, UPGRADE_PRICES[gloves_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            gloves_text = f"{bar_gloves}\n**Current:** {current_gloves} ({current_chain}% chain chance)\n**Next:** {next_gloves} ({next_chain}% chain chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            gloves_text = f"{bar_gloves}\n**Current:** {current_gloves} ({current_chain}% chain chance)"
        
        embed.add_field(
            name="🧤 PATH 3: GLOVES",
            value=gloves_text,
            inline=False
        )
        
        # Path 4: Soil (GMO Chance) — round % to avoid float display
        soil_tier = upgrades["soil"]
        current_soil = "Regular Soil" if soil_tier == 0 else SOIL_UPGRADES[soil_tier - 1]["name"]
        current_gmo = 0 if soil_tier == 0 else round(SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] * 100, 1)
        bar_soil = _upgrade_bar(soil_tier)
        if soil_tier < 10:
            next_soil = SOIL_UPGRADES[soil_tier]["name"]
            next_gmo = round(SOIL_UPGRADES[soil_tier]["gmo_boost"] * 100, 1)
            next_cost = bloom_scaled_price(self.user_id, UPGRADE_PRICES[soil_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            soil_text = f"{bar_soil}\n**Current:** {current_soil} (+{current_gmo}% GMO chance)\n**Next:** {next_soil} (+{next_gmo}% GMO chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            soil_text = f"{bar_soil}\n**Current:** {current_soil} (+{current_gmo}% GMO chance)"
        
        embed.add_field(
            name="🌱 PATH 4: SOIL",
            value=soil_text,
            inline=False
        )
        
        embed.set_footer(text="Click a button below to purchase an upgrade!")
        
        return embed
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="🧺", row=0)
    async def buy_basket(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "basket", BASKET_UPGRADES, "Basket")
        except Exception as e:
            print(f"Error in buy_basket: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="👟", row=0)
    async def buy_shoes(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "shoes", SHOES_UPGRADES, "Shoes")
        except Exception as e:
            print(f"Error in buy_shoes: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="🧤", row=1)
    async def buy_gloves(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "gloves", GLOVES_UPGRADES, "Gloves")
        except Exception as e:
            print(f"Error in buy_gloves: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="🌱", row=1)
    async def buy_soil(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "soil", SOIL_UPGRADES, "Soil")
        except Exception as e:
            print(f"Error in buy_soil: {e}")
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
            
            cost = bloom_scaled_price(self.user_id, UPGRADE_PRICES[current_tier])
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
            
            # Check for Maxed Out achievement (gear was the only upgrade path that did not call this)
            achievement_unlocked = check_maxed_out_achievement(self.user_id)
            
            # Send quick confirmation and update the main embed
            await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.", ephemeral=True)
            
            if achievement_unlocked:
                await send_hidden_achievement_notification(interaction, "maxed_out")
            
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
        
        def _upgrade_bar(tier: int, max_tier: int = 10) -> str:
            return PROGRESS_Y * tier + PROGRESS_N * (max_tier - tier)
        
        # Path 1: Car (Extra Items)
        car_tier = upgrades["car"]
        current_car = "Just Yourself" if car_tier == 0 else HARVEST_CAR_UPGRADES[car_tier - 1]["name"]
        current_extra = 0 if car_tier == 0 else HARVEST_CAR_UPGRADES[car_tier - 1]["extra_items"]
        bar_car = _upgrade_bar(car_tier)
        if car_tier < 10:
            next_car = HARVEST_CAR_UPGRADES[car_tier]["name"]
            next_extra = HARVEST_CAR_UPGRADES[car_tier]["extra_items"]
            next_cost = bloom_scaled_price(self.user_id, HARVEST_CAR_PRICES[car_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            car_text = f"{bar_car}\n**Current:** {current_car} (+{current_extra} extra items)\n**Next:** {next_car} (+{next_extra} extra items)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            car_text = f"{bar_car}\n**Current:** {current_car} (+{current_extra} extra items)"
        
        embed.add_field(
            name="🚗 PATH 1: VEHICLE",
            value=car_text,
            inline=False
        )
        
        # Path 2: Chain Chance (Season) — round % to avoid float display
        chain_tier = upgrades["chain"]
        current_season = "No Season" if chain_tier == 0 else HARVEST_CHAIN_UPGRADES[chain_tier - 1]["name"]
        current_chain = 0 if chain_tier == 0 else round(HARVEST_CHAIN_UPGRADES[chain_tier - 1]["chain_chance"] * 100, 1)
        bar_chain = _upgrade_bar(chain_tier)
        if chain_tier < 10:
            next_season = HARVEST_CHAIN_UPGRADES[chain_tier]["name"]
            next_chain = round(HARVEST_CHAIN_UPGRADES[chain_tier]["chain_chance"] * 100, 1)
            next_cost = bloom_scaled_price(self.user_id, HARVEST_CHAIN_PRICES[chain_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            chain_text = f"{bar_chain}\n**Current:** {current_season} ({current_chain}% chain chance)\n**Next:** {next_season} ({next_chain}% chain chance)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            chain_text = f"{bar_chain}\n**Current:** {current_season} ({current_chain}% chain chance)"
        
        embed.add_field(
            name="🌾 PATH 2: YIELD",
            value=chain_text,
            inline=False
        )
        
        # Path 3: Fertilizer (Money Multiplier) — round % to avoid float display
        fertilizer_tier = upgrades["fertilizer"]
        current_fertilizer = "No Fertilizer" if fertilizer_tier == 0 else HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["name"]
        current_multiplier = 0 if fertilizer_tier == 0 else round(HARVEST_FERTILIZER_UPGRADES[fertilizer_tier - 1]["multiplier"] * 100, 1)
        bar_fertilizer = _upgrade_bar(fertilizer_tier)
        if fertilizer_tier < 10:
            next_fertilizer = HARVEST_FERTILIZER_UPGRADES[fertilizer_tier]["name"]
            next_multiplier = round(HARVEST_FERTILIZER_UPGRADES[fertilizer_tier]["multiplier"] * 100, 1)
            next_cost = bloom_scaled_price(self.user_id, HARVEST_FERTILIZER_PRICES[fertilizer_tier])
            can_afford = "✅" if balance >= next_cost else "❌"
            fertilizer_text = f"{bar_fertilizer}\n**Current:** {current_fertilizer} (+{current_multiplier}% money)\n**Next:** {next_fertilizer} (+{next_multiplier}% money)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            fertilizer_text = f"{bar_fertilizer}\n**Current:** {current_fertilizer} (+{current_multiplier}% money)"
        
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
            next_cost = bloom_scaled_price(self.user_id, HARVEST_COOLDOWN_PRICES[cooldown_tier])
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
            
            bar_cooldown = _upgrade_bar(cooldown_tier)
            cooldown_text = f"{bar_cooldown}\n**Current:** {current_workers} ({current_reduction_text} cooldown)\n**Next:** {next_workers} ({reduction_text} cooldown)\n**Cost:** ${next_cost:,.2f} {can_afford}"
        else:
            bar_cooldown = _upgrade_bar(cooldown_tier)
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
            cooldown_text = f"{bar_cooldown}\n**Current:** {current_workers} ({current_reduction_text} cooldown)"
        
        embed.add_field(
            name="⚡ PATH 4: COOLDOWN REDUCTION",
            value=cooldown_text,
            inline=False
        )
        
        embed.set_footer(text="Click a button below to purchase an upgrade!")
        
        return embed
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="🚗", row=0)
    async def buy_car(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "car", HARVEST_CAR_UPGRADES, HARVEST_CAR_PRICES, "Vehicle")
        except Exception as e:
            print(f"Error in buy_car: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="🌾", row=0)
    async def buy_chain(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "chain", HARVEST_CHAIN_UPGRADES, HARVEST_CHAIN_PRICES, "Yield")
        except Exception as e:
            print(f"Error in buy_chain: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="💩", row=1)
    async def buy_fertilizer(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "fertilizer", HARVEST_FERTILIZER_UPGRADES, HARVEST_FERTILIZER_PRICES, "Fertilizer")
        except Exception as e:
            print(f"Error in buy_fertilizer: {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.primary, emoji="⚡", row=1)
    async def buy_cooldown(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.handle_purchase(interaction, "cooldown", HARVEST_COOLDOWN_UPGRADES, HARVEST_COOLDOWN_PRICES, "Workers")
        except Exception as e:
            print(f"Error in buy_cooldown: {e}")
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
            
            cost = bloom_scaled_price(self.user_id, price_list[current_tier])
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
            await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.", ephemeral=True)
            
            if achievement_unlocked:
                await send_hidden_achievement_notification(interaction, "maxed_out")
            
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


# ============================================================
# IMBUE (Imbuement) System
# ============================================================

class ImbueView(discord.ui.View):
    """View with Replace / Keep Current Imbuement / Recast buttons for the /imbue command."""

    def __init__(self, user_id: int, tool_type: str, rolled_enchant: dict, current_enchant: dict | None,
                 channel: discord.abc.Messageable, user_name: str, timeout=60):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.tool_type = tool_type  # "hoe" or "tractor"
        self.rolled_enchant = rolled_enchant
        self.current_enchant = current_enchant
        self.channel = channel  # For public announcement
        self.user_name = user_name
        self.cost = IMBUE_HOE_COST if tool_type == "hoe" else IMBUE_TRACTOR_COST
        # First-ever roll: user had no imbuement — cannot "keep" nothing; hide only "Keep Current"
        if self.current_enchant is None:
            for child in list(self.children):
                if getattr(child, "label", None) == "Keep Current":
                    self.remove_item(child)
                    break

    def _build_embed(self) -> discord.Embed:
        """Build the ephemeral imbuement choice embed."""
        type_label = "GATHER" if self.tool_type == "hoe" else "HARVEST"
        rarity_color = RARITY_COLORS.get(self.rolled_enchant["rarity"], 0x808080)
        embed = discord.Embed(
            title=f"{type_label} IMBUEMENT \u2728",
            color=discord.Color(rarity_color),
        )

        # New Imbuement Rolled (mask SECRET imbue stats with mysterious GIF block until claimed)
        if self.rolled_enchant.get("rarity") == "SECRET":
            tool_word = "HOE" if self.tool_type == "hoe" else "TRACTOR"
            g1, g2 = IMBUE_SEC_GIF_1, IMBUE_SEC_GIF_2
            # Gather (hoe) shows ABUNDANCE-style cryptic line; Harvest (tractor) shows NATURE'S FAVOR binary line
            if self.tool_type == "hoe":
                effect_line = f"\" (or ABUN{g2}{g2}NCE {g1})\""
            else:
                effect_line = f"NATURE'S {g2}AVOR 110011011010"
            rolled_block = (
                f"{g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1} {g1}\n"
                f"*\"?????? {tool_word} ?????? PENULTI{g1} ?????\"*\n"
                f"RESO{g2}ANCE {g2}\n"
                f"imbue.prosperity = {g2}\n"
                "`ERROR on line 4129: renewalNotFound`\n"
                f"{effect_line}"
            )
        else:
            rolled_block = format_enchant_block(self.rolled_enchant, self.tool_type)
        embed.add_field(name="New Imbuement Rolled", value=rolled_block, inline=False)

        # Current Imbuement
        if self.current_enchant:
            current_block = format_enchant_block(self.current_enchant, self.tool_type)
        else:
            current_block = "**NONE**"
        embed.add_field(name="Current Imbuement", value=current_block, inline=False)

        # Footer with balance and cost
        balance = get_user_balance(self.user_id)
        embed.set_footer(text=f"Balance: ${balance:,.2f}     |     Cost/Imbuement: ${self.cost:,.0f}")
        return embed

    async def _send_public_announcement(self, enchant: dict):
        """Send the public announcement embed when a user gets an imbuement."""
        type_label = "GATHER" if self.tool_type == "hoe" else "HARVEST"
        tool_label = "hoe" if self.tool_type == "hoe" else "tractor"
        rarity_color = RARITY_COLORS.get(enchant["rarity"], 0x808080)

        embed = discord.Embed(
            title=f"{type_label} IMBUEMENT \u2728 \U0001f33a",
            description=(
                f"\u2728 **{self.user_name}** has enchanted their {tool_label} with:\n\n"
                f"{format_enchant_block(enchant, self.tool_type)}"
            ),
            color=discord.Color(rarity_color),
        )
        try:
            await self.channel.send(embed=embed)
        except Exception as e:
            print(f"Error sending imbue announcement: {e}")

    @discord.ui.button(label="Replace", style=discord.ButtonStyle.success, emoji="\u2705")
    async def replace_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "\u274c This isn't your imbuement menu!", ephemeral=True)
            return

        # Save the new imbuement
        if self.tool_type == "hoe":
            set_user_hoe_attunement(self.user_id, self.rolled_enchant)
        else:
            set_user_tractor_attunement(self.user_id, self.rolled_enchant)

        # Disable all buttons
        for child in self.children:
            child.disabled = True

        # Update the ephemeral message
        confirm_embed = discord.Embed(
            title="\u2705 Imbuement Replaced!",
            description=f"Your {self.tool_type} has been enchanted with **{self.rolled_enchant['name']}**!",
            color=discord.Color.green(),
        )
        await safe_interaction_response(interaction, interaction.response.edit_message, embed=confirm_embed, view=self)

        # Send public announcement
        await self._send_public_announcement(self.rolled_enchant)

        # Auto-log to #rares when user keeps a netherite+ imbue (Replace = keeping this new one)
        guild = getattr(interaction, "guild", None)
        if guild and self.rolled_enchant.get("rarity") in IMBUE_RARES_RARITIES:
            asyncio.create_task(_post_rares_imbue(guild, interaction.user, self.rolled_enchant, self.tool_type))

        # Check for hidden achievement: High Reroller (NETHERITE, LUMINITE, CELESTIAL, or SECRET)
        high_rarities = {"NETHERITE", "LUMINITE", "CELESTIAL", "SECRET"}
        if self.rolled_enchant.get("rarity") in high_rarities:
            if unlock_hidden_achievement(self.user_id, "high_reroller"):
                try:
                    await interaction.followup.send(
                        embed=discord.Embed(
                            title="🏆 Hidden Achievement Unlocked!",
                            description=f"**High Reroller**\nGet an imbue enchantment that is NETHERITE, LUMINITE, CELESTIAL, or SECRET",
                            color=discord.Color.gold()
                        ),
                        ephemeral=True
                    )
                except Exception as e:
                    print(f"Error sending High Reroller achievement notification: {e}")

        self.stop()

    @discord.ui.button(label="Keep Current", style=discord.ButtonStyle.secondary, emoji="\U0001f6e1\ufe0f")
    async def keep_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "\u274c This isn't your imbuement menu!", ephemeral=True)
            return

        # Disable all buttons
        for child in self.children:
            child.disabled = True

        keep_embed = discord.Embed(
            title="\U0001f6e1\ufe0f Imbuement Kept",
            description="You kept your current imbuement.",
            color=discord.Color.light_grey(),
        )
        await safe_interaction_response(interaction, interaction.response.edit_message, embed=keep_embed, view=self)

        self.stop()

    @discord.ui.button(label="Recast", style=discord.ButtonStyle.primary, emoji="\U0001f504")
    async def recast_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "\u274c This isn't your imbuement menu!", ephemeral=True)
            return

        # Atomic deduction: check + deduct in one DB call (no race condition)
        success, new_balance = atomic_deduct_balance(self.user_id, self.cost)
        if not success:
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"\u274c You can't afford another recast! You need **${self.cost:,.0f}** but only have **${new_balance:,.2f}**.",
                ephemeral=True)
            return

        try:
            # Roll new imbuement (exclude current so you can't roll the same one)
            self.rolled_enchant = roll_attunement(self.tool_type, self.user_id, self.current_enchant)

            # Update embed
            embed = self._build_embed()
            await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            # Refund if we took money but failed to update the message
            refund_balance(self.user_id, self.cost)
            print(f"Error in recast_button (refunded ${self.cost:,.0f} to {self.user_id}): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message,
                "\u274c An error occurred and your money has been refunded. Please try again.", ephemeral=True)

    async def on_timeout(self):
        """When the view times out, disable all buttons."""
        for child in self.children:
            child.disabled = True
        # We can't easily edit the message on timeout from a view,
        # but the buttons will be non-functional after timeout anyway.


@bot.tree.command(name="imbue", description="Enchant your hoe or tractor with magical powers!")
@app_commands.describe(tool="Choose which tool to enchant")
@app_commands.choices(tool=[
    app_commands.Choice(name="Hoe (Gather imbuement - $750,000)", value="hoe"),
    app_commands.Choice(name="Tractor (Harvest imbuement - $4,000,000)", value="tractor"),
])
async def imbue(interaction: discord.Interaction, tool: app_commands.Choice[str]):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return

        # Channel restriction: #imbue only
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "imbue":
            await safe_interaction_response(interaction, interaction.followup.send,
                "\u274c This command can only be used in the #imbue channel!", ephemeral=True)
            return

        user_id = interaction.user.id
        tool_type = tool.value  # "hoe" or "tractor"
        cost = IMBUE_HOE_COST if tool_type == "hoe" else IMBUE_TRACTOR_COST

        # Per-user lock prevents concurrent imbue operations
        if user_id not in _imbue_locks:
            _imbue_locks[user_id] = asyncio.Lock()
        async with _imbue_locks[user_id]:
            # Atomic deduction: check + deduct in one DB call (no race condition)
            success, new_balance = await asyncio.to_thread(atomic_deduct_balance, user_id, cost)
            if not success:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"\u274c You don't have enough money! You need **${cost:,.0f}** but only have **${new_balance:,.2f}**.",
                    ephemeral=True)
                return

            try:
                # Get current imbuement
                if tool_type == "hoe":
                    current_enchant = get_user_hoe_attunement(user_id)
                else:
                    current_enchant = get_user_tractor_attunement(user_id)

                # Roll a new imbuement (exclude current so you can't roll the same one)
                rolled_enchant = roll_attunement(tool_type, user_id, current_enchant)

                # Create the view and embed
                view = ImbueView(
                    user_id=user_id,
                    tool_type=tool_type,
                    rolled_enchant=rolled_enchant,
                    current_enchant=current_enchant,
                    channel=interaction.channel,
                    user_name=interaction.user.name,
                    timeout=60,
                )
                embed = view._build_embed()

                await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view, ephemeral=True)
            except Exception as inner_e:
                # Refund if we took money but failed to show the imbue menu
                refund_balance(user_id, cost)
                print(f"Error in imbue command (refunded ${cost:,.0f} to {user_id}): {inner_e}")
                await safe_interaction_response(interaction, interaction.followup.send,
                    "\u274c An error occurred and your money has been refunded. Please try again.", ephemeral=True)
    except Exception as e:
        print(f"Error in imbue command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send,
            "\u274c An error occurred. Please try again.", ephemeral=True)


# Hire View with pagination (gardeners 1-5, then premium 6-9 by tier, then Secret Gardener)
class HireView(discord.ui.View):
    def __init__(self, user_id: int, timeout=300):
        super().__init__(timeout=timeout)
        self.user_id = user_id
        self.current_page = 0
        # 5 regular + premium tier pages (1-4) + 1 secret = 6 + tier
        self.total_pages = 6 + get_user_premium_tier(user_id)

    def _page_to_slot_id(self, page: int) -> int | None:
        """Return gardener slot_id (1-9) for this page, or None if Secret Gardener page."""
        if page == self.total_pages - 1:
            return None  # Secret Gardener
        if page < 5:
            return page + 1  # Gardeners 1-5
        return 6 + (page - 5)  # Premium gardeners 6-9

    def create_embed(self, page: int) -> discord.Embed:
        """Create the embed for a specific gardener page."""
        slot_id = self._page_to_slot_id(page)
        # Secret Gardener (last page)
        if slot_id is None:
            return self._create_secret_gardener_embed()
        # Premium gardener (slot 6-9)
        if slot_id >= 6:
            tier = get_user_premium_tier(self.user_id)
            if slot_id > 5 + tier:
                return self._create_secret_gardener_embed()  # shouldn't happen
            tier_name = PREMIUM_ROLE_NAMES[slot_id - 6]  # Seed, Sprout, Sapling, Evergreen
            tier_label = tier_name.upper()
            chance = PREMIUM_GARDENER_CHANCES.get(slot_id, 0.05) * 100
            # Premium gardeners use fixed auto-harvest chances by tier
            harvest_chance = PREMIUM_GARDENER_HARVEST_CHANCES.get(slot_id, 0.0)
            harvest_chance_pct = max(0.0, min(100.0, harvest_chance * 100))
            color = PREMIUM_GARDENER_GATHER_COLORS.get(slot_id, discord.Color.blue())
            tier_emoji = PREMIUM_GARDENER_EMOJI.get(slot_id, "🪴")
            embed = discord.Embed(
                title=f"{tier_emoji} {tier_label} RANK GARDENER",
                description=(
                    f"This gardener has a **{chance:.0f}%** chance to /gather every minute, "
                    f"and a **{harvest_chance_pct:.0f}%** chance to auto-harvest!"
                ),
                color=color
            )
            embed.add_field(name="Status", value="**HIRED** ✅", inline=False)
            embed.add_field(name="Plants Gathered", value="**N/A**", inline=True)
            embed.add_field(name="Total Money Earned", value="**N/A**", inline=True)
            embed.set_footer(text=f"Page {page + 1} of {self.total_pages}")
            return embed
        # Regular gardener 1-5
        gardeners = get_user_gardeners(self.user_id)
        balance = get_user_balance(self.user_id)
        gardener_dict = {g["id"]: g for g in gardeners}
        gardener = gardener_dict.get(slot_id)
        price = bloom_scaled_price(self.user_id, GARDENER_PRICES[slot_id - 1])
        gardener_chance = GARDENER_CHANCES.get(slot_id, 0.05) * 100
        description_text = f"💰 Your Balance: **${balance:,.2f}**\n\nHire gardeners to automatically gather items for you! This gardener has a **{gardener_chance:.0f}%** chance to gather every minute."
        embed = discord.Embed(
            title=f"🌱 Gardener #{slot_id}",
            description=description_text,
            color=discord.Color.green()
        )
        if gardener:
            plants_gathered = gardener.get("plants_gathered", 0)
            total_money = gardener.get("total_money_earned", 0.0)
            has_tool = gardener.get("has_tool", False)
            tool_info = GARDENER_TOOLS.get(slot_id, {"name": "Tool", "cost": 0, "chance": 0})
            tool_chance_pct = round(tool_info["chance"] * 100, 1)
            tool_chance_str = f"{tool_chance_pct:.1f}%" if tool_chance_pct < 1 else f"{tool_chance_pct:.0f}%"
            embed.add_field(name="Status", value="**HIRED** ✅", inline=False)
            embed.add_field(name="Plants Gathered", value=f"**{plants_gathered}**", inline=True)
            embed.add_field(name="Total Money Earned", value=f"**${total_money:,.2f}**", inline=True)
            if has_tool:
                embed.add_field(name="Tool", value=f"**{tool_info['name']}** ✅ for a **{tool_chance_str}** chance to auto harvest", inline=False)
            else:
                embed.add_field(name="Tool", value=f"Buy **{tool_info['name']}** for a **{tool_chance_str}** chance to auto harvest", inline=False)
        else:
            can_afford = "✅" if balance >= price else "❌"
            embed.add_field(name="Status", value="**Available**", inline=False)
            embed.add_field(name="Price", value=f"**${price:,.2f}** {can_afford}", inline=True)
        embed.set_footer(text=f"Page {page + 1} of {self.total_pages}")
        return embed

    def _create_secret_gardener_embed(self) -> discord.Embed:
        """Create the Secret Gardener page embed (last page)."""
        sg_unlocked = has_secret_gardener(self.user_id)
        sg_harvest = has_secret_gardener_harvest(self.user_id)
        embed = discord.Embed(
            title="\U0001f331\u2728 Secret Gardener",
            description="A mysterious gardener with incredible abilities!",
            color=discord.Color.purple() if sg_unlocked else discord.Color.dark_grey()
        )
        if sg_unlocked:
            embed.add_field(name="Status", value="**UNLOCKED** \u2705", inline=False)
        else:
            embed.add_field(name="Status", value="\U0001f512 Locked \u2014 Invite 10 people to unlock!", inline=False)
        embed.add_field(name="Gather Chance", value="**50%** chance to /gather every minute", inline=False)
        if sg_harvest:
            embed.add_field(name="Auto Harvest", value="**UNLOCKED** \u2705 \u2014 Can auto-harvest like other gardeners!", inline=False)
        else:
            embed.add_field(name="Auto Harvest", value="\U0001f512 Locked \u2014 Invite 12 people to unlock!", inline=False)
        embed.set_footer(text=f"Page {self.total_pages} of {self.total_pages}")
        return embed
    
    def update_buttons(self):
        """Update button states based on current page and gardener status."""
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1
        slot_id = self._page_to_slot_id(self.current_page)

        # Secret Gardener page (last page)
        if slot_id is None:
            self.hire_button.disabled = True
            self.hire_button.label = "INVITE REWARD ONLY"
            self.hire_button.style = discord.ButtonStyle.secondary
            self.buy_tool_button.disabled = True
            self.buy_tool_button.label = "INVITE REWARD ONLY"
            self.buy_tool_button.style = discord.ButtonStyle.secondary
            return

        # Premium gardener page (slot 6-9): no hire, no tool
        if slot_id >= 6:
            tier_name = PREMIUM_ROLE_NAMES[slot_id - 6]
            tier_label = tier_name.upper()
            self.hire_button.disabled = True
            self.hire_button.label = f"REQUIRES {tier_label}"
            self.hire_button.style = discord.ButtonStyle.secondary
            self.buy_tool_button.disabled = True
            self.buy_tool_button.label = f"REQUIRES {tier_label}"
            self.buy_tool_button.style = discord.ButtonStyle.secondary
            return

        # Regular gardener 1-5
        gardeners = get_user_gardeners(self.user_id)
        gardener_dict = {g["id"]: g for g in gardeners}
        gardener = gardener_dict.get(slot_id)
        balance = get_user_balance(self.user_id)
        price = bloom_scaled_price(self.user_id, GARDENER_PRICES[slot_id - 1])
        if gardener:
            self.hire_button.disabled = True
            self.hire_button.label = "Already Hired"
            self.hire_button.style = discord.ButtonStyle.secondary
        elif balance < price:
            self.hire_button.disabled = True
            self.hire_button.label = f"Hire (Need ${price:,.0f})"
            self.hire_button.style = discord.ButtonStyle.secondary
        elif len(gardeners) >= 5:
            self.hire_button.disabled = True
            self.hire_button.label = "Max Gardeners"
            self.hire_button.style = discord.ButtonStyle.secondary
        else:
            self.hire_button.disabled = False
            self.hire_button.label = f"Hire for ${price:,.0f}"
            self.hire_button.style = discord.ButtonStyle.success
        tool_info = GARDENER_TOOLS.get(slot_id, {"name": "Tool", "cost": 0})
        if gardener:
            if gardener.get("has_tool", False):
                self.buy_tool_button.disabled = True
                self.buy_tool_button.label = f"Tool: {tool_info['name']} \u2713"
                self.buy_tool_button.style = discord.ButtonStyle.secondary
            else:
                tool_cost = bloom_scaled_price(self.user_id, tool_info["cost"])
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
            slot_id = self._page_to_slot_id(self.current_page)
            if slot_id is None or slot_id >= 6:
                await safe_defer(interaction)
                return
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
            
            price = bloom_scaled_price(self.user_id, GARDENER_PRICES[slot_id - 1])
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
            await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Hired **Gardener #{slot_id}** for ${price:,.2f}! They'll start gathering for you automatically.", ephemeral=True)
            
            if achievement_unlocked:
                await send_hidden_achievement_notification(interaction, "maxed_out")
            
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
            slot_id = self._page_to_slot_id(self.current_page)
            if slot_id is None or slot_id >= 6:
                await safe_defer(interaction)
                return
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
            tool_cost = bloom_scaled_price(self.user_id, tool_info["cost"])
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
            
            chance_pct = round(tool_info["chance"] * 100, 1)
            await safe_interaction_response(interaction, interaction.response.send_message,
                f"✅ **{tool_info['name']}** purchased for ${tool_cost:,.2f}! This gardener's auto gather now has a **{chance_pct}%** chance to upgrade to a full harvest!", ephemeral=True)
            
            if achievement_unlocked:
                await send_hidden_achievement_notification(interaction, "maxed_out")
            
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
        sync_premium_tier_from_member(interaction.user)
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
        price = bloom_scaled_price(self.user_id, gpu_info["price"])
        
        embed = discord.Embed(
            title=f"🖥️ {gpu_name}",
            description=f"💰 Balance: **${balance:,.2f}**\n\nBuy GPUs to boost your mining!",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="Mining Boost",
            value=f"**+{gpu_info['percent_increase']}%**",
            inline=True
        )
        embed.add_field(
            name="Time Boost",
            value=f"**+{gpu_info['seconds_increase']}** seconds",
            inline=True
        )
        embed.add_field(
            name="Price",
            value=f"**${price:,.2f}** {'✅' if balance >= price else '❌'}",
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
        price = bloom_scaled_price(self.user_id, gpu_info["price"])
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
            price = bloom_scaled_price(self.user_id, gpu_info["price"])
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
            await safe_interaction_response(interaction, interaction.response.send_message, f"✅ Purchased **{gpu_name}** for ${price:,.2f}! It will boost your mining!", ephemeral=True)
            
            if achievement_unlocked:
                await send_hidden_achievement_notification(interaction, "maxed_out")
            
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


@bot.tree.command(name="startcelestialevent", description="[ADMIN] Start a Solar Eclipse or Blood Moon event manually")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(event=[
    app_commands.Choice(name="☀️ Solar Eclipse", value="solar_eclipse"),
    app_commands.Choice(name="🌙 Blood Moon", value="blood_moon")
])
@app_commands.choices(duration=[
    app_commands.Choice(name="30 minutes", value=30),
    app_commands.Choice(name="1 hour", value=60),
    app_commands.Choice(name="2 hours", value=120),
    app_commands.Choice(name="6 hours", value=360)
])
async def startcelestialevent(interaction: discord.Interaction, event: str, duration: int):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return

        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return

        existing = get_active_event_by_type(event)
        if existing and existing.get("end_time", 0) > time.time():
            label = SOLAR_ECLIPSE_TITLE if event == "solar_eclipse" else BLOOD_MOON_TITLE
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ **Error**: **{label}** is already active. Use `/endevent` to end it first.",
                ephemeral=True)
            return
        if existing:
            clear_event(existing.get("event_id", ""))

        # End any active hourly and daily (send their end embeds, then clear) before starting celestial
        for ev_type in ("hourly", "daily"):
            other = get_active_event_by_type(ev_type)
            if other:
                await _end_active_event_for_all_guilds(other)
                print(f"Admin start celestial: ended active {ev_type} event.")

        start_time = time.time()
        end_time = start_time + duration * 60
        event_id = f"{event}_{int(start_time)}"
        title = SOLAR_ECLIPSE_TITLE if event == "solar_eclipse" else BLOOD_MOON_TITLE

        set_active_event(
            event_id=event_id,
            event_type=event,
            event_name=title,
            start_time=start_time,
            end_time=end_time,
            effects={"event_id": event}
        )

        for guild in bot.guilds:
            try:
                await send_event_start_embed(guild, {
                    "event_type": event,
                    "event_id": event,
                    "event_name": title,
                    "effects": {"event_id": event}
                }, duration)
            except Exception as e:
                print(f"Error sending celestial start embed to {guild.name}: {e}")

        embed = discord.Embed(
            title="✅ Celestial Event Started",
            description=f"**{title}** for {duration} minutes.",
            color=discord.Color.green()
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} started celestial event: {title} for {duration} minutes")
    except Exception as e:
        print(f"Error in startcelestialevent command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="endevent", description="[ADMIN] End the currently active hourly, daily, Solar Eclipse, or Blood Moon event")
@app_commands.default_permissions(administrator=True)
@app_commands.choices(event_type=[
    app_commands.Choice(name="hourly", value="hourly"),
    app_commands.Choice(name="daily", value="daily"),
    app_commands.Choice(name="Solar Eclipse", value="solar_eclipse"),
    app_commands.Choice(name="Blood Moon", value="blood_moon")
])
async def endevent(interaction: discord.Interaction, event_type: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return

        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return

        active_event = get_active_event_by_type(event_type)
        if not active_event:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ **Error**: No active {event_type} event found.",
                ephemeral=True)
            return

        # Celestial events: no lookup in HOURLY/DAILY_EVENTS; use event_name from active_event
        if event_type in ("solar_eclipse", "blood_moon"):
            display_name = active_event.get("event_name", event_type)
            clear_event(active_event.get("event_id", ""))
            for guild in bot.guilds:
                try:
                    await send_event_end_embed(guild, active_event)
                except Exception as e:
                    print(f"Error sending end embed to {guild.name}: {e}")
            embed = discord.Embed(
                title="✅ Event Ended Successfully",
                description=f"**{display_name}**",
                color=discord.Color.orange()
            )
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
            print(f"Admin {interaction.user.name} ended {event_type} event: {display_name}")
            return

        # Hourly/daily: get event_info for emoji and name
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

        clear_event(active_event.get("event_id", ""))

        for guild in bot.guilds:
            try:
                await send_event_end_embed(guild, active_event)
            except Exception as e:
                print(f"Error sending end embed to {guild.name}: {e}")

        embed = discord.Embed(
            title="✅ Event Ended Successfully",
            description=f"**{event_info['emoji']} {event_info['name']}**",
            color=discord.Color.orange()
        )
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} ended {event_type} event: {event_info['name']}")
    except Exception as e:
        print(f"Error in endevent command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Spawn commands - Admin only. Split into spawn_animal and spawn_boss so each has ≤25 choices (Discord limit).
def _spawn_animal_choices():
    choices = [app_commands.Choice(name=f"{a['emoji']} {a['name']}", value=a["name"].lower().replace(" ", "_")) for a in PVE_WILD_ANIMALS_SINGLE]
    choices.append(app_commands.Choice(name="🐜 Bullet Ant Swarm", value="bullet_ant_swarm"))
    choices.extend([app_commands.Choice(name=f"{a['emoji']} {a['name']} (Jungle)", value="jungle_" + a["id"]) for a in PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE_SINGLE])
    choices.append(app_commands.Choice(name="🐝 Bee Swarm (Jungle)", value="bee_swarm"))
    return choices

def _spawn_boss_choices():
    non_twins = [b for b in PVE_BOSSES if b["id"] not in PVE_BOSSES_TWINS_IDS]
    choices = [app_commands.Choice(name=f"{b['emoji']} {b['name']}", value=b["id"]) for b in non_twins]
    choices.append(app_commands.Choice(name="👁️🔥 The Twins", value="twins"))
    return choices

_SPAWN_CHANNEL_CHOICES = [
    app_commands.Choice(name="#forest", value="forest"),
    app_commands.Choice(name="#grove", value="grove"),
    app_commands.Choice(name="#marsh", value="marsh"),
    app_commands.Choice(name="#bog", value="bog"),
    app_commands.Choice(name="#mire", value="mire"),
    app_commands.Choice(name="#underground-jungle", value="underground-jungle"),
]

async def _spawn_resolve_channel(interaction: discord.Interaction, channel: str):
    """Returns (target, area_mult) or (None, None) after sending an error if invalid."""
    target = discord.utils.get(interaction.guild.text_channels, name=channel)
    if not target:
        await safe_interaction_response(interaction, interaction.followup.send,
            f"❌ Channel **#{channel}** not found in this server.", ephemeral=True)
        return None, None
    area = GATHERING_AREAS.get(channel)
    if not area:
        await safe_interaction_response(interaction, interaction.followup.send,
            f"❌ **{channel}** is not a valid gathering channel.", ephemeral=True)
        return None, None
    return target, area.get("multiplier", 1.0)

@bot.tree.command(name="spawn_animal", description="[ADMIN] Spawn a wild animal in a gathering channel")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(animal="Animal to spawn", channel="Gathering channel to spawn in")
@app_commands.choices(animal=_spawn_animal_choices())
@app_commands.choices(channel=_SPAWN_CHANNEL_CHOICES)
async def spawn_animal(interaction: discord.Interaction, animal: str, channel: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        target, area_mult = await _spawn_resolve_channel(interaction, channel)
        if target is None:
            return

        # Bullet Ant Swarm
        if animal == "bullet_ant_swarm":
            await trigger_bullet_ant_swarm_event(target, area_mult)
            await safe_interaction_response(interaction, interaction.followup.send,
                f"✅ Spawned **Bullet Ant Swarm** in {target.mention}!", ephemeral=True)
            return

        # Bee Swarm (underground-jungle animal)
        if animal == "bee_swarm":
            await trigger_bee_swarm_event(target, area_mult)
            await safe_interaction_response(interaction, interaction.followup.send,
                f"✅ Spawned **Bee Swarm** in {target.mention}!", ephemeral=True)
            return

        # Jungle-only single animal (admin spawn by jungle_<id>)
        if animal.startswith("jungle_"):
            jungle_id = animal[7:]
            chosen = next((a for a in PVE_WILD_ANIMALS_UNDERGROUND_JUNGLE_SINGLE if a["id"] == jungle_id), None)
            if chosen:
                if target.id in active_pve_events:
                    await safe_interaction_response(interaction, interaction.followup.send,
                        f"❌ There is already an active PvE event in {target.mention}. Defeat it first.", ephemeral=True)
                    return
                hp = random.randint(*chosen["hp_range"])
                active_pve_events[target.id] = {"animal": chosen, "hp": hp, "start_time": time.time()}
                embed = discord.Embed(
                    title=f"🚨 {chosen['emoji']} Wild {chosen['name']} Appeared! 🚨",
                    description=chosen["description"],
                    color=chosen["color"])
                embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{'🟥' * 20}", inline=False)
                embed.add_field(name="⚔️ How to Fight", value="Press **Attack**! Each hit deals **1 damage** and earns **1 plant**!", inline=False)
                embed.set_footer(text="All gathering commands are BLOCKED until the wild animal is defeated")
                view = WildAnimalView(animal=chosen, hp=hp, channel_id=target.id, area_multiplier=area_mult)
                await target.send(embed=embed, view=view)
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"✅ Spawned **{chosen['name']}** in {target.mention}!", ephemeral=True)
                return

        # Single wild animal (match by name normalized to value form)
        animal_key = animal.replace("_", " ").title()
        if animal_key == "Homeless Man On Fent":
            animal_key = "Homeless Man on Fent"
        chosen = next((a for a in PVE_WILD_ANIMALS_SINGLE if a["name"] == animal_key), None)
        if not chosen:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Unknown animal **{animal}**.", ephemeral=True)
            return

        if target.id in active_pve_events:
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ There is already an active PvE event in {target.mention}. Defeat it first.", ephemeral=True)
            return

        hp = random.randint(*chosen["hp_range"])
        active_pve_events[target.id] = {"animal": chosen, "hp": hp, "start_time": time.time()}
        embed = discord.Embed(
            title=f"🚨 {chosen['emoji']} Wild {chosen['name']} Appeared! 🚨",
            description=chosen["description"],
            color=chosen["color"])
        embed.add_field(name="HP", value=f"**{hp}** / **{hp}**\n{'🟥' * 20}", inline=False)
        embed.add_field(name="⚔️ How to Fight", value="Press **Attack**! Each hit deals **1 damage** and earns **1 plant**!", inline=False)
        embed.set_footer(text="All gathering commands are BLOCKED until the wild animal is defeated")
        view = WildAnimalView(animal=chosen, hp=hp, channel_id=target.id, area_multiplier=area_mult)
        await target.send(embed=embed, view=view)
        await safe_interaction_response(interaction, interaction.followup.send,
            f"✅ Spawned **{chosen['name']}** in {target.mention}!", ephemeral=True)
    except Exception as e:
        print(f"Error in spawn_animal command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="spawn_boss", description="[ADMIN] Spawn a boss in a gathering channel")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(boss="Boss to spawn", channel="Gathering channel to spawn in")
@app_commands.choices(boss=_spawn_boss_choices())
@app_commands.choices(channel=_SPAWN_CHANNEL_CHOICES)
async def spawn_boss(interaction: discord.Interaction, boss: str, channel: str):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        target, area_mult = await _spawn_resolve_channel(interaction, channel)
        if target is None:
            return

        # The Twins (single choice spawns both Retinazer and Spazmatism as two boss embeds)
        if boss == "twins":
            await trigger_twins_boss_event(target, area_mult)
            await safe_interaction_response(interaction, interaction.followup.send,
                f"✅ Spawned **The Twins** in {target.mention}!", ephemeral=True)
            return

        # Single boss
        boss_obj = next((b for b in PVE_BOSSES if b["id"] == boss), None)
        if boss_obj:
            # Send server intro text to all gathering channels (like natural boss spawns)
            await _send_boss_warning_embed(interaction.guild, boss_obj, target)
            
            if boss_obj["id"] == ENDER_DRAGON_ID:
                await trigger_ender_dragon_event(target, area_mult)
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"✅ Spawned **{boss_obj['name']}** in {target.mention} (and Obsidian Towers in other channels)!", ephemeral=True)
            else:
                await trigger_boss_event(target, boss_obj, area_mult)
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"✅ Spawned **{boss_obj['name']}** in {target.mention}!", ephemeral=True)
            return

        await safe_interaction_response(interaction, interaction.followup.send, f"❌ Unknown boss **{boss}**.", ephemeral=True)
    except Exception as e:
        print(f"Error in spawn_boss command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# User admin command - full dossier: userstats, items, achievements, hidden achievements, and Wild Animals spawn reference
@bot.tree.command(name="user", description="[ADMIN] View everything about a user: stats, items, achievements, hidden achievements")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(member="The user to inspect (defaults to yourself)")
async def user_admin(interaction: discord.Interaction, member: discord.Member = None):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        if interaction.guild is None:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: This command can only be used in a server.", ephemeral=True)
            return
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return

        target = member if member is not None else interaction.user
        user_id = target.id
        display_name = getattr(target, "display_name", None) or getattr(target, "name", "Unknown")

        # Single DB read for full user dossier (replaces 25+ individual queries)
        doc = await asyncio.to_thread(get_user_dossier, user_id)

        user_balance = doc["balance"]
        total_items = doc["gather_stats_total_items"]
        cycle_plants = doc["bloom_cycle_plants"]
        items_needed = None
        next_rank = None
        if cycle_plants < 50:
            items_needed = 50 - cycle_plants
            next_rank = "PLANTER II"
        elif cycle_plants < 150:
            items_needed = 150 - cycle_plants
            next_rank = "PLANTER III"
        elif cycle_plants < 300:
            items_needed = 300 - cycle_plants
            next_rank = "PLANTER IV"
        elif cycle_plants < 500:
            items_needed = 500 - cycle_plants
            next_rank = "PLANTER V"
        elif cycle_plants < 1000:
            items_needed = 1000 - cycle_plants
            next_rank = "PLANTER VI"
        elif cycle_plants < 2000:
            items_needed = 2000 - cycle_plants
            next_rank = "PLANTER VII"
        elif cycle_plants < 4000:
            items_needed = 4000 - cycle_plants
            next_rank = "PLANTER VIII"
        elif cycle_plants < 10000:
            items_needed = 10000 - cycle_plants
            next_rank = "PLANTER IX"
        elif cycle_plants < 15000:
            items_needed = 15000 - cycle_plants
            next_rank = "PLANTER X"
        else:
            items_needed = 0
            next_rank = "MAX RANK"

        bloom_rank = _bloom_count_to_rank(doc["bloom_count"])
        tree_rings = doc["tree_rings"]
        bloom_multiplier = 1.0 + (tree_rings * 0.005)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id, full_data={"bloom_count": doc["bloom_count"]})
        water_streak = doc["consecutive_water_days"]
        shop_inv = doc.get("shop_inventory") or {}
        daily_rate = 0.04 if shop_inv.get("golden_watering_can", 0) >= 1 else 0.02
        daily_bonus_multiplier = 1.0 + (water_streak * daily_rate)
        hoe_attunement = doc.get("hoe_enchantment")
        tractor_attunement = doc.get("tractor_enchantment")
        ach_data = doc.get("achievements") or {}
        hidden_map = ach_data.get("hidden_achievements", {})
        hidden_achievements_count = int(ach_data.get("hidden_achievements_discovered", 0))
        full_data_ach = {"achievements": ach_data}
        total_boost_multiplier = get_achievement_multiplier(user_id, full_data=full_data_ach)
        total_boost_percent = (total_boost_multiplier - 1.0) * 100

        # ── 1. Userstats embed ──
        embed_stats = discord.Embed(
            title=f"📊 {display_name}'s Stats (userstats)",
            color=discord.Color.blue()
        )
        embed_stats.add_field(name="💰 Balance", value=f"**${user_balance:.2f}**", inline=True)
        embed_stats.add_field(name="🌱 Plants Gathered (Total)", value=f"**{total_items}** plants", inline=True)
        embed_stats.add_field(name="🌿 Plants Gathered (This Bloom)", value=f"**{cycle_plants}** plants", inline=True)
        embed_stats.add_field(name="🌲 Bloom Rank", value=f"**{bloom_rank}**", inline=True)
        tree_ring_boost_pct = (bloom_multiplier - 1.0) * 100
        embed_stats.add_field(name="<:TreeRing:1474244868288282817> Tree Rings", value=f"**{tree_rings}** (+{tree_ring_boost_pct:.1f}% boost)", inline=True)
        if bloom_rank != "PINE I":
            embed_stats.add_field(name="⭐ Rank Boost", value=f"**{rank_perma_buff_multiplier:.2f}x**", inline=True)
        day_text = "day" if water_streak == 1 else "days"
        water_streak_pct = (daily_bonus_multiplier - 1.0) * 100
        embed_stats.add_field(name="💧 Water Streak", value=f"**{water_streak}** {day_text} (+{water_streak_pct:.1f}%)", inline=True)
        if hoe_attunement:
            hoe_name = hoe_attunement.get("name", "Unknown")
            hoe_rarity = hoe_attunement.get("rarity", "COMMON")
            hoe_rarity_display = RARITY_EMOJI.get(hoe_rarity, f"[{hoe_rarity}]")
            embed_stats.add_field(name="✨ Gather Attunement", value=f"**{hoe_name}** {hoe_rarity_display}", inline=True)
        else:
            embed_stats.add_field(name="✨ Gather Attunement", value="**None**", inline=True)
        if tractor_attunement:
            tractor_name = tractor_attunement.get("name", "Unknown")
            tractor_rarity = tractor_attunement.get("rarity", "COMMON")
            tractor_rarity_display = RARITY_EMOJI.get(tractor_rarity, f"[{tractor_rarity}]")
            embed_stats.add_field(name="✨ Harvest Attunement", value=f"**{tractor_name}** {tractor_rarity_display}", inline=True)
        else:
            embed_stats.add_field(name="✨ Harvest Attunement", value="**None**", inline=True)
        if items_needed == 0:
            embed_stats.add_field(name="🏆 Rank Status", value=f"**{next_rank}**", inline=False)
        else:
            embed_stats.add_field(name="📈 Next Rank", value=f"**{items_needed}** more plants until **{next_rank}**", inline=False)

        # ── 2. Gathered items (almanac) ──
        user_items = doc.get("items") or {}
        if user_items:
            sorted_items = sorted(user_items.items(), key=lambda x: x[1], reverse=True)
            almanac_lines = [f"[ {name.upper()} ] x{count}" for name, count in sorted_items]
            embed_items = discord.Embed(
                title=f"📚 {display_name}'s Gathered Items (Almanac)",
                description="\n".join(almanac_lines)[:4000] or "—",
                color=discord.Color.green()
            )
        else:
            embed_items = discord.Embed(
                title=f"📚 {display_name}'s Gathered Items (Almanac)",
                description="*No gathered items.*",
                color=discord.Color.green()
            )

        # ── 3. Shop inventory ──
        if shop_inv:
            shop_lines = [f"**{k}**: {v}" for k, v in shop_inv.items()]
            embed_shop = discord.Embed(
                title=f"🛒 {display_name}'s Shop Inventory",
                description="\n".join(shop_lines)[:4000] or "—",
                color=discord.Color.gold()
            )
        else:
            embed_shop = discord.Embed(
                title=f"🛒 {display_name}'s Shop Inventory",
                description="*Empty.*",
                color=discord.Color.gold()
            )

        # ── 4. Achievements ──
        embed_ach = discord.Embed(
            title=f"🏆 {display_name}'s Achievements",
            color=discord.Color.gold()
        )
        for achievement_name, achievement_data in ACHIEVEMENTS.items():
            current_level = int(ach_data.get(achievement_name, 0))
            levels = achievement_data["levels"]
            if current_level < len(levels):
                current_level_data = levels[current_level]
            else:
                current_level_data = levels[-1]
            max_earnable = len(levels) - 1
            progress_bar = PROGRESS_Y * current_level + PROGRESS_N * (max_earnable - current_level)
            achievement_display_name = current_level_data["name"]
            boost_percent = current_level_data["boost"] * 100
            embed_ach.add_field(
                name=achievement_display_name,
                value=f"Level **{current_level}**\n{progress_bar}\nBoost: **{boost_percent:.1f}%**",
                inline=True
            )
        embed_ach.add_field(name="━━━ Total Boost ━━━", value=f"**{total_boost_percent:.1f}%**", inline=False)

        # ── 5. Hidden achievements (max 25 fields per embed; batch into groups of 6) ──
        embed_hidden = discord.Embed(
            title=f"🔒 {display_name}'s Hidden Achievements",
            description=f"Discovered: **{hidden_achievements_count}/{TOTAL_HIDDEN_ACHIEVEMENTS}**",
            color=discord.Color.dark_gray()
        )
        _hidden_list = list(HIDDEN_ACHIEVEMENTS.items())
        _chunk_size = 6
        for idx in range(0, len(_hidden_list), _chunk_size):
            chunk = _hidden_list[idx : idx + _chunk_size]
            lines = []
            for key, data in chunk:
                name = data["name"]
                if hidden_map.get(key, False):
                    lines.append(f"**{name}**: {data['description']}")
                else:
                    lines.append(f"{name}: {ALMANAC_HIDDEN_EMOJI}{ALMANAC_HIDDEN_EMOJI}{ALMANAC_HIDDEN_EMOJI}")
            field_value = "\n".join(lines)
            if len(field_value) > 1024:
                field_value = field_value[:1021] + "…"
            embed_hidden.add_field(
                name=f"Achievements {idx + 1}–{idx + len(chunk)}",
                value=field_value,
                inline=False,
            )

        # ── 6. Wild Animals spawn reference (for admin) ──
        animal_mult, _ = _pve_spawn_multiplier()
        base_gather = PVE_TRIGGER_CHANCE_GATHER * 100
        base_harvest = PVE_TRIGGER_CHANCE_HARVEST * 100
        effective_gather = min(100.0, PVE_TRIGGER_CHANCE_GATHER * animal_mult * 100)
        effective_harvest = min(100.0, PVE_TRIGGER_CHANCE_HARVEST * animal_mult * 100)
        jungle_mult = UNDERGROUND_JUNGLE_ANIMAL_SPAWN_MULT
        embed_ref = discord.Embed(
            title="🐾 Wild Animals Spawn Chances (Reference)",
            color=discord.Color.dark_green()
        )
        embed_ref.add_field(
            name="Base chances",
            value=f"**/gather**: {base_gather:.1f}% per use\n**/harvest**: {base_harvest:.1f}% per use",
            inline=True
        )
        embed_ref.add_field(
            name="Current (with event mult)",
            value=f"**/gather**: {effective_gather:.1f}%\n**/harvest**: {effective_harvest:.1f}%\n*(Solar Eclipse / Blood Moon = 2.5x)*",
            inline=True
        )
        embed_ref.add_field(
            name="#underground-jungle",
            value=f"Animal spawn mult: **{jungle_mult}x** (then capped at 100%)",
            inline=False
        )

        await safe_interaction_response(interaction, interaction.followup.send,
            embeds=[embed_stats, embed_items, embed_shop, embed_ach, embed_hidden, embed_ref], ephemeral=True)
    except Exception as e:
        import traceback
        print(f"Error in user admin command: {e}")
        traceback.print_exc()
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


# Rollback command - Admin only, #hidden channel; runs scripts/rollback.sh (git reset --hard HEAD~1 and restart)
@bot.tree.command(name="rollback", description="[ADMIN] Roll back one commit and restart the bot (runs rollback script)")
@app_commands.default_permissions(administrator=True)
async def rollback(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        if not hasattr(interaction.channel, "name") or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ This command can only be used in the #hidden channel.", ephemeral=True)
            return
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(repo_dir, "scripts", "rollback.sh")
        if not os.path.isfile(script_path):
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Rollback script not found: `scripts/rollback.sh`", ephemeral=True)
            return
        if sys.platform == "win32":
            proc = subprocess.Popen(["bash", script_path], cwd=repo_dir, shell=False, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        else:
            proc = subprocess.Popen(["/bin/bash", script_path], cwd=repo_dir, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if proc.poll() is None:
            await safe_interaction_response(interaction, interaction.followup.send,
                "✅ Rollback script started. The bot will restart shortly (you may see this message before it disconnects).", ephemeral=True)
        else:
            err = proc.stderr.read().decode("utf-8", errors="replace") if proc.stderr else ""
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ Rollback script exited immediately. Stderr: {err[:500] or 'none'}", ephemeral=True)
        print(f"Admin {interaction.user.name} ran /rollback")
    except Exception as e:
        print(f"Error in rollback command: {e}")
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
        
        # Get all non-bot members in the guild (single pass)
        guild = interaction.guild
        if not guild:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Could not get guild information.", ephemeral=True)
            return

        members = [m for m in guild.members if not m.bot]
        user_ids = [m.id for m in members]
        wiped_count = 0

        if type == "money":
            wiped_count = await asyncio.to_thread(wipe_guild_money, user_ids)
            embed = discord.Embed(
                title="✅ Money Wiped",
                description=f"Reset money to default for **{wiped_count}** users in this server.\n\n**Stock market has been reset** - all shares returned, making all stocks available at max capacity.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Money (balance)\n• Stock holdings (shares)\n• Crypto holdings (portfolio)", inline=False)
            embed.add_field(name="What was kept", value="• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Plants", inline=False)
        elif type == "plants":
            wiped_count = await asyncio.to_thread(wipe_guild_plants, user_ids)
            # Assign PLANTER I in parallel (chunks to reduce rate-limit risk)
            _ROLE_CHUNK = 25
            for i in range(0, len(members), _ROLE_CHUNK):
                chunk = members[i : i + _ROLE_CHUNK]
                await asyncio.gather(*[assign_gatherer_role(m, guild, force_planter_role="PLANTER I") for m in chunk], return_exceptions=True)
            embed = discord.Embed(
                title="✅ Plants Wiped",
                description=f"Reset collected plants for **{wiped_count}** users in this server.\nAll users have been set to **PLANTER I** rank.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Collected items\n• Gather stats\n• Ripeness stats\n• Tree Rings\n• Rank (set to PLANTER I)\n• Planter achievement\n• /water streak and water count\n• All cooldowns", inline=False)
            embed.add_field(name="What was kept", value="• Money (balance)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs", inline=False)
        elif type == "crypto":
            wiped_count = await asyncio.to_thread(wipe_guild_crypto, user_ids)
            embed = discord.Embed(
                title="✅ Crypto Wiped",
                description=f"Reset crypto holdings to 0 for **{wiped_count}** users in this server.",
                color=discord.Color.orange()
            )
            embed.add_field(name="What was reset", value="• Crypto holdings (portfolio)", inline=False)
            embed.add_field(name="What was kept", value="• Money (balance)\n• Stock holdings (shares)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Plants", inline=False)
        else:  # type == "all"
            wiped_count = await asyncio.to_thread(wipe_guild_all, user_ids)
            # Assign PLANTER I and PINE I in parallel (chunks)
            _ROLE_CHUNK = 25
            for i in range(0, len(members), _ROLE_CHUNK):
                chunk = members[i : i + _ROLE_CHUNK]
                gather_tasks = []
                for m in chunk:
                    gather_tasks.append(assign_gatherer_role(m, guild, force_planter_role="PLANTER I"))
                    gather_tasks.append(assign_bloom_rank_role(m, guild))
                await asyncio.gather(*gather_tasks, return_exceptions=True)
            embed = discord.Embed(
                title="✅ All Data Wiped",
                description=f"Reset everything for **{wiped_count}** users in this server.\nAll users have been set to **PLANTER I** rank and **PINE I** Bloom rank.\n\n**Market has been reset** - all shares returned, making all stocks available at max capacity.",
                color=discord.Color.red()
            )
            embed.add_field(name="What was reset", value="• Money (balance)\n• Basket upgrades\n• Shoes upgrades\n• Gloves upgrades\n• Soil upgrades\n• Harvest upgrades (Car, Yield, Fertilizer, Workers)\n• Gardeners\n• GPUs\n• Stock holdings (shares)\n• Crypto holdings (portfolio)\n• Collected items\n• Gather stats\n• Ripeness stats\n• Tree Rings\n• Rank (set to PLANTER I)\n• Bloom rank (set to PINE I)\n• All achievements and achievement stats\n• All cooldowns\n• Daily shop inventory and purchase count", inline=False)

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
        print(f"Admin {interaction.user.name} wiped {type} data for {wiped_count} users")
    except Exception as e:
        print(f"Error in wipe command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Set command - Admin only, #hidden channel
@bot.tree.command(name="set", description="[ADMIN] Set a user's money, plants, crypto, or invites")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    user="The user to set the value for (defaults to yourself)",
    amount="The amount to set",
    type="The type of value to set: money, plants, crypto, or invites",
    coin="The crypto coin type (RTC, TER, or CNY) - required if type is crypto"
)
@app_commands.choices(type=[
    app_commands.Choice(name="Money", value="money"),
    app_commands.Choice(name="Plants", value="plants"),
    app_commands.Choice(name="Crypto", value="crypto"),
    app_commands.Choice(name="Invites", value="invites"),
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
            await safe_interaction_response(interaction, interaction.followup.send, "\u274c **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        
        # Check if command is being used in the #hidden channel
        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"\u274c This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return
        
        # Determine target user (default to command user if not specified)
        target_user = user if user else interaction.user
        user_id = target_user.id
        
        # Validate and normalize type
        type_lower = type.lower()
        if type_lower not in ["money", "plants", "crypto", "invites"]:
            await safe_interaction_response(interaction, interaction.followup.send,
                "\u274c **Error**: Type must be one of: `money`, `plants`, `crypto`, or `invites`.",
                ephemeral=True)
            return
        
        # Validate amount
        if amount < 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                "\u274c **Error**: Amount cannot be negative.",
                ephemeral=True)
            return
        
        users = _get_users_collection()
        
        # Handle each type
        if type_lower == "money":
            update_user_balance(user_id, amount)
            embed = discord.Embed(
                title="\u2705 Money Set",
                description=f"{target_user.mention}'s balance has been set to **${amount:,.2f}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s money to ${amount:,.2f}")
        
        elif type_lower == "plants":
            # Set gather_stats.total_items, total_forage_count, and bloom_cycle_plants
            users.update_one(
                {"_id": int(user_id)},
                {
                    "$set": {
                        "gather_stats.total_items": int(amount),
                        "total_forage_count": int(amount),
                        "bloom_cycle_plants": int(amount)
                    }
                },
                upsert=True
            )
            embed = discord.Embed(
                title="\u2705 Plants Set",
                description=f"{target_user.mention}'s plant count has been set to **{int(amount):,}**!\n*(Lifetime total and current bloom cycle both updated)*",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s plants to {int(amount):,}")
        
        elif type_lower == "invites":
            # Set invite_stats.total_joins directly
            users.update_one(
                {"_id": int(user_id)},
                {
                    "$set": {
                        "invite_stats.total_joins": int(amount)
                    }
                },
                upsert=True
            )
            embed = discord.Embed(
                title="\u2705 Invites Set",
                description=f"{target_user.mention}'s invite count has been set to **{int(amount)}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s invites to {int(amount)}")
        
        elif type_lower == "crypto":
            # Validate coin parameter
            if not coin:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "\u274c **Error**: `coin` parameter is required when type is `crypto`. Choose from: `RTC`, `TER`, or `CNY`.",
                    ephemeral=True)
                return
            
            coin_upper = coin.upper()
            if coin_upper not in ["RTC", "TER", "CNY"]:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "\u274c **Error**: Coin must be one of: `RTC`, `TER`, or `CNY`.",
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
                title="\u2705 Crypto Set",
                description=f"{target_user.mention}'s {coin_upper} holdings have been set to **{amount:,.2f}**!",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /set to set {target_user.name}'s {coin_upper} to {amount:,.2f}")
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in set command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "\u274c An error occurred. Please try again.", ephemeral=True)


# Setrank admin command - set a user's BLOOM rank (PINE → REDWOOD)
_BLOOM_RANK_TO_COUNT = {
    "PINE I": 0, "PINE II": 1, "PINE III": 2,
    "CEDAR I": 3, "CEDAR II": 4, "CEDAR III": 5,
    "BIRCH I": 6, "BIRCH II": 7, "BIRCH III": 8,
    "MAPLE I": 9, "MAPLE II": 10, "MAPLE III": 11,
    "OAK I": 12, "OAK II": 13, "OAK III": 14,
    "FIR I": 15, "FIR II": 16, "FIR III": 17,
    "REDWOOD": 18,
}


# Planter rank (PLANTER I–X) minimum bloom_cycle_plants so DB and Discord stay in sync when admin sets rank
_PLANTER_RANK_MIN_CYCLE_PLANTS = {
    "PLANTER I": 0, "PLANTER II": 50, "PLANTER III": 150, "PLANTER IV": 300, "PLANTER V": 500,
    "PLANTER VI": 1000, "PLANTER VII": 2000, "PLANTER VIII": 4000, "PLANTER IX": 10000, "PLANTER X": 15000,
}


@bot.tree.command(name="setrank", description="[ADMIN] Set a user's BLOOM rank (PINE–REDWOOD) or PLANTER rank (1–10). Use in #hidden.")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    user="The user to set the rank for",
    rank_type="BLOOM (tree rank) or PLANTER (gatherer rank I–X)",
    rank="BLOOM rank name (required when rank_type is BLOOM)",
    tier="BLOOM tier I, II, or III (required when rank_type is BLOOM; omit for Redwood)",
    planter="PLANTER level 1–10 (required when rank_type is PLANTER)",
)
@app_commands.choices(rank_type=[
    app_commands.Choice(name="BLOOM", value="BLOOM"),
    app_commands.Choice(name="PLANTER", value="PLANTER"),
])
@app_commands.choices(rank=[
    app_commands.Choice(name="PINE", value="PINE"),
    app_commands.Choice(name="CEDAR", value="CEDAR"),
    app_commands.Choice(name="BIRCH", value="BIRCH"),
    app_commands.Choice(name="MAPLE", value="MAPLE"),
    app_commands.Choice(name="OAK", value="OAK"),
    app_commands.Choice(name="FIR", value="FIR"),
    app_commands.Choice(name="REDWOOD", value="REDWOOD"),
])
@app_commands.choices(tier=[
    app_commands.Choice(name="I", value="I"),
    app_commands.Choice(name="II", value="II"),
    app_commands.Choice(name="III", value="III"),
])
@app_commands.choices(planter=[
    app_commands.Choice(name="1", value="1"),
    app_commands.Choice(name="2", value="2"),
    app_commands.Choice(name="3", value="3"),
    app_commands.Choice(name="4", value="4"),
    app_commands.Choice(name="5", value="5"),
    app_commands.Choice(name="6", value="6"),
    app_commands.Choice(name="7", value="7"),
    app_commands.Choice(name="8", value="8"),
    app_commands.Choice(name="9", value="9"),
    app_commands.Choice(name="10", value="10"),
])
async def setrank(
    interaction: discord.Interaction,
    user: discord.Member,
    rank_type: app_commands.Choice[str],
    rank: app_commands.Choice[str] = None,
    tier: app_commands.Choice[str] = None,
    planter: app_commands.Choice[str] = None,
):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        if interaction.guild is None:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: This command must be used in a server.", ephemeral=True)
            return
        if not hasattr(interaction.channel, "name") or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return

        if rank_type.value == "PLANTER":
            if planter is None:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: When rank_type is PLANTER, choose planter (1–10).", ephemeral=True)
                return
            level = int(planter.value)
            rank_name = f"PLANTER {['I','II','III','IV','V','VI','VII','VIII','IX','X'][level - 1]}"
            min_plants = _PLANTER_RANK_MIN_CYCLE_PLANTS.get(rank_name, 0)
            set_user_bloom_cycle_plants(user.id, min_plants)
            try:
                member = await interaction.guild.fetch_member(user.id)
            except Exception:
                member = user
            await assign_gatherer_role(member, interaction.guild, force_planter_role=rank_name)
            embed = discord.Embed(
                title="✅ Planter rank set",
                description=f"{user.mention}'s PLANTER rank has been set to **{rank_name}** (cycle plants set to {min_plants:,}).",
                color=discord.Color.green()
            )
            print(f"Admin {interaction.user.name} used /setrank PLANTER to set {user.name} to {rank_name}")
            await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
            return

        # BLOOM
        if rank is None:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: When rank_type is BLOOM, choose rank (PINE, CEDAR, …).", ephemeral=True)
            return
        rank_name = rank.value
        if rank_name == "REDWOOD":
            rank_str = "REDWOOD"
        else:
            if not tier:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Tier (I, II, or III) is required for this BLOOM rank. Redwood has no tier.", ephemeral=True)
                return
            rank_str = f"{rank_name} {tier.value}"
        if rank_str not in _BLOOM_RANK_TO_COUNT:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Invalid rank.", ephemeral=True)
            return
        bloom_count = _BLOOM_RANK_TO_COUNT[rank_str]
        set_user_bloom_count(user.id, bloom_count)
        await assign_bloom_rank_role(user, interaction.guild)
        embed = discord.Embed(
            title="✅ Rank Set",
            description=f"{user.mention}'s BLOOM rank has been set to **{rank_str}**!",
            color=discord.Color.green()
        )
        print(f"Admin {interaction.user.name} used /setrank to set {user.name}'s rank to {rank_str}")
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in setrank command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# Market admin command
@bot.tree.command(name="market", description="[ADMIN] Toggle market news on/off and reset stock prices")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(news="Turn market news on or off. Toggling also resets stock prices to real-life values.")
@app_commands.choices(news=[
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
])
async def market_admin(interaction: discord.Interaction, news: app_commands.Choice[str]):
    try:
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ You don't have permission to use this command.", ephemeral=True)
            return
        
        if not await safe_defer(interaction, ephemeral=True):
            return
        
        guild_id = interaction.guild.id
        enabled = news.value == "on"
        market_news_enabled[guild_id] = enabled
        
        # Reset all stock prices to real-life API values (remove news multiplier)
        reset_count = 0
        if guild_id in stock_data:
            for symbol, info in stock_data[guild_id].items():
                # Reset news multiplier
                info["news_multiplier"] = 1.0
                
                # Re-fetch real price from API
                real_ticker = REAL_STOCK_MAPPING.get(symbol)
                if real_ticker:
                    real_data = await asyncio.to_thread(fetch_real_stock_data, real_ticker)
                    if real_data:
                        info["real_price"] = real_data["price"]
                        info["price"] = real_data["price"]
                        info["shares_outstanding"] = real_data["shares_outstanding"]
                        info["market_cap"] = real_data.get("market_cap")
                        info["price_history"] = [real_data["price"]] * 6
                        info["last_api_fetch"] = time.time()
                        reset_count += 1
                    else:
                        # If API fails, just reset multiplier (price = real_price * 1.0)
                        real_price = info.get("real_price", info.get("price", 0))
                        info["price"] = real_price
                        info["price_history"] = [real_price] * 6
                        reset_count += 1
                else:
                    real_price = info.get("real_price", info.get("price", 0))
                    info["price"] = real_price
                    info["price_history"] = [real_price] * 6
                    reset_count += 1
        
        status_str = "**ON** \u2705" if enabled else "**OFF** \u274c"
        embed = discord.Embed(
            title="\U0001f4f0 Market News Updated",
            description=f"Market news is now {status_str}",
            color=discord.Color.green() if enabled else discord.Color.red()
        )
        embed.add_field(name="Prices Reset", value=f"**{reset_count}** stock(s) reset to real-life API values", inline=False)
        
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in market admin command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "\u274c An error occurred. Please try again.", ephemeral=True)


# Shutdown command (bot owner only)
def _get_bot_owner_ids() -> set[int]:
    """Return set of Discord user IDs allowed to use /shutdown (from env BOT_OWNER_ID, comma-separated)."""
    raw = os.getenv("BOT_OWNER_ID", "").strip()
    if not raw:
        return set()
    return {int(x.strip()) for x in raw.split(",") if x.strip().isdigit()}


@bot.tree.command(name="shutdown", description="[OWNER] Gracefully stop the bot. Only the bot owner can use this.")
async def shutdown(interaction: discord.Interaction):
    try:
        owner_ids = _get_bot_owner_ids()
        if not owner_ids:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "❌ Shutdown is disabled: `BOT_OWNER_ID` is not set in the environment.", ephemeral=True)
            return
        if interaction.user.id not in owner_ids:
            await safe_interaction_response(interaction, interaction.response.send_message,
                "❌ Only the bot owner can shut down the bot.", ephemeral=True)
            return
        await safe_interaction_response(interaction, interaction.response.send_message,
            "🛑 Shutting down the bot...", ephemeral=True)
        print(f"Bot shutdown requested by {interaction.user} ({interaction.user.id})")
        await bot.close()
    except Exception as e:
        print(f"Error in shutdown command: {e}")
        try:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Shutdown failed.", ephemeral=True)
        except Exception:
            pass


# ── Give admin command (direct give to one user; admin-only #hidden) ─────
async def _give_imbue_name_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Autocomplete for imbue names – filters by the tool_type and rarity
    the user already typed, then fuzzy-matches the typed text."""
    namespace = interaction.namespace

    tool_raw = getattr(namespace, "tool_type", None)
    rarity_raw = getattr(namespace, "rarity", None)

    pool = HOE_ENCHANTMENTS if tool_raw == "hoe" else TRACTOR_ENCHANTMENTS if tool_raw == "tractor" else {}

    if rarity_raw and rarity_raw in pool:
        names = [e["name"] for e in pool[rarity_raw]]
    else:
        names = [e["name"] for rarity_list in pool.values() for e in rarity_list]

    current_upper = current.upper()
    matches = [n for n in names if current_upper in n] if current else names
    return [app_commands.Choice(name=n, value=n) for n in matches[:25]]


async def _give_shop_item_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    """Autocomplete for daily shop item – filters by typed text. Supports 25+ items (Discord limit is 25 static choices)."""
    current_upper = (current or "").strip().upper()
    choices = []
    for item_id, info in DAILY_SHOP_ITEMS.items():
        name = info.get("name", item_id)
        if not current_upper or current_upper in name.upper() or current_upper in item_id.upper().replace("_", " "):
            choices.append(app_commands.Choice(name=name, value=item_id))
    choices.sort(key=lambda c: c.name.upper())
    return choices[:25]


@bot.tree.command(name="give", description="[ADMIN] Give a user money, imbues, water streak, tree rings, or daily shop items")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    user="The user to give the reward to",
    type="The type of reward to give",
    amount="Amount – money ($), water streak (days), tree rings (count), or shop item quantity",
    tool_type="Hoe or Tractor (required for imbue type)",
    rarity="Imbue rarity tier (required for imbue type)",
    imbue_name="Specific imbue name (required for imbue type) – autocompletes based on tool & rarity",
    shop_item="Daily shop item to give (required for shop_item type) – type to search",
)
@app_commands.choices(type=[
    app_commands.Choice(name="Money", value="money"),
    app_commands.Choice(name="Imbue", value="imbue"),
    app_commands.Choice(name="Water Streak", value="water_streak"),
    app_commands.Choice(name="Tree Rings", value="tree_rings"),
    app_commands.Choice(name="Shop Item", value="shop_item"),
])
@app_commands.choices(tool_type=[
    app_commands.Choice(name="Hoe (Gather)", value="hoe"),
    app_commands.Choice(name="Tractor (Harvest)", value="tractor"),
])
@app_commands.choices(rarity=[
    app_commands.Choice(name="Common", value="COMMON"),
    app_commands.Choice(name="Uncommon", value="UNCOMMON"),
    app_commands.Choice(name="Rare", value="RARE"),
    app_commands.Choice(name="Super Rare", value="SUPER RARE"),
    app_commands.Choice(name="Legendary", value="LEGENDARY"),
    app_commands.Choice(name="Netherite", value="NETHERITE"),
    app_commands.Choice(name="Luminite", value="LUMINITE"),
    app_commands.Choice(name="Celestial", value="CELESTIAL"),
    app_commands.Choice(name="Secret", value="SECRET"),
])
@app_commands.autocomplete(imbue_name=_give_imbue_name_autocomplete, shop_item=_give_shop_item_autocomplete)
async def give(
    interaction: discord.Interaction,
    user: discord.Member,
    type: str,
    amount: float = None,
    tool_type: str = None,
    rarity: str = None,
    imbue_name: str = None,
    shop_item: str = None,
):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return

        # ── permission & channel gate ──
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return

        if not hasattr(interaction.channel, 'name') or interaction.channel.name != "hidden":
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in the #hidden channel, {interaction.user.name}!",
                ephemeral=True)
            return

        user_id = user.id
        type_lower = type.lower()

        # ── MONEY ──
        if type_lower == "money":
            if amount is None or amount <= 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please provide a positive `amount` for money.", ephemeral=True)
                return

            current_balance = get_user_balance(user_id)
            new_balance = normalize_money(current_balance + amount)
            update_user_balance(user_id, new_balance)

            embed = discord.Embed(
                title="🎉 Giveaway – Money",
                description=f"**${amount:,.2f}** has been given to {user.mention}!",
                color=discord.Color.gold()
            )
            embed.add_field(name="Previous Balance", value=f"${current_balance:,.2f}", inline=True)
            embed.add_field(name="New Balance", value=f"${new_balance:,.2f}", inline=True)
            embed.set_footer(text=f"Given by {interaction.user.name}")
            print(f"Admin {interaction.user.name} used /give money to give {user.name} ${amount:,.2f}")

        # ── IMBUE ──
        elif type_lower == "imbue":
            if not tool_type:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please select a `tool_type` (Hoe or Tractor) for imbue giveaways.", ephemeral=True)
                return
            if not rarity:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please select a `rarity` tier for the imbue.", ephemeral=True)
                return
            if not imbue_name:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please provide an `imbue_name`. Use the autocomplete suggestions.", ephemeral=True)
                return

            pool = HOE_ENCHANTMENTS if tool_type == "hoe" else TRACTOR_ENCHANTMENTS
            rarity_upper = rarity.upper()

            if rarity_upper not in pool:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ **Error**: Invalid rarity `{rarity}`. Valid options: {', '.join(pool.keys())}",
                    ephemeral=True)
                return

            matched = [e for e in pool[rarity_upper] if e["name"].upper() == imbue_name.upper()]
            if not matched:
                valid_names = ", ".join(f"`{e['name']}`" for e in pool[rarity_upper])
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ **Error**: No imbue named `{imbue_name}` found in **{rarity_upper}** {tool_type} imbues.\n"
                    f"Valid names: {valid_names}",
                    ephemeral=True)
                return

            attunement = dict(matched[0])

            if tool_type == "hoe":
                old = get_user_hoe_attunement(user_id)
                set_user_hoe_attunement(user_id, attunement)
            else:
                old = get_user_tractor_attunement(user_id)
                set_user_tractor_attunement(user_id, attunement)

            rarity_emoji = RARITY_EMOJI.get(rarity_upper, f"[{rarity_upper}]")
            tool_label = "Hoe (Gather)" if tool_type == "hoe" else "Tractor (Harvest)"
            old_display = f"**{old['name']}** {RARITY_EMOJI.get(old.get('rarity', ''), '')}" if old else "None"

            embed = discord.Embed(
                title="🎉 Giveaway – Imbue",
                description=(
                    f"{user.mention} has been given the **{attunement['name']}** "
                    f"{rarity_emoji} imbue for their **{tool_label}**!"
                ),
                color=RARITY_COLORS.get(rarity_upper, discord.Color.gold().value)
            )
            embed.add_field(name="Tool", value=tool_label, inline=True)
            embed.add_field(name="Rarity", value=f"{rarity_emoji} {rarity_upper}", inline=True)
            embed.add_field(name="Previous Imbue", value=old_display, inline=False)
            embed.add_field(name="Description", value=f"*\"{attunement.get('description', '')}\"*", inline=False)
            embed.set_footer(text=f"Given by {interaction.user.name}")
            print(f"Admin {interaction.user.name} used /give imbue to give {user.name} {attunement['name']} ({rarity_upper} {tool_type})")

        # ── WATER STREAK ──
        elif type_lower == "water_streak":
            if amount is None or amount < 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please provide a non-negative `amount` for water streak (days).", ephemeral=True)
                return

            days = int(amount)
            old_streak = get_user_consecutive_water_days(user_id)
            set_user_consecutive_water_days(user_id, days)

            embed = discord.Embed(
                title="🎉 Giveaway – Water Streak",
                description=f"{user.mention}'s water streak has been set to **{days:,}** days!",
                color=discord.Color.blue()
            )
            embed.add_field(name="Previous Streak", value=f"{old_streak:,} days", inline=True)
            embed.add_field(name="New Streak", value=f"{days:,} days", inline=True)
            embed.set_footer(text=f"Set by {interaction.user.name}")
            print(f"Admin {interaction.user.name} used /give water_streak to set {user.name}'s streak to {days}")

        # ── TREE RINGS ──
        elif type_lower == "tree_rings":
            if amount is None or amount <= 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please provide a positive `amount` of tree rings to give.", ephemeral=True)
                return

            rings = int(amount)
            old_rings = get_user_tree_rings(user_id)
            increment_tree_rings(user_id, rings)
            new_rings = old_rings + rings

            embed = discord.Embed(
                title="🎉 Giveaway – Tree Rings",
                description=f"**{rings:,}** Tree Ring(s) have been given to {user.mention}!",
                color=discord.Color.dark_green()
            )
            embed.add_field(name="Previous Rings", value=f"{old_rings:,}", inline=True)
            embed.add_field(name="New Total", value=f"{new_rings:,}", inline=True)
            embed.set_footer(text=f"Given by {interaction.user.name}")
            print(f"Admin {interaction.user.name} used /give tree_rings to give {user.name} {rings} rings")

        # ── SHOP ITEM (Daily Shop) ──
        elif type_lower == "shop_item":
            if not shop_item or shop_item not in DAILY_SHOP_ITEMS:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Please select a valid `shop_item` from the Daily Shop list.", ephemeral=True)
                return
            qty = 1  # Daily shop items are one-per-user; giveaway gives 1 (add_shop_item_to_user caps at 1)
            add_shop_item_to_user(user_id, shop_item, qty)
            info = DAILY_SHOP_ITEMS[shop_item]
            embed = discord.Embed(
                title="🎉 Giveaway – Daily Shop Item",
                description=f"**{info['name']}** has been given to {user.mention}!",
                color=discord.Color.gold()
            )
            embed.add_field(name="Effect", value=f"***{info['effect']}***", inline=False)
            embed.set_footer(text=f"Given by {interaction.user.name}")
            print(f"Admin {interaction.user.name} used /give shop_item to give {user.name} {info['name']}")

        else:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Invalid type. Choose from: `money`, `imbue`, `water_streak`, `tree_rings`, `shop_item`.",
                ephemeral=True)
            return

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, ephemeral=True)
    except Exception as e:
        print(f"Error in give command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# ── /bot_pay: Admin give money to a user (logged in channel) ──────────────
GIVEAWAY_CLAIM_USER_ID = 843578539424350248  # Danny – contact for claiming prize


@bot.tree.command(name="bot_pay", description="[ADMIN] Give money to a user from the bot (#hidden or #giveaways)")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    user="The user to pay",
    amount="Amount of money to give",
)
async def bot_pay(interaction: discord.Interaction, user: discord.Member, amount: float):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        ch_name = getattr(interaction.channel, "name", None) if hasattr(interaction.channel, "name") else None
        if ch_name not in ("hidden", "giveaways"):
            await safe_interaction_response(interaction, interaction.followup.send,
                f"❌ This command can only be used in **#hidden** or **#giveaways**, {interaction.user.name}!", ephemeral=True)
            return
        if amount <= 0:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Amount must be positive.", ephemeral=True)
            return
        if user.bot:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You cannot pay bots.", ephemeral=True)
            return
        user_id = user.id
        current = get_user_balance(user_id)
        new_balance = normalize_money(current + amount)
        update_user_balance(user_id, new_balance)
        log_msg = f"{PROGRESS_Y} **/GATHER** paid ${amount:,.2f} to {user.mention}!"
        await safe_interaction_response(interaction, interaction.followup.send, log_msg, ephemeral=False)
        print(f"Admin {interaction.user.name} used /bot_pay to give {user.name} ${amount:,.2f}")
    except Exception as e:
        print(f"Error in bot_pay command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


# ── /giveaway: Reaction-based giveaway in #giveaways (admin-only) ─────────
# Edge cases: admin-only + #giveaways only; duration 1–10080 min; num_winners 1–50; only NETHER_STAR counts;
# bot adds reaction first; if message deleted/channel gone at end we skip; no entrants → "no valid entries";
# fewer entrants than num_winners → we pick all entrants; bot restart loses in-memory giveaways (no auto-resolve).
_active_giveaways: dict[int, dict] = {}  # message_id -> {channel_id, guild_id, end_at_ts, prize_display, prize_data, num_winners}


def _apply_giveaway_prize(user_id: int, prize_data: dict) -> None:
    """Apply a giveaway prize to a user (money, shop_item, or imbue). Runs in thread if needed."""
    ptype = prize_data.get("type")
    if ptype == "money":
        amt = float(prize_data["amount"])
        cur = get_user_balance(user_id)
        update_user_balance(user_id, normalize_money(cur + amt))
    elif ptype == "shop_item":
        add_shop_item_to_user(user_id, prize_data["item_id"], 1)
    elif ptype == "imbue":
        att = prize_data["attunement"]
        if prize_data.get("tool_type") == "hoe":
            set_user_hoe_attunement(user_id, att)
        else:
            set_user_tractor_attunement(user_id, att)


async def _giveaway_end_task(
    message_id: int,
    channel_id: int,
    guild_id: int,
    end_at_ts: float,
    prize_display: str,
    prize_data: dict,
    num_winners: int,
):
    """Wait until giveaway ends, then resolve winners and post congrats."""
    await asyncio.sleep(max(0, end_at_ts - time.time()))
    _active_giveaways.pop(message_id, None)
    channel = bot.get_channel(channel_id)
    if not channel or not isinstance(channel, discord.TextChannel):
        return
    try:
        message = await channel.fetch_message(message_id)
    except Exception:
        return
    # Find :PROGRESS_Y: reaction (giveaway entry)
    entry_reaction = None
    for r in message.reactions:
        if getattr(r.emoji, "id", None) == PROGRESS_Y_PARTIAL.id or (isinstance(r.emoji, str) and "progress_y" in str(r.emoji).lower()):
            entry_reaction = r
            break
    if not entry_reaction:
        await channel.send("🫐 Giveaway ended with no valid entries (:PROGRESS_Y: reaction not found).")
        return
    entrants = []
    async for u in entry_reaction.users():
        if not u.bot:
            entrants.append(u)
    if not entrants:
        await channel.send("🫐 Giveaway ended with no valid entries.")
        return
    winners = random.sample(entrants, min(num_winners, len(entrants)))
    danny_mention = f"<@{GIVEAWAY_CLAIM_USER_ID}>"
    for w in winners:
        try:
            await asyncio.to_thread(_apply_giveaway_prize, w.id, prize_data)
        except Exception as e:
            print(f"[Giveaway] Error applying prize to {w.id}: {e}")
        await channel.send(
            f"🎁 Congratulations {w.mention}! You won: **{prize_display}**\n"
            f"Contact {danny_mention} to claim your prize!"
        )


@bot.tree.command(name="giveaway", description="[ADMIN] Start a reaction-based giveaway in #giveaways (money, item, or imbue)")
@app_commands.default_permissions(administrator=True)
@app_commands.describe(
    prize_type="Type of prize",
    duration_minutes="How long the giveaway runs (minutes)",
    num_winners="Number of winners to pick (default 1)",
    amount="Money amount (for money prize)",
    shop_item="Daily shop item (for item prize)",
    tool_type="Hoe or Tractor (for imbue prize)",
    rarity="Imbue rarity (for imbue prize)",
    imbue_name="Imbue name (for imbue prize)",
)
@app_commands.choices(prize_type=[
    app_commands.Choice(name="Money", value="money"),
    app_commands.Choice(name="Item (Daily Shop)", value="shop_item"),
    app_commands.Choice(name="Imbue", value="imbue"),
])
@app_commands.choices(tool_type=[
    app_commands.Choice(name="Hoe (Gather)", value="hoe"),
    app_commands.Choice(name="Tractor (Harvest)", value="tractor"),
])
@app_commands.choices(rarity=[
    app_commands.Choice(name="Common", value="COMMON"),
    app_commands.Choice(name="Uncommon", value="UNCOMMON"),
    app_commands.Choice(name="Rare", value="RARE"),
    app_commands.Choice(name="Super Rare", value="SUPER RARE"),
    app_commands.Choice(name="Legendary", value="LEGENDARY"),
    app_commands.Choice(name="Netherite", value="NETHERITE"),
    app_commands.Choice(name="Luminite", value="LUMINITE"),
    app_commands.Choice(name="Celestial", value="CELESTIAL"),
    app_commands.Choice(name="Secret", value="SECRET"),
])
@app_commands.autocomplete(shop_item=_give_shop_item_autocomplete, imbue_name=_give_imbue_name_autocomplete)
async def giveaway(
    interaction: discord.Interaction,
    prize_type: str,
    duration_minutes: float,
    num_winners: int = 1,
    amount: float = None,
    shop_item: str = None,
    tool_type: str = None,
    rarity: str = None,
    imbue_name: str = None,
):
    try:
        if not await safe_defer(interaction, ephemeral=True):
            return
        if not interaction.user.guild_permissions.administrator:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: You need administrator permissions to use this command.", ephemeral=True)
            return
        if not hasattr(interaction.channel, "name") or interaction.channel.name != "giveaways":
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ This command can only be used in the **#giveaways** channel.", ephemeral=True)
            return
        guild = interaction.guild
        giveaways_ch = discord.utils.get(guild.text_channels, name="giveaways")
        if not giveaways_ch:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **#giveaways** channel not found.", ephemeral=True)
            return
        if duration_minutes < 1 or duration_minutes > 10080:  # max ~1 week
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ **Error**: Duration must be between 1 and 10080 minutes (1 week).", ephemeral=True)
            return
        num_winners = max(1, min(50, int(num_winners)))
        end_at_ts = time.time() + duration_minutes * 60
        prize_display = ""
        prize_data = {}
        pt = prize_type.lower()
        if pt == "money":
            if amount is None or amount <= 0:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Provide a positive `amount` for money prize.", ephemeral=True)
                return
            prize_display = f"${amount:,.2f}"
            prize_data = {"type": "money", "amount": amount}
        elif pt == "shop_item":
            if not shop_item or shop_item not in DAILY_SHOP_ITEMS:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Select a valid `shop_item`.", ephemeral=True)
                return
            info = DAILY_SHOP_ITEMS[shop_item]
            prize_display = info.get("name", shop_item)
            prize_data = {"type": "shop_item", "item_id": shop_item}
        elif pt == "imbue":
            if not tool_type or not rarity or not imbue_name:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: For imbue prize, set `tool_type`, `rarity`, and `imbue_name`.", ephemeral=True)
                return
            pool = HOE_ENCHANTMENTS if tool_type == "hoe" else TRACTOR_ENCHANTMENTS
            rarity_upper = rarity.upper()
            if rarity_upper not in pool:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ **Error**: Invalid rarity.", ephemeral=True)
                return
            matched = [e for e in pool[rarity_upper] if e["name"].upper() == imbue_name.upper()]
            if not matched:
                await safe_interaction_response(interaction, interaction.followup.send,
                    "❌ **Error**: Imbue name not found for that tool and rarity.", ephemeral=True)
                return
            attunement = dict(matched[0])
            rank_emoji = RARITY_EMOJI.get(rarity_upper, f"[{rarity_upper}]")
            prize_display = f"{rank_emoji} **{attunement['name']}**"
            prize_data = {"type": "imbue", "tool_type": tool_type, "attunement": attunement}
        else:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ **Error**: Invalid prize type.", ephemeral=True)
            return
        title = f"🫐 **/GATHER GIVEAWAY** {PROGRESS_Y}"
        embed = discord.Embed(title=title, color=discord.Color.gold())
        embed.add_field(name="PRIZE", value=prize_display, inline=False)
        embed.add_field(name="ENDS AT", value=f"<t:{int(end_at_ts)}:F>", inline=False)
        embed.set_footer(text="REACT BELOW TO ENTER")
        message = await giveaways_ch.send(embed=embed)
        await message.add_reaction(PROGRESS_Y_PARTIAL)
        _active_giveaways[message.id] = {
            "channel_id": giveaways_ch.id,
            "guild_id": guild.id,
            "end_at_ts": end_at_ts,
            "prize_display": prize_display,
            "prize_data": prize_data,
            "num_winners": num_winners,
        }
        asyncio.create_task(_giveaway_end_task(
            message.id, giveaways_ch.id, guild.id, end_at_ts, prize_display, prize_data, num_winners
        ))
        await safe_interaction_response(interaction, interaction.followup.send,
            f"✅ Giveaway started in #giveaways! Ends in **{int(duration_minutes)}** minutes. React with {PROGRESS_Y} to enter.", ephemeral=True)
        print(f"Admin {interaction.user.name} started /giveaway in #giveaways: {prize_display}, {num_winners} winner(s)")
    except Exception as e:
        print(f"Error in giveaway command: {e}")
        import traceback
        traceback.print_exc()
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
        # DM the recipient since we can't send ephemeral to someone who didn't initiate the interaction
        if amount >= 1000000.0:
            newly_unlocked = unlock_hidden_achievement(recipient_id, "beneficiary")
            if newly_unlocked:
                await send_hidden_achievement_notification_dm(recipient_id, "beneficiary")
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
                    emoji = "<:TreeRing:1474244868288282817>"
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
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.current_page > 0:
                self.current_page -= 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in previous_button (leaderboard): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not await safe_defer(interaction, ephemeral=False):
                return
            if self.current_page < self.total_pages - 1:
                self.current_page += 1
                self.update_buttons()
                embed = self.create_embed(self.current_page)
                await safe_interaction_response(interaction, interaction.response.edit_message, embed=embed, view=self)
        except Exception as e:
            print(f"Error in next_button (leaderboard): {e}")
            await safe_interaction_response(interaction, interaction.response.send_message, "❌ An error occurred. Please try again.", ephemeral=True)
    
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
    
    # Get leaderboard data (plants uses Planters Gathered Total = gather_stats.total_items, same as /userstats)
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
                emoji = "<:TreeRing:1474244868288282817>"
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
    {"name": "Maizy's", "symbol": "M", "base_price": 50.0, "max_shares": 10000, "emoji": "<:MZ:1466552134836158465>"},
    {"name": "Meadow", "symbol": "MEDO", "base_price": 75.0, "max_shares": 20000, "emoji": "<:MEDO:1472431415160209511>"},
    {"name": "IVM", "symbol": "IVM", "base_price": 100.0, "max_shares": 15000, "emoji": "<:IVM:1466497224379731968>"},
    {"name": "CisGrow", "symbol": "CSGO", "base_price": 60.0, "max_shares": 12000, "emoji": "<:CG:1472431245433508082>"},
    {"name": "Sowny", "symbol": "SWNY", "base_price": 90.0, "max_shares": 11000, "emoji": "<:SWNY:1472431904493142147>"},
    {"name": "General Mowers", "symbol": "GM", "base_price": 45.0, "max_shares": 20000, "emoji": "<:GM:1473422888035160321>"},
    {"name": "Raytheorn", "symbol": "RTH", "base_price": 125.0, "max_shares": 16000, "emoji": "<:RTH:1473426824074891326>"},
    {"name": "Wells Fargrow", "symbol": "WFG", "base_price": 70.0, "max_shares": 18000, "emoji": "<:WFG:1473412133797498900>"},
    {"name": "Apple", "symbol": "AAPL", "base_price": 150.0, "max_shares": 17000, "emoji": "<:AAPL:1466507980164956283>"},
    {"name": "Sproutify", "symbol": "SPRT", "base_price": 55.0, "max_shares": 16000, "emoji": "<:SPRT:1473422604172792024>"},
]

# Stock data storage: {guild_id: {ticker_symbol: {"price": float, "price_history": [float], "available_shares": int, "real_price": float, "shares_outstanding": int, "market_cap": float, "news_multiplier": float, "last_api_fetch": float}}}
stock_data = {}

# Market news toggle: {guild_id: bool} — True = enabled (default)
market_news_enabled = {}

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
        
        # Create stock line (with company emoji)
        company_emoji = ticker.get("emoji", "")
        stock_line = f"{company_emoji} **{ticker['name']} ({symbol})**\n"
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
        # Check if market news is enabled for this guild
        if not market_news_enabled.get(guild.id, True):
            return
        
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
        company_display = f"{ticker.get('emoji', '')} {company_name}"
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
        
        # Format the news message with company name (with emoji)
        news_message = news_template.format(company=company_display)
        
        # Create embed
        embed = discord.Embed(
            title=f"{emoji} ***THIS JUST IN!***",
            description=news_message,
            color=color
        )
        embed.add_field(name="Company", value=f"**{company_display} ({symbol})**", inline=True)
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
    """Update or create the crypto market message in #fernbase channel."""
    # Find the fernbase channel
    fernbase_channel = discord.utils.get(guild.text_channels, name="fernbase")
    
    if not fernbase_channel:
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
        async for message in fernbase_channel.history(limit=50):
            if message.author == bot.user and message.embeds and message.embeds[0].title == "💰 CRYPTO MARKET 💰":
                await message.edit(embed=embed)
                return
        
        # No existing message found, create new one
        await fernbase_channel.send(embed=embed)
        
    except Exception as e:
        print(f"Error updating fernbase in {guild.name}: {e}")

async def update_all_coinbase():
    """Background task to update all #fernbase channels every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(5)
    
    logging.info("Fernbase update task started")
    
    while not bot.is_closed():
        try:
            # Update prices first (now async)
            logging.info("Starting crypto price update...")
            await update_crypto_prices_market()
            
            # Update fernbase channels for all guilds the bot is in
            for guild in bot.guilds:
                await update_coinbase_message(guild)
                await asyncio.sleep(1)  # Small delay between updates
        except Exception as e:
            logging.error(f"Error in fernbase update task: {e}", exc_info=True)
        
        # Wait 60 seconds before next update
        await asyncio.sleep(60)


async def gardener_background_task():
    """Background task to check gardener actions every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for bot to fully initialize
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Get all users with gardeners (run in thread to avoid blocking event loop)
            users_with_gardeners = await asyncio.to_thread(get_all_users_with_gardeners)
            premium_user_ids = await asyncio.to_thread(get_all_user_ids_with_premium_tier)
            premium_user_ids_set = set(premium_user_ids)
            all_user_ids = set(uid for uid, _ in users_with_gardeners) | premium_user_ids_set
            
            for user_id in all_user_ids:
                # Sync premium tier from Discord roles when member is available (so benefits use role, not stale DB)
                if user_id in premium_user_ids_set:
                    for guild in bot.guilds:
                        member = guild.get_member(user_id)
                        if member is not None:
                            sync_premium_tier_from_member(member)
                            break
                db_gardeners = await asyncio.to_thread(get_user_gardeners, user_id)
                premium_gardeners = get_premium_virtual_gardeners(user_id)
                gardeners = db_gardeners + premium_gardeners
                if not gardeners:
                    continue
                # Process each gardener (regular 1-5 and premium virtual 6-9)
                for gardener in gardeners:
                    gardener_id = gardener.get("id")
                    if not gardener_id:
                        continue
                    is_premium_gardener = gardener_id >= 6
                    gardener_chance = PREMIUM_GARDENER_CHANCES.get(gardener_id) if is_premium_gardener else GARDENER_CHANCES.get(gardener_id, 0.05)
                    if random.random() < gardener_chance:
                        try:
                            # Stacked tool chance: any regular gardener (1-5) with a tool can trigger a harvest upgrade; premium gardeners (6-9) also benefit from this
                            total_harvest_upgrade_chance = sum(
                                GARDENER_TOOLS.get(g["id"], {}).get("chance", 0)
                                for g in gardeners
                                if g.get("has_tool") and g.get("id", 0) <= 5
                            )
                            if is_premium_gardener:
                                base_harvest_chance = PREMIUM_GARDENER_HARVEST_CHANCES.get(gardener_id, 0.0)
                                effective_harvest_chance = min(1.0, base_harvest_chance + total_harvest_upgrade_chance)
                                upgraded_to_harvest = effective_harvest_chance > 0 and random.random() < effective_harvest_chance
                            else:
                                upgraded_to_harvest = total_harvest_upgrade_chance > 0 and random.random() < total_harvest_upgrade_chance
                            
                            if upgraded_to_harvest:
                                # Perform full harvest instead of single gather (orchard upgrades apply; no chain)
                                harvest_result = await perform_harvest_for_user(user_id, allow_chain=False)
                                total_value = harvest_result["total_value"]
                                current_balance = harvest_result["current_balance"]
                                item_count = len(harvest_result["gathered_items"])
                                
                                # Update gardener stats only for regular gardeners (premium 6-9 have no DB record)
                                if gardener_id <= 5:
                                    await asyncio.to_thread(update_gardener_stats, user_id, gardener_id, total_value, item_count)
                                
                                # Send cool upgrade message to #lawn
                                for guild in bot.guilds:
                                    member = guild.get_member(user_id)
                                    if member:
                                        lawn_channel = discord.utils.get(guild.text_channels, name="lawn")
                                        if lawn_channel and lawn_channel.permissions_for(guild.me).send_messages:
                                            try:
                                                mention = member.mention
                                                harvest_color = PREMIUM_GARDENER_HARVEST_COLORS.get(gardener_id, discord.Color.gold()) if is_premium_gardener else discord.Color.gold()
                                                embed = discord.Embed(
                                                    title="🌾✨ GATHER UPGRADED TO HARVEST! ✨🌾",
                                                    description=f"{mention}, **the gardener's tool sparked!**",
                                                    color=harvest_color
                                                )
                                                
                                                lines = []
                                                for item in harvest_result["gathered_items"][:20]:
                                                    rip_em = get_ripeness_imbue_emoji(item.get("ripeness", ""))
                                                    emoji = get_item_display_emoji(item["name"])
                                                    gmo = " GMO! ✨" if item["is_gmo"] else ""
                                                    prefix = f"{rip_em} " if rip_em else ""
                                                    lines.append(f"{prefix}{emoji} (**{item['ripeness']}**){gmo}")
                                                items_display = "\n".join(lines) or "No items"
                                                # Discord embed field value limit is 1024 characters
                                                if len(items_display) > 1024:
                                                    items_display = items_display[:1021] + "..."
                                                embed.add_field(name="📦 Items Harvested", value=items_display, inline=False)
                                                embed.add_field(name="💰 Total Value", value=f"**${total_value:,.2f}**", inline=True)
                                                embed.add_field(name="💵 New Balance", value=f"**${current_balance:,.2f}**", inline=True)
                                                await lawn_channel.send(embed=embed)
                                                # Hidden achievement: One in a Mikellion (gardener harvest included Mikellion)
                                                has_mikellion = any(item.get("ripeness") == "Mikellion" for item in harvest_result.get("gathered_items", []))
                                                if has_mikellion and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
                                                    await send_hidden_achievement_notification_dm(user_id, "one_in_a_mikellion")
                                                await check_almanac_achievements_async(user_id, lawn_channel, mention)
                                                break
                                            except Exception as e:
                                                print(f"Error sending gardener harvest-upgrade notification to #lawn in {guild.name} for user {user_id}: {e}")
                                        break
                            else:
                                # Normal single gather (orchard fertilizer applies; no chain)
                                gather_result = await perform_gather_for_user(user_id, apply_cooldown=False, apply_orchard_fertilizer=True)
                                if gardener_id <= 5:
                                    await asyncio.to_thread(update_gardener_stats, user_id, gardener_id, gather_result["value"], 1)
                                
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
                                                    rip_em = get_ripeness_imbue_emoji(gather_result.get("ripeness", ""))
                                                    desc_prefix = f"{rip_em} " if rip_em else ""
                                                    gather_color = PREMIUM_GARDENER_GATHER_COLORS.get(gardener_id, discord.Color.green()) if is_premium_gardener else discord.Color.green()
                                                    gardener_emoji = PREMIUM_GARDENER_EMOJI.get(gardener_id, "🌿") if is_premium_gardener else "🌿"
                                                    embed = discord.Embed(
                                                        title=f"{gardener_emoji} {user_name}'s Gardener gathered!",
                                                        description=f"{desc_prefix}Their gardener found a **{gather_result['name']}**!",
                                                        color=gather_color
                                                    )
                                                    embed.add_field(name="Value", value=f"**${gather_result['base_value']:.2f}**", inline=True)
                                                    embed.add_field(name="Ripeness", value=f"{rip_em} {gather_result['ripeness']}".strip(), inline=True)
                                                    embed.add_field(name="GMO?", value="Yes ✨" if gather_result['is_gmo'] else "No", inline=False)
                                                    await lawn_channel.send(embed=embed)
                                                    # Hidden achievement: One in a Mikellion (gardener gathered Mikellion)
                                                    if gather_result.get("ripeness") == "Mikellion" and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
                                                        await send_hidden_achievement_notification_dm(user_id, "one_in_a_mikellion")
                                                    await check_almanac_achievements_async(user_id, lawn_channel, member.mention)
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


async def secret_gardener_background_task():
    """Background task for the Secret Gardener to auto-gather every minute (50% chance)."""
    await bot.wait_until_ready()
    await asyncio.sleep(8)
    
    while not bot.is_closed():
        try:
            sg_user_ids = await asyncio.to_thread(get_all_users_with_secret_gardener)
            
            for user_id in sg_user_ids:
                # 50% chance to gather every minute
                if random.random() < 0.50:
                    try:
                        sg_has_harvest = await asyncio.to_thread(has_secret_gardener_harvest, user_id)
                        
                        # Determine if this is a harvest or gather
                        # If user has auto-harvest (tier 12), use the same tool-upgrade logic
                        if sg_has_harvest and random.random() < 0.25:
                            harvest_result = await perform_harvest_for_user(user_id, allow_chain=False)
                            total_value = harvest_result["total_value"]
                            
                            # Notify in #lawn
                            for guild in bot.guilds:
                                member = guild.get_member(user_id)
                                if member:
                                    lawn_channel = discord.utils.get(guild.text_channels, name="lawn")
                                    if lawn_channel and lawn_channel.permissions_for(guild.me).send_messages:
                                        try:
                                            embed = discord.Embed(
                                                title="\U0001f33f\u2728 SECRET GARDENER HARVEST! \u2728\U0001f33f",
                                                description=f"{member.mention}, the Secret Gardener sparked!",
                                                color=discord.Color.purple()
                                            )
                                            lines = []
                                            for item in harvest_result["gathered_items"][:20]:
                                                rip_em = get_ripeness_imbue_emoji(item.get("ripeness", ""))
                                                emoji = get_item_display_emoji(item["name"])
                                                gmo = " GMO! ✨" if item["is_gmo"] else ""
                                                prefix = f"{rip_em} " if rip_em else ""
                                                lines.append(f"{prefix}{emoji} (**{item['ripeness']}**){gmo}")
                                            items_display = "\n".join(lines) or "No items"
                                            embed.add_field(name="\U0001f4e6 Items Harvested", value=items_display, inline=False)
                                            embed.add_field(name="\U0001f4b0 Total Value", value=f"**${total_value:,.2f}**", inline=True)
                                            embed.add_field(name="\U0001f4b5 New Balance", value=f"**${harvest_result['current_balance']:,.2f}**", inline=True)
                                            await lawn_channel.send(embed=embed)
                                            # Hidden achievement: One in a Mikellion (secret gardener harvest included Mikellion)
                                            has_mikellion = any(item.get("ripeness") == "Mikellion" for item in harvest_result.get("gathered_items", []))
                                            if has_mikellion and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
                                                await send_hidden_achievement_notification_dm(user_id, "one_in_a_mikellion")
                                            await check_almanac_achievements_async(user_id, lawn_channel, member.mention)
                                        except Exception as e:
                                            print(f"Error sending secret gardener harvest notification: {e}")
                                    break
                        else:
                            # Normal single gather
                            gather_result = await perform_gather_for_user(user_id, apply_cooldown=False, apply_orchard_fertilizer=True)
                            
                            user_name = "User"
                            for guild in bot.guilds:
                                member = guild.get_member(user_id)
                                if member:
                                    user_name = member.display_name or member.name
                                    lawn_channel = discord.utils.get(guild.text_channels, name="lawn")
                                    if lawn_channel and lawn_channel.permissions_for(guild.me).send_messages:
                                        try:
                                            rip_em = get_ripeness_imbue_emoji(gather_result.get("ripeness", ""))
                                            desc_prefix = f"{rip_em} " if rip_em else ""
                                            embed = discord.Embed(
                                                title=f"\U0001f33f\u2728 {user_name}'s Secret Gardener gathered!",
                                                description=f"{desc_prefix}The Secret Gardener found a **{gather_result['name']}**!",
                                                color=discord.Color.purple()
                                            )
                                            embed.add_field(name="Value", value=f"**${gather_result['base_value']:,.2f}**", inline=True)
                                            embed.add_field(name="Ripeness", value=f"{rip_em} {gather_result['ripeness']}".strip(), inline=True)
                                            embed.add_field(name="GMO?", value="Yes \u2728" if gather_result['is_gmo'] else "No", inline=False)
                                            await lawn_channel.send(embed=embed)
                                            # Hidden achievement: One in a Mikellion (secret gardener gathered Mikellion)
                                            if gather_result.get("ripeness") == "Mikellion" and unlock_hidden_achievement(user_id, "one_in_a_mikellion"):
                                                await send_hidden_achievement_notification_dm(user_id, "one_in_a_mikellion")
                                            await check_almanac_achievements_async(user_id, lawn_channel, member.mention)
                                        except Exception as e:
                                            print(f"Error sending secret gardener notification: {e}")
                                    break
                    except Exception as e:
                        print(f"Error processing secret gardener for user {user_id}: {e}")
            
            await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in secret gardener background task: {e}")
        
        await asyncio.sleep(60)


async def gpu_background_task():
    """Background task to check GPU mining actions every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for bot to fully initialize
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Get all users with GPUs (run in thread to avoid blocking event loop)
            users_with_gpus = await asyncio.to_thread(get_all_users_with_gpus)
            
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
                            
                            # Add crypto to user's holdings (run in thread to avoid blocking event loop)
                            await asyncio.to_thread(update_user_crypto_holdings, user_id, symbol, amount)
                            
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
    
    # Celestial events: Solar Eclipse and Blood Moon (custom embeds)
    if event["event_type"] == "solar_eclipse":
        embed = discord.Embed(
            title=SOLAR_ECLIPSE_TITLE,
            description=SOLAR_ECLIPSE_DESCRIPTION,
            color=SOLAR_ECLIPSE_COLOR
        )
        embed.set_footer(text=SOLAR_ECLIPSE_FOOTER)
        try:
            await events_channel.send("@here", embed=embed)
            return True
        except Exception as e:
            print(f"ERROR sending Solar Eclipse start embed in {guild.name}: {e}")
            return False
    if event["event_type"] == "blood_moon":
        embed = discord.Embed(
            title=BLOOD_MOON_TITLE,
            description=BLOOD_MOON_DESCRIPTION,
            color=BLOOD_MOON_COLOR
        )
        embed.set_footer(text=BLOOD_MOON_FOOTER)
        try:
            await events_channel.send("@here", embed=embed)
            return True
        except Exception as e:
            print(f"ERROR sending Blood Moon start embed in {guild.name}: {e}")
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
        title=f"{event_info['emoji']} {event_name} Event Started!",
        description=event_info["description"],
        color=discord.Color.green()
    )
    # Display duration appropriately (seconds if < 1 minute, minutes otherwise)
    if duration_minutes < 1:
        duration_seconds = int(duration_minutes * 60)
        duration_display = f"{duration_seconds} Seconds"
    else:
        duration_display = f"{duration_minutes} Minutes"
    embed.add_field(name="Effect", value=event_info["effect"], inline=False)
    embed.add_field(name="Duration", value=duration_display, inline=False)
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
    
    # Celestial events: Solar Eclipse and Blood Moon (custom end embeds)
    if event["event_type"] == "solar_eclipse":
        embed = discord.Embed(
            title=f"{SOLAR_ECLIPSE_TITLE} Event Ended",
            description="Conditions are back to normal. Stay tuned for any future events...",
            color=SOLAR_ECLIPSE_COLOR
        )
        try:
            await events_channel.send(embed=embed)
        except Exception as e:
            print(f"Error sending Solar Eclipse end embed in {guild.name}: {e}")
        return
    if event["event_type"] == "blood_moon":
        embed = discord.Embed(
            title=f"{BLOOD_MOON_TITLE} Event Ended",
            description="Conditions are back to normal. Stay tuned for any future events...",
            color=BLOOD_MOON_COLOR
        )
        try:
            await events_channel.send(embed=embed)
        except Exception as e:
            print(f"Error sending Blood Moon end embed in {guild.name}: {e}")
        return
    
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
        title=f"{event_info['emoji']} {event_name} Event Ended",
        description="Conditions are back to normal. Stay tuned for any future events...",
        color=discord.Color.red()
    )
    
    try:
        await events_channel.send(embed=embed)
    except Exception as e:
        print(f"Error sending event end embed in {guild.name}: {e}")


def _apply_auto_water_for_user(user_id: int, now_est: datetime.datetime) -> bool:
    """Apply one water for a user (irrigation). Returns True if water was applied."""
    EST_OFFSET = datetime.timedelta(hours=-5)
    last_water_time = get_user_last_water_time(user_id)
    current_date = now_est.date()
    current_hour = now_est.hour
    has_double_water = get_invite_cooldown_reductions(user_id).get("water_double", False)
    if last_water_time > 0:
        last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
        last_water_est = last_water_utc + EST_OFFSET
        last_water_date = last_water_est.date()
        last_water_hour = last_water_est.hour
        already_watered = False
        if last_water_date == current_date:
            if has_double_water:
                in_pm = current_hour >= 12
                last_in_pm = last_water_hour >= 12
                if in_pm and last_in_pm:
                    already_watered = True
                elif not in_pm and not last_in_pm:
                    already_watered = True
            else:
                already_watered = True
        if already_watered:
            return False
    consecutive_days = get_user_consecutive_water_days(user_id)
    is_first_water_today = True
    if last_water_time > 0:
        last_water_utc = datetime.datetime.utcfromtimestamp(last_water_time)
        last_water_est = last_water_utc + EST_OFFSET
        last_water_date = last_water_est.date()
        if last_water_date == current_date:
            is_first_water_today = False
        else:
            yesterday = (now_est - datetime.timedelta(days=1)).date()
            if last_water_date != yesterday and last_water_date != current_date:
                consecutive_days = 0
    if is_first_water_today:
        consecutive_days += 1
        set_user_consecutive_water_days(user_id, consecutive_days)
    now_ts = time.time()
    update_user_last_water_time(user_id, now_ts)
    increment_user_water_count(user_id)
    money_reward = consecutive_days * 5000.0
    money_reward = normalize_money(money_reward)
    current_balance = get_user_balance(user_id)
    new_balance = normalize_money(current_balance + money_reward)
    update_user_balance(user_id, new_balance)
    if consecutive_days == 5 and is_first_water_today:
        increment_tree_rings(user_id, 10)
    return True


async def irrigation_auto_water_task():
    """At 12 PM and 12 AM EST, auto-water users who have the Irrigation System."""
    await bot.wait_until_ready()
    EST_OFFSET = datetime.timedelta(hours=-5)
    await asyncio.sleep(60)
    last_run_date_hour = None
    while not bot.is_closed():
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_est = now_utc + EST_OFFSET
            if now_est.hour in (0, 12) and now_est.minute < 2:
                key = (now_est.date(), now_est.hour)
                if key != last_run_date_hour:
                    last_run_date_hour = key
                    user_ids = await asyncio.to_thread(get_user_ids_with_shop_item, "irrigation_system")
                    for uid in user_ids:
                        try:
                            applied = await asyncio.to_thread(_apply_auto_water_for_user, uid, now_est)
                            if applied:
                                print(f"Irrigation: auto-watered user {uid}")
                        except Exception as e:
                            print(f"Irrigation: error watering user {uid}: {e}")
            await asyncio.sleep(60)
        except Exception as e:
            print(f"Error in irrigation_auto_water_task: {e}")
            await asyncio.sleep(60)


async def hourly_event_check():
    """Background task to trigger hourly events at configurable intervals with 50% chance."""
    await bot.wait_until_ready()
    
    # Wait a short initial delay before first check
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # CRITICAL: Send end embeds for any expired events first (e.g. after bot restart), then clear from DB
            await _send_end_embeds_for_expired_events()
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
            
            # Do not start hourly events during Solar Eclipse or Blood Moon
            celestial_active = get_active_event_by_type("solar_eclipse") or get_active_event_by_type("blood_moon")
            if not existing_hourly and not celestial_active:
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
                    
                    # Clear event from DB BEFORE sending embeds so event_cleanup_task/celestial loop can't also send (prevents duplicate end embeds)
                    clear_event(event_id)
                    clear_expired_events()
                    
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
                    print(f"Ended hourly event: {event_info['name']}")
            
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
            # CRITICAL: Send end embeds for any expired events first (e.g. after bot restart), then clear from DB
            await _send_end_embeds_for_expired_events()
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
            
            # Do not start daily events during Solar Eclipse or Blood Moon
            celestial_active = get_active_event_by_type("solar_eclipse") or get_active_event_by_type("blood_moon")
            if not existing_daily and not celestial_active:
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
                    
                    # Clear event from DB BEFORE sending embeds so event_cleanup_task/celestial loop can't also send (prevents duplicate end embeds)
                    clear_event(event_id)
                    clear_expired_events()
                    
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
                    print(f"Ended daily event: {event_info['name']}")
            
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
    """Background task to clean up any orphaned expired events. Sends end embeds to #events before clearing."""
    await bot.wait_until_ready()
    await asyncio.sleep(5)

    while not bot.is_closed():
        try:
            await _send_end_embeds_for_expired_events()
            clear_expired_events()
            await asyncio.sleep(60)  # Check every minute
        except Exception as e:
            print(f"Error in event_cleanup_task: {e}")
            await asyncio.sleep(60)


async def _end_active_event_for_all_guilds(event: dict):
    """Send event end embed to #events in all guilds and clear the event from DB. Clears from DB first so no other task can send a duplicate end embed."""
    event_id = event.get("event_id", "")
    clear_event(event_id)
    for guild in bot.guilds:
        try:
            await send_event_end_embed(guild, event)
        except Exception as e:
            print(f"Error sending event end embed to {guild.name}: {e}")


async def _send_end_embeds_for_expired_events():
    """Get any events that have already ended, send their end embeds to #events in all guilds, then clear them. Call on startup and before clear_expired_events() in loops."""
    expired = get_expired_events()
    for event in expired:
        try:
            await _end_active_event_for_all_guilds(event)
            print(f"Event recovery: sent end embed for {event.get('event_type')} '{event.get('event_name')}'")
        except Exception as e:
            print(f"Error sending end embed for expired event {event.get('event_id')}: {e}")
            clear_event(event.get("event_id", ""))
    if expired:
        clear_expired_events()


async def celestial_event_check():
    """At 4:30 EST roll 50% for Solar Eclipse (until 19:30 EST). At 19:30 EST roll 50% for Blood Moon (until 4:30 EST next day). Start/end embeds in #events."""
    from zoneinfo import ZoneInfo
    await bot.wait_until_ready()
    await asyncio.sleep(15)
    last_solar_trigger_date = None  # (year, month, day) in EST
    last_blood_trigger_date = None
    est = ZoneInfo("America/New_York")
    while not bot.is_closed():
        try:
            await _send_end_embeds_for_expired_events()
            clear_expired_events()
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            now_est = now_utc.astimezone(est)
            now_ts = now_utc.timestamp()
            today_est = (now_est.year, now_est.month, now_est.day)

            # End expired celestial events (send end embed, then clear)
            se = get_active_event_by_type("solar_eclipse")
            if se and now_ts >= se.get("end_time", 0):
                await _end_active_event_for_all_guilds(se)
                print("Solar Eclipse ended (time expired).")
            bm = get_active_event_by_type("blood_moon")
            if bm and now_ts >= bm.get("end_time", 0):
                await _end_active_event_for_all_guilds(bm)
                print("Blood Moon ended (time expired).")

            # At 4:30 EST: 50% chance to start Solar Eclipse (day: 4:30 -> 19:30 EST)
            if (now_est.hour, now_est.minute) == CELESTIAL_DAY_START_EST and today_est != last_solar_trigger_date:
                last_solar_trigger_date = today_est
                if not get_active_event_by_type("solar_eclipse") and random.random() < CELESTIAL_TRIGGER_CHANCE:
                    # End any active hourly and daily (send their end embeds, then clear)
                    for ev_type in ("hourly", "daily"):
                        existing = get_active_event_by_type(ev_type)
                        if existing:
                            await _end_active_event_for_all_guilds(existing)
                            print(f"Ended active {ev_type} event for Solar Eclipse start.")
                    end_est = now_est.replace(hour=CELESTIAL_NIGHT_START_EST[0], minute=CELESTIAL_NIGHT_START_EST[1], second=0, microsecond=0)
                    end_ts = end_est.timestamp()
                    event_id = f"solar_eclipse_{int(now_ts)}"
                    set_active_event(
                        event_id=event_id,
                        event_type="solar_eclipse",
                        event_name=SOLAR_ECLIPSE_TITLE,
                        start_time=now_ts,
                        end_time=end_ts,
                        effects={"event_id": "solar_eclipse"}
                    )
                    for guild in bot.guilds:
                        try:
                            await send_event_start_embed(guild, {
                                "event_type": "solar_eclipse",
                                "event_id": "solar_eclipse",
                                "event_name": SOLAR_ECLIPSE_TITLE,
                                "effects": {"event_id": "solar_eclipse"}
                            }, 0)
                        except Exception as e:
                            print(f"Error sending Solar Eclipse start to {guild.name}: {e}")
                    print("Started Solar Eclipse (50% roll at 4:30 EST).")

            # At 19:30 EST: 50% chance to start Blood Moon (night: 19:30 -> 4:30 EST next day)
            if (now_est.hour, now_est.minute) == CELESTIAL_NIGHT_START_EST and today_est != last_blood_trigger_date:
                last_blood_trigger_date = today_est
                if not get_active_event_by_type("blood_moon") and random.random() < CELESTIAL_TRIGGER_CHANCE:
                    for ev_type in ("hourly", "daily"):
                        existing = get_active_event_by_type(ev_type)
                        if existing:
                            await _end_active_event_for_all_guilds(existing)
                            print(f"Ended active {ev_type} event for Blood Moon start.")
                    next_day = now_est.date() + datetime.timedelta(days=1)
                    end_est = now_est.replace(year=next_day.year, month=next_day.month, day=next_day.day,
                                             hour=CELESTIAL_DAY_START_EST[0], minute=CELESTIAL_DAY_START_EST[1], second=0, microsecond=0)
                    end_ts = end_est.timestamp()
                    event_id = f"blood_moon_{int(now_ts)}"
                    set_active_event(
                        event_id=event_id,
                        event_type="blood_moon",
                        event_name=BLOOD_MOON_TITLE,
                        start_time=now_ts,
                        end_time=end_ts,
                        effects={"event_id": "blood_moon"}
                    )
                    for guild in bot.guilds:
                        try:
                            await send_event_start_embed(guild, {
                                "event_type": "blood_moon",
                                "event_id": "blood_moon",
                                "event_name": BLOOD_MOON_TITLE,
                                "effects": {"event_id": "blood_moon"}
                            }, 0)
                        except Exception as e:
                            print(f"Error sending Blood Moon start to {guild.name}: {e}")
                    print("Started Blood Moon (50% roll at 19:30 EST).")

            await asyncio.sleep(60)
        except Exception as e:
            print(f"Error in celestial_event_check: {e}")
            import traceback
            traceback.print_exc()
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
            session_summary += f"{coin_name} ({sym}): **{amt:.4f}**\n"
        
        # Create GPU info text
        gpu_text = ""
        if self.gpus_used:
            gpu_text = "\n".join(f"**{g}**" for g in self.gpus_used)
        
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
        success_embed.add_field(name="**Total Mines**", value=f"**{self.total_mines}**", inline=True)
        if gpu_text:
            success_embed.add_field(name="**GPUs Active**", value=gpu_text, inline=False)
        if session_summary:
            success_embed.add_field(name="Mined", value=session_summary.strip(), inline=False)
        success_embed.set_footer(text="Keep clicking!")
        
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
            title="⏰ Mine Session Over",
            description="Time's up!",
            color=discord.Color.orange()
        )
        
        if self.total_mines > 0:
            timeout_embed.add_field(
                name="\u200b",
                value=f"Total Mines: **{self.total_mines}**",
                inline=False
            )
            
            # Add GPU info if GPUs were used
            if self.gpus_used:
                timeout_embed.add_field(name="GPUs Used", value="\n".join(f"**{g}**" for g in self.gpus_used), inline=False)
            
            session_summary = ""
            for sym, amt in self.session_mined.items():
                coin_name = next(c["name"] for c in CRYPTO_COINS if c["symbol"] == sym)
                session_summary += f"{coin_name} ({sym}): **{amt:.4f}**\n"
            
            if session_summary:
                timeout_embed.add_field(name="Crypto Mined", value=session_summary.strip(), inline=False)
            
            # Add hidden achievement message if unlocked
            if self.blockchain_achievement_unlocked:
                timeout_embed.add_field(name="🎉 Hidden Achievement Unlocked!", value="**Blockchain**", inline=False)

            timeout_embed.set_footer(text="Use /sell to sell your crypto")
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
            # Best Buy Geek Squad: every click counts as two
            if has_shop_item(interaction.user.id, "best_buy_geek_squad"):
                amount *= 2
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


def _mine_prepare_sync(member, user_id: int) -> dict:
    """Run in thread: sync premium + roulette check + cooldown data + GPUs. Returns dict for mine command."""
    sync_premium_tier_from_member(member)
    is_roulette_cooldown, roulette_time_left = check_roulette_elimination_cooldown(user_id)
    last_mine_time = get_user_last_mine_time(user_id)
    invite_reductions = get_invite_cooldown_reductions(user_id)
    premium_cd = get_premium_cooldown_reductions(user_id)
    user_gpus = get_user_gpus(user_id)
    return {
        "is_roulette_cooldown": is_roulette_cooldown,
        "roulette_time_left": roulette_time_left,
        "last_mine_time": last_mine_time,
        "invite_reductions": invite_reductions,
        "premium_cd": premium_cd,
        "user_gpus": user_gpus,
    }


@bot.tree.command(name="mine", description="Mine cryptocurrency! (1 hour cooldown)")
async def mine(interaction: discord.Interaction):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return

        user_id = interaction.user.id
        data = await asyncio.to_thread(_mine_prepare_sync, interaction.user, user_id)

        # Check if user is on Russian Roulette elimination cooldown (dead)
        if data["is_roulette_cooldown"]:
            roulette_time_left = data["roulette_time_left"]
            if roulette_time_left < 60:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"Sorry, {interaction.user.name}, you're dead. You cannot mine for {roulette_time_left} second(s)", ephemeral=True)
            else:
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

        # Check cooldown
        last_mine_time = data["last_mine_time"]
        current_time = time.time()
        mine_cooldown = MINE_COOLDOWN
        mine_cooldown = max(0, mine_cooldown - data["invite_reductions"].get("mine_reduction", 0))
        mine_cooldown = max(0, mine_cooldown - data["premium_cd"].get("mine_reduction", 0))
        
        # Apply event cooldown reductions
        active_events = get_active_events_cached()
        hourly_event = next((e for e in active_events if e["event_type"] == "hourly"), None)
        daily_event = next((e for e in active_events if e["event_type"] == "daily"), None)
        
        if hourly_event:
            event_id = hourly_event.get("effects", {}).get("event_id", "")
            if event_id == "speed_harvest":  # Adrenaline Boost
                mine_cooldown = max(0, mine_cooldown - 450)  # 7.5 minutes = 450 seconds
        
        if daily_event:
            event_id = daily_event.get("effects", {}).get("event_id", "")
            if event_id == "speed_day":  # Overdrive
                mine_cooldown = max(0, mine_cooldown - 300)  # 5 minutes = 300 seconds

        if last_mine_time > 0:
            cooldown_end = last_mine_time + mine_cooldown
            if current_time < cooldown_end:
                time_left = int(cooldown_end - current_time)
                minutes_left = time_left // 60
                seconds_left = time_left % 60
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"⏰ You must wait {minutes_left} minutes and {seconds_left} seconds before mining again, {interaction.user.name}.",
                    ephemeral=True)
                return

        # Get user's GPUs and calculate bonuses (from thread result)
        user_gpus = data["user_gpus"]
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
            embed.add_field(name="GPUs Active", value="\n".join(f"**{g}**" for g in gpus_used), inline=False)
        
        view = MiningView(user_id, timeout=total_time, gpu_percent_boost=total_percent_boost, gpu_seconds_boost=total_seconds_boost, gpus_used=gpus_used)
        message = await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
        # Store message reference in view for timeout handling
        if message:
            view.message = message
        # Don't start the timeout checker here - it will start when the user clicks the button
    except Exception as e:
        print(f"Error in mine command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


def _sell_initial_sync(member, user_id: int) -> dict:
    """Run in thread: sync beta/booster/premium + load holdings and prices. Returns dict for sell command."""
    sync_beta_tester_from_member(member)
    sync_server_booster_from_member(member)
    sync_premium_tier_from_member(member)
    holdings = get_user_crypto_holdings(user_id)
    prices = get_crypto_prices()
    return {"holdings": holdings, "prices": prices}


def _sell_critical_path(member, user_id: int, coin: str, amount: float | None) -> dict:
    """
    Run in thread: all DB work + math for /sell.

    Returns a dict describing either an error:
        {"error": <message>, "ephemeral": bool}
    or a success:
        {"embed": discord.Embed}
    so the async /sell handler can send the response without touching the DB.
    """
    initial = _sell_initial_sync(member, user_id)
    holdings: dict = initial["holdings"]
    prices: dict = initial["prices"]

    member_name = getattr(member, "name", "you")

    # Handle "all" option
    if coin == "all":
        # Sell all crypto holdings
        base_sale_value = 0.0
        sold_items: list[str] = []
        # Track updated holdings locally to avoid an extra DB read
        updated_holdings = {
            "RTC": float(holdings.get("RTC", 0.0)),
            "TER": float(holdings.get("TER", 0.0)),
            "CNY": float(holdings.get("CNY", 0.0)),
        }

        for crypto_coin in CRYPTO_COINS:
            symbol = crypto_coin["symbol"]
            user_holding = holdings.get(symbol, 0.0)

            if user_holding > 0:
                coin_base_price = crypto_coin["base_price"]
                coin_price = prices.get(symbol, coin_base_price)
                sale_value = user_holding * coin_price
                base_sale_value += sale_value

                # Update holdings in DB (subtract) and locally
                update_user_crypto_holdings(user_id, symbol, -user_holding)
                updated_holdings[symbol] = updated_holdings.get(symbol, 0.0) - user_holding

                sold_items.append(f"{symbol}: {user_holding:.4f} (${sale_value:.2f})")

        if base_sale_value == 0:
            return {
                "error": f"❌ You don't have any cryptocurrency to sell, {member_name}!",
                "ephemeral": True,
            }

        # Apply boosts to sale value (additive from base, then rank multiplies subtotal)
        bloom_multiplier = get_bloom_multiplier(user_id)
        water_multiplier = get_water_multiplier(user_id)
        rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
        achievement_multiplier = get_achievement_multiplier(user_id)
        daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)

        # Calculate additive boosts from base
        extra_from_bloom = base_sale_value * (bloom_multiplier - 1.0)
        extra_from_water = base_sale_value * (water_multiplier - 1.0)
        extra_from_achievement = base_sale_value * (achievement_multiplier - 1.0)
        extra_from_daily = base_sale_value * (daily_bonus_multiplier - 1.0)

        # Subtotal before rank
        subtotal = base_sale_value + extra_from_bloom + extra_from_water + extra_from_achievement + extra_from_daily
        # Rank is multiplicative on subtotal
        extra_from_rank = subtotal * (rank_perma_buff_multiplier - 1.0)
        # All money buffs below apply to the SAME base value (additive stacking)
        base_for_buffs = float(subtotal + extra_from_rank)
        beta_mult = get_beta_tester_money_multiplier(user_id)
        sb_mult = get_server_booster_money_multiplier(user_id)
        prem_mult = get_premium_tier_money_multiplier(user_id)
        nether_mult = get_nether_star_money_multiplier(user_id)
        black_shard_mult = get_black_shard_money_multiplier(user_id)
        shadow_crystal_mult = get_shadow_crystal_money_multiplier(user_id)
        edward_mult = get_edward_splash_money_multiplier(user_id)
        eclipse_mult = get_eclipse_glasses_money_multiplier(user_id)
        extra_beta = base_for_buffs * (beta_mult - 1.0) if beta_mult > 1.0 else 0.0
        extra_booster = base_for_buffs * (sb_mult - 1.0) if sb_mult > 1.0 else 0.0
        extra_premium = base_for_buffs * (prem_mult - 1.0) if prem_mult > 1.0 else 0.0
        extra_ns = base_for_buffs * (nether_mult - 1.0) if nether_mult > 1.0 else 0.0
        extra_bs = base_for_buffs * (black_shard_mult - 1.0) if black_shard_mult > 1.0 else 0.0
        extra_sc = base_for_buffs * (shadow_crystal_mult - 1.0) if shadow_crystal_mult > 1.0 else 0.0
        extra_edward = base_for_buffs * (edward_mult - 1.0) if edward_mult > 1.0 else 0.0
        extra_eclipse = base_for_buffs * (eclipse_mult - 1.0) if eclipse_mult > 1.0 else 0.0
        extra_msi = base_for_buffs * 0.20 if has_shop_item(user_id, "msi_afterburner") else 0.0
        total_sale_value = base_for_buffs + extra_beta + extra_booster + extra_premium + extra_ns + extra_bs + extra_sc + extra_edward + extra_eclipse + extra_msi

        # Add money to balance (with boosts)
        current_balance = get_user_balance(user_id)
        new_balance = current_balance + total_sale_value
        update_user_balance(user_id, new_balance)

        # Create success embed
        embed = discord.Embed(
            title="💰 Sale Successful!",
            description=f"You sold all your cryptocurrency for **${total_sale_value:.2f}**!",
            color=discord.Color.green(),
        )
        embed.add_field(name="Sold", value="\n".join(sold_items) if sold_items else "None", inline=False)

        # Show boosts if applicable
        bloom_count = get_user_bloom_count(user_id)
        if bloom_count > 0 and extra_from_bloom > 0:
            multiplier_percent = (bloom_multiplier - 1.0) * 100
            embed.add_field(
                name="<:TreeRing:1474244868288282817> Tree Ring Boost",
                value=f"+{multiplier_percent:.1f}% - **+${extra_from_bloom:.2f}**",
                inline=False,
            )
        # Show achievement boost if applicable
        if extra_from_achievement > 0:
            achievement_percent = (achievement_multiplier - 1.0) * 100
            embed.add_field(
                name="🏆 Achievement Boost",
                value=f"+{achievement_percent:.1f}% - **+${extra_from_achievement:.2f}**",
                inline=False,
            )
        if extra_from_daily > 0:
            daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
            embed.add_field(
                name="💧 Water Streak Boost",
                value=f"+{daily_bonus_percent:.1f}% - **+${extra_from_daily:.2f}**",
                inline=False,
            )
        # Show rank perma buff if applicable (only if not PINE I) - multiplicative on subtotal
        bloom_rank = get_bloom_rank(user_id)
        if bloom_rank != "PINE I" and extra_from_rank > 0:
            rank_percent = (rank_perma_buff_multiplier - 1.0) * 100
            embed.add_field(
                name="⭐ Rank Boost",
                value=f"+{rank_percent:.1f}% - **+${extra_from_rank:.2f}**",
                inline=False,
            )
        if extra_beta > 0:
            beta_percent = (beta_mult - 1.0) * 100
            embed.add_field(
                name="🧪 Beta Tester!",
                value=f"+{beta_percent:.1f}% - **+${extra_beta:.2f}**",
                inline=False,
            )
        if extra_booster > 0:
            booster_percent = (sb_mult - 1.0) * 100
            embed.add_field(
                name=f"{SERVER_BOOSTER_EMOJI} Server Booster!",
                value=f"+{booster_percent:.1f}% - **+${extra_booster:.2f}**",
                inline=False,
            )
        premium_tier = get_user_premium_tier(user_id)
        if extra_premium > 0:
            mult = PREMIUM_MONEY_MULTIPLIERS.get(premium_tier, 1.0)
            premium_percent = (mult - 1.0) * 100
            embed.add_field(
                name=PREMIUM_DISPLAY.get(premium_tier, "Premium"),
                value=f"+{premium_percent:.1f}% - **+${extra_premium:.2f}**",
                inline=False,
            )

        # Item Boosts (shop / dailyshop items affecting this sale)
        item_boost_sources = []
        # Get counts for Nether Star and Black Shard (permanent + dayboosts)
        if nether_mult > 1.0:
            has_permanent = has_shop_item(user_id, "nether_star")
            dayboost_count = get_dayboost_count(user_id, "nether_star")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Nether Star", total_count))
        if black_shard_mult > 1.0:
            has_permanent = has_shop_item(user_id, "black_shard")
            dayboost_count = get_dayboost_count(user_id, "black_shard")
            total_count = (1 if has_permanent else 0) + dayboost_count
            if total_count > 0:
                item_boost_sources.append(("Black Shard", total_count))

        if item_boost_sources:
            total_item_extra = extra_ns + extra_bs
            value_parts = [f"**+${total_item_extra:.2f}**"] if total_item_extra > 0 else []
            value_parts.append("\n".join(_format_item_boost_source(name, count) for name, count in item_boost_sources))
            embed.add_field(
                name="📦 Item Boost",
                value="\n".join(value_parts),
                inline=False,
            )

        embed.add_field(
            name="Remaining Holdings",
            value=f"RTC: {updated_holdings['RTC']:.4f}\nTER: {updated_holdings['TER']:.4f}\nCNY: {updated_holdings['CNY']:.4f}",
            inline=False,
        )
        embed.add_field(name="New Balance", value=f"${new_balance:.2f}", inline=False)

        return {"embed": embed}

    # --- Selling a specific coin ---
    user_holding = holdings.get(coin, 0.0)

    if user_holding <= 0:
        return {
            "error": f"❌ You don't have any {coin} to sell, {member_name}!",
            "ephemeral": True,
        }

    # If amount not specified, sell all
    if amount is None:
        amount = user_holding
    elif amount > user_holding:
        return {
            "error": f"❌ You only have {user_holding:.4f} {coin}, but tried to sell {amount:.4f} {coin}!",
            "ephemeral": True,
        }
    elif amount <= 0:
        return {
            "error": "❌ Invalid amount! Please sell a positive amount.",
            "ephemeral": True,
        }

    # Calculate base sale value
    # Get base price for the coin
    coin_info = next((c for c in CRYPTO_COINS if c["symbol"] == coin), None)
    coin_base_price = coin_info["base_price"] if coin_info else 855.0
    coin_price = prices.get(coin, coin_base_price)
    base_sale_value = amount * coin_price
    if has_shop_item(user_id, "cryptobro_shadow"):
        base_sale_value *= 1.50

    # Apply boosts to sale value (additive from base, then rank multiplies subtotal)
    bloom_multiplier = get_bloom_multiplier(user_id)
    water_multiplier = get_water_multiplier(user_id)
    rank_perma_buff_multiplier = get_rank_perma_buff_multiplier(user_id)
    achievement_multiplier = get_achievement_multiplier(user_id)
    daily_bonus_multiplier = get_daily_bonus_multiplier(user_id)

    # Calculate additive boosts from base
    extra_from_bloom = base_sale_value * (bloom_multiplier - 1.0)
    extra_from_water = base_sale_value * (water_multiplier - 1.0)
    extra_from_achievement = base_sale_value * (achievement_multiplier - 1.0)
    extra_from_daily = base_sale_value * (daily_bonus_multiplier - 1.0)

    # Subtotal before rank
    subtotal = base_sale_value + extra_from_bloom + extra_from_water + extra_from_achievement + extra_from_daily
    # Rank is multiplicative on subtotal
    extra_from_rank = subtotal * (rank_perma_buff_multiplier - 1.0)
    # All money buffs below apply to the SAME base value (additive stacking)
    base_for_buffs = float(subtotal + extra_from_rank)
    beta_mult = get_beta_tester_money_multiplier(user_id)
    sb_mult = get_server_booster_money_multiplier(user_id)
    prem_mult = get_premium_tier_money_multiplier(user_id)
    nether_mult = get_nether_star_money_multiplier(user_id)
    black_shard_mult = get_black_shard_money_multiplier(user_id)
    shadow_crystal_mult = get_shadow_crystal_money_multiplier(user_id)
    edward_mult = get_edward_splash_money_multiplier(user_id)
    eclipse_mult = get_eclipse_glasses_money_multiplier(user_id)
    extra_beta = base_for_buffs * (beta_mult - 1.0) if beta_mult > 1.0 else 0.0
    extra_booster = base_for_buffs * (sb_mult - 1.0) if sb_mult > 1.0 else 0.0
    extra_premium = base_for_buffs * (prem_mult - 1.0) if prem_mult > 1.0 else 0.0
    extra_ns = base_for_buffs * (nether_mult - 1.0) if nether_mult > 1.0 else 0.0
    extra_bs = base_for_buffs * (black_shard_mult - 1.0) if black_shard_mult > 1.0 else 0.0
    extra_sc = base_for_buffs * (shadow_crystal_mult - 1.0) if shadow_crystal_mult > 1.0 else 0.0
    extra_edward = base_for_buffs * (edward_mult - 1.0) if edward_mult > 1.0 else 0.0
    extra_eclipse = base_for_buffs * (eclipse_mult - 1.0) if eclipse_mult > 1.0 else 0.0
    extra_msi = base_for_buffs * 0.20 if has_shop_item(user_id, "msi_afterburner") else 0.0
    sale_value = base_for_buffs + extra_beta + extra_booster + extra_premium + extra_ns + extra_bs + extra_sc + extra_edward + extra_eclipse + extra_msi

    # Update holdings (subtract) in DB and locally
    update_user_crypto_holdings(user_id, coin, -amount)
    updated_holdings = {
        "RTC": float(holdings.get("RTC", 0.0)),
        "TER": float(holdings.get("TER", 0.0)),
        "CNY": float(holdings.get("CNY", 0.0)),
    }
    updated_holdings[coin] = updated_holdings.get(coin, 0.0) - float(amount)

    # Add money to balance (with boosts)
    current_balance = get_user_balance(user_id)
    new_balance = current_balance + sale_value
    update_user_balance(user_id, new_balance)

    # Create success embed
    embed = discord.Embed(
        title="💰 Sale Successful!",
        description=f"You sold **{amount:.4f} {coin}** for **${sale_value:.2f}**!",
        color=discord.Color.green(),
    )

    # Show boosts if applicable
    bloom_count = get_user_bloom_count(user_id)
    if bloom_count > 0 and extra_from_bloom > 0:
        multiplier_percent = (bloom_multiplier - 1.0) * 100
        embed.add_field(
            name="<:TreeRing:1474244868288282817> Tree Ring Boost",
            value=f"+{multiplier_percent:.1f}% - **+${extra_from_bloom:.2f}**",
            inline=False,
        )
    # Show achievement boost if applicable
    if extra_from_achievement > 0:
        achievement_percent = (achievement_multiplier - 1.0) * 100
        embed.add_field(
            name="🏆 Achievement Boost",
            value=f"+{achievement_percent:.1f}% - **+${extra_from_achievement:.2f}**",
            inline=False,
        )
    if extra_from_daily > 0:
        daily_bonus_percent = (daily_bonus_multiplier - 1.0) * 100
        embed.add_field(
            name="💧 Water Streak Boost",
            value=f"+{daily_bonus_percent:.1f}% - **+${extra_from_daily:.2f}**",
            inline=False,
        )
    # Show rank perma buff if applicable (only if not PINE I) - multiplicative on subtotal
    bloom_rank = get_bloom_rank(user_id)
    if bloom_rank != "PINE I" and extra_from_rank > 0:
        rank_percent = (rank_perma_buff_multiplier - 1.0) * 100
        embed.add_field(
            name="⭐ Rank Boost",
            value=f"+{rank_percent:.1f}% - **+${extra_from_rank:.2f}**",
            inline=False,
        )
    if extra_beta > 0:
        beta_percent = (beta_mult - 1.0) * 100
        embed.add_field(
            name="🧪 Beta Tester!",
            value=f"+{beta_percent:.1f}% - **+${extra_beta:.2f}**",
            inline=False,
        )
    if extra_booster > 0:
        booster_percent = (sb_mult - 1.0) * 100
        embed.add_field(
            name=f"{SERVER_BOOSTER_EMOJI} Server Booster!",
            value=f"+{booster_percent:.1f}% - **+${extra_booster:.2f}**",
            inline=False,
        )
    premium_tier = get_user_premium_tier(user_id)
    if extra_premium > 0:
        mult = PREMIUM_MONEY_MULTIPLIERS.get(premium_tier, 1.0)
        premium_percent = (mult - 1.0) * 100
        embed.add_field(
            name=PREMIUM_DISPLAY.get(premium_tier, "Premium"),
            value=f"+{premium_percent:.1f}% - **+${extra_premium:.2f}**",
            inline=False,
        )
    # Item Boosts (shop / dailyshop items affecting this sale)
    item_boost_sources = []
    if has_shop_item(user_id, "cryptobro_shadow"):
        item_boost_sources.append(("Cryptobro's Shadow", 1))
    # Get counts for Nether Star and Black Shard (permanent + dayboosts)
    if nether_mult > 1.0:
        has_permanent = has_shop_item(user_id, "nether_star")
        dayboost_count = get_dayboost_count(user_id, "nether_star")
        total_count = (1 if has_permanent else 0) + dayboost_count
        if total_count > 0:
            item_boost_sources.append(("Nether Star", total_count))
    if black_shard_mult > 1.0:
        has_permanent = has_shop_item(user_id, "black_shard")
        dayboost_count = get_dayboost_count(user_id, "black_shard")
        total_count = (1 if has_permanent else 0) + dayboost_count
        if total_count > 0:
            item_boost_sources.append(("Black Shard", total_count))

    if item_boost_sources:
        total_item_extra = extra_ns + extra_bs
        value_parts = [f"**+${total_item_extra:.2f}**"] if total_item_extra > 0 else []
        value_parts.append("\n".join(_format_item_boost_source(name, count) for name, count in item_boost_sources))
        embed.add_field(
            name="📦 Item Boost",
            value="\n".join(value_parts),
            inline=False,
        )

    embed.add_field(
        name="Remaining Holdings",
        value=f"RTC: {updated_holdings['RTC']:.4f}\nTER: {updated_holdings['TER']:.4f}\nCNY: {updated_holdings['CNY']:.4f}",
        inline=False,
    )
    embed.add_field(name="New Balance", value=f"${new_balance:.2f}", inline=False)

    return {"embed": embed}


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
        result = await asyncio.to_thread(_sell_critical_path, interaction.user, user_id, coin, amount)

        if result.get("error"):
            await safe_interaction_response(
                interaction,
                interaction.followup.send,
                result["error"],
                ephemeral=bool(result.get("ephemeral", True)),
            )
            return

        embed: discord.Embed = result["embed"]
        await safe_interaction_response(interaction, interaction.followup.send, embed=embed)
    except Exception as e:
        print(f"Error in sell command: {e}")
        await safe_interaction_response(
            interaction,
            interaction.followup.send,
            "❌ An error occurred. Please try again.",
            ephemeral=True,
        )


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
                    f"❌ No shares available! All shares of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker}) have been purchased.",
                    ephemeral=True)
                return
            
            if available_shares < amount:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ Not enough shares available!\n\n"
                    f"Only **{available_shares:,} share(s)** of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker}) are available, "
                    f"but you tried to buy **{amount:,} share(s)**.",
                    ephemeral=True)
                return
            
            # Calculate total cost
            total_cost = amount * current_price
            
            # Check if user has enough balance
            user_balance = get_user_balance(user_id)
            if user_balance < total_cost:
                await safe_interaction_response(interaction, interaction.followup.send,
                    f"❌ You don't have enough balance to buy {amount} share(s) of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker})!\n\n"
                    f"You need **${total_cost:.2f}** but only have **${user_balance:.2f}**.",
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
            ceo_unlocked = False
            if shares_outstanding > 0 and (user_owned_shares / shares_outstanding) > 0.5:
                ceo_unlocked = unlock_hidden_achievement(user_id, "ceo")
            
            # Create success embed
            embed = discord.Embed(
                title="✅ Purchase Successful!",
                description=f"You bought **{amount:,} share(s)** of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker}) at **${current_price:.2f}** each.",
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
                    f"You only have **{current_shares:,} share(s)** of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker}), "
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
                description=f"You sold **{amount:,} share(s)** of {ticker_info['emoji']} **{ticker_info['name']}** ({ticker}) at **${current_price:.2f}** each.",
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
        
        # Send CEO achievement embed (ephemeral = hidden to user only); fallback to DM if needed
        if action == "buy" and ceo_unlocked:
            try:
                await send_hidden_achievement_notification(interaction, "ceo")
            except Exception:
                await send_hidden_achievement_notification_dm(user_id, "ceo")
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
            embed.add_field(name="ℹ️ How to Play", value="Click **Start** to begin your solo adventure!", inline=False)
        else:
            embed.add_field(name="ℹ️ How to Play", value=f"Waiting for {players-1} more players to join! Host can click **Start** when ready!", inline=False)

        await safe_interaction_response(interaction, interaction.followup.send, embed=embed, view=view)
    except Exception as e:
        print(f"Error in russian command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=True)


@bot.tree.command(name="gathemon", description="Challenge a user to a turn-based GathéMon battle for plants!")
@app_commands.describe(
    user="The user you challenge to a GathéMon battle",
    plants="Number of plants to wager (1–10). Both players must have this many."
)
async def gathemon(interaction: discord.Interaction, user: discord.Member, plants: int):
    try:
        if not await safe_defer(interaction, ephemeral=False):
            return
        channel_name = (interaction.channel.name or "").lower().replace(" ", "-")
        if channel_name not in VALID_GATHEMON_CHANNELS:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ GathéMon can only be played in **#gathemon-1** through **#gathemon-5**!", ephemeral=False)
            return
        channel_id = interaction.channel.id
        challenger_id = interaction.user.id
        opponent_id = user.id
        if opponent_id == challenger_id:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't challenge yourself!", ephemeral=False)
            return
        if user.bot:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't challenge a bot!", ephemeral=False)
            return
        if plants < 1 or plants > 10:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Plants must be between 1 and 10!", ephemeral=False)
            return
        channel_has_challenge = any(c.get("channel_id") == channel_id for c in active_gathemon_challenges.values())
        channel_has_battle = any(b.channel_id == channel_id for b in active_gathemon_battles.values())
        if channel_has_challenge or channel_has_battle:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ A GathéMon game is already in progress in this channel. Wait for it to finish!", ephemeral=False)
            return
        if challenger_id in user_active_gathemon or opponent_id in user_active_gathemon:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You or your opponent is already in a GathéMon battle!", ephemeral=False)
            return
        if get_user_bloom_cycle_plants(challenger_id) < plants:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You don't have enough plants for this wager!", ephemeral=False)
            return
        challenge_id = str(uuid.uuid4())[:8]
        active_gathemon_challenges[challenge_id] = {
            "challenger_id": challenger_id,
            "opponent_id": opponent_id,
            "bet": plants,
            "channel_id": channel_id,
        }
        view = GathemonLobbyView(challenge_id, challenger_id, opponent_id, plants, timeout=300)
        embed = discord.Embed(
            title=f"{GATHEMON_POKEBALL_EMOJI} GathéMon Battle!",
            description=f"{interaction.user.mention} challenges {user.mention} to a **GathéMon** battle for {GATHEMON_POKEBALL_EMOJI} **{plants}** plants!\n\n{user.mention}, click **Accept** to start the battle!",
            color=discord.Color.green(),
        )
        embed.set_footer(text="You have 5 minutes to accept!")
        msg = await interaction.followup.send(embed=embed, view=view, ephemeral=False)
        view.message = msg
    except Exception as e:
        print(f"Error in gathemon command: {e}")
        await safe_interaction_response(interaction, interaction.followup.send, "❌ An error occurred. Please try again.", ephemeral=False)


@bot.tree.command(name="mayflower", description="Challenge someone to MAYFLOWER!")
@app_commands.describe(
    user="The user you challenge to Mayflower",
    bet="Amount to bet",
    ships="Number of ships to place (1–5)"
)
async def gathership(interaction: discord.Interaction, user: discord.Member, bet: float, ships: int):
    try:
        if not await safe_defer(interaction):
            return
        host_id = interaction.user.id
        host_name = interaction.user.name
        opponent_id = user.id
        opponent_name = user.name
        channel_id = interaction.channel.id
        channel_name = (interaction.channel.name or "").lower().replace(" ", "-")
        if channel_name not in VALID_MAYFLOWER_CHANNELS:
            await safe_interaction_response(interaction, interaction.followup.send,
                "❌ Mayflower can only be played in **#mayflower-1** through **#mayflower-5**!", ephemeral=True)
            return

        if opponent_id == host_id:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't challenge yourself!", ephemeral=True)
            return
        if user.bot:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You can't challenge a bot!", ephemeral=True)
            return
        if bet <= 0:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Bet must be greater than $0.00!", ephemeral=True)
            return
        if not validate_money_precision(bet):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Bet must be in dollars and cents (max 2 decimal places)!", ephemeral=True)
            return
        if ships < 1 or ships > 5:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ Ships must be between 1 and 5!", ephemeral=True)
            return

        bet = normalize_money(bet)
        host_balance = get_user_balance(host_id)
        if not can_afford_rounded(normalize_money(host_balance), bet):
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You don't have enough balance for that bet!", ephemeral=True)
            return

        # One game per channel (mayflower-1 and mayflower-2 can each have one game running at once)
        if channel_id in channel_gathership and channel_gathership[channel_id] in active_gathership_games:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ There's already a Mayflower game in this channel!", ephemeral=True)
            return
        if host_id in user_active_gathership or opponent_id in user_active_gathership:
            await safe_interaction_response(interaction, interaction.followup.send, "❌ You or your opponent is already in a Mayflower game!", ephemeral=True)
            return

        game_id = str(uuid.uuid4())[:8]
        game = GathershipGame(game_id, host_id, host_name, opponent_id, opponent_name, bet, ships, channel_id)
        active_gathership_games[game_id] = game
        user_active_gathership[host_id] = game_id
        channel_gathership[channel_id] = game_id

        new_balance = normalize_money(host_balance - bet)
        update_user_balance(host_id, new_balance)

        embed = discord.Embed(
            title="⚓ MAYFLOWER ⚓",
            description=f"{interaction.user.mention} is challenging {user.mention} to **MAYFLOWER**!\n\n{user.mention}, click **Join** to accept. Host can **Start** when ready.",
            color=discord.Color.blue()
        )
        embed.add_field(name="💰 Bet", value=f"${bet:.2f} each", inline=True)
        embed.add_field(name="🚢 Ships", value=str(ships), inline=True)
        embed.set_footer(text="You have 5 minutes to accept!")
        view = GathershipLobbyView(game_id, host_id, opponent_id, timeout=300)
        await safe_interaction_response(interaction, interaction.followup.send, content=user.mention, embed=embed, view=view)
    except Exception as e:
        print(f"Error in mayflower command: {e}")
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