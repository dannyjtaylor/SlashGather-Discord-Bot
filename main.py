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
    increment_forage_count,
    get_forage_count,
    increment_gather_stats,
    add_user_item,
    add_ripeness_stat,
    get_all_users_balance,
    get_all_users_total_items,
    get_user_total_items,
    get_user_items,
    get_user_basket_upgrades,
    set_user_basket_upgrade,
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

handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='/', intents=intents)
GATHER_COOLDOWN = 60 #(seconds)
HARVEST_COOLDOWN = 60 * 60 #(an hour)

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

def can_harvest(user_id):
    last_harvest_time = get_user_last_harvest_time(user_id)
    current_time = time.time()
    if last_harvest_time == 0:
        return True, 0
    cooldown_end = last_harvest_time + HARVEST_COOLDOWN
    if current_time >= cooldown_end:
        return True, 0
    else:
        time_left = int(cooldown_end - current_time)
        return False, time_left

def set_harvest_cooldown(user_id):
    update_user_last_harvest_time(user_id, time.time())

    
async def assign_gatherer_role(member: discord.Member, guild: discord.Guild) -> tuple[str | None, str | None]:
    #assign gatherer role to the user
    #gatherer 1 - 0-50 items gathered
    #gatherer 2 - 51-150 items gathered
    #gatherer 3 - 150-299 items gathered
    #gatherer 4 - 300-499 items gathered
    #gatherer 5 - 500+ items gathered

    user_id = member.id
    total_items = get_user_total_items(user_id)
    planter_roles = ["PLANTER I", "PLANTER II", "PLANTER III", "PLANTER IV", "PLANTER V"]

    # Find the user's current planter role
    previous_role_name = next((role.name for role in member.roles if role.name in planter_roles), None)
    
    # Determine the target role based on total items gathered
    target_role_name = None
    if total_items < 50:
        target_role_name = "PLANTER I"
    elif total_items < 150:
        target_role_name = "PLANTER II"
    elif total_items < 299:
        target_role_name = "PLANTER III"
    elif total_items < 499:
        target_role_name = "PLANTER IV"
    else: #500+
        target_role_name = "PLANTER V"

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




def can_gather(user_id):
    last_gather_time = get_user_last_gather_time(user_id)
    current_time = time.time()
    #check if the user is on cooldown, return true/false and how much time left
    #right off the bat if the user is new they have no cooldown
    if last_gather_time == 0:
        return True, 0
    
    # Get shoes upgrade cooldown reduction
    user_upgrades = get_user_basket_upgrades(user_id)
    shoes_tier = user_upgrades["shoes"]
    cooldown_reduction = 0
    if shoes_tier > 0:
        cooldown_reduction = SHOES_UPGRADES[shoes_tier - 1]["reduction"]
    
    # Calculate effective cooldown (base cooldown minus reduction, minimum 0)
    effective_cooldown = max(0, GATHER_COOLDOWN - cooldown_reduction)
    cooldown_end = last_gather_time + effective_cooldown
    
    if current_time >= cooldown_end:
        return True, 0
    else:
        time_left = int(cooldown_end - current_time)
        return False, time_left

def set_cooldown(user_id):
    # set cooldown for user, p self explanatory
    update_user_last_gather_time(user_id, time.time())

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


    {"category": "Fruit","name": "Strawberry 🍓", "base_value": 8},
    {"category": "Fruit","name": "Blueberry 🫐", "base_value": 10},
    {"category": "Fruit","name": "Raspberry", "base_value": 2},
    {"category": "Fruit","name": "Cherry 🍒", "base_value": 1},
    {"category": "Fruit","name": "Apple 🍎", "base_value": 9},
    {"category": "Fruit","name": "Pear 🍐", "base_value": 14},
    {"category": "Fruit","name": "Orange 🍊", "base_value": 6},
    {"category": "Fruit","name": "Grape 🍇", "base_value": 7},

    {"category": "Vegetable","name": "Carrot 🥕", "base_value": 2},
    {"category": "Vegetable","name": "Potato 🥔", "base_value": 1},
    {"category": "Vegetable","name": "Onion 🧅", "base_value": 3},
    {"category": "Vegetable","name": "Garlic 🧄", "base_value": 7},
    {"category": "Vegetable","name": "Tomato 🍅", "base_value": 4},
    {"category": "Vegetable","name": "Lettuce 🥬", "base_value": 3},
    {"category": "Vegetable","name": "Cabbage 🥬", "base_value": 10},
    {"category": "Vegetable","name": "Broccoli 🥦", "base_value": 5},
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
    "Strawberry 🍓": "Sweet and juicy, nature's candy!",
    "Blueberry 🫐": "Tiny but packed with flavor!",
    "Raspberry": "Tart and tangy, perfect for desserts!",
    "Cherry 🍒": "Small and sweet, a summer treat!",
    "Apple 🍎": "One a day keeps the doctor away!",
    "Pear 🍐": "Sweet and crisp!",
    "Orange 🍊": "Yeah, we're from Florida. Hey Apple!",
    "Grape 🍇": "Not statuatory!",
    "Carrot 🥕": "Good for your eyes!",
    "Potato 🥔": "An Irish delight!",
    "Onion 🧅": "Makes you cry...!",
    "Garlic 🧄": "Wards off vampires!",
    "Tomato 🍅": "Technically a fruit!",
    "Lettuce 🥬": "THIS is what the Titanic hit?",
    "Cabbage 🥬": "Round and leafy, great for coleslaw!",
    "Broccoli 🥦": "A tiny tree that's super healthy!",
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
        bullet_multiplier = 1.3 ** self.initial_bullets
        round_multiplier = 1.3 ** rounds_survived
        return bullet_multiplier * round_multiplier

    #if a player loses, get them out and add their money to the pot
    def eliminate(self, player_id):
        if player_id in self.players:
            self.players[player_id]["alive"] = False
            self.pot += self.players[player_id]["current_stake"]
        #print the player out
        # print(f"{self.players[player_id]['name']} has been eliminated!")

    #when playersl live, increase their number of rounds
    def player_survived_round(self, player_id):
        if (player_id in self.players and self.players[player_id]["alive"]):
            self.players[player_id]["rounds_survived"] += 1
            # update stack w/ new multiplier
            multiplier = self.calculate_total_multiplier(self.players[player_id]["rounds_survived"])
            self.players[player_id]["current_stake"] = self.bet_amount * multiplier






#start rusian roulette
async def start_roulette_game(channel, game_id):
    if game_id in active_roulette_games:
        game = active_roulette_games[game_id]
        game.game_started = True
        #start on first round
        game.round_number = 1

        await asyncio.sleep(2)

        #start message
        total_pot = game.bet_amount * len(game.players)
        embed = discord.Embed(
            title = "🎲 RUSSIAN ROULETTE 🎲",
            description = f"**{game.host_name}**'s game has started!\n*The cylinder spins.. click.. click.. click.. click..*",
            color = discord.Color.dark_red()
        )
        embed.add_field(name="🔫 Bullets Loaded", value=f"{game.bullets}/6", inline=True)
        embed.add_field(name="💰 Total Pot", value=f"${game.bet_amount:.2f}", inline=True)
        embed.add_field(name="🎮 Players", value=f"{len(game.players)}/{game.max_players}", inline=True)
        await channel.send(embed=embed)
        await asyncio.sleep(2)

        #play round!!
        await play_roulette_round(channel, game_id)

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
        description="*The cylinder re-spins for this turn...*\n\n🔄 🔄 🔄\n\n**The chamber is re-spun every time it's someone's turn!**",
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
        user_id = interaction.user.id
        if self.game_id not in active_roulette_games:
            await interaction.response.send_message("❌ Game no longer exists!", ephemeral=True)
            return
            
        game = active_roulette_games[self.game_id]
        
        # Check if user is already in this game
        if user_id in game.players:
            await interaction.response.send_message("❌ You're already in this game!", ephemeral=True)
            return
            
        # Check if user is in another game
        if user_id in user_active_games:
            await interaction.response.send_message("❌ You're already in another game!", ephemeral=True)
            return
            
        # Check if game is full
        if len(game.players) >= game.max_players:
            await interaction.response.send_message("❌ Game is full!", ephemeral=True)
            return
            
        # Check if game already started
        if game.game_started:
            await interaction.response.send_message("❌ Game already started!", ephemeral=True)
            return
            
        # Check user balance
        user_balance = get_user_balance(user_id)
        if user_balance < game.bet_amount:
            await interaction.response.send_message("❌ You don't have enough balance to join!", ephemeral=True)
            return
            
        # Join the game
        game.add_player(user_id, interaction.user.name)
        user_active_games[user_id] = self.game_id
        
        # Deduct bet
        update_user_balance(user_id, user_balance - game.bet_amount)
        
        # Update the embed
        embed = interaction.message.embeds[0]
        embed.description = f"**{game.host_name}** is playing with **{len(game.players)}/{game.max_players}** players!\n\n*How long can you survive?*"
        
        # Update the view (disable join button if full)
        if len(game.players) >= game.max_players:
            button.disabled = True
            button.label = "Game Full"
        
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.blurple, emoji="🚀")
    async def start_game(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only host can start
        if interaction.user.id != self.host_id:
            await interaction.response.send_message("❌ Only the game host can start the game!", ephemeral=True)
            return
            
        if self.game_id not in active_roulette_games:
            await interaction.response.send_message("❌ Game no longer exists!", ephemeral=True)
            return
            
        game = active_roulette_games[self.game_id]
        
        if game.game_started:
            await interaction.response.send_message("❌ Game already started!", ephemeral=True)
            return
            
        # Start the game
        game.game_started = True
        game.pot = game.bet_amount * len(game.players)
        
        await interaction.response.edit_message(content="🎮 **Game Started!**", view=None)
        
        # Start the actual game
        await start_roulette_game(interaction.channel, self.game_id)
    
    async def on_timeout(self):
        # Auto-start the game after 5 minutes if host hasn't started it
        if self.game_id in active_roulette_games:
            game = active_roulette_games[self.game_id]
            if not game.game_started and len(game.players) >= 1:  # At least host is in game
                game.game_started = True
                game.pot = game.bet_amount * len(game.players)
                
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



# roulette continue view
class RouletteContinueView(discord.ui.View):
    def __init__(self, game_id, timeout=300, allow_cashout=True):
        super().__init__(timeout=timeout)
        self.game_id = game_id
        self.allow_cashout = allow_cashout
    
    @discord.ui.button(label="Pull Trigger", style=discord.ButtonStyle.danger, emoji="🔫")
    async def continue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.game_id not in active_roulette_games:
            await interaction.response.send_message("❌ Game no longer exists!", ephemeral=True)
            return
        
        game = active_roulette_games[self.game_id]
        current_player_id = game.get_current_player()
        
        if interaction.user.id != current_player_id:
            await interaction.response.send_message("❌ It's not your turn!", ephemeral=True)
            return
        
        await interaction.response.defer()
        await interaction.message.delete()
        
        # Continue the game
        await play_roulette_round(interaction.channel, self.game_id)
    
    @discord.ui.button(label="Cash Out", style=discord.ButtonStyle.secondary, emoji="💰")
    async def cashout_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.allow_cashout:
            await interaction.response.send_message("❌ Cash out is not available on the very first turn!", ephemeral=True)
            return
        
        if self.game_id not in active_roulette_games:
            await interaction.response.send_message("❌ Game no longer exists!", ephemeral=True)
            return
        
        game = active_roulette_games[self.game_id]
        current_player_id = game.get_current_player()
        
        if interaction.user.id != current_player_id:
            await interaction.response.send_message("❌ It's not your turn!", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        # Cash out - player gets their stake back
        player = game.players[current_player_id]
        winnings = player['current_stake']
        
        # Add winnings to player balance
        current_balance = get_user_balance(current_player_id)
        update_user_balance(current_player_id, current_balance + winnings)
        
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
        embed.add_field(name="💸 Profit", value=f"${winnings - game.bet_amount:.2f}", inline=True)
        embed.add_field(name="📈 Multiplier Achieved", value=f"{game.calculate_total_multiplier(player['rounds_survived']):.2f}x", inline=True)
        embed.add_field(name="🎯 Rounds Survived", value=f"{player['rounds_survived']}", inline=True)
        
        await interaction.message.edit(embed=embed, view=None)
        
        # Check if game ends
        alive_count = len(game.get_alive_players())
        
        if alive_count == 0 or (alive_count == 1 and game.max_players > 1):
            await asyncio.sleep(2)
            await end_roulette_game(interaction.channel, self.game_id)
        else:
            game.next_turn()
            await asyncio.sleep(2)
            await play_roulette_round(interaction.channel, self.game_id)
    
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
        winnings = player['current_stake']
        
        # Add winnings to player balance
        current_balance = get_user_balance(current_player_id)
        update_user_balance(current_player_id, current_balance + winnings)
        
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
        embed.add_field(name="💸 Profit", value=f"${winnings - game.bet_amount:.2f}", inline=True)
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
    await interaction.response.defer(ephemeral=False)
    user_id = interaction.user.id
    current_balance = get_user_balance(user_id)
    if current_balance < bet:
        await interaction.followup.send(f"You do not have enough balance to bet **${bet:.2f}**, {interaction.user.name}.", ephemeral=False)
        return
    
    # Deduct bet first
    update_user_balance(user_id, current_balance - bet)
    
    # Flip the coin - randomly choose heads or tails (lowercase)
    coin_result = random.choice(["heads", "tails"])
    
    # Check if they won (their choice matches the result, both lowercase)
    won = choice.lower() == coin_result
    
    # Calculate new balance
    if won:
        # They win - get double their bet back (bet was already deducted, so add 2*bet)
        new_balance = current_balance - bet + (bet * 2)
        update_user_balance(user_id, new_balance)
        message = f"You placed **${bet:.2f}** on **{choice}**!\nThe coin landed on **{coin_result}**! You doubled your bet!!\nYour new balance is **${new_balance:.2f}**."
    else:
        # They lose - bet was already deducted
        new_balance = current_balance - bet
        # Get the opposite choice for display (lowercase)
        opposite = "tails" if choice.lower() == "heads" else "heads"
        message = f"You placed **${bet:.2f}** on **{choice}**!\nOuch {interaction.user.name}, the coin landed on **{opposite}**. You lost **${bet:.2f}**.\nYour new balance is **${new_balance:.2f}**."
    
    await interaction.followup.send(message, ephemeral=False)


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
<<<<<<< HEAD
            name="🌳 running /gather on V0.1.0 :3"
=======
            name="🌳 running /gather on V0.0.2 :3"
>>>>>>> 3fd3b851207020725fa328265e33a45fb09bbf1e
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
    #use defer for custom message
    await interaction.response.defer(ephemeral=False)


    #check if the user is on cooldown (default 1 min), if so let them know how much time they have left
    user_id = interaction.user.id
    can_user_gather, time_left = can_gather(user_id)
    if not can_user_gather:
        #then user is on cooldown
        await interaction.followup.send(
            f"You must wait {time_left} seconds before gathering again, {interaction.user.name}.", ephemeral=True
        )
        return
    set_cooldown(user_id)

    #choose a random item, take its value, name, and ripeness
    item = random.choice(GATHERABLE_ITEMS)
    name = item["name"]  
    if item["category"] == "Fruit":
        ripeness_list = LEVEL_OF_RIPENESS_FRUITS
    elif item["category"] == "Vegetable":
        ripeness_list = LEVEL_OF_RIPENESS_VEGETABLES
    elif item["category"] == "Flower":
        ripeness_list = LEVEL_OF_RIPENESS_FLOWERS
    else:
        ripeness_list = "Unknown"

    if ripeness_list:
        #use wiehgted random selection for the chance
        weights = [item["chance"] for item in ripeness_list]
        ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
        final_value = item["base_value"] * ripeness["multiplier"]
    else:
        final_value = item["base_value"]

    # Get user upgrades
    user_upgrades = get_user_basket_upgrades(user_id)
    
    # Apply soil upgrade GMO chance boost
    soil_tier = user_upgrades["soil"]
    base_gmo_chance = 0.05
    soil_gmo_boost = SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0
    gmo_chance = base_gmo_chance + soil_gmo_boost
    
    # see if the gathered item is a GMO
    is_gmo = random.choices([True, False], weights=[gmo_chance, 1-gmo_chance], k=1)[0]
    if is_gmo:
        final_value *= 2
    
    # Apply basket upgrade money multiplier
    basket_tier = user_upgrades["basket"]
    if basket_tier > 0:
        basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"]
        final_value *= basket_multiplier

    #add the value to the balance for the user
    current_balance = get_user_balance(user_id)
    new_balance = current_balance + final_value
    #save to database
    update_user_balance(user_id, new_balance)

    add_user_item(user_id, name)
    add_ripeness_stat(user_id, ripeness["name"])
    increment_gather_stats(user_id, item["category"], name)

    # assign role and check for rank-up
    old_role = None
    new_role = None
    try:
        old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
    except Exception as e:
        print(f"Error assigning gatherer role to user {user_id}: {e}")

    # Send rank-up notification if player advanced
    if new_role:
        rankup_embed = discord.Embed(
            title="🌱 Rank Up!",
            description=f"{interaction.user.mention} advanced from **{old_role or 'New Recruit'}** to **{new_role}**!",
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=rankup_embed)

    #create discord embed
    embed = discord.Embed(
        title= "You Gathered!",
        description = f"You foraged for a(n) **{name}**!",
        color = discord.Color.green()
    )

    embed.add_field(name="Value", value=f"**${final_value:.2f}**", inline=True)
    embed.add_field(name="Ripeness", value=f"{ripeness['name']}", inline=True)
    embed.add_field(name="GMO?", value=f"{'Yes ✨' if is_gmo else 'No'}", inline=False)
    # add a line to show [username] in [month]
    embed.add_field(name="~", value=f"{interaction.user.name} in {MONTHS[random.randint(0, 11)]}", inline=False)
    embed.add_field(name="new balance: ", value=f"**${new_balance:.2f}**", inline=False)
    
    # Check for chain chance (gloves upgrade)
    gloves_tier = user_upgrades["gloves"]
    chain_triggered = False
    if gloves_tier > 0:
        chain_chance = GLOVES_UPGRADES[gloves_tier - 1]["chain_chance"]
        chain_triggered = random.random() < chain_chance
        if chain_triggered:
            # Reset cooldown by setting last_gather_time to 0 (allows immediate next gather)
            update_user_last_gather_time(user_id, 0)
            embed.add_field(name="⚡ CHAIN!", value="Your cooldown has been reset! Gather again!", inline=False)
    
    await interaction.followup.send(embed=embed) 

#/harvest command, basically /castnet
@bot.tree.command(name="harvest", description="Harvest a bunch of plants at once!")
async def harvest(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    user_id = interaction.user.id
    can_user_harvest, time_left = can_harvest(user_id)
    if not can_user_harvest:
        minutes_left = time_left // 60
        seconds_left = time_left % 60   
        await interaction.followup.send(
            f"You must wait {minutes_left} minutes and {seconds_left} seconds before harvesting again, {interaction.user.name}.", ephemeral=True
        )
        return
    set_harvest_cooldown(user_id)
    
    # Get user upgrades (same for all 10 gathers)
    user_upgrades = get_user_basket_upgrades(user_id)
    basket_tier = user_upgrades["basket"]
    soil_tier = user_upgrades["soil"]
    
    # Get upgrade multipliers
    basket_multiplier = BASKET_UPGRADES[basket_tier - 1]["multiplier"] if basket_tier > 0 else 1.0
    base_gmo_chance = 0.05
    soil_gmo_boost = SOIL_UPGRADES[soil_tier - 1]["gmo_boost"] if soil_tier > 0 else 0
    gmo_chance = base_gmo_chance + soil_gmo_boost
    
    #/gather 10 times
    gathered_items = []
    total_value = 0.0
    current_balance = get_user_balance(user_id)

    for i in range(10):
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

        #calcualte value w/ ripeness
        if ripeness_list:
            weights = [r["chance"] for r in ripeness_list]
            ripeness = random.choices(ripeness_list, weights=weights, k=1)[0]
            final_value = item["base_value"] * ripeness["multiplier"]
        else:
            final_value = item["base_value"]
            ripeness = {"name": "Normal"}

        # Apply soil upgrade GMO chance boost and check for GMO
        is_gmo = random.choices([True, False], weights=[gmo_chance, 1-gmo_chance], k=1)[0]
        if is_gmo:
            final_value *= 1.5
        
        # Apply basket upgrade money multiplier
        final_value *= basket_multiplier

        #update new balance
        current_balance += final_value
        total_value += final_value

        #store items and stats
        add_user_item(user_id, name)
        add_ripeness_stat(user_id, ripeness["name"])
        increment_forage_count(user_id)
        increment_gather_stats(user_id, item["category"], name)

        #track what was gathered
        gathered_items.append({"name": name, "value": final_value, "ripeness": ripeness["name"], "is_gmo": is_gmo})

    #save_final_balance
    update_user_balance(user_id, current_balance)

    #assign role in case they hit a new gatherer level in a harvest
    old_role = None
    new_role = None
    try:
        old_role, new_role = await assign_gatherer_role(interaction.user, interaction.guild)
    except Exception as e:
        print(f"Error assigning gatherer role to user {user_id}: {e}")

    # Send rank-up notification if player advanced
    if new_role:
        rankup_embed = discord.Embed(
            title="🌾 Rank Up!",
            description=f"{interaction.user.mention} advanced from **{old_role or 'New Recruit'}** to **{new_role}**!",
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=rankup_embed)

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
    embed.add_field(name="~", value=f"{interaction.user.name} in {MONTHS[random.randint(0, 11)]}", inline=False)

    await interaction.followup.send(embed=embed)
    #end harvest


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
    await interaction.response.defer(ephemeral=False)
    
    user_id = interaction.user.id
    user_balance = get_user_balance(user_id)
    total_items = get_user_total_items(user_id)
    
    # Calculate items needed for next rankup
    # PLANTER I: 0-49 (need 50 for PLANTER II)
    # PLANTER II: 50-149 (need 150 for PLANTER III)
    # PLANTER III: 150-298 (need 299 for PLANTER IV)
    # PLANTER IV: 300-498 (need 499 for PLANTER V)
    # PLANTER V: 500+ (max rank)
    items_needed = None
    next_rank = None
    
    if total_items < 50:
        items_needed = 50 - total_items
        next_rank = "PLANTER II"
    elif total_items < 150:
        items_needed = 150 - total_items
        next_rank = "PLANTER III"
    elif total_items < 299:
        items_needed = 299 - total_items
        next_rank = "PLANTER IV"
    elif total_items < 499:
        items_needed = 499 - total_items
        next_rank = "PLANTER V"
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
    
    if items_needed == 0:
        embed.add_field(name="🏆 Rank Status", value=f"**{next_rank}** - You've reached the maximum rank!", inline=False)
    else:
        embed.add_field(name="📈 Next Rank", value=f"**{items_needed}** more plants until **{next_rank}**", inline=False)
    
    await interaction.followup.send(embed=embed)


# almanac command
@bot.tree.command(name="almanac", description="View your collection of your gathered items!")
async def almanac(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    
    user_id = interaction.user.id
    user_items = get_user_items(user_id)
    
    if not user_items:
        embed = discord.Embed(
            title=f"{interaction.user.name}'s Almanac",
            description="Your collection is empty! Start gathering items with `/gather` or `/harvest` to fill it up!",
            color=discord.Color.orange()
        )
        await interaction.followup.send(embed=embed)
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
    
    await interaction.followup.send(embed=embed)


<<<<<<< HEAD
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
        await self.handle_purchase(interaction, "basket", BASKET_UPGRADES, "Basket")
    
    @discord.ui.button(label="👟 Buy Shoes", style=discord.ButtonStyle.primary, row=0)
    async def buy_shoes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_purchase(interaction, "shoes", SHOES_UPGRADES, "Shoes")
    
    @discord.ui.button(label="🧤 Buy Gloves", style=discord.ButtonStyle.primary, row=1)
    async def buy_gloves(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_purchase(interaction, "gloves", GLOVES_UPGRADES, "Gloves")
    
    @discord.ui.button(label="🌱 Buy Soil", style=discord.ButtonStyle.primary, row=1)
    async def buy_soil(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.handle_purchase(interaction, "soil", SOIL_UPGRADES, "Soil")
    
    @discord.ui.button(label="🔄 Refresh", style=discord.ButtonStyle.secondary, row=2)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This is not your gear shop!", ephemeral=True)
            return
        
        embed = self.create_embed()
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def handle_purchase(self, interaction: discord.Interaction, upgrade_type: str, upgrade_list: list, upgrade_name: str):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(f"❌ This is not your gear shop!", ephemeral=True)
            return
        
        upgrades = get_user_basket_upgrades(self.user_id)
        current_tier = upgrades[upgrade_type]
        
        if current_tier >= 10:
            await interaction.response.send_message(f"❌ You already have the maximum {upgrade_name} upgrade!", ephemeral=True)
            return
        
        cost = UPGRADE_PRICES[current_tier]
        balance = get_user_balance(self.user_id)
        
        if balance < cost:
            await interaction.response.send_message(
                f"❌ You don't have enough money! You need **${cost:,.2f}** but only have **${balance:,.2f}**.", 
                ephemeral=True
            )
            return
        
        # Deduct money and upgrade
        new_balance = balance - cost
        update_user_balance(self.user_id, new_balance)
        set_user_basket_upgrade(self.user_id, upgrade_type, current_tier + 1)
        
        next_upgrade = upgrade_list[current_tier]
        
        # Send quick confirmation and update the main embed
        await interaction.response.send_message(f"✅ Purchased **{next_upgrade['name']}**! Updated your shop below.", ephemeral=True)
        
        embed = self.create_embed()
        await interaction.message.edit(embed=embed, view=self)


# Gear command
@bot.tree.command(name="gear", description="Upgrade your gathering equipment!")
async def gear(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=False)
    
    user_id = interaction.user.id
    view = BasketUpgradeView(user_id, interaction.guild)
    embed = view.create_embed()
    
    await interaction.followup.send(embed=embed, view=view)


# # Temporary admin command for dev - give yourself money
# @bot.tree.command(name="danny", description="Dev command - Give yourself money")
# async def danny(interaction: discord.Interaction):
#     await interaction.response.defer(ephemeral=True)
    
#     user_id = interaction.user.id
#     current_balance = get_user_balance(user_id)
#     new_balance = current_balance + 1_000_000_000  # 1 billion
#     update_user_balance(user_id, new_balance)
    
#     embed = discord.Embed(
#         title="💰 Money Added!",
#         description=f"You've been given **$1,000,000,000**!",
#         color=discord.Color.gold()
#     )
#     embed.add_field(name="💰 New Balance", value=f"${new_balance:,.2f}", inline=False)
    
#     await interaction.followup.send(embed=embed, ephemeral=True)
=======
# pay command
@bot.tree.command(name="pay", description="Pay money to another user!")
async def pay(interaction: discord.Interaction, amount: float, user: discord.Member):
    await interaction.response.defer(ephemeral=False)
    
    sender_id = interaction.user.id
    recipient_id = user.id
    
    # Can't pay yourself
    if sender_id == recipient_id:
        await interaction.followup.send("❌ You can't pay yourself!", ephemeral=True)
        return
    
    # Check sender balance
    sender_balance = get_user_balance(sender_id)
    if sender_balance < amount:
        await interaction.followup.send(f"❌ You don't have enough balance!", ephemeral=True)
        return
    
    # Get recipient balance
    recipient_balance = get_user_balance(recipient_id)
    
    # Transfer money
    update_user_balance(sender_id, sender_balance - amount)
    update_user_balance(recipient_id, recipient_balance + amount)
    
    # Send confirmation message
    embed = discord.Embed(
        title="💰 Payment Successful!",
        description=f"**{interaction.user.name}** paid **{user.name}** **${amount:.2f}**!",
        color=discord.Color.green()
    )
    await interaction.followup.send(embed=embed)
>>>>>>> 3fd3b851207020725fa328265e33a45fb09bbf1e


# Leaderboard pagination view
class LeaderboardView(discord.ui.View):
    def __init__(self, leaderboard_data: list[tuple[int, float | int]], leaderboard_type: str, guild: discord.Guild, timeout=300):
        super().__init__(timeout=timeout)
        self.leaderboard_data = leaderboard_data
        self.leaderboard_type = leaderboard_type
        self.guild = guild
        self.current_page = 0
        self.items_per_page = 10
        self.total_pages = (len(leaderboard_data) + self.items_per_page - 1) // self.items_per_page
        
    def get_page_data(self, page: int) -> list[tuple[int, float | int]]:
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
        else:  # money
            title = "**💰 MONEY**"
            description = ""
            value_name = "Balance"
        
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
            else:  # money
                # Top 3 get money bag, bottom 7 get cash emoji
                if rank <= 3:
                    emoji = "💰"
                else:
                    emoji = "💵"
                leaderboard_text += f"{emoji} **{rank}.** {username}: **${value:.2f}**\n"
        
        if not leaderboard_text:
            leaderboard_text = "No data available"
        
        embed.add_field(name="Rankings", value=leaderboard_text, inline=False)
        embed.set_footer(text=f"Page {page + 1} of {self.total_pages} | Total: {len(self.leaderboard_data)} users")
        
        return embed
    
    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.secondary, disabled=True)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            embed = self.create_embed(self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()
    
    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.update_buttons()
            embed = self.create_embed(self.current_page)
            await interaction.response.edit_message(embed=embed, view=self)
        else:
            await interaction.response.defer()
    
    def update_buttons(self):
        """Update button states based on current page."""
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page >= self.total_pages - 1


# Store leaderboard message IDs per guild and type
leaderboard_messages = {}  # {guild_id: {"plants": message_id, "money": message_id}}

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
    else:  # money
        all_data = get_all_users_balance()
    
    # Filter to only include users in the guild
    leaderboard_data = [(user_id, value) for user_id, value in all_data if user_id in guild_member_ids]
    
    if not leaderboard_data:
        return  # No data available
    
    # Create embed (first page only, no pagination for auto-updates)
    if leaderboard_type == "plants":
        title = "**🌱 PLANTS**"
        description = ""
    else:  # money
        title = "**💰 MONEY**"
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
        else:  # money
            # Top 3 get money bag, bottom 7 get cash emoji
            if rank <= 3:
                emoji = "💰"
            else:
                emoji = "💵"
            leaderboard_text += f"{emoji} **{rank}.** {username}: **${value:.2f}**\n"
    
    if not leaderboard_text:
        leaderboard_text = "No data available"
    
    embed.add_field(name="Top 10 Rankings", value=leaderboard_text, inline=False)
    embed.set_footer(text=f"Updates every minute | Total: {len(leaderboard_data)} users")
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
                await message.edit(embed=embed)
                return
            except discord.NotFound:
                # Message was deleted, search for existing one
                message_id = None
            except discord.HTTPException as e:
                # Other error (permissions, etc.), search for existing one
                print(f"Error editing {leaderboard_type} leaderboard in {guild.name}: {e}")
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
                           (leaderboard_type == "money" and "💰 MONEY" in embed_title):
                            # Found existing message, update it
                            message_id = message.id
                            leaderboard_messages[guild_id][leaderboard_type] = message_id
                            await message.edit(embed=embed)
                            return
            except Exception as e:
                print(f"Error searching for existing leaderboard message: {e}")
        
        # Create new message only if we couldn't find or edit existing one
        message = await leaderboard_channel.send(embed=embed)
        leaderboard_messages[guild_id][leaderboard_type] = message.id
    except Exception as e:
        print(f"Error updating {leaderboard_type} leaderboard in {guild.name}: {e}")

async def update_all_leaderboards():
    """Background task to update all leaderboards every minute."""
    await bot.wait_until_ready()
    
    # Wait a bit for guilds to fully load
    await asyncio.sleep(5)
    
    while not bot.is_closed():
        try:
            # Update leaderboards for all guilds the bot is in
            for guild in bot.guilds:
                await update_leaderboard_message(guild, "plants")
                await asyncio.sleep(1)  # Small delay between updates
                await update_leaderboard_message(guild, "money")
                await asyncio.sleep(1)
        except Exception as e:
            print(f"Error in leaderboard update task: {e}")
        
        # Wait 60 seconds before next update
        await asyncio.sleep(60)


# gambling commands
# russian roulette

@bot.tree.command(name="russian", description="Play Russian Roulette!")
async def russian(
    interaction: discord.Interaction,
    bullets: int,
    bet: float,
    players: int = 1 # default 1
):
    await interaction.response.defer()
    #start russian roullette
    user_id = interaction.user.id
    user_name = interaction.user.name
    channel_id = interaction.channel.id

    # make sure game is actually valid and the person can do it

    # first, check if game already in channel
    if channel_id in active_roulette_channel_games:
        existing_game_id = active_roulette_channel_games[channel_id]
        if existing_game_id in active_roulette_games:
            await interaction.followup.send(f"There's already a Russian Roulette game running in this channel!", ephemeral=True)
            return
        else:
            #clean up orphaned ref
            del active_roulette_channel_games[channel_id]
    

    # make sure user is not already in a game
    if user_id in user_active_games:
        existing_game_id = user_active_games[user_id]
        if existing_game_id in active_roulette_games:
            await interaction.followup.send(f"You're already in a game! Finish it or cash out first!", ephemeral=True)
            return
        else:
            #clean up orphaned ref
            del user_active_games[user_id]
        return


    if bullets < 1 or bullets > 5:
        await interaction.followup.send(f"Invalid number of bullets", ephemeral=True)
        return

    if players < 1 or players > 6:
        await interaction.followup.send(f"Invalid number of players", ephemeral=True)
        return
    if bet < 0:
        await interaction.followup.send(f"Invalid bet", ephemeral=True)
        return

    # get user balance
    user_balance = get_user_balance(user_id)
    if bet > user_balance:
        await interaction.followup.send(f"You don't have enough balance to play Russian Roulette.", ephemeral=True)
        return

    #create unique game ID
    game_id = str(uuid.uuid4())[:8]

    #create new game
    game = RouletteGame(game_id, user_id, user_name, bullets, bet, players)
    active_roulette_games[game_id] = game
    user_active_games[user_id] = game_id
    active_roulette_channel_games[channel_id] = game_id

    # deduct bet from host
    update_user_balance(user_id, user_balance - bet)
    # increase bullet multiplier
    bullet_multiplier = 1.3 ** bullets

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
    value="Each round you survive increases your winnings by **1.3x**!\nCash out anytime to keep your winnings, or keep playing for more!",
    inline=False
)
    
    #create join button
    view = RouletteJoinView(game_id, user_id,timeout = 300)

    if players == 1:
        embed.add_field(name="ℹ️ How to Play", value="Click **Start Game** to begin your solo adventure!", inline=False)
    else:
        embed.add_field(name="ℹ️ How to Play", value=f"Waiting for {players-1} more players to join! Host can click **Start Game** when ready!", inline=False)

    await interaction.followup.send(embed=embed, view=view)








#END

# Cloud Run compatibility - add simple HTTP server
import threading
import time
import os
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

def start_http_server():
    """Start a simple HTTP server for Cloud Run health checks"""
    try:
        port = int(os.environ.get('PORT', 8080))
        server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
        print(f"HTTP server listening on port {port}")
        server.serve_forever()
    except Exception as e:
        print(f"HTTP server error: {e}")

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
        
        bot.run(token, log_handler=handler, log_level=logging.DEBUG)
    except Exception as e:
        print(f"Discord bot error: {e}")
        # Keep the process alive even if bot fails
        while True:
            time.sleep(60)

if __name__ == "__main__":
    print("Starting SlashGather Discord Bot...")
    if is_production:
        http_thread = threading.Thread(target=start_http_server, daemon=True)
        http_thread.start()
    else:
        print("Health check server disabled in development mode")

    start_discord_bot()