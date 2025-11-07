from calendar import c
from tarfile import LENGTH_NAME
import discord
from discord.ext import commands
import logging
from dotenv import load_dotenv
import os
import random
import time
import asyncio
import uuid

# Load environment variables FIRST
load_dotenv()

# Database helpers (MongoDB only)
from database import (
    init_database,
    get_user_balance,
    update_user_balance,
    get_user_last_gather_time,
    update_user_last_gather_time,
    increment_forage_count,
    add_user_item,
    add_ripeness_stat,
)

environment = os.getenv('ENVIRONMENT', 'development')

try:
    init_database()
    print("Connected to MongoDB successfully")
except Exception as error:
    logging.exception("Failed to initialise MongoDB connection: %s", error)
    raise

# Load the correct token based on environment
if environment == 'production':
    token = os.getenv('DISCORD_TOKEN')
else:
    token = os.getenv('DISCORD_DEV_TOKEN')

handler = logging.FileHandler(filename='discord.log', encoding='utf-8', mode='w')
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='/', intents=intents)
GATHER_COOLDOWN = 2 #(seconds)

def can_gather(user_id):
    last_gather_time = get_user_last_gather_time(user_id)
    current_time = time.time()
    #check if the user is on cooldown, return true/false and how much time left
    #right off the bat if the user is new they have no cooldown
    if last_gather_time == 0:
        return True, 0
    cooldown_end = last_gather_time + GATHER_COOLDOWN
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

#level of ripeness - FRUITS
LEVEL_OF_RIPENESS_FRUITS = [
    {"name": "Budding", "multiplier": 0.9, "chance": 25},
    {"name": "Flowering", "multiplier": 1.2, "chance": 10},
    {"name": "Raw", "multiplier": 1.3, "chance": 15},
    {"name": "Slightly Ripe", "multiplier": 1.5, "chance": 25},
    {"name": "Perfectly Ripe", "multiplier": 2.5, "chance": 20},
    {"name": "Overripe", "multiplier": 1.6, "chance": 10},  
    {"name": "Spoiled", "multiplier": 0.9, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 0.000001},
]

#level of ripeness - VEGETABLES
LEVEL_OF_RIPENESS_VEGETABLES = [
    {"name": "Sproutling", "multiplier": 1, "chance": 25},
    {"name": "Raw", "multiplier": 1.3, "chance": 15},
    {"name": "Slightly Ripe", "multiplier": 1.5, "chance": 25},
    {"name": "Perfectly Ripe", "multiplier": 2.5, "chance": 20},
    {"name": "Overripe", "multiplier": 1.6, "chance": 10},
    {"name": "Spoiled", "multiplier": 0.9, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 0.000001},
]

#level of ripeness - FLOWERS
LEVEL_OF_RIPENESS_FLOWERS = [
    {"name": "Budded", "multiplier": 0.75, "chance": 30},
    {"name": "Blooming", "multiplier": 1, "chance": 45},
    {"name": "Full Bloom", "multiplier": 1.5, "chance": 20},
    {"name": "Wilted", "multiplier": 0.6, "chance": 4.99999},
    {"name": "One in a Million", "multiplier": 50, "chance": 0.000001},
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
        # # print the player out
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
        description="*Spinning the chamber...*\n\n🔄 🔄 🔄",
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
    
        #continue to next player
        game.next_turn()
        await asyncio.sleep(2)
        await play_roulette_round(channel, game_id)
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
    
    # Determine total winnings if they cash out now
    if len(alive_players) == 1:
        # Last player standing gets pot + their stake
        potential_winnings = game.pot + next_player['current_stake']
    else:
        # Multiplayer - just show their stake
        potential_winnings = next_player['current_stake']
    
    # Create continue/cashout view
    view = RouletteContinueView(game_id)
    
    embed = discord.Embed(
        title="⚠️ YOUR TURN ⚠️",
        description=f"**{next_player['name']}**, it's your turn!\n\nClick **Pull Trigger** to continue or **Cash Out** to leave with your winnings.",
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
        #auto-start the game after 5 minutes if host hasn't started it
            if self.game_id in active_roulette_games:
                game = active_roulette_games[self.game_id]
                if not game.game_started and len(game.players) >= 1:  # At least host is in game
                    game.game_started = True
                    game.pot = game.bet_amount * len(game.players)
                
                # find the channel where this game is running
                    channel = None
                    for ch_id, tracked_game_id in active_roulette_channel_games.items():
                        if tracked_game_id == self.game_id:
                            channel = bot.get_channel(ch_id)
                            break
                
                    if channel:
                        await channel.send("⏰ **Auto-starting game after 5 minutes!**")
                        await start_roulette_game(channel, self.game_id)



# roulette continue view
class RouletteContinueView(discord.ui.View):
    def __init__(self, game_id, timeout=60):
        super().__init__(timeout=timeout)
        self.game_id = game_id
    
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
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print(f"Error syncing commands: {e}")


@bot.event
async def on_member_join(member):
    # fix the thanking member name, need to be able to check who sent an invite
    await member.send(f"🌿Welcome to /GATHER, {member.name}! Thank you to {member.name} for inviting them!🌿")

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

    # see if the gathered item is a GMO
    gmo_chance = 0.05
    is_gmo = random.choices([True, False], weights=[gmo_chance, 1-gmo_chance], k=1)[0]
    if is_gmo:
        final_value *= 1.5
    else:
        final_value *= 1

    #add the value to the balance for the user
    user_id = interaction.user.id
    current_balance = get_user_balance(user_id)
    new_balance = current_balance + final_value
    #save to database
    update_user_balance(user_id, new_balance)

    add_user_item(user_id, name)
    add_ripeness_stat(user_id, ripeness["name"])

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
    await interaction.followup.send(embed=embed) 

# balance command
@bot.tree.command(name="balance", description="Check your current balance")
#use defer for thinking message
async def balance(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    user_id = interaction.user.id
    user_balance = get_user_balance(user_id)
    await interaction.followup.send(f"{interaction.user.name}, you have **${user_balance:.2f}**.")


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
        print(f"Environment: {os.environ.get('ENVIRONMENT', 'unknown')}")
        print(f"Database URL set: {'Yes' if os.environ.get('DATABASE_URL') else 'No'}")
        print(f"Discord Token set: {'Yes' if os.environ.get('DISCORD_TOKEN') else 'No'}")
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
    
    # Start HTTP server in a separate thread
    http_thread = threading.Thread(target=start_http_server)
    http_thread.daemon = True
    http_thread.start()
    
    # Start the Discord bot
    start_discord_bot()