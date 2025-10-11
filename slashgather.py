import discord

class MyClient(discord.Client):
    async def on_ready(self):
        print(f'Logged on as {self.user}')

    async def on_message(self,message):
        print(f'Message from {message.author}: {message.content}')

intents = discord.Intents.default()
intents.message_content = True

client = MyClient(intents=intents)
client.run('my token goes here')

# making a simple bot

intents = discord.Intents.default()
intents.message_content = True

# instance of a client (this is the connection to discord itself)
client = discord.Client(intents=intents)

@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content.startswith('$hello'):
        await message.channel.send('Hello!')

# need Discord token!
client.run('')
