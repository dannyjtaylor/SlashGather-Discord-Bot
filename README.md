# SlashGather Discord Bot

A Discord bot built with discord.py.

## Setup Instructions

### First Time Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/dannyjtaylor/SlashGather-Discord-Bot.git
   cd SlashGather-Discord-Bot
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment:**
   - **Windows:** `venv\Scripts\activate`
   - **Mac/Linux:** `source venv/bin/activate`

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

5. **Create a `.env` file:**
   Create a file named `.env` in the project root and add the required settings:
   ```
   # Discord
   DISCORD_TOKEN=your_production_bot_token
   DISCORD_DEV_TOKEN=your_development_bot_token
   ENVIRONMENT=development  # or production

   # MongoDB
   MONGODB_URI=your_mongodb_connection_string
   MONGODB_DB_NAME=slashgather
   DEFAULT_BALANCE=100.0
   ```
   - Generate your Discord token(s) in the [Discord Developer Portal](https://discord.com/developers/applications)
   - `MONGODB_URI` should point to the MongoDB deployment you want to use (Atlas or self-hosted)

### Running the Bot

```bash
python main.py
```

## Important Notes

- Never commit your `.env` file or share your Discord token
- The `venv/` directory is gitignored and should be recreated on each machine
- Rotate MongoDB credentials if the URI was ever exposed
- Regenerate your Discord token if it was ever exposed

