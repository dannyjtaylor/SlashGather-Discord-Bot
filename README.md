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
  DISCORD_DEV_TOKEN=your_development_bot_token
  DISCORD_TOKEN=your_production_bot_token   # optional locally, required for Cloud Run

  # Environment flags
  ENVIRONMENT=development
  DEFAULT_BALANCE=10000.0

  # Production overrides (used in Cloud Run / Secret Manager)
  ENVIRONMENT_PROD=production
  DEFAULT_BALANCE_PROD=100.0

  # MongoDB (local development)
  # Use separate databases on the same Atlas cluster, e.g. gatherdb (prod) and gatherdbdev (dev)
  MONGODB_URI=mongodb+srv://<user>:<password>@yourcluster.mongodb.net/?retryWrites=true&w=majority&appName=gatherdbdev
  MONGODB_DB_NAME=gatherdbdev
  ```
   - Generate your Discord token(s) in the [Discord Developer Portal](https://discord.com/developers/applications)
   - For production, create a Secret Manager entry with the production URI and database name (e.g. `gatherdb`)
   - For development, point to the dev database (e.g. `gatherdbdev`) in your local `.env`

### Running the Bot

```bash
python main.py
```

## Important Notes

- Never commit your `.env` file or share your Discord token
- The `venv/` directory is gitignored and should be recreated on each machine
- Rotate MongoDB credentials if the URI was ever exposed
- Regenerate your Discord token if it was ever exposed

