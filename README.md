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
   - Create a file named `.env` in the root directory
   - Add your Discord bot token:
     ```
     DISCORD_TOKEN=your_bot_token_here
     ```
   - Get your token from the [Discord Developer Portal](https://discord.com/developers/applications)

### Running the Bot

```bash
python slashgather.py
```

## Important Notes

- Never commit your `.env` file or share your Discord token
- The `venv/` directory is gitignored and should be recreated on each machine
- Make sure to regenerate your Discord token if it was ever exposed

