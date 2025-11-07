# MongoDB Setup Guide

## Getting Your MongoDB Connection String

### Step 1: Log into MongoDB Atlas
1. Go to https://cloud.mongodb.com
2. Log in with your MongoDB account credentials

### Step 2: Find Your Connection String
1. Click on your **cluster** (the one you want to use)
2. Click the **"Connect"** button
3. Select **"Connect your application"**
4. Choose **"Python"** as your driver (version 4.6 or later)
5. Copy the connection string that looks like:
   ```
   mongodb+srv://<username>:<password>@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
   ```

### Step 3: Replace Placeholders
Replace `<username>` and `<password>` in the connection string with your actual MongoDB username and password.

**Example:**
- If your username is `myuser` and password is `mypass123`
- And your connection string is: `mongodb+srv://<username>:<password>@cluster0.abc123.mongodb.net/?retryWrites=true&w=majority`
- Your final URI should be: `mongodb+srv://myuser:mypass123@cluster0.abc123.mongodb.net/?retryWrites=true&w=majority`

### Step 4: Choose a Database Name
- You can use any name you want (e.g., `slashgather`, `gatherbot`, `mybot`)
- MongoDB will automatically create the database when you first write data to it
- The database name is just an identifier - it doesn't need to exist yet

## Creating Your .env File

Create a file named `.env` in the project root with the following content:

```env
# Discord Bot Tokens
DISCORD_TOKEN=your_production_bot_token_here
DISCORD_DEV_TOKEN=your_development_bot_token_here

# Environment
ENVIRONMENT=development

# MongoDB Configuration
MONGODB_URI=mongodb+srv://your_username:your_password@cluster0.xxxxx.mongodb.net/?retryWrites=true&w=majority
MONGODB_DB_NAME=slashgather

# Default starting balance for new users
DEFAULT_BALANCE=100.0
```

**Important:** 
- Replace `your_username` and `your_password` with your actual MongoDB credentials
- Replace the cluster URL with your actual cluster URL from Step 2
- Never commit the `.env` file to git (it's already in `.gitignore`)

## Testing Your Connection

After creating your `.env` file, test the connection by running:

```bash
python main.py
```

You should see: `Connected to MongoDB successfully`

If you get an error, double-check:
- Your username and password are correct
- Your IP address is whitelisted in MongoDB Atlas (Network Access section)
- The connection string format is correct

