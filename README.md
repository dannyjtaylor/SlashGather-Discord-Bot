# SlashGather Discord Bot

This is a Discord bot I made for my friends! It uses the Python library **discord.py**. 

Invite Link:

[https://discord.gg/47JDbfrg](https://discord.gg/TDw8jUmd5h)

(This project is still a work in progress!!)

---

## Auto-update on the Pi when you push (Git)

The Pi can update itself whenever you push from your PC or laptop. Two options:

### Option A: Pi checks for updates every 5 minutes (simplest, no ports)

On the Pi, the repo must be a **git clone** (so it has `origin`). Then:

1. **One-time:** Make sure the Pi has the repo cloned and `.env` set up:
   ```bash
   cd ~
   git clone https://github.com/YOUR_USER/SlashGather-Discord-Bot.git
   cd SlashGather-Discord-Bot
   # add .env, venv, pip install -r requirements.txt, etc.
   ```

2. **Add a cron job** so the Pi pulls and restarts the bot if there are changes:
   ```bash
   crontab -e
   ```
   Add this line (runs every 5 minutes):
   ```
   */5 * * * * cd /home/slashgather/SlashGather-Discord-Bot && bash scripts/pull-and-restart.sh >> /home/slashgather/deploy.log 2>&1
   ```
   Use your actual username and path if different.

After that, when you push to `main` from any machine, the Pi will pick up the change within 5 minutes and restart the bot.

### Option B: Deploy instantly via GitHub Actions (on every push)

When you push to `main`, GitHub Actions SSHs into the Pi and runs `scripts/pull-and-restart.sh`. The Pi must be **reachable from the internet** (e.g. Tailscale, or router port 22 forwarded + dynamic DNS).

1. **Repo secrets** (Settings → Secrets and variables → Actions):  
   - `DEPLOY_HOST` – Pi’s hostname or IP (e.g. `slashgather-bot.local` or Tailscale IP).  
   - `DEPLOY_USER` – SSH user (e.g. `slashgather`).  
   - `DEPLOY_SSH_KEY` – Private SSH key that can log into the Pi (paste the whole key, including `-----BEGIN ... -----`).

2. On the Pi, ensure your SSH key is in `~/.ssh/authorized_keys` for `DEPLOY_USER`.

3. Push to `main`; the “Deploy to Pi” workflow will run and update the bot on the Pi.
