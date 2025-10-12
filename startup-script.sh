# DATABASE_URL=postgresql://postgres:u1Eg%25j%23NaCuD%7C%3B68@104.154.204.236:5432/postgres

apt-get update
apt-get install -y python3 python3-pip git

mkdir -p /opt/slashgather
cd /opt/slashgather

git clone https://github.com/dannyjtaylor/SlashGather-Discord-Bot.git .

pip3 install -r requirements.txt

cat > .env << EOF 
DISCORD_TOKEN=your_discord_token_here
DATABASE_URL=your_database_url_here
ENVIRONMENT=production
EOF


cat > /etc/systemd/system/slashgather.service << EOF
[Unit]
Description=SlashGather Discord Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/slashgather
EnvironmentFile=/opt/slashgather/.env
ExecStart=/usr/bin/python3 main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
systemctl daemon-reload
systemctl enable slashgather.service
systemctl start slashgather.service

# Check if it's running
systemctl status slashgather.service

# Show logs for debugging
echo "=== Bot Status ==="
systemctl is-active slashgather.service

echo "=== Recent Logs ==="
journalctl -u slashgather.service --no-pager -n 20