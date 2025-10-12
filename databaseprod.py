import psycopg2
import os
from typing import Optional, Dict, List

def get_database():
    """Get production database connection"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    conn = psycopg2.connect(database_url)
    return conn

def init_database():
    """Initialize production database tables"""
    conn = get_database()
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance DECIMAL(10,2) DEFAULT 100.0,
            last_gather_time DECIMAL(20,6) DEFAULT 0,
            total_forage_count INTEGER DEFAULT 0
        )
    """)
    
    # Create user_items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_items (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            item_name VARCHAR(100),
            quantity INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    
    # Create user_ripeness_stats table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_ripeness_stats (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            ripeness_name VARCHAR(50),
            count INTEGER DEFAULT 1,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """)
    
    conn.commit()
    conn.close()

# All your existing functions work the same way:
def get_user_balance(user_id: int) -> float:
    """Get user's balance from database"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("SELECT balance FROM users WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    
    if result is None:
        # Create new user with default balance
        cursor.execute("INSERT INTO users (user_id, balance) VALUES (%s, %s)", (user_id, 100.0))
        conn.commit()
        conn.close()
        return 100.0
    
    conn.close()
    return float(result[0])

def update_user_balance(user_id: int, new_balance: float):
    """Update user's balance in database"""
    conn = get_database()
    cursor = conn.cursor()
    
    # Check if user exists, if not create them
    cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO users (user_id, balance) VALUES (%s, %s)", (user_id, new_balance))
    else:
        cursor.execute("UPDATE users SET balance = %s WHERE user_id = %s", (new_balance, user_id))
    
    conn.commit()
    conn.close()


# Foraging statistics functions
def increment_forage_count(user_id: int):
    """Increment user's total forage count"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT OR REPLACE INTO users (user_id, total_foraged) 
        VALUES (?, COALESCE((SELECT total_foraged FROM users WHERE user_id = ?), 0) + 1)
    """, (user_id, user_id))
    
    conn.commit()
    conn.close()

def get_forage_count(user_id: int) -> int:
    """Get user's total forage count"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("SELECT total_foraged FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else 0

# Item tracking functions
def add_user_item(user_id: int, item_name: str):
    """Add or increment an item for a user"""
    conn = get_database()
    cursor = conn.cursor()
    
    # Check if user already has this item
    cursor.execute("SELECT quantity FROM user_items WHERE user_id = ? AND item_name = ?", (user_id, item_name))
    result = cursor.fetchone()
    
    if result:
        # Increment existing item
        cursor.execute("""
            UPDATE user_items 
            SET quantity = quantity + 1 
            WHERE user_id = ? AND item_name = ?
        """, (user_id, item_name))
    else:
        # Add new item
        cursor.execute("""
            INSERT INTO user_items (user_id, item_name, quantity) 
            VALUES (?, ?, 1)
        """, (user_id, item_name))
    
    conn.commit()
    conn.close()

def get_user_items(user_id: int) -> Dict[str, int]:
    """Get all items a user has foraged"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("SELECT item_name, quantity FROM user_items WHERE user_id = ? ORDER BY quantity DESC", (user_id,))
    results = cursor.fetchall()
    
    conn.close()
    return {item: quantity for item, quantity in results}

# Ripeness tracking functions
def add_ripeness_stat(user_id: int, ripeness_name: str):
    """Add or increment a ripeness stat for a user"""
    conn = get_database()
    cursor = conn.cursor()
    
    # Check if user already has this ripeness stat
    cursor.execute("SELECT quantity FROM user_ripeness_stats WHERE user_id = ? AND ripeness_name = ?", (user_id, ripeness_name))
    result = cursor.fetchone()
    
    if result:
        # Increment existing stat
        cursor.execute("""
            UPDATE user_ripeness_stats 
            SET quantity = quantity + 1 
            WHERE user_id = ? AND ripeness_name = ?
        """, (user_id, ripeness_name))
    else:
        # Add new stat
        cursor.execute("""
            INSERT INTO user_ripeness_stats (user_id, ripeness_name, quantity) 
            VALUES (?, ?, 1)
        """, (user_id, ripeness_name))
    
    conn.commit()
    conn.close()

def get_user_ripeness_stats(user_id: int) -> Dict[str, int]:
    """Get all ripeness stats for a user"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("SELECT ripeness_name, quantity FROM user_ripeness_stats WHERE user_id = ? ORDER BY quantity DESC", (user_id,))
    results = cursor.fetchall()
    
    conn.close()
    return {ripeness: quantity for ripeness, quantity in results}

# Cooldown functions
def get_user_last_gather_time(user_id: int) -> float:
    """Get user's last gather time from database"""
    conn = get_database()
    cursor = conn.cursor()
    
    cursor.execute("SELECT last_gather_time FROM users WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else 0.0

def update_user_last_gather_time(user_id: int, timestamp: float):
    """Update user's last gather time in database"""
    conn = get_database()
    cursor = conn.cursor()
    
    # Check if user exists, if not create them
    cursor.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if cursor.fetchone() is None:
        cursor.execute("INSERT INTO users (user_id, last_gather_time) VALUES (?, ?)", (user_id, timestamp))
    else:
        cursor.execute("UPDATE users SET last_gather_time = ? WHERE user_id = ?", (timestamp, user_id))
    
    conn.commit()
    conn.close()