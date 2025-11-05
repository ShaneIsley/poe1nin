import requests
import sqlite3
import datetime
import re
import os
import logging
import time
import json

# --- Configuration ---
DB_FILE = "poe_economy_keepers.db"  # UPDATED: Points to the new database file
LEAGUE_NAME = "Keepers"
REQUEST_DELAY = 1.5  # Delay in seconds between API requests
DATA_DIR = "data"    # Directory to store raw JSON responses

# A hardcoded map of categories. Display Name -> API Type Name
# This replaces the dynamic fetching for better stability and control.
ITEM_CATEGORY_MAPPINGS = {
    "Currency": "Currency", "Fragments": "Fragment", "Tattoos": "Tattoo",
    "Oils": "Oil", "Incubators": "Incubator", "Scarabs": "Scarab",
    "Delirium Orbs": "DeliriumOrb", "Essences": "Essence", "Divination Cards": "DivinationCard",
    "Skill Gems": "SkillGem", "Cluster Jewels": "ClusterJewel", "Maps": "Map",
    "Unique Jewels": "UniqueJewel", "Unique Flasks": "UniqueFlask", "Unique Weapons": "UniqueWeapon",
    "Unique Armours": "UniqueArmour", "Unique Accessories": "UniqueAccessory"
}
# This set still defines which API types use the 'currencyoverview' endpoint.
CURRENCY_TYPES = {"Currency", "Fragment"}

def sanitize_filename(name: str) -> str:
    """Converts a string into a safe filename."""
    name = name.lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_.-]', '', name)
    return f"{name}.json"

def create_database_schema(cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    """
    Creates the new, optimized database schema with current_prices and price_history tables.
    """
    logging.info("Ensuring optimized database schema exists...")
    # --- Base Tables ---
    cursor.execute("CREATE TABLE IF NOT EXISTS leagues (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);")
    cursor.execute("CREATE TABLE IF NOT EXISTS item_categories (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, api_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
        image_url TEXT, category_id INTEGER,
        FOREIGN KEY (category_id) REFERENCES item_categories (id)
    );""")

    # --- NEW: High-performance Price Tables ---
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS current_prices (
        item_id INTEGER PRIMARY KEY,
        chaos_value REAL,
        divine_value REAL,
        last_updated_timestamp DATETIME NOT NULL,
        FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE
    );""")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        league_id INTEGER NOT NULL,
        timestamp DATETIME NOT NULL,
        chaos_value REAL,
        divine_value REAL,
        FOREIGN KEY (item_id) REFERENCES items(id) ON DELETE CASCADE,
        FOREIGN KEY (league_id) REFERENCES leagues(id)
    );""")

    # --- NEW: Essential Performance Index ---
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_history_item_timestamp ON price_history (item_id, timestamp);")
    
    conn.commit()
    logging.info("Schema is ready.")

def fetch_poe_ninja_data(league_name: str, item_type: str) -> dict | None:
    """
    Fetches economic data. Uses the correct endpoint based on whether the
    item_type is in CURRENCY_TYPES.
    """
    endpoint = "currencyoverview" if item_type in CURRENCY_TYPES else "itemoverview"
    url = f"https://poe.ninja/api/data/{endpoint}"
    params = {'league': league_name, 'type': item_type}
    logging.info(f"Fetching data for '{item_type}' from endpoint: '{endpoint}'")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while fetching data for {item_type}: {e}")
        return None

def process_and_insert_data(data: dict, league_name: str, category_display_name: str, cursor: sqlite3.Cursor, conn: sqlite3.Connection):
    """
    Processes JSON data, checks for price changes, and inserts data into the
    new 'current_prices' and 'price_history' tables.
    """
    if not data:
        logging.warning("No valid data to process.")
        return

    current_timestamp = datetime.datetime.now()
    cursor.execute("INSERT OR IGNORE INTO leagues (name) VALUES (?)", (league_name,))
    cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
    league_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO item_categories (name) VALUES (?)", (category_display_name,))
    cursor.execute("SELECT id FROM item_categories WHERE name = ?", (category_display_name,))
    category_id = cursor.fetchone()[0]

    items_updated = 0
    items_added = 0
    items_skipped = 0
    
    lines = data.get('lines', [])
    if not lines:
        logging.warning(f"No item lines found in the response for category '{category_display_name}'.")
        return

    for item_data in lines:
        is_currency = 'currencyTypeName' in item_data
        
        item_name = item_data.get('currencyTypeName') if is_currency else item_data.get('name')
        api_id = item_data.get('detailsId') if is_currency else item_data.get('id')
        image_url = item_data.get('icon')
        
        chaos_value = item_data.get('chaosEquivalent') if is_currency else item_data.get('chaosValue')
        divine_value = item_data.get('divineValue')
        
        if not api_id or not item_name:
            continue
            
        if is_currency:
            receive_details = item_data.get('receive')
            if receive_details and receive_details.get('value', 0) > 1 and chaos_value and chaos_value > 1:
                chaos_value = 1 / chaos_value

        # --- Get or create the item's master record ---
        cursor.execute("INSERT OR IGNORE INTO items (api_id, name, image_url, category_id) VALUES (?, ?, ?, ?)",
                       (api_id, item_name, image_url, category_id))
        cursor.execute("SELECT id FROM items WHERE api_id = ?", (api_id,))
        db_item_id = cursor.fetchone()[0]

        # --- NEW: Change-Detection Logic ---
        cursor.execute("SELECT chaos_value, divine_value FROM current_prices WHERE item_id = ?", (db_item_id,))
        old_price = cursor.fetchone()

        price_has_changed = False
        if not old_price:
            price_has_changed = True
            items_added += 1
        # Compare, ensuring we handle None values correctly
        elif old_price[0] != chaos_value or old_price[1] != divine_value:
            price_has_changed = True
            items_updated += 1
        else:
            items_skipped += 1

        # --- NEW: Insert into new tables ONLY if the price has changed ---
        if price_has_changed:
            # 1. Log the change in the history table
            cursor.execute("""
            INSERT INTO price_history (item_id, league_id, timestamp, chaos_value, divine_value)
            VALUES (?, ?, ?, ?, ?)
            """, (db_item_id, league_id, current_timestamp, chaos_value, divine_value))
            
            # 2. Update the current price table using an UPSERT
            cursor.execute("""
            INSERT INTO current_prices (item_id, chaos_value, divine_value, last_updated_timestamp)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(item_id) DO UPDATE SET
                chaos_value = excluded.chaos_value,
                divine_value = excluded.divine_value,
                last_updated_timestamp = excluded.last_updated_timestamp;
            """, (db_item_id, chaos_value, divine_value, current_timestamp))

    conn.commit()
    logging.info(f"'{category_display_name}' Report: {items_added} new items, {items_updated} updated, {items_skipped} skipped (price unchanged).")

def main():
    """The main function to run the entire update process."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info(f"--- Starting PoE Economy Data Fetch for {LEAGUE_NAME} League (Optimized Schema) ---")

    league_data_dir = os.path.join(DATA_DIR, LEAGUE_NAME.lower().replace(" ", "_"))
    os.makedirs(league_data_dir, exist_ok=True)
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_database_schema(cursor, conn)

    overviews_to_fetch = ITEM_CATEGORY_MAPPINGS
    logging.info(f"Processing {len(overviews_to_fetch)} hardcoded categories.")
    logging.info("-" * 40)

    for display_name, api_type in overviews_to_fetch.items():
        logging.info(f"Processing Category: '{display_name}' (using API type: '{api_type}')")
        api_data = fetch_poe_ninja_data(LEAGUE_NAME, api_type)
        
        if api_data:
            filename = sanitize_filename(display_name)
            filepath = os.path.join(league_data_dir, filename)
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(api_data, f, indent=4)
                logging.info(f"Successfully saved raw data to '{filepath}'")
            except IOError as e:
                logging.error(f"Could not write to file '{filepath}': {e}")
            
            process_and_insert_data(api_data, LEAGUE_NAME, display_name, cursor, conn)
        else:
            logging.warning(f"Skipping category '{display_name}' due to fetch error or no data.")
        
        logging.info(f"Waiting for {REQUEST_DELAY} seconds before next request...")
        time.sleep(REQUEST_DELAY)
        logging.info("-" * 40)
    
    conn.close()
    logging.info("--- Full Process Complete ---")

if __name__ == "__main__":
    main()
