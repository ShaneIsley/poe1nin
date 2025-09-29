# fetch_data.py (v2 - handles multiple endpoint structures)
import requests
import sqlite3
import datetime
import re
import os

# --- Configuration ---
DB_FILE = "poe1_economy.db"
LEAGUE = "mercenaries" 
# This set contains the item types that use the special 'currencyoverview' endpoint.
CURRENCY_TYPES = {"Currency", "Fragment"}

# --- Database Schema (No changes needed) ---
def create_database_schema(cursor, conn):
    """Creates the necessary tables if they don't exist."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS leagues (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS item_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT, api_id TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
        image_url TEXT, category_id INTEGER,
        FOREIGN KEY (category_id) REFERENCES item_categories (id)
    );""")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS price_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER NOT NULL, league_id INTEGER NOT NULL,
        timestamp DATETIME NOT NULL, chaos_value REAL, divine_value REAL, exalted_value REAL,
        volume_chaos REAL, volume_divine REAL, volume_exalted REAL, max_volume_currency TEXT,
        FOREIGN KEY (item_id) REFERENCES items (id), FOREIGN KEY (league_id) REFERENCES leagues (id)
    );""")
    conn.commit()

# --- API Fetching (Adapted for multiple endpoints) ---
def fetch_all_item_overviews():
    """Fetches and parses the JS file to get all PoE 1 item category API endpoints."""
    url = 'https://poe.ninja/chunk.D3O5eA5x.mjs'
    print(f"Fetching item category list from: {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        js_content = response.text
        overview_pairs = re.findall(r'{url:"[^"]+",type:"([^"]+)",name:"([^"]+)"', js_content)
        all_pairs = [("Currency", "Currency"), ("Fragment", "Fragments")] + overview_pairs
        if not all_pairs:
            print("Could not find any item category pairs.")
            return []
        print(f"Successfully extracted {len(all_pairs)} item overview pairs.")
        return all_pairs
    except requests.exceptions.RequestException as e:
        print(f"Error fetching JavaScript file: {e}")
        return []

def fetch_poe_ninja_data(league_name, item_type):
    """
    Fetches economic data.
    Uses the correct endpoint based on whether the item_type is in CURRENCY_TYPES.
    """
    if item_type in CURRENCY_TYPES:
        endpoint = "currencyoverview"
    else:
        endpoint = "itemoverview"
        
    url = f"https://poe.ninja/api/data/{endpoint}?league={league_name}&type={item_type}"
    print(f"Fetching data for '{item_type}' from endpoint: '{endpoint}'")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for {item_type}: {e}")
        return None

# --- Data Processing (Adapted for multiple JSON structures) ---
def process_and_insert_data(data, league_name, category_display_name, cursor, conn):
    """
    Processes JSON data and inserts it into the SQLite database.
    It automatically detects the JSON structure and parses accordingly.
    """
    current_timestamp = datetime.datetime.now()
    cursor.execute("INSERT OR IGNORE INTO leagues (name) VALUES (?)", (league_name,))
    cursor.execute("SELECT id FROM leagues WHERE name = ?", (league_name,))
    league_id = cursor.fetchone()[0]

    cursor.execute("INSERT OR IGNORE INTO item_categories (name) VALUES (?)", (category_display_name,))
    cursor.execute("SELECT id FROM item_categories WHERE name = ?", (category_display_name,))
    category_id = cursor.fetchone()[0]
    
    items_processed = 0
    
    # --- Logic for 'currencyoverview' endpoint structure ---
    if "currencyDetails" in data:
        lines = data.get('lines', [])
        if not lines:
            print("No currency lines found in the response.")
            return

        for item_data in lines:
            item_name = item_data.get('currencyTypeName')
            # Use 'detailsId' as the unique API identifier
            api_id = item_data.get('detailsId') 
            if not api_id or not item_name:
                continue

            # This endpoint doesn't include the icon in each line, so we can leave it null.
            # An advanced version could build a lookup from the 'currencyDetails' array, but this is fine.
            cursor.execute("INSERT OR IGNORE INTO items (api_id, name, image_url, category_id) VALUES (?, ?, ?, ?)",
                           (api_id, item_name, None, category_id))
            
            cursor.execute("SELECT id FROM items WHERE api_id = ?", (api_id,))
            db_item_id_tuple = cursor.fetchone()
            if not db_item_id_tuple: continue
            db_item_id = db_item_id_tuple[0]

            # Use 'chaosEquivalent' for the chaos value
            chaos_value = item_data.get('chaosEquivalent')
            cursor.execute("INSERT INTO price_entries (item_id, league_id, timestamp, chaos_value) VALUES (?, ?, ?, ?)",
                           (db_item_id, league_id, current_timestamp, chaos_value))
            items_processed += 1
            
    # --- Logic for 'itemoverview' endpoint structure (original logic) ---
    else:
        lines = data.get('lines', [])
        if not lines:
            print("No item lines found in the response.")
            return

        for item_data in lines:
            item_id = item_data.get('id')
            item_name = item_data.get('name')
            if not item_id or not item_name:
                continue

            cursor.execute("INSERT OR IGNORE INTO items (api_id, name, image_url, category_id) VALUES (?, ?, ?, ?)",
                           (item_id, item_name, item_data.get('icon'), category_id))
            
            cursor.execute("SELECT id FROM items WHERE api_id = ?", (item_id,))
            db_item_id_tuple = cursor.fetchone()
            if not db_item_id_tuple: continue
            db_item_id = db_item_id_tuple[0]

            cursor.execute("""
            INSERT INTO price_entries (item_id, league_id, timestamp, chaos_value, divine_value, exalted_value) 
            VALUES (?, ?, ?, ?, ?, ?)
            """, (db_item_id, league_id, current_timestamp, 
                  item_data.get('chaosValue'), item_data.get('divineValue'), item_data.get('exaltedValue')))
            items_processed += 1
    
    conn.commit()
    print(f"Successfully processed and inserted/updated data for {items_processed} items in the '{category_display_name}' category.")

# --- Main Execution (No changes needed) ---
def main():
    """The main function to run the entire update process."""
    print("--- Starting PoE 1 Economy Data Fetch ---")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    create_database_schema(cursor, conn)

    overviews_to_fetch = fetch_all_item_overviews()
    if not overviews_to_fetch:
        print("Halting execution: Could not retrieve item categories.")
        conn.close()
        return

    print(f"\nFound {len(overviews_to_fetch)} categories to process.")
    print("-" * 40)

    for api_type, display_name in overviews_to_fetch:
        print(f"Processing Category: '{display_name}' (using API type: '{api_type}')")
        api_data = fetch_poe_ninja_data(LEAGUE, api_type)
        if api_data:
            process_and_insert_data(api_data, LEAGUE, display_name, cursor, conn)
        else:
            print(f"Skipping category '{display_name}' due to fetch error or no data.")
        print("-" * 40)
    
    conn.close()
    print("--- Full Process Complete ---")

if __name__ == "__main__":
    main()
