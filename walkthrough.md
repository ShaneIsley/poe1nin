# PoE Economy Tracker: A Code Walkthrough

*2026-03-17T10:39:42Z by Showboat 0.6.1*
<!-- showboat-id: eac9a5e8-7eff-44b7-80d4-d7962fae1f0a -->

## Section 1: Project Overview

This repository is a **Path of Exile (PoE) economy tracker** for the Keepers league. Path of Exile is an online action RPG with a complex player-driven economy — items are traded for in-game currency like Chaos Orbs and Divine Orbs, and prices fluctuate constantly.

The project has two main Python scripts:

- **fetch_data.py**: Polls the poe.ninja public API every 30 minutes (via GitHub Actions) to pull current market prices for 17 item categories, stores them in a SQLite database, and saves raw JSON snapshots to disk.
- **analysis.py**: Reads the database, imputes a unified chaos-equivalent value for every item, finds the biggest market movers, and injects charts and tables directly into the README.

The pipeline is fully automated — a GitHub Actions workflow runs on a cron schedule, commits updated data back to the repo, and pushes. No server required.

Let's start with the dependencies:

```bash
cat requirements.txt
```

```output
requests
pandas
plotly
kaleido
```

Four dependencies: `requests` for HTTP calls, `pandas` for tabular data manipulation, `plotly` for generating charts, and `kaleido` to render those charts to static PNG files.

## Section 2: Configuration Constants (fetch_data.py lines 1–28)

At the top of `fetch_data.py` are all the knobs that control the script's behaviour, kept together so they're easy to change without hunting through the code.

Key points:
- `DB_FILE` names the SQLite database file that persists all price history.
- `LEAGUE_NAME = "Keepers"` is passed verbatim to the poe.ninja API; change this to track a different league.
- `REQUEST_DELAY = 1.5` is a courtesy sleep between API requests so we don't hammer poe.ninja.
- `ITEM_CATEGORY_MAPPINGS` is a **hardcoded dict** mapping human-readable category names (used as display names and filenames) to the API type codes poe.ninja expects. This replaced a dynamic discovery approach for stability.
- `CURRENCY_TYPES` identifies which API types need the `currencyoverview` endpoint rather than the generic `itemoverview` endpoint — Currency and Fragment have a different JSON response structure.

```bash
sed -n '1,28p' fetch_data.py
```

```output
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

```

## Section 3: sanitize_filename (fetch_data.py lines 29–34)

Before saving a raw JSON snapshot for each category, the category's display name (e.g. "Delirium Orbs", "Skill Gems") needs to become a valid filename. This tiny helper does exactly that:

1. Lowercase everything.
2. Replace runs of whitespace with underscores.
3. Strip any character that isn't a lowercase letter, digit, underscore, dot, or hyphen.
4. Append `.json`.

So "Skill Gems" becomes `skill_gems.json`, "Delirium Orbs" becomes `delirium_orbs.json`, etc.

```bash
sed -n '29,34p' fetch_data.py
```

```output
def sanitize_filename(name: str) -> str:
    """Converts a string into a safe filename."""
    name = name.lower()
    name = re.sub(r'\s+', '_', name)
    name = re.sub(r'[^a-z0-9_.-]', '', name)
    return f"{name}.json"
```

## Section 4: Database Schema (fetch_data.py lines 36–77)

The `create_database_schema` function is called once at startup and creates all tables with `CREATE TABLE IF NOT EXISTS`, so re-running the script never destroys existing data.

The schema is a **two-tier price store**:

**Reference tables (write-once, rarely change):**
- `leagues` — one row per league name (just "Keepers" in practice)
- `item_categories` — one row per display category (17 rows)
- `items` — one row per unique item, with a UNIQUE constraint on `api_id` so poe.ninja's identifier is the canonical key

**Price tables (write-frequently):**
- `current_prices` — one row per item, storing only the *latest* price. Uses `item_id` as its own primary key (1:1 with items). This table is the fast lookup path for analysis.
- `price_history` — append-only log of every price *change*, with a timestamp. Never updates, only inserts.

The composite index `idx_price_history_item_timestamp ON price_history (item_id, timestamp)` makes window-function queries over history fast — which `analysis.py` relies on heavily.

```bash
sed -n '36,77p' fetch_data.py
```

```output
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
```

## Section 5: Fetching Data from poe.ninja (fetch_data.py lines 79–95)

`fetch_poe_ninja_data` is a thin HTTP wrapper. The key decision is which endpoint to call:

- If the `item_type` is in `CURRENCY_TYPES` (Currency or Fragment), use `/api/data/currencyoverview`
- Otherwise use `/api/data/itemoverview`

These two endpoints return structurally different JSON (different field names for price and item identity), which `process_and_insert_data` will handle later.

The function passes `league` and `type` as query parameters, sets a 30-second timeout (poe.ninja can be slow), and calls `raise_for_status()` to turn HTTP error codes into exceptions. Any network or HTTP error is caught, logged, and `None` is returned so the caller can skip this category gracefully.

```bash
sed -n '79,95p' fetch_data.py
```

```output
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
```

## Section 6: Processing and Change Detection (fetch_data.py lines 97–182)

`process_and_insert_data` is the heart of the script. It receives the raw API JSON for one category and writes to the database. Here's the logic step by step:

**Setup:**
- Grab the current timestamp once, to be consistent across all inserts in this batch.
- `INSERT OR IGNORE` the league and category names — idempotent, creates them on first run only.

**Per-item loop:**
The function iterates over `data['lines']` — each element is one tradeable item. It first detects whether this is a currency-type item by checking for the `currencyTypeName` field:

- **Currency items** use `currencyTypeName` as the name, `detailsId` as the API id, and `chaosEquivalent` as the price.
- **Regular items** use `name`, `id`, and `chaosValue`.

**Currency rate inversion:**
poe.ninja reports some currency exchange rates from the other direction (e.g. "how many of X per Chaos"). When `receive.value > 1` and `chaos_value > 1`, the value is a rate that needs inverting: `1 / chaos_value`.

**Change detection (the key optimisation):**
Instead of blindly inserting a new price row every 30 minutes for every item (which would create ~250k rows/day), the code queries `current_prices` first. It only writes to `price_history` and updates `current_prices` **when the price actually changed**. Items whose price is unchanged are simply counted as skipped.

**Two-table write:**
When a price change is detected, two writes happen atomically (both inside the same transaction):
1. `INSERT INTO price_history` — appends the change to the audit log
2. `INSERT ... ON CONFLICT DO UPDATE` (UPSERT) into `current_prices` — keeps the latest price always accessible without a subquery

```bash
sed -n '97,145p' fetch_data.py
```

```output
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
```

```bash
sed -n '145,183p' fetch_data.py
```

```output
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

```

## Section 7: Main Orchestration (fetch_data.py lines 184–226)

`main()` ties everything together into a single, linear run:

1. **Logging** is configured with timestamps so every CI run produces a readable audit trail.
2. The **data directory** is created if it doesn't exist (e.g. `data/keepers/`).
3. A **SQLite connection** is opened and the schema is ensured.
4. For each of the 17 categories in `ITEM_CATEGORY_MAPPINGS`:
   - Fetch from poe.ninja
   - If successful, write the raw JSON to disk (as a cache/backup) and process into the DB
   - If failed, log a warning and skip
   - **Sleep 1.5 seconds** before the next request (rate limiting)
5. Close the connection and log completion.

The raw JSON saves in step 4 mean you can inspect exactly what the API returned on any given run, and the data/ directory is committed to git — so there's a full history of API snapshots too.

```bash
sed -n '184,226p' fetch_data.py
```

```output
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
```

## Section 8: Loading Data for Analysis (analysis.py lines 13–44)

`analysis.py` opens with a sophisticated SQL query inside `get_latest_data_df`. It uses a **Common Table Expression (CTE)** with a window function to pull both the current price and the previous price for each item in a single query.

The CTE `PreviousPrices` uses `ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp DESC)` to number each item's price history entries from newest to oldest. Row 1 is the most recent (already in `current_prices`), row 2 is the one before that — the "previous" price we want for change calculations.

The main query then:
- Selects from `current_prices` (fast, one row per item)
- Joins in item name and category from the reference tables
- LEFT JOINs `PreviousPrices` at `rn = 2` (the second-most-recent entry)
- Filters to items updated within the last 2 days

The result is a flat DataFrame with columns: `name`, `category`, `chaos_value`, `divine_value`, `prev_chaos_value`, `prev_divine_value`.

```bash
sed -n '13,44p' analysis.py
```

```output
def get_latest_data_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Fetches the latest price from 'current_prices' and the previous price from 'price_history'.
    This is much more efficient than the original query.
    """
    query = """
    WITH PreviousPrices AS (
        -- For each item, find its second most recent price entry in the history table.
        -- This represents the "previous" price before the current one.
        SELECT
            item_id,
            chaos_value as prev_chaos_value,
            divine_value as prev_divine_value,
            ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY timestamp DESC) as rn
        FROM price_history
    )
    -- Main query to assemble the current and previous prices
    SELECT
        i.name,
        c.name AS category,
        cp.chaos_value,
        cp.divine_value,
        pp.prev_chaos_value,
        pp.prev_divine_value
    FROM current_prices cp
    JOIN items i ON cp.item_id = i.id
    JOIN item_categories c ON i.category_id = c.id
    JOIN leagues l ON l.name = ? -- Filter by league name (assuming only one league is tracked)
    LEFT JOIN PreviousPrices pp ON cp.item_id = pp.item_id AND pp.rn = 2 -- Join with the 2nd-to-last entry
    WHERE cp.last_updated_timestamp >= DATETIME('now', '-2 days');
    """
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))
```

## Section 9: Price Imputation (analysis.py lines 46–68)

PoE uses two primary currencies: **Chaos Orbs** (the base unit) and **Divine Orbs** (the high-value unit, worth ~260 Chaos). The poe.ninja API returns `chaos_value` for most items and `divine_value` for expensive ones, but not always both.

`calculate_imputed_values_poe1` creates a single comparable column — `imputed_chaos_value` — for every item:

1. Find the row for "Divine Orb" in the dataset and read its `chaos_value`. This is the current exchange rate (e.g. 1 Divine = 260 Chaos).
2. For each item, prefer `chaos_value` if it exists. If only `divine_value` is available, multiply it by the rate to get chaos.
3. Apply the same logic to the previous-price columns to produce `prev_imputed_chaos_value`.

This gives every item a single comparable number in chaos terms, enabling straightforward sorting and percentage-change calculations regardless of which currency the API happened to express the price in.

```bash
sed -n '46,68p' analysis.py
```

```output
def calculate_imputed_values_poe1(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates a single 'imputed_chaos_value' for easier analysis."""
    divine_to_chaos_rate = None
    try:
        # In PoE 1, we find the Divine Orb's value in Chaos to get the rate.
        divine_orb_entry = df[df['name'] == 'Divine Orb'].iloc[0]
        if pd.notna(divine_orb_entry['chaos_value']):
            divine_to_chaos_rate = divine_orb_entry['chaos_value']
        print(f"Rate for analysis: 1 Divine Orb = {divine_to_chaos_rate or 'N/A'} Chaos Orbs")
    except (IndexError, KeyError):
        print("Warning: Could not find 'Divine Orb' in the dataset. Imputation will be limited.")

    def impute_price(row, chaos_col, divine_col):
        chaos_val, divine_val = row[chaos_col], row[divine_col]
        if pd.notna(chaos_val):
            return chaos_val
        if pd.notna(divine_val) and pd.notna(divine_to_chaos_rate):
            return divine_val * divine_to_chaos_rate
        return None

    df['imputed_chaos_value'] = df.apply(lambda r: impute_price(r, 'chaos_value', 'divine_value'), axis=1)
    df['prev_imputed_chaos_value'] = df.apply(lambda r: impute_price(r, 'prev_chaos_value', 'prev_divine_value'), axis=1)
    return df
```

## Section 10: Maintenance Table and Markdown Helper (analysis.py lines 70–91)

Two small utility functions support the analysis pipeline:

**`generate_maintenance_table`** queries the database for two summary stats:
- `MAX(last_updated_timestamp)` from `current_prices` — the most recent time any price was written, which tells us when the last fetch run actually changed something
- `COUNT(*)` from `price_history` — the total number of price changes ever logged, a proxy for how active the tracker has been

These get formatted as a two-row GitHub markdown table that appears at the top of the README as a maintenance dashboard.

**`df_to_markdown`** is a generic pandas-DataFrame-to-GFM-table converter. It takes a DataFrame and a header list, and produces a markdown table string with `| col1 | col2 |` rows and `:---` alignment separators. This is used to turn pandas analysis results into README-embeddable tables.

```bash
sed -n '70,91p' analysis.py
```

```output
def generate_maintenance_table() -> str:
    """Creates a markdown table with script maintenance info from the new schema."""
    if not Path(DB_FILE).exists(): return "No database file found."
    with sqlite3.connect(DB_FILE) as conn:
        try:
            # UPDATED: Query the new tables for more meaningful stats
            latest_run_time = pd.read_sql("SELECT MAX(last_updated_timestamp) as last_run FROM current_prices", conn).iloc[0]['last_run']
            total_rows = pd.read_sql("SELECT COUNT(*) as count FROM price_history", conn).iloc[0]['count']
        except (pd.io.sql.DatabaseError, IndexError):
            return "Database is empty or corrupt."
    table = "| Metric | Value |\n|:---|:---|\n"
    table += f"| Last Price Update (UTC) | `{latest_run_time}` |\n"
    table += f"| Total Price Changes Logged | `{total_rows:,}` |\n"
    return table

def df_to_markdown(dataframe: pd.DataFrame, headers: list) -> str:
    """Converts a pandas DataFrame to a GitHub-flavored markdown table."""
    md = f"| {' | '.join(headers)} |\n"
    md += f"|{' :--- |' * len(headers)}\n"
    for _, row in dataframe.iterrows():
        md += f"| {' | '.join(map(str, row))} |\n"
    return md
```

## Section 11: Analysis and Visualizations (analysis.py lines 93–144)

`generate_analysis_content` produces three distinct outputs from the imputed DataFrame:

**1. Market Movers Chart and Table**
Identifies items that have changed the most since their previous price:
- Filter to items with a previous price available AND current value > 10 chaos (removes noise from penny items)
- Calculate `% change = ((current - previous) / previous) × 100`
- Take top 10 gainers and top 10 losers
- Plot a bar chart coloured with the `RdYlGn` (red-yellow-green) scale via Plotly Express, exported to `charts/market_movers.png`

**2. Top 10 Most Valuable Items Table**
A simple sort by `imputed_chaos_value` descending, formatted as a markdown table. This gives a quick glance at the most expensive items in the game right now.

**3. Category Analysis**
For each of the 17 item categories:
- Find the single most valuable item (using `idxmax()`)
- Compute the **median** value across all items in the category

The median is key here: most categories have a long tail of cheap items, so the mean would be skewed by a handful of ultra-rares. The median gives a better sense of "what does a typical item in this category cost?".

A bar chart of median values (log scale, top 20 categories) is exported to `charts/category_analysis.png`.

```bash
sed -n '93,144p' analysis.py
```

```output
def generate_analysis_content(df: pd.DataFrame) -> tuple[str, str, str, str]:
    """Performs all analysis and generates markdown tables and chart paths."""
    if df.empty or 'imputed_chaos_value' not in df.columns or df['imputed_chaos_value'].isna().all():
        return "Not enough data for analysis.", "Please wait for another run.", "", ""
        
    charts_path = Path(CHARTS_DIR); charts_path.mkdir(exist_ok=True)
    df_analysis = df.dropna(subset=['imputed_chaos_value']).copy()
    df_analysis['imputed_chaos_value'] = pd.to_numeric(df_analysis['imputed_chaos_value'])
    
    # --- Market Movers Analysis ---
    df_movers = df_analysis[df_analysis['prev_imputed_chaos_value'].notna() & (df_analysis['imputed_chaos_value'] > 10)].copy()
    movers_chart_path_str = ""
    if not df_movers.empty:
        df_movers = df_movers[df_movers['prev_imputed_chaos_value'] > 0]
        df_movers['change'] = ((df_movers['imputed_chaos_value'] - df_movers['prev_imputed_chaos_value']) / df_movers['prev_imputed_chaos_value']) * 100
        df_movers = df_movers.sort_values(by='change', ascending=False).dropna(subset=['change'])
        top_gainers = df_movers.head(10)
        top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)
        
        movers_chart_df = pd.concat([top_gainers, top_losers])
        if not movers_chart_df.empty:
            fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn', title='Top Market Movers (Since Last Price Change)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
            movers_chart_path = charts_path / "market_movers.png"
            fig_movers.write_image(movers_chart_path, width=1000, height=600)
            movers_chart_path_str = str(movers_chart_path)

    # --- Top Valuable Items Table ---
    top_valuable = df_analysis.sort_values(by='imputed_chaos_value', ascending=False).head(10)[['name', 'imputed_chaos_value']]
    top_valuable['imputed_chaos_value'] = top_valuable['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")
    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Imputed Chaos Value'])
    
    # --- Category Analysis ---
    top_items_list = []
    for category in df_analysis['category'].unique():
        df_category = df_analysis[df_analysis['category'] == category]
        if not df_category.empty:
            top_items_list.append(df_category.loc[df_category['imputed_chaos_value'].idxmax()])
            
    top_item_per_category = pd.DataFrame(top_items_list)
    top_item_per_category = top_item_per_category.sort_values(by='imputed_chaos_value', ascending=False)[['category', 'name', 'imputed_chaos_value']].head(15)
    top_item_per_category['imputed_chaos_value'] = top_item_per_category['imputed_chaos_value'].apply(lambda x: f"{x:,.1f}")
    
    median_by_category = df_analysis.groupby('category')['imputed_chaos_value'].median().sort_values(ascending=False).reset_index()
    fig_category = px.bar(median_by_category.head(20), x='category', y='imputed_chaos_value', title='Median Item Value by Category (Top 20)', log_y=True, labels={'category': 'Item Category', 'imputed_chaos_value': 'Median Chaos Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"
    fig_category.write_image(category_chart_path, width=1000, height=600)
    
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', 'Imputed Chaos Value'])
    
    return market_movers_md, category_md, movers_chart_path_str, str(category_chart_path)
```

## Section 12: Updating the README (analysis.py lines 146–171)

`update_readme` uses a clever marker-based injection pattern to keep the README auto-updateable without overwriting static content the author wrote.

The README contains HTML comment pairs like:

```
<!-- START_MAINTENANCE -->
...old content...
<!-- END_MAINTENANCE -->
```

The function uses `re.sub` with the `re.DOTALL` flag (so `.` matches newlines too) to replace everything between each pair of markers with freshly generated content. Three sections are updated:

- `START_MAINTENANCE` / `END_MAINTENANCE` — the stats table
- `START_ANALYSIS` / `END_ANALYSIS` — top 10 items table + market movers chart
- `START_CATEGORY_ANALYSIS` / `END_CATEGORY_ANALYSIS` — category table + category chart

If the README doesn't exist (first run), the function creates a minimal one with the markers pre-populated so the next write works correctly.

Charts are embedded as standard markdown image syntax: `![alt text](path/to/chart.png)`.

```bash
sed -n '146,171p' analysis.py
```

```output
def update_readme(maintenance_md, market_md, category_md, movers_chart, category_chart):
    """Injects all analysis content into the README.md between markers."""
    try:
        with open(README_FILE, 'r', encoding='utf-8') as f:
            readme_content = f.read()
    except FileNotFoundError:
        readme_content = f"""# PoE Economy Tracker for {LEAGUE_NAME}

<!-- START_MAINTENANCE -->
<!-- END_MAINTENANCE -->

<!-- START_CATEGORY_ANALYSIS -->
<!-- END_CATEGORY_ANALYSIS -->

<!-- START_ANALYSIS -->
<!-- END_ANALYSIS -->"""
    
    new_content = re.sub(r"<!-- START_MAINTENANCE -->.*<!-- END_MAINTENANCE -->", f"<!-- START_MAINTENANCE -->\n{maintenance_md}\n<!-- END_MAINTENANCE -->", readme_content, flags=re.DOTALL)
    full_market_content = f"{market_md}\n\n![Market Movers Chart]({movers_chart})" if movers_chart else market_md
    new_content = re.sub(r"<!-- START_ANALYSIS -->.*<!-- END_ANALYSIS -->", f"<!-- START_ANALYSIS -->\n{full_market_content}\n<!-- END_ANALYSIS -->", new_content, flags=re.DOTALL)
    full_category_content = f"{category_md}\n\n![Category Analysis Chart]({category_chart})" if category_chart else category_md
    new_content = re.sub(r"<!-- START_CATEGORY_ANALYSIS -->.*<!-- END_CATEGORY_ANALYSIS -->", f"<!-- START_CATEGORY_ANALYSIS -->\n{full_category_content}\n<!-- END_CATEGORY_ANALYSIS -->", new_content, flags=re.DOTALL)
    
    with open(README_FILE, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print(f"Successfully updated {README_FILE}")
```

## Section 13: CI/CD Automation (.github/workflows/main.yml)

The whole pipeline runs unattended via a GitHub Actions workflow. Key design decisions:

**Trigger:** A cron expression `*/30 * * * *` fires the job every 30 minutes. There's also a `workflow_dispatch` trigger for on-demand manual runs.

**Permissions:** The workflow explicitly grants `contents: write` to the `GITHUB_TOKEN`. Without this, the final git push would be rejected by GitHub.

**Steps:**
1. Checkout the repo (with LFS support for large files)
2. Set up Python 3.10
3. `pip install -r requirements.txt`
4. Run `fetch_data.py`
5. `git add data/` — only stage the raw JSON snapshots (the `*.db` and `README.md` commits are commented out, so the DB and analysis outputs aren't currently committed)
6. `git diff --staged --quiet || git commit ...` — only commit if there's actually something staged (prevents empty commits when prices didn't change)
7. `git push`

The analysis step is **commented out** in the workflow, meaning chart generation and README updates currently only happen if you run `analysis.py` locally.

```bash
cat .github/workflows/main.yml
```

```output
name: Fetch PoE 1 Economy Data

on:
  workflow_dispatch: # Allows you to run the job manually from the Actions tab
  schedule:
    # Runs every 30 minutes.
    - cron: "*/30 * * * *"

jobs:
  fetch-and-commit:
    runs-on: ubuntu-latest

    # --- FIX: Add this permissions block ---
    # This grants the GITHUB_TOKEN the permissions to write to the repository.
    permissions:
      contents: write

    steps:
      # Step 1: Check out your repository's code
      - name: Checkout Repo
        uses: actions/checkout@v4
        with:
          lfs: true

      # Step 2: Set up the Python version
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      # Step 3: Install the Python libraries
      - name: Install dependencies
        run: pip install -r requirements.txt

      # Step 4: Run the Python script to fetch new data
      - name: Run Fetch Data Script
        run: python fetch_data.py

      # Step 5: (Optional but good practice) Compact the database
      # - name: Vacuum Database
      #  run: |
      #    if [ -f "poe1_economy.db" ]; then
      #      sqlite3 poe1_economy.db "VACUUM;"
      #    fi

      # Step 6: Run the analysis script to generate charts and update the README
      #- name: Run Analysis Script
      #  run: python analysis.py

      # Step 7: Commit all updated files back to the repository
      - name: Commit and push if changed
        run: |
          git config user.name "GitHub Actions Bot"
          git config user.email "actions@github.com"
          # Add all relevant generated files
          #git add *.db README.md charts/ data/
          git add data/
          git status
          # Commit only if there are staged changes, then push
          git diff --staged --quiet || git commit -m "Update economy data and analysis"
          git push
```

## Section 14: Data in Action

Let's look at what the actual fetched data looks like. The `data/keepers/` directory contains one JSON file per category, saved by the fetch script. Here's a sample from the currency data — the raw poe.ninja API response for currency items:

```bash
python3 -c "import json; d=json.load(open('data/keepers/currency.json')); print(json.dumps(d['lines'][:2], indent=2))"
```

```output
[
  {
    "currencyTypeName": "Divine Orb",
    "pay": {
      "id": 0,
      "league_id": 295,
      "pay_currency_id": 3,
      "get_currency_id": 1,
      "sample_time_utc": "2026-03-02T22:08:42.9300071Z",
      "count": 29,
      "value": 0.003262,
      "data_point_count": 1,
      "includes_secondary": false,
      "listing_count": 103
    },
    "receive": {
      "id": 0,
      "league_id": 295,
      "pay_currency_id": 1,
      "get_currency_id": 3,
      "sample_time_utc": "2026-03-02T22:08:42.9300071Z",
      "count": 10,
      "value": 260.0,
      "data_point_count": 1,
      "includes_secondary": false,
      "listing_count": 55
    },
    "paySparkLine": {
      "totalChange": 48.68,
      "data": [
        0,
        1.89,
        6.59,
        6.59,
        9.98,
        15.75,
        48.68
      ]
    },
    "receiveSparkLine": {
      "totalChange": 12.55,
      "data": [
        0,
        12.55,
        12.55,
        12.55,
        12.94,
        17.45,
        12.55
      ]
    },
    "chaosEquivalent": 306.6,
    "lowConfidencePaySparkLine": {
      "totalChange": 48.68,
      "data": [
        0,
        1.89,
        6.59,
        6.59,
        9.98,
        15.75,
        48.68
      ]
    },
    "lowConfidenceReceiveSparkLine": {
      "totalChange": 12.55,
      "data": [
        0,
        12.55,
        12.55,
        12.55,
        12.94,
        17.45,
        12.55
      ]
    },
    "detailsId": "divine-orb"
  },
  {
    "currencyTypeName": "Exalted Implant",
    "receive": {
      "id": 0,
      "league_id": 295,
      "pay_currency_id": 1,
      "get_currency_id": 264,
      "sample_time_utc": "2026-03-02T22:08:42.9300071Z",
      "count": 17,
      "value": 3.6,
      "data_point_count": 1,
      "includes_secondary": false,
      "listing_count": 2146
    },
    "paySparkLine": {
      "totalChange": 0,
      "data": []
    },
    "receiveSparkLine": {
      "totalChange": 16.13,
      "data": [
        0,
        -3.23,
        -3.23,
        -3.23,
        -35.48,
        -19.35,
        16.13
      ]
    },
    "chaosEquivalent": 3.6,
    "lowConfidencePaySparkLine": {
      "totalChange": 0,
      "data": []
    },
    "lowConfidenceReceiveSparkLine": {
      "totalChange": 16.13,
      "data": [
        0,
        -3.23,
        -3.23,
        -3.23,
        -35.48,
        -19.35,
        16.13
      ]
    },
    "detailsId": "exalted-implant"
  }
]
```

Notice the structure: each currency entry has `currencyTypeName` (not `name`), `chaosEquivalent` (not `chaosValue`), `detailsId` (not `id`), and separate `pay`/`receive` objects for the exchange rate in each direction. The `receive.value` for Divine Orb is 260.0 — meaning 1 Divine costs 260 Chaos — and `chaosEquivalent` is 306.6, reflecting the actual traded rate including spread.

The `paySparkLine` data array is a 7-point trend (totalChange: +48.68%) showing Divine Orb has nearly doubled in price recently. This sparkline data is fetched but not currently used by the analysis script.

Now let's look at a regular item (non-currency) entry for comparison:

```bash
python3 -c "import json; d=json.load(open('data/keepers/skill_gems.json')); entry=d['lines'][0]; print(json.dumps({k: entry[k] for k in ['name','id','chaosValue','divineValue','icon'] if k in entry}, indent=2))"
```

```output
{
  "name": "Volatility Support",
  "id": 106774,
  "chaosValue": 3761624,
  "divineValue": 9999,
  "icon": "https://web.poecdn.com/gen/image/WzI1LDE0LHsiZiI6IjJESXRlbXMvR2Vtcy9TdXBwb3J0L1ZvbGF0aWxpdHkiLCJ3IjoxLCJoIjoxLCJzY2FsZSI6MX1d/89efd990e3/Volatility.png"
}
```

Regular items use `name`, `id`, `chaosValue`, and `divineValue` — the different field names that `process_and_insert_data` detects via the `is_currency` flag. The top skill gem here — Volatility Support — is worth 3.7 million Chaos Orbs (capped at 9999 divine in the API), illustrating why the log scale is essential in the category chart.

## How It All Fits Together

Here's the complete data flow from API call to README update:

```
GitHub Actions (cron: */30 * * * *)
  └─ fetch_data.py
       ├─ For each of 17 categories:
       │   ├─ fetch_poe_ninja_data()  →  poe.ninja API
       │   ├─ sanitize_filename()     →  data/keepers/<category>.json
       │   └─ process_and_insert_data()
       │       ├─ Detect currency vs item fields
       │       ├─ Handle rate inversion for currency
       │       ├─ Compare with current_prices (change detection)
       │       ├─ INSERT price_history  (if changed)
       │       └─ UPSERT current_prices (if changed)
       └─ git add data/ && git commit && git push

(run locally or enable in workflow)
  └─ analysis.py
       ├─ get_latest_data_df()        →  SQL CTE with ROW_NUMBER window fn
       ├─ calculate_imputed_values()  →  unified chaos-equivalent column
       ├─ generate_analysis_content()
       │   ├─ Market movers (% change, top 10 each way)  →  charts/market_movers.png
       │   ├─ Top 10 most valuable items table
       │   └─ Category analysis (median, top item per cat) →  charts/category_analysis.png
       └─ update_readme()             →  regex injection into README.md markers
```

The two-tier database design — `current_prices` for fast reads, `price_history` as an append-only audit log — is the key architectural decision. It avoids writing duplicate rows every 30 minutes when prices are stable, while still preserving a complete change history for trend analysis.
