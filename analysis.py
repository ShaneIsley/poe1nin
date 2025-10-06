import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
import re

# --- CONFIGURATION ---
DB_FILE = "poe1_economy.db"
LEAGUE_NAME = "Mercenaries" # Should match the name in fetch_data.py
CHARTS_DIR = "charts"
README_FILE = "README.md"

def get_latest_data_df(conn: sqlite3.Connection) -> pd.DataFrame:
    """Fetches the latest and previous price for all items from the last 2 days."""
    query = """
    WITH PriceHistory AS (
        SELECT
            p.item_id, p.timestamp,
            p.chaos_value, p.divine_value,
            LAG(p.chaos_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_chaos_value,
            LAG(p.divine_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_divine_value,
            ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn
        FROM price_entries p
        JOIN leagues l ON p.league_id = l.id
        WHERE l.name = ? AND p.timestamp >= DATETIME('now', '-2 days')
    ),
    LatestPrices AS (
        SELECT * FROM PriceHistory WHERE rn = 1
    )
    SELECT
        i.name, c.name AS category,
        lp.chaos_value, lp.divine_value,
        lp.prev_chaos_value, lp.prev_divine_value
    FROM LatestPrices lp
    JOIN items i ON lp.item_id = i.id
    JOIN item_categories c ON i.category_id = c.id;
    """
    return pd.read_sql(query, conn, params=(LEAGUE_NAME,))

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

def generate_maintenance_table() -> str:
    """Creates a markdown table with script maintenance info."""
    if not Path(DB_FILE).exists(): return "No database file found."
    with sqlite3.connect(DB_FILE) as conn:
        try:
            latest_run_time = pd.read_sql("SELECT MAX(timestamp) as last_run FROM price_entries", conn).iloc[0]['last_run']
            total_rows = pd.read_sql("SELECT COUNT(*) as count FROM price_entries", conn).iloc[0]['count']
        except (pd.io.sql.DatabaseError, IndexError):
            return "Database is empty or corrupt."
    table = "| Metric | Value |\n|:---|:---|\n"
    table += f"| Last Successful Run (UTC) | `{latest_run_time}` |\n"
    table += f"| Total Price Entries in DB | `{total_rows:,}` |\n"
    return table

def df_to_markdown(dataframe: pd.DataFrame, headers: list) -> str:
    """Converts a pandas DataFrame to a GitHub-flavored markdown table."""
    md = f"| {' | '.join(headers)} |\n"
    md += f"|{' :--- |' * len(headers)}\n"
    for _, row in dataframe.iterrows():
        md += f"| {' | '.join(map(str, row))} |\n"
    return md

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
            fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change', color_continuous_scale='RdYlGn', title='Top Market Movers (Last ~24 Hours)', labels={'name': 'Item', 'change': '% Change in Chaos Value'})
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

if __name__ == "__main__":
    print("--- Starting Analysis ---")
    maintenance_table = generate_maintenance_table()
    try:
        conn = sqlite3.connect(DB_FILE)
        df_raw = get_latest_data_df(conn)
        conn.close()
        
        if not df_raw.empty:
            df_imputed = calculate_imputed_values_poe1(df_raw)
            market_movers_markdown, category_markdown, movers_chart, category_chart = generate_analysis_content(df_imputed)
            update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
        else:
            print("Database has no recent data to analyze.")
            update_readme(maintenance_table, "Database is empty or has no recent data.", "Skipping analysis.", "", "")
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
        update_readme(maintenance_table, f"An error occurred during analysis: {e}", "", "", "")
    print("--- Analysis Complete ---")
