# analysis.py
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
import re
from datetime import datetime

# --- CONFIGURATION ---
# IMPORTANT: Make sure this matches the DB_FILE in your fetch_data.py
# For PoE 1 repo:
DB_FILE = "poe1_economy.db"
# For PoE 2 repo:
# DB_FILE = "poe_economy.db" 

CHARTS_DIR = "charts"
README_FILE = "README.md"
LEAGUE_NAME = "Mercenaries" #

CHARTS_DIR = "charts"
README_FILE = "README.md"

def generate_maintenance_table() -> str:
    """Creates a markdown table with script maintenance info."""
    if not Path(DB_FILE).exists():
        return "No database file found."
        
    with sqlite3.connect(DB_FILE) as conn:
        latest_run_time = pd.read_sql("SELECT MAX(timestamp) as last_run FROM price_entries", conn).iloc[0]['last_run']
        total_rows = pd.read_sql("SELECT COUNT(*) as count FROM price_entries", conn).iloc[0]['count']

    table = "| Metric | Value |\n"
    table += "|:---|:---|\n"
    table += f"| Last Successful Run (UTC) | `{latest_run_time}` |\n"
    table += f"| Total Price Entries in DB | `{total_rows:,}` |\n"
    return table

def generate_market_analysis() -> tuple[str, str, str, str]:
    """
    Performs market analysis and generates markdown/charts.
    Returns (market_movers_md, category_md, movers_chart_path, category_chart_path).
    """
    if not Path(DB_FILE).exists():
        return "No database file found.", "", "", ""

    with sqlite3.connect(DB_FILE) as conn:
        query = f"""
        WITH RankedPrices AS (
            SELECT
                p.item_id, i.name, c.name as category, p.chaos_value, p.timestamp,
                LAG(p.chaos_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_chaos_value,
                ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn
            FROM price_entries p
            JOIN items i ON p.item_id = i.id
            JOIN item_categories c ON i.category_id = c.id
            WHERE p.timestamp >= DATETIME('now', '-2 days') AND p.chaos_value IS NOT NULL
        )
        SELECT name, category, chaos_value, prev_chaos_value
        FROM RankedPrices
        WHERE rn = 1;
        """
        df = pd.read_sql(query, conn)

    if df.empty:
        return "Not enough historical data.", "Requires at least two runs.", "", ""
    
    charts_path = Path(CHARTS_DIR)
    charts_path.mkdir(exist_ok=True)
    
    # --- Helper function for markdown tables ---
    def df_to_markdown(dataframe, headers):
        md = f"| {' | '.join(headers)} |\n"
        md += f"|{' :--- |' * len(headers)}\n"
        for _, row in dataframe.iterrows():
            md += f"| {' | '.join(map(str, row))} |\n"
        return md

    # === Part 1: Market Movers Analysis ===
    df_movers = df[df['prev_chaos_value'].notna() & (df['chaos_value'] > 10)].copy()
    df_movers['change'] = ((df_movers['chaos_value'] - df_movers['prev_chaos_value']) / df_movers['prev_chaos_value']) * 100
    df_movers = df_movers.sort_values(by='change', ascending=False)
    
    top_gainers = df_movers.head(10)
    top_losers = df_movers.tail(10).sort_values(by='change', ascending=True)

    # Generate Market Movers Plotly Chart
    movers_chart_df = pd.concat([top_gainers, top_losers])
    fig_movers = px.bar(movers_chart_df, x='name', y='change', color='change',
                        color_continuous_scale='RdYlGn', title='Top Market Movers (Last 24 Hours)',
                        labels={'name': 'Item', 'change': '% Change in Chaos Value'})
    fig_movers.update_layout(xaxis={'categoryorder':'total descending'})
    movers_chart_path = charts_path / "market_movers.png"
    fig_movers.write_image(movers_chart_path, width=1000, height=600)

    # Generate Market Movers Markdown
    top_valuable = df.sort_values(by='chaos_value', ascending=False).head(10)[['name', 'chaos_value']]
    top_valuable['chaos_value'] = top_valuable['chaos_value'].round(1)
    
    market_movers_md = "### Top 10 Most Valuable Items (Overall)\n"
    market_movers_md += df_to_markdown(top_valuable, ['Item', 'Chaos Value'])
    
    if not top_gainers.empty:
        gainers_table_df = top_gainers[['name', 'chaos_value', 'change']].copy()
        gainers_table_df['change'] = gainers_table_df['change'].round(1).astype(str) + '%'
        market_movers_md += "\n### Top 10 Gainers (24h)\n"
        market_movers_md += df_to_markdown(gainers_table_df, ['Item', 'Chaos Value', '% Change'])
    
    if not top_losers.empty:
        losers_table_df = top_losers[['name', 'chaos_value', 'change']].copy()
        losers_table_df['change'] = losers_table_df['change'].round(1).astype(str) + '%'
        market_movers_md += "\n### Top 10 Losers (24h)\n"
        market_movers_md += df_to_markdown(losers_table_df, ['Item', 'Chaos Value', '% Change'])

    # === Part 2: Category Analysis ===
    # Find the most valuable item in each category
    top_item_per_category = df.loc[df.groupby('category')['chaos_value'].idxmax()]
    top_item_per_category = top_item_per_category.sort_values(by='chaos_value', ascending=False)
    top_item_per_category = top_item_per_category[['category', 'name', 'chaos_value']].head(15)
    top_item_per_category['chaos_value'] = top_item_per_category['chaos_value'].round(1)

    # Calculate median value per category
    median_by_category = df.groupby('category')['chaos_value'].median().sort_values(ascending=False).reset_index()
    
    # Generate Category Value Plotly Chart
    fig_category = px.bar(median_by_category.head(20), x='category', y='chaos_value',
                          title='Median Item Value by Category (Top 20)', log_y=True,
                          labels={'category': 'Item Category', 'chaos_value': 'Median Chaos Value (Log Scale)'})
    category_chart_path = charts_path / "category_analysis.png"
    fig_category.write_image(category_chart_path, width=1000, height=600)
    
    # Generate Category Markdown
    category_md = "### Most Valuable Item by Category\n"
    category_md += df_to_markdown(top_item_per_category, ['Category', 'Top Item', 'Chaos Value'])
    
    return market_movers_md, category_md, str(movers_chart_path), str(category_chart_path)

def update_readme(maintenance_md: str, market_md: str, category_md: str, movers_chart_path: str, category_chart_path: str):
    """Injects all analysis content into the README.md between markers."""
    try:
        with open(README_FILE, 'r') as f:
            readme_content = f.read()
    except FileNotFoundError:
        # Create a basic README if it doesn't exist
        readme_content = f"# PoE Economy Tracker for {LEAGUE_NAME}\n\n<!-- START_MAINTENANCE -->\n<!-- END_MAINTENANCE -->\n\n<!-- START_ANALYSIS -->\n<!-- END_ANALYSIS -->\n\n<!-- START_CATEGORY_ANALYSIS -->\n<!-- END_CATEGORY_ANALYSIS -->"
        print(f"{README_FILE} not found. A new one will be created.")

    # Inject maintenance content
    new_content = re.sub(
        r"<!-- START_MAINTENANCE -->.*<!-- END_MAINTENANCE -->",
        f"<!-- START_MAINTENANCE -->\n{maintenance_md}\n<!-- END_MAINTENANCE -->",
        readme_content, flags=re.DOTALL)

    # Inject market movers content
    movers_chart_md = f"![Market Movers Chart]({movers_chart_path})" if movers_chart_path else ""
    full_market_content = f"{market_md}\n\n{movers_chart_md}"
    new_content = re.sub(
        r"<!-- START_ANALYSIS -->.*<!-- END_ANALYSIS -->",
        f"<!-- START_ANALYSIS -->\n{full_market_content}\n<!-- END_ANALYSIS -->",
        new_content, flags=re.DOTALL)

    # Inject category analysis content
    category_chart_md = f"![Category Analysis Chart]({category_chart_path})" if category_chart_path else ""
    full_category_content = f"{category_md}\n\n{category_chart_md}"
    new_content = re.sub(
        r"<!-- START_CATEGORY_ANALYSIS -->.*<!-- END_CATEGORY_ANALYSIS -->",
        f"<!-- START_CATEGORY_ANALYSIS -->\n{full_category_content}\n<!-- END_CATEGORY_ANALYSIS -->",
        new_content, flags=re.DOTALL)
    
    with open(README_FILE, 'w') as f:
        f.write(new_content)
    print(f"Successfully updated {README_FILE}")

if __name__ == "__main__":
    print("--- Starting Analysis ---")
    maintenance_table = generate_maintenance_table()
    market_movers_markdown, category_markdown, movers_chart, category_chart = generate_market_analysis()
    update_readme(maintenance_table, market_movers_markdown, category_markdown, movers_chart, category_chart)
    print("--- Analysis Complete ---")
    
