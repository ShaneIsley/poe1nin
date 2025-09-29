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

def generate_market_analysis() -> tuple[str, str]:
    """
    Performs market analysis and generates markdown tables and a Plotly chart.
    Returns (markdown_tables, chart_path).
    """
    if not Path(DB_FILE).exists():
        return "No database file found.", ""

    with sqlite3.connect(DB_FILE) as conn:
        # This query gets the latest price and the price from ~24h ago for each item
        query = f"""
        WITH RankedPrices AS (
            SELECT
                i.name, c.name as category, p.chaos_value, p.timestamp,
                LAG(p.chaos_value, 1) OVER (PARTITION BY p.item_id ORDER BY p.timestamp) as prev_chaos_value,
                ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.timestamp DESC) as rn
            FROM price_entries p
            JOIN items i ON p.item_id = i.id
            JOIN item_categories c ON i.category_id = c.id
            WHERE p.timestamp >= DATETIME('now', '-2 days')
        )
        SELECT name, category, chaos_value, prev_chaos_value
        FROM RankedPrices
        WHERE rn = 1 AND prev_chaos_value IS NOT NULL AND chaos_value > 10;
        """
        df = pd.read_sql(query, conn)

    if df.empty:
        return "Not enough historical data to perform market analysis (requires at least two runs).", ""
        
    # --- Calculate Market Movers ---
    df['change'] = ((df['chaos_value'] - df['prev_chaos_value']) / df['prev_chaos_value']) * 100
    df = df.sort_values(by='change', ascending=False)
    
    top_gainers = df.head(10)
    top_losers = df.tail(10).sort_values(by='change', ascending=True)

    # --- Generate Plotly Chart ---
    movers_df = pd.concat([top_gainers, top_losers])
    
    fig = px.bar(
        movers_df,
        x='name',
        y='change',
        color='change',
        color_continuous_scale='RdYlGn',
        title='Top Market Movers (Last 24 Hours)',
        labels={'name': 'Item', 'change': '% Change in Chaos Value'},
        height=600
    )
    fig.update_layout(xaxis={'categoryorder':'total descending'})
    
    # Save chart as a static image
    charts_path = Path(CHARTS_DIR)
    charts_path.mkdir(exist_ok=True)
    chart_filepath = charts_path / "market_movers.png"
    fig.write_image(chart_filepath, width=1000, height=600)

    # --- Generate Markdown Tables ---
    def df_to_markdown(dataframe, headers):
        md = f"| {' | '.join(headers)} |\n"
        md += f"|{' :--- |' * len(headers)}\n"
        for _, row in dataframe.iterrows():
            md += f"| {' | '.join(map(str, row))} |\n"
        return md

    top_valuable = df.sort_values(by='chaos_value', ascending=False).head(10)[['name', 'chaos_value']]
    top_valuable['chaos_value'] = top_valuable['chaos_value'].round(1)

    gainers_table_df = top_gainers[['name', 'chaos_value', 'change']]
    gainers_table_df['chaos_value'] = gainers_table_df['chaos_value'].round(1)
    gainers_table_df['change'] = gainers_table_df['change'].round(1).astype(str) + '%'

    losers_table_df = top_losers[['name', 'chaos_value', 'change']]
    losers_table_df['chaos_value'] = losers_table_df['chaos_value'].round(1)
    losers_table_df['change'] = losers_table_df['change'].round(1).astype(str) + '%'

    markdown = "### Top 10 Most Valuable Items\n"
    markdown += df_to_markdown(top_valuable, ['Item', 'Chaos Value']) + "\n"
    markdown += "### Top 10 Gainers (24h)\n"
    markdown += df_to_markdown(gainers_table_df, ['Item', 'Chaos Value', '% Change']) + "\n"
    markdown += "### Top 10 Losers (24h)\n"
    markdown += df_to_markdown(losers_table_df, ['Item', 'Chaos Value', '% Change']) + "\n"
    
    return markdown, str(chart_filepath)

def update_readme(maintenance_md: str, market_md: str, chart_path: str):
    """Injects the analysis content into the README.md between markers."""
    try:
        with open(README_FILE, 'r') as f:
            readme_content = f.read()
    except FileNotFoundError:
        print(f"{README_FILE} not found. Please create it first.")
        return

    # Replace maintenance content
    new_content = re.sub(
        r"<!-- START_MAINTENANCE -->.*<!-- END_MAINTENANCE -->",
        f"<!-- START_MAINTENANCE -->\n{maintenance_md}\n<!-- END_MAINTENANCE -->",
        readme_content,
        flags=re.DOTALL
    )

    # Replace market analysis content
    chart_md = f"![Market Movers Chart]({chart_path})" if chart_path else ""
    full_market_content = f"{market_md}\n{chart_md}"
    new_content = re.sub(
        r"<!-- START_ANALYSIS -->.*<!-- END_ANALYSIS -->",
        f"<!-- START_ANALYSIS -->\n{full_market_content}\n<!-- END_ANALYSIS -->",
        new_content,
        flags=re.DOTALL
    )
    
    with open(README_FILE, 'w') as f:
        f.write(new_content)
    print(f"Successfully updated {README_FILE}")

if __name__ == "__main__":
    print("--- Starting Analysis ---")
    maintenance_table = generate_maintenance_table()
    market_tables, chart_file = generate_market_analysis()
    update_readme(maintenance_table, market_tables, chart_file)
    print("--- Analysis Complete ---")
