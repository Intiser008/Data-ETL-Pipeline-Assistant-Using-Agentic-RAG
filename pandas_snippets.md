# Pandas ETL Snippets

Curated patterns for safe extract-transform-load operations used by the Agentic RAG Assistant. All paths are workspace-relative and write artifacts under `data/etl/processed/`. Replace placeholder variables (prefixed with colon-style `:param`) before execution. These snippets avoid destructive filesystem ops and limit in-memory previews with `.head()` or `.sample()`.

## File Ingestion

### Read CSV with Explicit Schema
```python
import pandas as pd

dtypes = {
    "Date": "string",
    "Ticker": "string",
    "Close": "float64",
    "Volume": "int64",
}
df = pd.read_csv("data/etl/raw_prices.csv", dtype=dtypes, parse_dates=["Date"])
df.head(5)
```

### Read Multiple CSV Files and Concatenate
```python
import pandas as pd
from pathlib import Path

files = sorted(Path("data/etl/daily_trades").glob("trades_*.csv"))
frames = [pd.read_csv(path, parse_dates=["timestamp"]) for path in files]
df = pd.concat(frames, ignore_index=True)
df.info()
```

### Read Parquet with Column Subset
```python
import pandas as pd

df = pd.read_parquet("data/etl/processed/positions_snapshot.parquet", columns=["portfolio_id", "ticker", "market_value"])
df.describe()
```

## Data Cleaning

### Rename Columns to Match Database Schema
```python
rename_map = {
    "Date": "date",
    "Adj Close": "adj_close",
    "Open": "open_price",
    "High": "high_price",
    "Low": "low_price",
    "Close": "close_price",
    "Volume": "volume",
}
prices = prices_raw.rename(columns=rename_map)
prices.columns
```

### Cast Types and Handle Missing Values
```python
prices = prices.assign(
    volume=prices["volume"].fillna(0).astype("int64"),
    close_price=prices["close_price"].astype("float64"),
)
prices = prices.dropna(subset=["date", "ticker"])
```

### Deduplicate on Business Keys
```python
prices = prices.sort_values(["ticker", "date"])
prices = prices.drop_duplicates(subset=["ticker", "date"], keep="last")
prices.shape
```

### Filter Date Range Safely
```python
filtered = prices.query("date >= @start_date and date <= @end_date").copy()
filtered.head()
```

## Derived Metrics

### Daily Returns
```python
prices = prices.sort_values(["ticker", "date"])
prices["return_1d"] = prices.groupby("ticker")["close_price"].pct_change().round(6)
```

### Rolling Volume Average
```python
prices["volume_avg_5"] = (
    prices.sort_values(["ticker", "date"])
    .groupby("ticker")["volume"]
    .transform(lambda s: s.rolling(window=5, min_periods=1).mean())
    .round(2)
)
```

### Trade Notional and Direction Flag
```python
trades = trades_raw.assign(
    notional=(trades_raw["qty"] * trades_raw["price"]).round(2),
    is_buy=trades_raw["qty"] > 0,
)
```

### Estimate Trade Costs with Basis Points
```python
bps_cost = 0.5  # 0.5 basis points
trades["trade_cost"] = (trades["notional"].abs() * bps_cost / 10000).round(2)
```

### Position Weights vs Portfolio Total
```python
totals = positions.groupby(["date", "portfolio_id"], as_index=False)["market_value"].sum().rename(columns={"market_value": "portfolio_total"})
positions = positions.merge(totals, on=["date", "portfolio_id"], how="left")
positions["weight"] = (positions["market_value"] / positions["portfolio_total"]).round(6)
```

### Compute Unrealized P&L From Cost Basis
```python
positions["unrealized_pnl"] = (positions["market_value"] - positions["cost_basis"]).round(2)
```

## Joins and Enrichment

### Enrich Prices with Ticker Metadata
```python
prices = prices.merge(tickers_df[["ticker", "sector"]], on="ticker", how="left")
```

### Combine Risk Metrics With Positions
```python
risk_positions = risk_metrics.merge(
    positions,
    on=["date", "portfolio_id"],
    how="left",
    suffixes=("_risk", "_pos"),
)
risk_positions.head()
```

### Lookup Prices for Trades (Same-Day Close)
```python
trades["trade_date"] = trades["timestamp"].dt.date
trades = trades.merge(
    prices[["date", "ticker", "close_price"]],
    left_on=["trade_date", "ticker"],
    right_on=["date", "ticker"],
    how="left",
    suffixes=("", "_close"),
)
```

## Output

### Write to Parquet with Compression
```python
output_path = "data/etl/processed/prices_enriched.parquet"
prices.to_parquet(output_path, index=False, compression="snappy")
output_path
```

### Write to CSV With Safe Index Handling
```python
output_path = "data/etl/processed/trades_enriched.csv"
trades.to_csv(output_path, index=False)
output_path
```

### Upsert to Database Using SQLAlchemy Engine (Read-Only Guards Off)
```python
from sqlalchemy import create_engine

engine = create_engine(":db_url")
rows_written = prices.to_sql(
    "prices",
    engine,
    schema="public",
    if_exists="append",
    index=False,
    method="multi",
    chunksize=500,
)
rows_written
```

> Note: The app enforces read-only mode for production execution; use `to_sql` only in controlled ETL jobs with explicit approval.

## Validation

### Assert Schema Before Load
```python
expected_cols = {"date", "ticker", "close_price", "volume", "open_price", "high_price", "low_price", "adj_close", "return_1d", "volume_avg_5"}
missing = expected_cols - set(prices.columns)
if missing:
    raise ValueError(f"Missing columns before load: {missing}")
```

### Row-Level Sanity Checks
```python
if (prices["volume"] < 0).any():
    raise ValueError("Negative volume detected")
if prices["return_1d"].abs().max() > 5:
    raise ValueError("Return outlier above 500% detected")
```

### Compare Totals With Source
```python
source_total = raw_positions["Quantity"].sum()
target_total = positions["quantity"].sum()
assert source_total == target_total, "Position share counts do not reconcile"
```

