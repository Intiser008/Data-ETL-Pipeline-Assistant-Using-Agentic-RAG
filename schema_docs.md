# Finance Demo SQL Schema

This document captures the relational structure and key business semantics of the finance demo database loaded from `finance_demo_seed.sql`. It is intended for retrieval-augmented generation workflows so natural-language prompts can quickly ground themselves in the available tables, columns, and safe join patterns.

## Schema Overview

- 8 tables model market data, executed trades, daily positions, trader P&L, and portfolio risk metrics.
- Referential integrity is enforced through foreign keys; all analytical queries should join on the declared keys rather than string matching.
- Data coverage:
  - `prices`: 1,440 daily close snapshots per ticker spanning **2024-01-02 → 2024-07-19**.
  - `trades`: 1,000 intraday executions across 10 tickers **2024-01-02 → 2024-07-19**.
  - `positions`: 1,500 end-of-day holdings for three portfolios **2024-01-02 → 2024-07-24**.
  - `pnl`: 720 daily realized/unrealized P&L entries for five traders **2024-01-02 → 2024-07-19**.
  - `risk_metrics`: 321 portfolio risk snapshots **2024-01-02 → 2024-10-24**.

| Table          | Purpose                                                | Rows | Primary Key           | Foreign Keys                                                                 |
| -------------- | ------------------------------------------------------ | ---- | --------------------- | ---------------------------------------------------------------------------- |
| `tickers`      | Master list of traded symbols                          | 10   | `ticker`              | —                                                                            |
| `traders`      | Traders executing orders                               | 5    | `trader_id`           | —                                                                            |
| `portfolios`   | Portfolios for position/risk reporting                 | 3    | `portfolio_id`        | —                                                                            |
| `prices`       | Daily close price and volume by ticker                 | 1,440| `id` (surrogate)      | `ticker → tickers.ticker`                                                    |
| `trades`       | Executed trades with price/qty and responsible trader  | 1,000| `trade_id` (surrogate)| `ticker → tickers.ticker`, `trader_id → traders.trader_id`                   |
| `positions`    | End-of-day holdings with market value                  | 1,500| `id` (surrogate)      | `portfolio_id → portfolios.portfolio_id`, `ticker → tickers.ticker`          |
| `pnl`          | Daily realized/unrealized P&L per trader               | 720  | `id` (surrogate)      | `trader_id → traders.trader_id`                                              |
| `risk_metrics` | Daily portfolio risk (VaR, beta, exposure) snapshots   | 321  | `id` (surrogate)      | `portfolio_id → portfolios.portfolio_id`                                     |

## Entity & Join Notes

- **Ticker-centric analytics**: join `prices`, `trades`, and `positions` through `ticker`.
- **Trader performance**: link `trades` and `pnl` through `trader_id`; aggregate on `date` or rolling windows.
- **Portfolio analytics**: `positions` and `risk_metrics` share `date` + `portfolio_id`; join to compare exposure vs risk.
- All fact tables (`prices`, `trades`, `positions`, `pnl`, `risk_metrics`) reference a conformed date dimension via the `date` column; ensure date filters use ISO `YYYY-MM-DD` strings.
- Surrogate integer primary keys (`id`, `trade_id`) simplify row-level auditing but are rarely needed in analytical queries.

## Table Dictionary

### tickers

- **Description**: Dimensions for publicly traded instruments.
- **Row count**: 10
- **Keys**: `ticker` (PK)

| Column | Type    | Notes                                                        |
| ------ | ------- | ------------------------------------------------------------ |
| `ticker` | VARCHAR | Primary identifier (e.g., `AAPL`, `MSFT`).                  |
| `name`   | TEXT    | Company name.                                               |
| `sector` | TEXT    | Sector classification used for grouping and filtering.      |

**Usage Patterns**
- Join to any fact table on `ticker` for sector-based aggregations.
- Filter analytics to a subset of symbols or sectors.

### traders

- **Description**: Traders responsible for executions and P&L.
- **Row count**: 5
- **Keys**: `trader_id` (PK)

| Column       | Type | Notes                                           |
| ------------ | ---- | ----------------------------------------------- |
| `trader_id`  | INT  | Primary identifier (101–105).                   |
| `name`       | TEXT | Trader display name.                            |

**Usage Patterns**
- Join to `trades` for execution attribution.
- Join to `pnl` for realized/unrealized tracking.

### portfolios

- **Description**: Portfolio dimension referenced by positions and risk.
- **Row count**: 3 (`201–203`)
- **Keys**: `portfolio_id` (PK)

| Column         | Type | Notes                        |
| -------------- | ---- | ---------------------------- |
| `portfolio_id` | INT  | Portfolio identifier.        |
| `name`         | TEXT | Friendly label for reporting.|

**Usage Patterns**
- Join to `positions` and `risk_metrics` on `portfolio_id`.
- Use for portfolio-level rollups and comparisons.

### prices

- **Description**: Daily closing metrics by ticker.
- **Row count**: 1,440 (144 days × 10 tickers)
- **Date coverage**: 2024-01-02 → 2024-07-19
- **Keys**: `id` (PK), `ticker` FK → `tickers`

| Column        | Type            | Notes                                                                           |
| ------------- | --------------- | --------------------------------------------------------------------------------|
| `id`          | SERIAL          | Surrogate primary key; not needed in analytics.                                 |
| `date`        | DATE            | Trade date of the close price.                                                  |
| `ticker`      | VARCHAR         | Symbol reference (FK).                                                          |
| `close_price` | NUMERIC(10,2)   | End-of-day close price in USD.                                                  |
| `volume`      | INT             | Daily trading volume (shares).                                                  |

**Usage Patterns**
- Compute daily/rolling averages, percent changes, or volatility per ticker.
- Join with `positions` for mark-to-market analyses (`positions.quantity * prices.close_price`).
- Aggregate volume by sector through `tickers`.

### trades

- **Description**: Executed orders with timestamp, quantity, and price.
- **Row count**: 1,000
- **Timestamp coverage**: 2024-01-02 13:29 → 2024-07-19 16:28 (US market hours)
- **Keys**: `trade_id` (PK), `ticker` FK → `tickers`, `trader_id` FK → `traders`

| Column       | Type            | Notes                                                            |
| ------------ | --------------- | ---------------------------------------------------------------- |
| `trade_id`   | SERIAL          | Unique identifier per execution.                                |
| `timestamp`  | TIMESTAMP       | Execution time (assume UTC).                                    |
| `ticker`     | VARCHAR         | Symbol traded (FK).                                             |
| `qty`        | INT             | Shares traded (positive values imply buy; no short flag in schema). |
| `price`      | NUMERIC(10,2)   | Execution price in USD.                                         |
| `trader_id`  | INT             | Trader responsible (FK).                                        |

**Usage Patterns**
- Aggregate daily volume per trader (`SUM(qty)`) or dollar notional (`SUM(qty * price)`).
- Join to `prices` to compare execution price vs close price (slippage).
- Join to `pnl` on `trader_id` + `DATE(timestamp)` for attribution.

### positions

- **Description**: End-of-day holdings per portfolio and ticker.
- **Row count**: 1,500
- **Date coverage**: 2024-01-02 → 2024-07-24
- **Keys**: `id` (PK), `portfolio_id` FK → `portfolios`, `ticker` FK → `tickers`

| Column        | Type            | Notes                                                           |
| ------------- | --------------- | --------------------------------------------------------------- |
| `id`          | SERIAL          | Surrogate primary key.                                         |
| `date`        | DATE            | As-of date of the position snapshot.                           |
| `portfolio_id`| INT             | Portfolio reference (FK).                                      |
| `ticker`      | VARCHAR         | Symbol held (FK).                                               |
| `quantity`    | INT             | Shares held (long only in seed data).                           |
| `market_value`| NUMERIC(12,2)   | USD market value at snapshot (quantity × price).               |

**Usage Patterns**
- Roll up daily exposure by portfolio or sector.
- Join to `prices` on `date` + `ticker` to recompute market value or apply different price metrics.
- Join to `risk_metrics` on `date` + `portfolio_id` to compare exposure vs VaR.

### pnl

- **Description**: Daily realized and unrealized P&L by trader.
- **Row count**: 720
- **Date coverage**: 2024-01-02 → 2024-07-19
- **Keys**: `id` (PK), `trader_id` FK → `traders`

| Column          | Type            | Notes                                                          |
| --------------- | --------------- | -------------------------------------------------------------- |
| `id`            | SERIAL          | Surrogate primary key.                                        |
| `date`          | DATE            | P&L date.                                                      |
| `trader_id`     | INT             | Trader reference (FK).                                         |
| `realized_pnl`  | NUMERIC(12,2)   | Realized profit/loss in USD.                                   |
| `unrealized_pnl`| NUMERIC(12,2)   | Mark-to-market gain/loss in USD.                               |

**Usage Patterns**
- Combine with `trades` to attribute P&L to execution activity.
- Compute cumulative P&L per trader or compare realized vs unrealized components.
- Use in guardrails to ensure SQL always groups by `trader_id` when aggregating P&L.

### risk_metrics

- **Description**: Portfolio risk measures (95% VaR, beta, exposure).
- **Row count**: 321
- **Date coverage**: 2024-01-02 → 2024-10-24
- **Keys**: `id` (PK), `portfolio_id` FK → `portfolios`

| Column        | Type            | Notes                                                        |
| ------------- | --------------- | ------------------------------------------------------------ |
| `id`          | SERIAL          | Surrogate primary key.                                      |
| `date`        | DATE            | Risk snapshot date.                                         |
| `portfolio_id`| INT             | Portfolio reference (FK).                                   |
| `var_95`      | NUMERIC(12,2)   | 95% historical Value-at-Risk (USD).                         |
| `beta`        | NUMERIC(6,3)    | Portfolio beta vs benchmark.                                |
| `exposure`    | NUMERIC(12,2)   | Total portfolio notional exposure (USD).                    |

**Usage Patterns**
- Trend portfolio risk through time; compare VaR vs exposure.
- Join to `positions` to reconcile holdings with risk metrics.
- Filter on `beta` or `var_95` thresholds for compliance dashboards.

## Query Building Blocks

- **Typical joins**:
  - Prices ↔ Positions: `positions.date = prices.date AND positions.ticker = prices.ticker`.
  - Positions ↔ Risk: `positions.date = risk_metrics.date AND positions.portfolio_id = risk_metrics.portfolio_id`.
  - Trades ↔ PnL: `DATE(trades.timestamp) = pnl.date AND trades.trader_id = pnl.trader_id`.
  - Trades ↔ Prices: `DATE(trades.timestamp) = prices.date AND trades.ticker = prices.ticker`.
- **Aggregations**:
  - Daily volume or notional: `SUM(qty)` or `SUM(qty * price)` grouped by `DATE(timestamp), ticker`.
  - Rolling price stats: use window functions on `prices` (e.g., 5-day moving average).
  - Portfolio exposure: `SUM(positions.market_value)` grouped by `date, portfolio_id`.
  - Risk-adjusted exposure: `AVG(risk_metrics.var_95 / NULLIF(risk_metrics.exposure,0))`.
- **Guardrails**:
  - Always include `LIMIT 1000` in generated SQL (aligns with executor policy).
  - Use only read-only operations (`SELECT`, `WITH`, CTEs).
  - Reference explicit column names; avoid `SELECT *` to keep responses compact.

This documentation should be ingested into the RAG index so the agent can cite precise tables, columns, and join keys when turning natural-language prompts into SQL.
