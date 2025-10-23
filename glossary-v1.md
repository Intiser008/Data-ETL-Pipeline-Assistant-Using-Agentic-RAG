# Business Glossary v1 — Finance Demo

Mappings from analyst-friendly terminology to SQL logic against the finance demo schema. Unless noted, filter placeholders such as `{start_date}`, `{end_date}`, `{ticker_list}` should be parameterized by the caller. All example queries are read-only and include `LIMIT 1000` when returning result sets.

### Daily Closing Price
- **Definition:** Closing equity price for a given ticker and date.
- **SQL logic:**  
  ```sql
  SELECT date, ticker, close_price
  FROM prices
  WHERE ticker = :ticker AND date = :as_of_date
  LIMIT 1000;
  ```

### Average Closing Price
- **Definition:** Mean closing price across a date window for each ticker.
- **SQL logic:**  
  ```sql
  SELECT ticker, AVG(close_price) AS avg_close_price
  FROM prices
  WHERE date BETWEEN :start_date AND :end_date
  GROUP BY ticker
  LIMIT 1000;
  ```

### 5-Day Moving Average Close
- **Definition:** Rolling 5-trading-day average close price per ticker.
- **SQL logic:**  
  ```sql
  SELECT
      ticker,
      date,
      AVG(close_price) OVER (
          PARTITION BY ticker ORDER BY date
          ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      ) AS ma_5
  FROM prices
  WHERE ticker = :ticker
  LIMIT 1000;
  ```

### Daily Trading Volume
- **Definition:** Shares traded on a specific date for each ticker.
- **SQL logic:**  
  ```sql
  SELECT date, ticker, volume
  FROM prices
  WHERE date = :as_of_date
  LIMIT 1000;
  ```

### Average Daily Volume (30 Days)
- **Definition:** Mean of the last 30 trading days' volumes per ticker.
- **SQL logic:**  
  ```sql
  SELECT ticker, AVG(volume) AS avg_volume_30d
  FROM prices
  WHERE date BETWEEN :start_date AND :end_date
  GROUP BY ticker
  LIMIT 1000;
  ```

### Total Volume by Sector
- **Definition:** Aggregate traded volume for tickers grouped by sector over a window.
- **SQL logic:**  
  ```sql
  SELECT t.sector, SUM(p.volume) AS total_volume
  FROM prices p
  JOIN tickers t ON p.ticker = t.ticker
  WHERE p.date BETWEEN :start_date AND :end_date
  GROUP BY t.sector
  LIMIT 1000;
  ```

### Top Movers by Close Change
- **Definition:** Tickers with largest day-over-day close price change.
- **SQL logic:**  
  ```sql
  SELECT ticker,
         date,
         close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY date) AS close_change
  FROM prices
  WHERE date BETWEEN :start_date AND :end_date
  ORDER BY ABS(close_change) DESC
  LIMIT 1000;
  ```

### Daily Notional Traded
- **Definition:** Total dollar amount traded per ticker per day.
- **SQL logic:**  
  ```sql
  SELECT
      DATE(timestamp) AS trade_date,
      ticker,
      SUM(qty * price) AS notional_usd
  FROM trades
  WHERE timestamp BETWEEN :start_ts AND :end_ts
  GROUP BY trade_date, ticker
  LIMIT 1000;
  ```

### Net Trade Flow
- **Definition:** Net shares bought minus sold per ticker.
- **SQL logic:**  
  ```sql
  SELECT ticker, SUM(qty) AS net_shares
  FROM trades
  WHERE timestamp BETWEEN :start_ts AND :end_ts
  GROUP BY ticker
  LIMIT 1000;
  ```

### Trade Count by Trader
- **Definition:** Number of executions each trader performed in a window.
- **SQL logic:**  
  ```sql
  SELECT trader_id, COUNT(*) AS trade_count
  FROM trades
  WHERE timestamp BETWEEN :start_ts AND :end_ts
  GROUP BY trader_id
  LIMIT 1000;
  ```

### Average Execution Price
- **Definition:** Volume-weighted average execution price per trader and ticker.
- **SQL logic:**  
  ```sql
  SELECT trader_id,
         ticker,
         SUM(qty * price) / NULLIF(SUM(qty), 0) AS vwap
  FROM trades
  WHERE timestamp BETWEEN :start_ts AND :end_ts
  GROUP BY trader_id, ticker
  LIMIT 1000;
  ```

### Portfolio Holdings
- **Definition:** Position quantities and market values per portfolio on a date.
- **SQL logic:**  
  ```sql
  SELECT portfolio_id, ticker, quantity, market_value
  FROM positions
  WHERE date = :as_of_date
  LIMIT 1000;
  ```

### Portfolio Market Value
- **Definition:** Total market value for a portfolio on a given date.
- **SQL logic:**  
  ```sql
  SELECT portfolio_id, SUM(market_value) AS total_market_value
  FROM positions
  WHERE date = :as_of_date
  GROUP BY portfolio_id
  LIMIT 1000;
  ```

### Position Weight
- **Definition:** Share of portfolio market value contributed by each ticker.
- **SQL logic:**  
  ```sql
  WITH totals AS (
      SELECT portfolio_id, SUM(market_value) AS portfolio_value
      FROM positions
      WHERE date = :as_of_date
      GROUP BY portfolio_id
  )
  SELECT p.portfolio_id,
         p.ticker,
         p.market_value / NULLIF(t.portfolio_value, 0) AS weight
  FROM positions p
  JOIN totals t ON p.portfolio_id = t.portfolio_id
  WHERE p.date = :as_of_date
  LIMIT 1000;
  ```

### Exposure by Sector
- **Definition:** Sum of market value per sector within a portfolio on a date.
- **SQL logic:**  
  ```sql
  SELECT p.portfolio_id, t.sector, SUM(p.market_value) AS sector_exposure
  FROM positions p
  JOIN tickers t ON p.ticker = t.ticker
  WHERE p.date = :as_of_date
  GROUP BY p.portfolio_id, t.sector
  LIMIT 1000;
  ```

### Trader Daily P&L
- **Definition:** Realized and unrealized profit & loss per trader per day.
- **SQL logic:**  
  ```sql
  SELECT date, trader_id, realized_pnl, unrealized_pnl
  FROM pnl
  WHERE date BETWEEN :start_date AND :end_date
  LIMIT 1000;
  ```

### Total Daily P&L
- **Definition:** Aggregated realized plus unrealized P&L per day across traders.
- **SQL logic:**  
  ```sql
  SELECT date,
         SUM(realized_pnl) AS total_realized_pnl,
         SUM(unrealized_pnl) AS total_unrealized_pnl,
         SUM(realized_pnl + unrealized_pnl) AS total_pnl
  FROM pnl
  WHERE date BETWEEN :start_date AND :end_date
  GROUP BY date
  LIMIT 1000;
  ```

### Trader P&L Ranking
- **Definition:** Top or bottom traders by cumulative P&L over a window.
- **SQL logic:**  
  ```sql
  SELECT trader_id,
         SUM(realized_pnl + unrealized_pnl) AS cumulative_pnl
  FROM pnl
  WHERE date BETWEEN :start_date AND :end_date
  GROUP BY trader_id
  ORDER BY cumulative_pnl DESC
  LIMIT 1000;
  ```

### Portfolio VAR (95%)
- **Definition:** 95% Value-at-Risk reported for a portfolio on a date.
- **SQL logic:**  
  ```sql
  SELECT date, portfolio_id, var_95
  FROM risk_metrics
  WHERE date BETWEEN :start_date AND :end_date
  LIMIT 1000;
  ```

### Portfolio Beta
- **Definition:** Portfolio beta relative to benchmark per risk snapshot.
- **SQL logic:**  
  ```sql
  SELECT date, portfolio_id, beta
  FROM risk_metrics
  WHERE portfolio_id = :portfolio_id
  ORDER BY date
  LIMIT 1000;
  ```

### Net Market Exposure
- **Definition:** Net exposure amount reported for a portfolio.
- **SQL logic:**  
  ```sql
  SELECT date, portfolio_id, exposure
  FROM risk_metrics
  WHERE date BETWEEN :start_date AND :end_date
  LIMIT 1000;
  ```

### Correlating Risk and Exposure
- **Definition:** Combine risk metrics with positions to compare VAR and holdings.
- **SQL logic:**  
  ```sql
  SELECT r.date,
         r.portfolio_id,
         r.var_95,
         r.exposure,
         SUM(p.market_value) AS total_market_value
  FROM risk_metrics r
  JOIN positions p
    ON r.portfolio_id = p.portfolio_id AND r.date = p.date
  WHERE r.date BETWEEN :start_date AND :end_date
  GROUP BY r.date, r.portfolio_id, r.var_95, r.exposure
  LIMIT 1000;
  ```

### Sector Allocation vs VAR
- **Definition:** Sector breakdown of portfolios alongside Value-at-Risk.
- **SQL logic:**  
  ```sql
  SELECT r.date,
         r.portfolio_id,
         t.sector,
         SUM(p.market_value) AS sector_value,
         MAX(r.var_95) AS var_95
  FROM risk_metrics r
  JOIN positions p
    ON r.portfolio_id = p.portfolio_id AND r.date = p.date
  JOIN tickers t ON p.ticker = t.ticker
  WHERE r.date = :as_of_date
  GROUP BY r.date, r.portfolio_id, t.sector
  LIMIT 1000;
  ```

### Active Ticker List
- **Definition:** All tickers with metadata (name, sector).
- **SQL logic:**  
  ```sql
  SELECT ticker, name, sector
  FROM tickers
  ORDER BY ticker
  LIMIT 1000;
  ```

### Traders Directory
- **Definition:** Trader identifiers mapped to names.
- **SQL logic:**  
  ```sql
  SELECT trader_id, name
  FROM traders
  ORDER BY trader_id
  LIMIT 1000;
  ```

### Portfolios Directory
- **Definition:** Portfolio identifiers and display names.
- **SQL logic:**  
  ```sql
  SELECT portfolio_id, name
  FROM portfolios
  ORDER BY portfolio_id
  LIMIT 1000;
  ```

### Daily Price Return
- **Definition:** Day-over-day percentage change in closing price.
- **SQL logic:**  
  ```sql
  SELECT
      ticker,
      date,
      (close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY date))
        / NULLIF(LAG(close_price) OVER (PARTITION BY ticker ORDER BY date), 0) AS daily_return
  FROM prices
  WHERE ticker = :ticker
  LIMIT 1000;
  ```

### Maximum Drawdown (Approximate)
- **Definition:** Largest peak-to-trough decline over a window, approximated via running max.
- **SQL logic:**  
  ```sql
  SELECT
      ticker,
      date,
      close_price,
      close_price / NULLIF(MAX(close_price) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0) - 1 AS drawdown
  FROM prices
  WHERE date BETWEEN :start_date AND :end_date
  LIMIT 1000;
  ```

### Volume Spike Detection
- **Definition:** Days where volume exceeds 150% of 30-day average.
- **SQL logic:**  
  ```sql
  SELECT
      ticker,
      date,
      volume,
      AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_volume_30d
  FROM prices
  WHERE volume > 1.5 * AVG(volume) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
  LIMIT 1000;
  ```

### Trade Participation by Sector
- **Definition:** Proportion of trade notional per sector.
- **SQL logic:**  
  ```sql
  WITH sector_notional AS (
      SELECT t.sector, SUM(tr.qty * tr.price) AS sector_notional
      FROM trades tr
      JOIN tickers t ON tr.ticker = t.ticker
      WHERE tr.timestamp BETWEEN :start_ts AND :end_ts
      GROUP BY t.sector
  )
  SELECT sector,
         sector_notional,
         sector_notional / NULLIF(SUM(sector_notional) OVER (), 0) AS sector_share
  FROM sector_notional
  LIMIT 1000;
  ```

### Trader Win Rate
- **Definition:** Fraction of trading days with positive total P&L for each trader.
- **SQL logic:**  
  ```sql
  WITH daily_totals AS (
      SELECT date,
             trader_id,
             (realized_pnl + unrealized_pnl) AS total_pnl
      FROM pnl
      WHERE date BETWEEN :start_date AND :end_date
  )
  SELECT trader_id,
         AVG(CASE WHEN total_pnl > 0 THEN 1 ELSE 0 END) AS win_rate
  FROM daily_totals
  GROUP BY trader_id
  LIMIT 1000;
  ```

### Portfolio Risk-Adjusted Return
- **Definition:** Ratio of total P&L to Value-at-Risk per portfolio.
- **SQL logic:**  
  ```sql
  SELECT
      r.date,
      r.portfolio_id,
      SUM(pn.realized_pnl + pn.unrealized_pnl) AS total_pnl,
      AVG(r.var_95) AS avg_var_95,
      SUM(pn.realized_pnl + pn.unrealized_pnl) / NULLIF(AVG(r.var_95), 0) AS pnl_to_var
  FROM risk_metrics r
  JOIN pnl pn
    ON pn.date = r.date
  WHERE r.date BETWEEN :start_date AND :end_date
  GROUP BY r.date, r.portfolio_id
  LIMIT 1000;
  ```

### Trader Exposure Coverage
- **Definition:** Align trader P&L with portfolio holdings if trader is assigned to a portfolio.
- **SQL logic:**  
  ```sql
  SELECT pn.date,
         pn.trader_id,
         SUM(pn.realized_pnl + pn.unrealized_pnl) AS trader_pnl,
         SUM(pos.market_value) AS portfolio_market_value
  FROM pnl pn
  JOIN trades tr ON pn.trader_id = tr.trader_id AND DATE(tr.timestamp) = pn.date
  JOIN positions pos ON tr.ticker = pos.ticker AND pos.date = pn.date
  WHERE pn.date BETWEEN :start_date AND :end_date
  GROUP BY pn.date, pn.trader_id
  LIMIT 1000;
  ```

### Price vs Position Coverage
- **Definition:** Compare closing prices to position valuations for a given portfolio.
- **SQL logic:**  
  ```sql
  SELECT
      pos.date,
      pos.portfolio_id,
      pos.ticker,
      pos.quantity,
      pos.market_value,
      pr.close_price
  FROM positions pos
  JOIN prices pr
    ON pos.ticker = pr.ticker AND pos.date = pr.date
  WHERE pos.portfolio_id = :portfolio_id
    AND pos.date BETWEEN :start_date AND :end_date
  LIMIT 1000;
  ```

