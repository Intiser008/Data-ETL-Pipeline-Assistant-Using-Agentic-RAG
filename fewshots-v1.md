# Few-Shot NL ↔ SQL Examples (Finance Demo)

Canonically answered questions to guide the LLM when translating natural-language prompts. All SQL statements are read-only and capped with `LIMIT 1000`.

---

**Q1.** *“Show me the closing price and volume for AAPL on 2024-01-10.”*  
```sql
SELECT date, ticker, close_price, volume
FROM prices
WHERE ticker = 'AAPL'
  AND date = DATE '2024-01-10'
LIMIT 1000;
```

---

**Q2.** *“List the average closing price for each ticker between 2024-01-01 and 2024-01-15.”*  
```sql
SELECT ticker, AVG(close_price) AS avg_close_price
FROM prices
WHERE date BETWEEN DATE '2024-01-01' AND DATE '2024-01-15'
GROUP BY ticker
ORDER BY ticker
LIMIT 1000;
```

---

**Q3.** *“Which sector traded the most volume during the first week of January 2024?”*  
```sql
SELECT t.sector, SUM(p.volume) AS total_volume
FROM prices p
JOIN tickers t ON p.ticker = t.ticker
WHERE p.date BETWEEN DATE '2024-01-02' AND DATE '2024-01-08'
GROUP BY t.sector
ORDER BY total_volume DESC
LIMIT 1000;
```

---

**Q4.** *“Give me daily net shares bought or sold for NVDA from Jan 5 to Jan 12.”*  
```sql
SELECT DATE(timestamp) AS trade_date,
       SUM(qty) AS net_shares
FROM trades
WHERE ticker = 'NVDA'
  AND timestamp BETWEEN TIMESTAMP '2024-01-05 00:00:00'
                      AND TIMESTAMP '2024-01-12 23:59:59'
GROUP BY trade_date
ORDER BY trade_date
LIMIT 1000;
```

---

**Q5.** *“What was trader 101’s total notional traded in January 2024?”*  
```sql
SELECT SUM(qty * price) AS trader_notional_usd
FROM trades
WHERE trader_id = 101
  AND timestamp BETWEEN TIMESTAMP '2024-01-01 00:00:00'
                      AND TIMESTAMP '2024-01-31 23:59:59'
LIMIT 1000;
```

---

**Q6.** *“Return portfolio 201 holdings (quantity and market value) on 2024-01-09.”*  
```sql
SELECT ticker, quantity, market_value
FROM positions
WHERE portfolio_id = 201
  AND date = DATE '2024-01-09'
ORDER BY ticker
LIMIT 1000;
```

---

**Q7.** *“Compute total market value per portfolio on 2024-01-11.”*  
```sql
SELECT portfolio_id, SUM(market_value) AS total_market_value
FROM positions
WHERE date = DATE '2024-01-11'
GROUP BY portfolio_id
ORDER BY total_market_value DESC
LIMIT 1000;
```

---

**Q8.** *“Show realized vs unrealized P&L for each trader between Jan 3 and Jan 10.”*  
```sql
SELECT date,
       trader_id,
       realized_pnl,
       unrealized_pnl,
       realized_pnl + unrealized_pnl AS total_pnl
FROM pnl
WHERE date BETWEEN DATE '2024-01-03' AND DATE '2024-01-10'
ORDER BY date, trader_id
LIMIT 1000;
```

---

**Q9.** *“Rank traders by cumulative P&L during the first half of January 2024.”*  
```sql
SELECT trader_id,
       SUM(realized_pnl + unrealized_pnl) AS cumulative_pnl
FROM pnl
WHERE date BETWEEN DATE '2024-01-01' AND DATE '2024-01-15'
GROUP BY trader_id
ORDER BY cumulative_pnl DESC
LIMIT 1000;
```

---

**Q10.** *“Provide VAR, beta, and exposure for each portfolio on 2024-01-08.”*  
```sql
SELECT portfolio_id, var_95, beta, exposure
FROM risk_metrics
WHERE date = DATE '2024-01-08'
ORDER BY portfolio_id
LIMIT 1000;
```

---

**Q11.** *“Combine risk metrics with portfolio market value for Jan 7 through Jan 9.”*  
```sql
SELECT r.date,
       r.portfolio_id,
       r.var_95,
       r.beta,
       r.exposure,
       SUM(p.market_value) AS total_market_value
FROM risk_metrics r
JOIN positions p
  ON r.portfolio_id = p.portfolio_id
 AND r.date = p.date
WHERE r.date BETWEEN DATE '2024-01-07' AND DATE '2024-01-09'
GROUP BY r.date, r.portfolio_id, r.var_95, r.beta, r.exposure
ORDER BY r.date, r.portfolio_id
LIMIT 1000;
```

---

**Q12.** *“For each ticker, calculate the day-over-day close price change between Jan 4 and Jan 12.”*  
```sql
SELECT ticker,
       date,
       close_price,
       close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY date) AS close_change
FROM prices
WHERE date BETWEEN DATE '2024-01-04' AND DATE '2024-01-12'
ORDER BY ticker, date
LIMIT 1000;
```

---

**Q13.** *“Find the top 5 sector allocations for portfolio 202 on 2024-01-10.”*  
```sql
SELECT t.sector,
       SUM(p.market_value) AS sector_market_value
FROM positions p
JOIN tickers t ON p.ticker = t.ticker
WHERE p.portfolio_id = 202
  AND p.date = DATE '2024-01-10'
GROUP BY t.sector
ORDER BY sector_market_value DESC
LIMIT 5;
```

---

**Q14.** *“List trades for META on Jan 6 ordered by execution time.”*  
```sql
SELECT trade_id, timestamp, qty, price, trader_id
FROM trades
WHERE ticker = 'META'
  AND timestamp BETWEEN TIMESTAMP '2024-01-06 00:00:00'
                      AND TIMESTAMP '2024-01-06 23:59:59'
ORDER BY timestamp
LIMIT 1000;
```

---

**Q15.** *“Show price vs position market value for portfolio 203 between Jan 8 and Jan 12.”*  
```sql
SELECT p.date,
       p.ticker,
       p.quantity,
       p.market_value,
       pr.close_price
FROM positions p
JOIN prices pr
  ON p.ticker = pr.ticker
 AND p.date = pr.date
WHERE p.portfolio_id = 203
  AND p.date BETWEEN DATE '2024-01-08' AND DATE '2024-01-12'
ORDER BY p.date, p.ticker
LIMIT 1000;
```

