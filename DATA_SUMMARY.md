# MAOF DuckDB Data Summary

> Auto-generated: 2026-03-26. Source: `~/fg3-data/maof.duckdb` (52.9 GiB).
> Materialized on 2024-03-24 from Hive-partitioned Parquet.

---

## Overview

The database contains **intraday tick-level data** for **TA-35 monthly options** traded on the TASE MAOF market. It spans **7 years** (2016--2023, excluding 2014--2015 and 2022) across **1,719 trading days**.


| Table       | Rows          | Description                                                  |
| ----------- | ------------- | ------------------------------------------------------------ |
| `trades`    | 61,675,705    | Every executed trade (price, quantity, direction, order IDs) |
| `quotes`    | 1,087,341,126 | 5-layer bid/ask order-book snapshots with underlying index   |
| `_metadata` | 1             | Materialization timestamp and years included                 |


**Years included:** 2016, 2017, 2018, 2019, 2020, 2021, 2023

**Gap:** 2014--2015 and 2022 are not materialized (raw data may exist but was not ingested into this DuckDB).

---

## Date Coverage


| Year | Trading Days (Trades) | Trading Days (Quotes) |
| ---- | --------------------- | --------------------- |
| 2016 | 245                   | 243                   |
| 2017 | 245                   | 245                   |
| 2018 | 245                   | 245                   |
| 2019 | 244                   | 244                   |
| 2020 | 248                   | 248                   |
| 2021 | 243                   | 244                   |
| 2023 | 249                   | 249                   |


- **Earliest date:** 2016-01-03
- **Latest date:** 2023-12-31
- Each year has 12 monthly expiration cycles fully covered.

---

## Trades

### Volume by Year


| Year | Trades     | Avg Daily Trades | Total Volume (contracts) | Avg Daily Volume |
| ---- | ---------- | ---------------- | ------------------------ | ---------------- |
| 2016 | 11,953,740 | ~48,791          | 29,599,880               | ~120,816         |
| 2017 | 8,662,752  | ~35,358          | 24,557,814               | ~100,236         |
| 2018 | 9,904,522  | ~40,427          | 24,446,104               | ~99,780          |
| 2019 | 7,517,533  | ~30,810          | 16,370,443               | ~67,092          |
| 2020 | 9,431,003  | ~38,028          | 18,936,465               | ~76,357          |
| 2021 | 7,500,549  | ~30,866          | 16,449,869               | ~67,695          |
| 2023 | 6,705,606  | ~26,930          | 15,303,048               | ~61,458          |


**Trend:** Trade activity declined significantly from 2016 to 2023. Average daily volume fell from ~121K contracts (2016) to ~61K (2023) -- roughly a 50% decline. The 2020 COVID-era spike temporarily reversed the trend.

### Call vs Put Volume


| Year | Calls      | Puts       | Put Share |
| ---- | ---------- | ---------- | --------- |
| 2016 | 15,074,344 | 14,525,536 | 49.1%     |
| 2017 | 12,059,215 | 12,498,599 | 50.9%     |
| 2018 | 11,691,255 | 12,754,849 | 52.2%     |
| 2019 | 7,524,040  | 8,846,403  | 54.0%     |
| 2020 | 8,658,635  | 10,277,830 | 54.3%     |
| 2021 | 7,554,925  | 8,894,944  | 54.1%     |
| 2023 | 7,240,150  | 8,062,898  | 52.7%     |


**Puts dominate** (53.3% overall). The market is structurally put-heavy, consistent with hedging demand from institutional long equity portfolios. The skew intensified from ~50/50 in 2016 to 54% puts by 2019--2021.

### Moneyness Distribution (All Years)


| Moneyness Bucket     | Trades     | Share |
| -------------------- | ---------- | ----- |
| ATM (< 2% from spot) | 35,518,429 | 57.7% |
| Near-ATM (2--5%)     | 17,188,458 | 27.9% |
| OTM (5--10%)         | 6,898,497  | 11.2% |
| Deep OTM (> 10%)     | 1,920,661  | 3.1%  |


Nearly 86% of all trades occur within 5% of the current spot level.

### Distinct Contracts by Year


| Year | Contracts |
| ---- | --------- |
| 2016 | 1,669     |
| 2017 | 1,494     |
| 2018 | 1,686     |
| 2019 | 1,593     |
| 2020 | 1,785     |
| 2021 | 1,684     |
| 2023 | 2,109     |


The wider strike range in 2020 and 2023 (more contracts) reflects higher volatility and a broader tradeable surface.

### Trading Session

- **Open:** 09:30 (shifted to 09:45 in 2021+)
- **Close:** 17:35 (continuous trading with closing auction)
- Timestamps are centisecond precision (HHMMSSCC).

### Top 10 Busiest Days


| Date       | Trades  | Volume  |
| ---------- | ------- | ------- |
| 2020-03-25 | 140,013 | 221,106 |
| 2020-02-26 | 137,058 | 281,942 |
| 2016-01-26 | 133,805 | 367,963 |
| 2016-01-14 | 127,841 | 311,821 |
| 2020-03-24 | 124,935 | 228,007 |
| 2016-01-27 | 121,793 | 302,002 |
| 2018-02-06 | 119,263 | 311,568 |
| 2016-02-24 | 118,530 | 300,737 |
| 2016-02-18 | 118,125 | 287,983 |
| 2018-12-25 | 117,087 | 317,303 |


COVID crash (Mar 2020) and early-2016 selloffs dominate the top. Feb 2018 (global vol spike) also stands out.

---

## Quotes (Order Book Snapshots)

### Volume by Year


| Year | Quote Updates | Avg Daily Updates |
| ---- | ------------- | ----------------- |
| 2016 | 150,978,867   | ~621,312          |
| 2017 | 100,913,132   | ~411,890          |
| 2018 | 130,136,066   | ~531,168          |
| 2019 | 134,034,517   | ~549,322          |
| 2020 | 188,412,371   | ~759,727          |
| 2021 | 149,589,088   | ~613,070          |
| 2023 | 233,277,085   | ~936,856          |


**2023 has the most quote activity** (~937K updates/day), reflecting increased algorithmic market-making activity despite lower trade volumes -- the market became more electronically quoted but less human-traded.

### Quote Depth

All years have **100% 5-layer** bid/ask depth. No 3-layer legacy records are present (those existed only in early 2014, before the coverage window of this DB).

### Bid-Ask Spreads (Level 1)


| Year | Avg Spread (ILS) | Median Spread (ILS) | Avg Relative Spread |
| ---- | ---------------- | ------------------- | ------------------- |
| 2016 | 65               | 20                  | 4.3%                |
| 2017 | 53               | 20                  | 5.5%                |
| 2018 | 98               | 20                  | 8.4%                |
| 2019 | 69               | 20                  | 7.5%                |
| 2020 | 141              | 30                  | 6.9%                |
| 2021 | 75               | 30                  | 7.6%                |
| 2023 | 107              | 40                  | 8.5%                |


**Spreads have widened over time.** Median spread doubled from 20 to 40 ILS; the average relative spread rose from ~4.3% to ~8.5%. The 2020 spike (avg 141 ILS) reflects the COVID volatility regime. The high means vs low medians indicate heavy right-skew from illiquid OTM options.

### Option Type Distribution in Quotes


| Type  | Rows        | Share |
| ----- | ----------- | ----- |
| Puts  | 622,991,775 | 57.3% |
| Calls | 464,349,351 | 42.7% |


Puts have disproportionately more quote updates than their trade share (57.3% vs 53.3%), suggesting market-makers quote puts more actively (likely driven by continuous hedging demand).

### Underlying Index (TA-35 Cash)


| Year | Min      | Max      | Avg      |
| ---- | -------- | -------- | -------- |
| 2016 | 1,361.58 | 1,552.74 | 1,444.63 |
| 2017 | 1,330.66 | 1,534.08 | 1,427.08 |
| 2018 | 1,360.68 | 1,708.94 | 1,547.31 |
| 2019 | 1,446.79 | 1,710.99 | 1,585.23 |
| 2020 | 1,142.16 | 1,751.79 | 1,423.71 |
| 2021 | 1,485.18 | 1,995.95 | 1,720.44 |
| 2023 | 1,565.72 | 1,967.25 | 1,782.34 |


- The 2020 low of **1,142** marks the COVID crash bottom.
- The index rose from ~1,445 (2016) to ~1,782 (2023) -- a ~23% increase over the period.
- **Zero null values** across all years: the spot-enrichment pipeline fully cleaned the underlying index.

### Strike Ranges


| Year | Min Strike | Max Strike | Distinct Strikes |
| ---- | ---------- | ---------- | ---------------- |
| 2016 | 1,110      | 1,890      | 79               |
| 2017 | 1,100      | 1,810      | 71               |
| 2018 | 1,120      | 2,010      | 90               |
| 2019 | 1,140      | 2,040      | 91               |
| 2020 | 940        | 2,100      | 117              |
| 2021 | 1,060      | 2,320      | 120              |
| 2023 | 1,290      | 2,360      | 101              |


The 2020 crash pushed the lower-bound strike to **940**, and post-2021 recovery extended the upper range to **2,360**.

---

## Monthly Seasonality (Avg Trade Count)


| Month | Avg Trades |
| ----- | ---------- |
| Jan   | 854,800    |
| Feb   | 833,000    |
| Mar   | 834,074    |
| Apr   | 653,736    |
| May   | 750,484    |
| Jun   | 679,569    |
| Jul   | 695,910    |
| Aug   | 763,539    |
| Sep   | 601,783    |
| Oct   | 621,967    |
| Nov   | 768,102    |
| Dec   | 753,853    |


**Q1 is the most active quarter.** September and October are the quietest (Jewish holiday season: Rosh Hashanah, Yom Kippur, Sukkot).

---

## Liquidity Metrics


| Year | Avg Trades per Contract per Day |
| ---- | ------------------------------- |
| 2016 | 308.2                           |
| 2017 | 271.9                           |
| 2018 | 279.1                           |
| 2019 | 219.4                           |
| 2020 | 250.0                           |
| 2021 | 219.0                           |
| 2023 | 178.4                           |


Per-contract liquidity has been declining steadily, from ~308 trades/contract/day in 2016 to ~178 in 2023. This reflects both the decline in overall volume and the expansion of the tradeable strike surface.

---

## Key Takeaways

1. **Scale:** 1.15 billion rows across trades + quotes. This is a serious research-grade tick dataset.
2. **Coverage gap:** 2014--2015 and 2022 are missing from the materialized DB.
3. **Declining volume:** Trade activity halved from 2016 to 2023, while quote activity increased -- the market is increasingly electronic/algo-driven.
4. **Put-heavy market:** Consistent 53--54% put share in trades, reflecting structural hedging demand.
5. **ATM concentration:** 86% of trades are within 5% of spot, typical for a liquid index-options market.
6. **Widening spreads:** Despite more quoting, spreads have widened, suggesting reduced competition among market makers or regulatory changes.
7. **Clean data:** Zero null underlying-index values after the enrichment pipeline, across all 1.09B quote rows.
8. **COVID signature:** 2020 shows the largest spreads, highest trade counts, widest strike ranges, and the deepest drawdown (TA-35 to 1,142).
9. **Holiday seasonality:** September--October are notably quieter (Jewish holidays), while Q1 sees the most activity.
10. **Session shift:** Trading session open moved from 09:30 to 09:45 starting in 2021.

