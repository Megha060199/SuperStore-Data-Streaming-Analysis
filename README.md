#  Order Data Analysis (Streams/Functional)

Pure-Python, stream-oriented analysis of Superstore-style order data (no pandas). Queries operate on iterators to avoid loading everything into memory, and helper aggregations keep the code DRY.

## Setup
- Require Python 3.10+ (tested with 3.13).
- Optional but recommended: create a virtualenv.
  ```python3 -m venv .venv
  source .venv/bin/activate
  ```
- Install deps (only coverage is non-stdlib):
  ```bash
  pip3 install -r requirements.txt
  ```

## Data
- Default dataset: `data/orders.csv` (Superstore-like). All queries default to this path unless you pass a custom one.

## Running analyses
-  Run the all analyses:
  ```bash
  python3 queries_run.py
  ```
- Key analytical functions (in `queries.py`):
  1) `sales_by_year_region_category(path)` – total sales per (Year, Market, Category).
  2) `yoy_category_sales_trends(path)` – Year by Year change per (Market, Category) in percentage.
  3) `profit_margin_by_category_subcategory(path)` – profit/sales per (Category, Sub-Category).
  4) `top_categories_by_margin(path, n=5)` – top N category/sub-category by margin (min-heap).
  5) `total_discounted_profit(path)` / `discounted_profit_share(path)` – profit from discounted orders and share of total profit.
  6) `average_fulfillment_days(path)` – mean days between Order.Date and Ship.Date.`
  7) ` profit volatility(profit std) and min, max, count per (Market, Category).`

- Console formatters live in `queries_run.py` (tables only; no logic).

## Unit Tests
- Run the unit suite (uses in-memory CSV via `StringIO` to avoid I/O):
  ```python3 -m unittest
  ```
- Coverage:
  ```bash
  python3 -m coverage run -m unittest
  python3 -m coverage report
  # optional HTML
  python3 -m coverage html
  ```

## Project structure
- `orders.py` – `Order` model, CSV streaming, datetime/product parsing.
- `grouping_aggregation_helpers.py` – reusable sum/mean/stddev/min-max-count by key.
- `queries.py` – analytical queries built on streams and helpers.
- `queries_run.py` – CLI-style output aggregating multiple reports.
- `test_queries_unittest.py` – `unittest` coverage for helpers, parsing, and queries (in-memory data).

## Notes
- Stream processing means large files are handled lazily; use `StringIO` in tests or pass file-like objects to `stream_orders`.
- Stddev defaults to sample (n-1); set `sample=False` if you need population metrics.
