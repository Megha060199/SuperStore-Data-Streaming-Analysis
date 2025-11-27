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

        
          - Identify high-performing regions and categories to focus marketing and supply-chain resources
          -  Detect underperforming markets requiring pricing changes, better distribution, or promotions.


  2) `yoy_category_sales_trends(path)` – Year by Year change per (Market, Category) in percentage.
             
             -  Detect growth or decline in specific markets or product categories.
             -   Support goal tracking, because YoY is a key KPI in performance dashboards.

  3) `profit_margin_by_category_subcategory(path)` – profit/sales per (Category, Sub-Category).
              -  Discover high-margin vs. low-margin product segments.
              -  Inform inventory and catalog decisions: remove low-margin items that occupy storage but don’t drive profit.
              
  4) `top_categories_by_margin(path, n=5)` – top N category/sub-category by margin (min-heap).

              - Highlight the most profitable categories for targeted advertising and homepage placemen
              - Support profit maximization by optimizing product mix.

  5) `total_discounted_profit(path)` / `discounted_profit_share(path)` – profit from discounted orders and total share of total profit.


             -   Evaluate whether discount strategies are profitable or harming margins.
             -   Detect margin leakage from overly aggressive discounting.


  6) `average_fulfillment_days(path)` – mean days between Order.Date and Ship.Date.`


            - Measure operational efficiency of supply chain and warehouse processes.
            - Evaluate carrier performance and SLA compliance.
            - Predict customer satisfaction (shipping delays → low ratings & high returns).


  7) ` profit volatility(profit std) and min, max, count per (Market, Category).`
  
          - Inform product managers which categories need stabilization strategies (pricing, inventory).
          - Help supply chain plan safety stock levels for unstable categories.

- Console formatters live in `queries_run.py` (tables only; no logic).

## Unit Tests
- Run the unit suite (uses in-memory CSV via `StringIO` to avoid I/O):
  ```bash
  python3 -m unittest
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
- Output is added in Analyses-Results.pdf. 
- Unit test coverage is added in Unit Test Coverage.png
- Stream processing means large files are handled lazily; use `StringIO` in tests or pass file-like objects to `stream_orders`.
- Stddev defaults to sample (n-1); set `sample=False` if you need population metrics.

## Additional notes to make it production grade 
-  Input validation & error handling: Wrap Order.from_row parsing with schema checks, helpful error messages (column + value), and a strategy for bad rows (skip-with-log vs fail-fast). Validate numeric conversions and date parsing instead of silent None.

-  Single-pass aggregations: Current queries reread the CSV multiple times; refactor to either share a cached stream (noting iterator exhaustion) or build combined aggregations in one pass per execution. For large files this is a meaningful perf win.

- Numerical semantics: Decide on sample stddev behavior for single observations; either return None/nan or document the current 0. Align with business definitions for YOY edge cases

- Testing: Add integration tests over a real sample file, and tests for failure paths (bad date, missing column, zero sales) to ensure validation and logging work. Include property-based or fuzz tests for aggregations if feasible.

- Testing: Add integration tests over a real sample file, and tests for failure paths (bad date, missing column, zero sales) to ensure validation and logging work. Include property-based or fuzz tests for aggregations if feasible.
