
import heapq
from collections import defaultdict
from grouping_aggregation_helpers import (
    aggregate_mean_by_key,
    aggregate_min_max_count_by_key,
    aggregate_stddev_by_key,
    aggregate_sum_by_key,
)
from orders import stream_orders        
from pathlib import Path
from typing import Dict, Optional

## this file contains all analysis methods that process orders data and give desired output.
def sales_by_year_region_category(path: str | Path) -> Dict[tuple[int, str, str], float]:
    """
    Compute total sales per (Year, Region, Category).
    Demonstrates composite key grouping using FP-style pipelines.
    """
    return aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.year, o.market, o.category),
        value_fn=lambda o: o.sales,
    )

def yoy_category_sales_trends(path: str | Path) -> Dict[tuple[str, str], list[tuple[int, float, float, float]]]:
    """
    Compute YOY (year-over-year) rise/drop for each (Region, Category).

    Returns:
        {
           (region, category): [
               (year, sales, prev_year_sales, yoy_change),
               ...
           ]
        }
    """
    # 1. Get raw totals per (year, region, category)
    totals = sales_by_year_region_category(path)

    # 2. Reorganize by (region, category)
    grouped: Dict[tuple[str, str], list[tuple[int, float]]] = defaultdict(list)

    for (year, region, category), sales in totals.items():
        grouped[(region, category)].append((year, sales))

    # 3. Sort years and compute YOY change
    yoy_results: Dict[tuple[str, str], list[tuple[int, float, float, float]]] = {}

    for key, entries in grouped.items():
        # Sort by year
        entries = sorted(entries, key=lambda x: x[0])

        trends = []
        prev_sales = None

        for year, sales in entries:
            if prev_sales is None:
                trends.append((year, sales, None, None))  # First year → no YOY comparison
            else:
                yoy_change = ( (sales - prev_sales) / prev_sales)  if prev_sales != 0 else None
                trends.append((year, sales, prev_sales, yoy_change))

            prev_sales = sales

        yoy_results[key] = trends

    return yoy_results

def profit_margin_by_category_subcategory(path: str | Path) -> Dict[tuple[str, str], float | None]:
    """
    Compute profit margin (profit / sales) per (Category, Sub-Category).

    Returns a dict of {(category, sub_category): margin}. If total sales is 0, margin is None.
    """
    total_sales = aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.category, o.sub_category),
        value_fn=lambda o: o.sales,
    )
    total_profit = aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.category, o.sub_category),
        value_fn=lambda o: o.profit,
    )

    margins: Dict[tuple[str, str], float | None] = {}
    for key, sales in total_sales.items():
        profit = total_profit.get(key, 0.0)
        margins[key] = (profit / sales) if sales else None

    return margins

def top_categories_by_margin(path: str | Path, n: int = 5) -> list[tuple[str, str, float]]:
    """
    Return the top N (category, sub-category) pairs by profit margin (profit / sales).
    Ignores entries with zero sales (margin is undefined).
    """
    total_sales = aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.category, o.sub_category),
        value_fn=lambda o: o.sales,
    )
    total_profit = aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.category, o.sub_category),
        value_fn=lambda o: o.profit,
    )

    heap: list[tuple[float, str, str]] = []  # (margin, category, sub_category)
    for (category, sub_category), sales in total_sales.items():
        if not sales:
            continue
        margin = total_profit.get((category, sub_category), 0.0) / sales
        heapq.heappush(heap, (margin, category, sub_category))
        if len(heap) > n:
            heapq.heappop(heap)  # keep only top n by popping smallest

    top = sorted(heap, key=lambda x: x[0], reverse=True)
    return [(category, sub_category, margin) for margin, category, sub_category in top]

def total_discounted_profit(path: str | Path) -> float:
    """
    Total profit from orders where a discount was applied.
    """
    totals = aggregate_sum_by_key(
        filter(lambda o: o.discount > 0, stream_orders(path)),
        key_fn=lambda _: "all",
        value_fn=lambda o: o.profit,
    )
    return totals.get("all", 0.0)


def discounted_profit_share(path: str | Path) -> Optional[float]:
    """
    Share of total profit that comes from discounted orders.

    Returns:
        float in [0,1] or None if total profit is zero (undefined share).
    """
    discounted = total_discounted_profit(path)
    total = aggregate_sum_by_key(
        stream_orders(path),
        key_fn=lambda _: "all",
        value_fn=lambda o: o.profit,
    ).get("all", 0.0)

    return (discounted / total) if total else None

def average_fulfillment_days(path: str | Path) -> Optional[float]:
    """
    Average fulfillment time in days: mean(Ship.Date - Order.Date).
    Returns None if no orders have both dates.
    """
    with_dates = filter(lambda o: o.order_date and o.ship_date, stream_orders(path))
    mean_map = aggregate_mean_by_key(
        with_dates,
        key_fn=lambda _: "all",
        value_fn=lambda o: (o.ship_date - o.order_date).total_seconds() / 86400.0,
    )
    return mean_map.get("all")



def profit_std_by_market_category(path: str | Path, sample: bool = True) -> Dict[tuple[str, str], float]:
    """
    Standard deviation of profit per (Market, Category).
    """
    return aggregate_stddev_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.market, o.category),
        value_fn=lambda o: o.profit,
        sample=sample,
    )


def profit_min_max_count_by_market_category(path: str | Path) -> Dict[tuple[str, str], tuple[float, float, int]]:
    """
    Min, max, count of profit per (Market, Category).
    """
    return aggregate_min_max_count_by_key(
        stream_orders(path),
        key_fn=lambda o: (o.market, o.category),
        value_fn=lambda o: o.profit,
    )

