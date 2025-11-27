

from pathlib import Path
from typing import Iterable, Sequence, Tuple


## This file just runs all queries and prints formatted tables to console.
from queries import (
    discounted_profit_share,
    profit_margin_by_category_subcategory,
    profit_min_max_count_by_market_category,
    sales_by_year_region_category,
    top_categories_by_margin,
    total_discounted_profit,
    profit_std_by_market_category,
    average_fulfillment_days,
    yoy_category_sales_trends,
)

CSV_PATH = Path("data/orders.csv")


def print_table(title: str, headers: Sequence[str], rows: Iterable[Sequence[str]]) -> None:
    rows = list(rows)
    print(title)
    if not rows:
        print("(no rows)\n")
        return

    col_widths = [len(h)+15 for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    fmt = " ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("-" * (sum(col_widths) + len(col_widths) - 1))
    for row in rows:
        print(fmt.format(*row))
    print()


def print_sales(path: Path = CSV_PATH) -> None:
    sales_summary = sales_by_year_region_category(path)
    rows = [
        (str(year), region, category, f"{total_sales:,.2f}")
        for (year, region, category), total_sales in sorted(sales_summary.items())
    ]
    print_table(
        "Sales by Year / Region / Category",
        ("Year", "Region", "Category", "Total Sales"),
        rows,
    )


def print_yoy(path: Path = CSV_PATH) -> None:
    yoy_data = yoy_category_sales_trends(path)
    rows = []
    for (region, category), entries in sorted(yoy_data.items()):
        for year, sales, prev_sales, change in entries:
            prev_fmt = "-" if prev_sales is None else f"{prev_sales:,.2f}"
            change_fmt = "-" if change is None else f"{change*100:,.2f}%"
            rows.append(
                (
                    region,
                    category,
                    str(year),
                    f"{sales:,.2f}",
                    prev_fmt,
                    change_fmt,
                )
            )

    print_table(
        "YOY Category Sales Trends",
        ("Region", "Category", "Year", "Sales", "Prev Sales", "YOY Change"),
        rows,
    )


def print_margins(path: Path = CSV_PATH) -> None:
    margins = profit_margin_by_category_subcategory(path)
    rows = [
        (category, sub_category, "N/A" if margin is None else f"{margin*100:,.2f}%")
        for (category, sub_category), margin in sorted(
            margins.items(),
            key=lambda kv: (kv[1] if kv[1] is not None else -float("inf")),
            reverse=True,
        )
    ]
    print_table(
        "Profit Margin by Category / Sub-Category",
        ("Category", "Sub-Category", "Margin"),
        rows,
    )


def print_top_category_margins(path: Path = CSV_PATH, n: int = 5) -> None:
    top = top_categories_by_margin(path, n)
    rows = [
        (category, sub_category, f"{margin*100:,.2f}%")
        for category, sub_category, margin in top
    ]
    print_table(
        f"Top {n} Category/Sub-Category by Profit Margin",
        ("Category", "Sub-Category", "Margin"),
        rows,
    )

def print_discounted_profit(path: Path = CSV_PATH) -> None:
    discounted = total_discounted_profit(path)
    share = discounted_profit_share(path)
    share_fmt = "N/A" if share is None else f"{share*100:,.2f}%"
    print_table(
        "Profit from Discounted Orders",
        ("Metric", "Value"),
        [
            ("Total Discounted Profit", f"{discounted:,.2f}"),
            ("Share of Total Profit", share_fmt),
        ],
    )

def print_average_fulfillment(path: Path = CSV_PATH) -> None:
    avg_days = average_fulfillment_days(path)
    avg_fmt = "N/A" if avg_days is None else f"{avg_days:.2f} days"
    print_table(
        "Average Fulfillment Time",
        ("Metric", "Value"),
        [("Avg (Ship.Date - Order.Date)", avg_fmt)],
    )

def print_profit_volatility(path: Path = CSV_PATH, sample: bool = True) -> None:
    stds = profit_std_by_market_category(path, sample=sample)
    ranges = profit_min_max_count_by_market_category(path)
    sorted_items = sorted(
        stds.items(),
        key=lambda kv: kv[1],  # sort by stddev
        reverse=True,
    )
    rows = []
    for (market, category), std_dev in sorted_items:
        min_p, max_p, count = ranges.get((market, category), (0, 0, 0))
        rows.append(
            (
                market,
                category,
                f"{std_dev:,.4f}",
                f"{min_p:,.2f}",
                f"{max_p:,.2f}",
                str(count),
            )
        )
    print_table(
        f"Profit StdDev by Market / Category ({'sample' if sample else 'population'})",
        ("Market", "Category", "Profit StdDev", "Min Profit", "Max Profit", "Count"),
        rows,
    )


def run_all(path: Path = CSV_PATH) -> None:
    print_sales(path)
    print_yoy(path)
    print_margins(path)
    print_top_category_margins(path)
    print_discounted_profit(path)
    print_average_fulfillment(path)
    print_profit_volatility(path)


if __name__ == "__main__":
    run_all()
