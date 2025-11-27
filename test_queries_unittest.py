import unittest
import tempfile
from io import StringIO
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from grouping_aggregation_helpers import (
    aggregate_mean_by_key,
    aggregate_stddev_by_key,
    aggregate_sum_by_key,
    aggregate_min_max_count_by_key,
)
from orders import Order, stream_orders
from queries import (
    sales_by_year_region_category,
    yoy_category_sales_trends,
    profit_margin_by_category_subcategory,
    top_categories_by_margin,
    total_discounted_profit,
    discounted_profit_share,
    average_fulfillment_days,
    profit_std_by_market_category,
    profit_min_max_count_by_market_category,
)
#this file contains unit tests for the data processing and querying functions.

class HelperAggregationTests(unittest.TestCase):
    def setUp(self):
        self.orders = [
            Order(
                category="Furniture",
                sub_category="Chairs",
                state="CA",
                country="USA",
                customer_id="C1",
                customer_name="Alice",
                year=2020,
                market="US",
                sales=100.0,
                profit=10.0,
                discount=0.0,
                quantity=2,
                shipping_cost=5.0,
            ),
            Order(
                category="Furniture",
                sub_category="Tables",
                state="CA",
                country="USA",
                customer_id="C2",
                customer_name="Bob",
                year=2020,
                market="US",
                sales=200.0,
                profit=20.0,
                discount=0.1,
                quantity=1,
                shipping_cost=7.0,
            ),
            Order(
                category="Office Supplies",
                sub_category="Paper",
                state="NY",
                country="USA",
                customer_id="C3",
                customer_name="Eve",
                year=2021,
                market="US",
                sales=50.0,
                profit=5.0,
                discount=0.0,
                quantity=5,
                shipping_cost=2.0,
            ),
        ]

    def test_aggregate_sum_by_key(self):
        totals = aggregate_sum_by_key(
            self.orders,
            key_fn=lambda o: o.category,
            value_fn=lambda o: o.sales,
        )
        self.assertEqual(totals["Furniture"], 300.0)
        self.assertEqual(totals["Office Supplies"], 50.0)

    def test_aggregate_mean_by_key(self):
        means = aggregate_mean_by_key(
            self.orders,
            key_fn=lambda o: o.category,
            value_fn=lambda o: o.profit,
        )
        self.assertEqual(means["Furniture"], 15.0)
        self.assertEqual(means["Office Supplies"], 5.0)

    def test_aggregate_stddev_by_key(self):
        stds = aggregate_stddev_by_key(
            self.orders,
            key_fn=lambda o: o.category,
            value_fn=lambda o: o.sales,
            sample=False,
        )
        # Furniture sales: [100, 200] -> mean=150, variance=2500, stddev=50
        self.assertAlmostEqual(stds["Furniture"], 50.0)

    def test_aggregate_min_max_count_by_key(self):
        stats = aggregate_min_max_count_by_key(
            self.orders,
            key_fn=lambda o: o.category,
            value_fn=lambda o: o.profit,
        )
        self.assertEqual(stats["Furniture"], (10.0, 20.0, 2))
        self.assertEqual(stats["Office Supplies"], (5.0, 5.0, 1))


class StreamParsingTests(unittest.TestCase):
    def test_stream_orders_parses_dates_and_products(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost,Order.Date,Ship.Date,Product.ID,Product.Name
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0,2020-01-01 00:00:00.000,2020-01-03 00:00:00.000,OFF-1,Chair A
Furniture,Chairs,CA,USA,C2,Bob,2020,US,150.0,15.0,0.0,1,4.0,2020/02/01 00:00:00.000,2020/02/04 00:00:00.000,OFF-2,Chair B
"""
        orders = list(stream_orders(StringIO(csv_text)))
        self.assertEqual(len(orders), 2)
        o1, o2 = orders
        self.assertEqual(o1.product_id, "OFF-1")
        self.assertEqual(o1.product_name, "Chair A")
        self.assertIsNotNone(o1.order_date)
        self.assertIsNotNone(o1.ship_date)
        self.assertEqual(o1.order_date.date(), datetime(2020, 1, 1).date())
        self.assertEqual(o1.ship_date.date(), datetime(2020, 1, 3).date())

        # Second row uses slash date format to hit the fallback parser
        self.assertEqual(o2.product_id, "OFF-2")
        self.assertEqual(o2.product_name, "Chair B")
        self.assertIsNotNone(o2.order_date)
        self.assertIsNotNone(o2.ship_date)
        self.assertEqual(o2.order_date.date(), datetime(2020, 2, 1).date())
        self.assertEqual(o2.ship_date.date(), datetime(2020, 2, 4).date())

    def test_stream_orders_reads_from_path(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "orders.csv"
            path.write_text(csv_text, encoding="utf-8")
            orders = list(stream_orders(path))

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].customer_id, "C1")


class QueryIntegrationTests(unittest.TestCase):
    def test_sales_and_yoy(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost,Order.Date,Ship.Date
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0,2020-01-01 00:00:00.000,2020-01-03 00:00:00.000
Furniture,Chairs,CA,USA,C2,Bob,2021,US,50.0,5.0,0.0,1,3.0,2021-01-01 00:00:00.000,2021-01-02 00:00:00.000
"""
        with patch("queries.stream_orders", lambda path: stream_orders(StringIO(csv_text))):
            sales = sales_by_year_region_category("ignored.csv")
            self.assertEqual(sales[(2020, "US", "Furniture")], 100.0)
            self.assertEqual(sales[(2021, "US", "Furniture")], 50.0)

            yoy = yoy_category_sales_trends("ignored.csv")
            trend = yoy[("US", "Furniture")]
            self.assertEqual(trend[0], (2020, 100.0, None, None))
            self.assertAlmostEqual(trend[1][3], -0.5)  # 50 vs 100 → -50%

    def test_profit_margin_and_top_categories(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0
Furniture,Tables,CA,USA,C2,Bob,2020,US,200.0,60.0,0.0,1,7.0
Office Supplies,Paper,NY,USA,C3,Eve,2020,US,50.0,5.0,0.0,5,2.0
"""
        with patch("queries.stream_orders", lambda path: stream_orders(StringIO(csv_text))):
            margins = profit_margin_by_category_subcategory("ignored.csv")
            self.assertAlmostEqual(margins[("Furniture", "Chairs")], 0.10)
            self.assertAlmostEqual(margins[("Furniture", "Tables")], 0.30)
            self.assertAlmostEqual(margins[("Office Supplies", "Paper")], 0.10)

            top = top_categories_by_margin("ignored.csv", n=2)
            # Expect Tables (30%) then Chairs (10%) for Furniture
            self.assertEqual(top[0][0], "Furniture")
            self.assertEqual(top[0][1], "Tables")

    def test_discounted_profit_and_share(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0
Furniture,Tables,CA,USA,C2,Bob,2020,US,200.0,50.0,0.1,1,7.0
"""
        with patch("queries.stream_orders", lambda path: stream_orders(StringIO(csv_text))):
            discounted = total_discounted_profit("ignored.csv")
            self.assertEqual(discounted, 50.0)

            share = discounted_profit_share("ignored.csv")
            # total profit = 10 + 50 = 60; discounted share = 50/60 = 0.8333...
            self.assertAlmostEqual(share, 50.0 / 60.0)

    def test_average_fulfillment_days(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost,Order.Date,Ship.Date
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0,2020-01-01 00:00:00.000,2020-01-03 00:00:00.000
Furniture,Tables,CA,USA,C2,Bob,2020,US,200.0,60.0,0.0,1,7.0,2020-01-02 00:00:00.000,2020-01-05 00:00:00.000
"""
        with patch("queries.stream_orders", lambda path: stream_orders(StringIO(csv_text))):
            avg = average_fulfillment_days("ignored.csv")
            # Durations: 2 days and 3 days -> mean 2.5
            self.assertAlmostEqual(avg, 2.5)

    def test_profit_volatility_by_market_category(self):
        csv_text = """Category,Sub.Category,State,Country,Customer.ID,Customer.Name,Year,Market,Sales,Profit,Discount,Quantity,Shipping.Cost
Furniture,Chairs,CA,USA,C1,Alice,2020,US,100.0,10.0,0.0,2,5.0
Furniture,Chairs,CA,USA,C2,Bob,2020,US,200.0,30.0,0.0,1,7.0
Furniture,Chairs,ON,CAN,C3,Eve,2020,CAN,150.0,20.0,0.0,1,6.0
"""
        with patch("queries.stream_orders", lambda path: stream_orders(StringIO(csv_text))):
            stds = profit_std_by_market_category("ignored.csv", sample=False)
            stats = profit_min_max_count_by_market_category("ignored.csv")

            # US/Furniture: profits 10,30 -> stddev of 10.0 (mean 20, variance 100)
            self.assertAlmostEqual(stds[("US", "Furniture")], 10.0)
            self.assertEqual(stats[("US", "Furniture")], (10.0, 30.0, 2))

            # CAN/Furniture: single value -> stddev 0, min/max same
            self.assertAlmostEqual(stds[("CAN", "Furniture")], 0.0)
            self.assertEqual(stats[("CAN", "Furniture")], (20.0, 20.0, 1))


if __name__ == "__main__":
    unittest.main()
