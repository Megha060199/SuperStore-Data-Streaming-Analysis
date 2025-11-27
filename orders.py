from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, Optional, TextIO

#this would normally be configured elsewhere
CSV_PATH = "data/orders.csv"

#this file defines the Order data model and a function to stream orders from a CSV file
@dataclass(frozen=True)
class Order:
    """
    Immutable representation of a single order line.

    Using a dataclass keeps the data model clean and explicit,
    but all processing is still done via functional-style functions.
    """
    category: str
    sub_category: str
    state: str
    country: str
    customer_id: str
    customer_name: str
    year: int
    market: str
    sales: float
    profit: float
    discount: float
    quantity: int
    shipping_cost: float
    product_id: Optional[str] = None
    product_name: Optional[str] = None
    order_date: Optional[datetime] = None
    ship_date: Optional[datetime] = None

    @staticmethod
    def from_row(row: Dict[str, str]) -> "Order":
        """Convert a CSV dict row into an Order instance."""
        def parse_dt(value: str) -> Optional[datetime]:
            if not value:
                return None
            cleaned = value.strip()
            parsers = [
                lambda v: datetime.fromisoformat(v.replace(" ", "T")),
                lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S.%f"),
                lambda v: datetime.strptime(v, "%Y/%m/%d %H:%M:%S.%f"),
            ]
            for parse in parsers:
                try:
                    return parse(cleaned)
                except ValueError:
                    continue
            return None

        return Order(
            category=row["Category"],
            sub_category=row["Sub.Category"],
            state=row["State"],
            country=row["Country"],
            customer_id=row["Customer.ID"],
            customer_name=row["Customer.Name"],
            year=int(row["Year"]),
            market=row["Market"],
            sales=float(row["Sales"]),
            profit=float(row["Profit"]),
            discount=float(row["Discount"]),
            quantity=int(row["Quantity"]),
            shipping_cost=float(row["Shipping.Cost"]),
            product_id=row.get("Product.ID"),
            product_name=row.get("Product.Name"),
            order_date=parse_dt(row.get("Order.Date", "")),
            ship_date=parse_dt(row.get("Ship.Date", "")),
        )

def stream_orders(source: str | Path | TextIO = CSV_PATH) -> Iterator[Order]:
    """
    Lazily stream Order objects from a CSV path or file-like object.
      1)sequential consumption
      2) does not load the entire dataset into memory
    """

    def iter_orders(file_obj: TextIO) -> Iterator[Order]:
        reader = csv.DictReader(file_obj)
        for order in map(lambda r: Order.from_row(r), reader):
            yield order

    # Accept either a file-like object (for tests/StringIO) or a filesystem path
    if hasattr(source, "read"):
        yield from iter_orders(source)
    else:
        path = Path(source)
        with path.open(newline="", encoding="utf-8") as f:
            yield from iter_orders(f)
