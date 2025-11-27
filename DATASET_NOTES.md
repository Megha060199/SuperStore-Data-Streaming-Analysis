# Why This Dataset?

1) **Structured, multi-faceted fields**: Includes categories, sub-categories, markets/regions, dates, and financials (sales, profit, discount), enabling diverse aggregations (group by, YOY, margins, volatility, fulfillment time).

2) **Stream-friendly size**: Large enough to demonstrate streaming/iterators and avoid loading all data into memory, but small enough for fast local runs and tests.

3) **Rich date fields**: `Order.Date` and `Ship.Date` allow fulfillment-time analysis beyond simple sums/means.

4) Ideal for Functional Programming
The data flows cleanly through FP pipelines (map, filter, reduce) because each row is independent and can be transformed without needing shared state.

5) Allows You to Show Multiple Testing Objectives in One Dataset

        Functional programming

        Stream operations

        Complex aggregations (sum/avg/YOY/trends)

        Lambda expressions

        Pure FP pipelines

Results of all analyses