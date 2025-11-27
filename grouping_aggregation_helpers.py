
from typing import Dict, Iterable, Iterator, Tuple, Any
from operator import itemgetter
from collections import defaultdict, Counter
from functools import reduce
from math import sqrt
from orders import Order

def aggregate_sum_by_key(
    orders: Iterable[Order],
    key_fn,
    value_fn,
) -> Dict[Any, float]:
    """
    Generic "group by key and sum value" helper.

    key_fn   : Order -> key
    value_fn : Order -> numeric value to be summed
    """
    totals: Dict[Any, float] = defaultdict(float)

    # map + lambda creates a stream of (key, value) pairs
    for key, value in map(lambda o: (key_fn(o), value_fn(o)), orders):
        totals[key] += value

    return dict(totals)

def aggregate_mean_by_key(
    orders: Iterable[Order],
    key_fn,
    value_fn,
) -> Dict[Any, float]:
    """
    Generic "group by key and compute mean(value)" helper.
    """
    total: Dict[Any, float] = defaultdict(float)
    count: Dict[Any, int] = defaultdict(int)

    for key, value in map(lambda o: (key_fn(o), value_fn(o)), orders):
        total[key] += value
        count[key] += 1

    return {
        key: total[key] / count[key]
        for key in total
        if count[key] > 0
    }

def aggregate_stddev_by_key(
    orders: Iterable[Order],
    key_fn,
    value_fn,
    sample: bool = True,
) -> Dict[Any, float]:
    """
    Generic "group by key and compute standard deviation of value" helper.
    Uses Welford's algorithm for numerical stability.
    Set sample=False to compute population stddev.
    """
    # Stats per key: count, mean, M2
    stats: Dict[Any, tuple[int, float, float]] = {}

    for key, value in map(lambda o: (key_fn(o), value_fn(o)), orders):
        count, mean, m2 = stats.get(key, (0, 0.0, 0.0))
        count += 1
        delta = value - mean
        mean += delta / count
        delta2 = value - mean
        m2 += delta * delta2
        stats[key] = (count, mean, m2)

    result: Dict[Any, float] = {}
    for key, (count, _, m2) in stats.items():
        if count == 0:
            continue
        denom = (count - 1) if sample and count > 1 else count
        if denom == 0:
            continue
        result[key] = sqrt(m2 / denom)

    return result


def aggregate_min_max_count_by_key(
    orders: Iterable[Order],
    key_fn,
    value_fn,
) -> Dict[Any, tuple[float, float, int]]:
    """
    Generic "group by key and compute min, max, count" helper.
    """
    stats: Dict[Any, tuple[float, float, int]] = {}
    for key, value in map(lambda o: (key_fn(o), value_fn(o)), orders):
        if key not in stats:
            stats[key] = (value, value, 1)
        else:
            current_min, current_max, count = stats[key]
            stats[key] = (
                min(current_min, value),
                max(current_max, value),
                count + 1,
            )
    return stats
