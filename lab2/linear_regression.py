from typing import Iterator, Tuple, List
from collections import defaultdict


MappedItem = Tuple[str, float]


def map_linear_regression(data: List[Tuple[float, float]]) -> Iterator[MappedItem]:
    for x, y in data:
        yield "sum_x", x
        yield "sum_y", y
        yield "sum_xy", x * y
        yield "sum_x2", x * x
        yield "count", 1.0


def shuffle(mapped_data: Iterator[MappedItem]) -> dict[str, List[float]]:
    grouped = defaultdict(list)
    for key, value in mapped_data:
        grouped[key].append(value)
    return grouped


def reduce_linear_regression(grouped_data: dict[str, List[float]]) -> Iterator[MappedItem]:
    for key, values in grouped_data.items():
        yield key, sum(values)


def linear_regression_mapreduce(data: List[Tuple[float, float]]) -> Tuple[float, float]:
    mapped = map_linear_regression(data)
    shuffled = shuffle(mapped)
    reduced = dict(reduce_linear_regression(shuffled))

    n = reduced["count"]
    sum_x = reduced["sum_x"]
    sum_y = reduced["sum_y"]
    sum_xy = reduced["sum_xy"]
    sum_x2 = reduced["sum_x2"]

    a = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
    b = (sum_y - a * sum_x) / n

    return a, b


if __name__ == "__main__":
    data = [
        (1, 2),
        (2, 3),
        (3, 5),
        (4, 4),
        (5, 6)
    ]

    a, b = linear_regression_mapreduce(data)

    print(f"Линейная регрессия:")
    print(f"y = {a:.3f}x + {b:.3f}")
