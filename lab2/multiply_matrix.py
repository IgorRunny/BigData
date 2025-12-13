from typing import Iterator, Tuple, List
from collections import defaultdict


Key = Tuple[int, int]
Value = int
MappedItem = Tuple[Key, Value]


def map_matrix_multiply(A: List[List[int]], B: List[List[int]]) -> Iterator[MappedItem]:
    n = len(A)
    m = len(A[0])
    k = len(B[0])

    for i in range(n):
        for r in range(m):
            for j in range(k):
                yield (i, j), A[i][r] * B[r][j]


def shuffle(mapped_data: Iterator[MappedItem]) -> dict[Key, List[Value]]:
    grouped = defaultdict(list)
    for key, value in mapped_data:
        grouped[key].append(value)
    return grouped


def reduce_matrix_multiply(grouped_data: dict[Key, List[Value]]) -> Iterator[MappedItem]:
    for key, values in grouped_data.items():
        yield key, sum(values)


def matrix_multiply_mapreduce(A: List[List[int]], B: List[List[int]]) -> List[List[int]]:
    mapped = map_matrix_multiply(A, B)
    shuffled = shuffle(mapped)
    reduced = reduce_matrix_multiply(shuffled)

    n = len(A)
    k = len(B[0])
    C = [[0 for _ in range(k)] for _ in range(n)]

    for (i, j), value in reduced:
        C[i][j] = value

    return C


if __name__ == "__main__":
    A = [
        [1, 2],
        [3, 4]
    ]

    B = [
        [5, 6],
        [7, 8]
    ]

    C = matrix_multiply_mapreduce(A, B)

    print("Результат:")
    for row in C:
        print(row)
