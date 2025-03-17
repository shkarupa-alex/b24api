try:
    from itertools import batched  # python 3.12+
except ImportError:
    from collections.abc import Generator, Iterable
    from itertools import islice

    def batched(iterable: Iterable, n: int) -> Generator[tuple, None, None]:
        if n < 1:
            raise ValueError("n must be at least one")
        iterator = iter(iterable)
        while batch := tuple(islice(iterator, n)):
            yield batch
