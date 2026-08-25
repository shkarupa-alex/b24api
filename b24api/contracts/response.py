"""Public response collection mechanics."""

from enum import StrEnum


class ResultCollectionShape(StrEnum):
    """How a selected result collection is interpreted."""

    SEQUENCE = "sequence"
    MAPPING_VALUES = "mapping_values"


__all__ = ["ResultCollectionShape"]
