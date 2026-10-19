from enum import IntEnum
from typing import Union


class Priority(IntEnum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

    @classmethod
    def from_value(cls, value: Union['Priority', str, int]) -> 'Priority':
        if isinstance(value, Priority):
            return value
        if isinstance(value, int):
            try:
                return cls(value)
            except ValueError:
                raise ValueError(
                    f'invalid priority integer: {value}. must be 0, 1, 2, or 3.')
        if isinstance(value, str):
            normalized = value.strip().upper()
            try:
                return cls[normalized]
            except KeyError:
                raise ValueError(
                    f"unknown priority name: '{value}'. valid options: {[p.name.lower() for p in cls]}")
        raise TypeError(
            f'priority must be priority, str, or int, not {type(value).__name__}')

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Priority):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, Priority):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Priority):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, Priority):
            return self.value >= other.value
        return NotImplemented
