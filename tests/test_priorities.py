import pytest
from requestflow.priorities import Priority


def test_priority_values_and_ordering():
    assert Priority.CRITICAL < Priority.HIGH
    assert Priority.HIGH < Priority.NORMAL
    assert Priority.NORMAL < Priority.LOW
    assert Priority.CRITICAL.value == 0
    assert Priority.HIGH.value == 1
    assert Priority.NORMAL.value == 2
    assert Priority.LOW.value == 3


def test_priority_from_value():
    assert Priority.from_value(Priority.HIGH) == Priority.HIGH
    assert Priority.from_value('critical') == Priority.CRITICAL
    assert Priority.from_value('HIGH') == Priority.HIGH
    assert Priority.from_value('normal') == Priority.NORMAL
    assert Priority.from_value('low') == Priority.LOW
    assert Priority.from_value(0) == Priority.CRITICAL
    assert Priority.from_value(3) == Priority.LOW
    with pytest.raises(ValueError):
        Priority.from_value('invalid_priority')
    with pytest.raises(ValueError):
        Priority.from_value(99)
    with pytest.raises(TypeError):
        Priority.from_value([1, 2, 3])
