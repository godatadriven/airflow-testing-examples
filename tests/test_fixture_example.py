import pytest


@pytest.fixture
def a():
    return 1


@pytest.fixture
def b():
    return 2


@pytest.fixture
def c():
    return 3


def test_sum_ab(a, b):
    assert sum([a, b]) == 3


def test_sum_ac(a, c):
    assert sum([a, c]) == 4
