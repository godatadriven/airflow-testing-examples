import pytest


@pytest.fixture(scope="module")
def a():
    return [1]


@pytest.fixture
def b():
    return [2]


class TestBla:
    def test_something(self, a):
        a.append(1)
        assert sum(a) == 2

    @pytest.mark.xfail(
        reason="This is expected to fail when run via the class TestBla, "
        "because the value of a is changed in test_something()."
    )
    def test_something_ab(self, a, b):
        assert sum(a + b) == 3
