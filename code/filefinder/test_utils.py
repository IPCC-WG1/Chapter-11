from .utils import _find_keys, atoi, natural_keys, product_dict


def test_find_keys():

    result = _find_keys("/path/{var_name}/{year}")
    expected = set(["var_name", "year"])

    assert result == expected


def test_atoi():

    assert atoi("10") == 10
    assert atoi("a10") == "a10"
    assert atoi("a") == "a"


def test_natural_keys_sort():

    lst = ["a10", "a1"]
    expected = ["a1", "a10"]
    assert not lst == expected

    lst.sort(key=natural_keys)
    assert lst == expected


def test_product_dict():

    result = list(product_dict(a=[1, 2], b=[3, 4], c=[5]))
    expected = [
        {"a": 1, "b": 3, "c": 5},
        {"a": 1, "b": 4, "c": 5},
        {"a": 2, "b": 3, "c": 5},
        {"a": 2, "b": 4, "c": 5},
    ]

    assert result == expected
