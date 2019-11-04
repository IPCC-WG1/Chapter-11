import numpy as np
import os as os
import re


def _find_keys(pattern):
    """find keys in a format string

        find all keys enclosed by curly brackets

        _find_keys("/path/{var_name}/{year}")
        >>> set("var_name", "year")

    """
    keys = set(re.findall(r"\{([A-Za-z0-9_]+)\}", pattern))

    return keys


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    """
    a_list.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html

    Example
    -------
    > l = ['a10', 'a1']
    > l.sort(key=natural_keys)
    > l
    ['a1', 'a10']

    """
    return [atoi(c) for c in re.split(r"(\d+)", text)]
