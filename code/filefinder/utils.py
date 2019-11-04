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


# file utilities
def _check_all_files_exist(fnames):
    """
    error if one file does not exist
    """
    fnames = _str2lst(fnames)
    if _any_file_does_not_exist(fnames):
        msg = "file(s) missing:\n" + "\n".join(fnames)
        raise RuntimeError(msg)


def _any_file_does_not_exist(fnames):
    """
    check if any file in the list does not exist
    """

    fnames = _str2lst(fnames)

    inexistent = [not os.path.isfile(fN) for fN in fnames]

    return np.any(np.array(inexistent))


def _source_files_newer_(source_files, dest_file):
    """
    check if the any of the source files is older than the dest file
    """

    source_files = _str2lst(source_files)

    # get timestamp of all files
    age_source = [os.path.getctime(sf) for sf in source_files]
    age_dest = os.path.getctime(dest_file)

    # compare timestamps
    source_is_older = np.array(age_source) < np.array(age_dest)

    # return true if any is older
    return np.all(source_is_older)


def _str2lst(list_or_string):
    """
    convert a string to a list
    """

    if isinstance(list_or_string, str):
        list_or_string = [list_or_string]

    return list_or_string


def _mkdir(directory):
    # create a directory if it doesent exist
    try:
        os.makedirs(directory)
    except OSError:
        pass
