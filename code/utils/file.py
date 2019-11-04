import numpy as np
import os as os


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
