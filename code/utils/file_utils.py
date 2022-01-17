import os

import numpy as np


def _check_all_files_exist(files):
    """error if one file does not exist"""

    if isinstance(files, str):
        files = [files]

    if _any_file_does_not_exist(files):
        msg = "file(s) missing:\n" + "\n".join(files)
        raise RuntimeError(msg)


def _file_exists(fname):

    return os.path.isfile(fname)


def _any_file_does_not_exist(files):
    """
    check if any file in the list does not exist
    """

    if isinstance(files, str):
        files = [files]

    return any(not os.path.isfile(fN) for fN in files)


def _source_files_newer_(source_files, dest_file):
    """
    check if the any of the source files is older than the dest file
    """

    if isinstance(source_files, str):
        source_files = [source_files]

    # get timestamp of all files
    age_source = [os.path.getctime(sf) for sf in source_files]
    age_dest = os.path.getctime(dest_file)

    # compare timestamps
    source_is_older = np.array(age_source) < np.array(age_dest)

    # return true if any is older
    return np.all(source_is_older)


def mkdir(directory):
    # create a directory if it doesent exist
    try:
        os.makedirs(directory)
    except OSError:
        pass
