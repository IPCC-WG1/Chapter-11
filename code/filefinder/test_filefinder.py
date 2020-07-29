from .cmip6 import FileFinder

cmip6_ng = FileFinder(
    root_path="/net/atmos/data/cmip6-ng",
    file_pattern="{varname}_{timeres}_{model}_{scenaio}_{ens}_{grid}.nc",
    path_pattern="{varname}/{timeres}/{grid}/",
)


def test_attributes():

    root_path = "root_path"
    file_pattern = "file_pattern"
    path_pattern = "path_pattern"

    ff = FileFinder(
        root_path=root_path, file_pattern=file_pattern, path_pattern=path_pattern
    )

    assert ff.root_path == root_path
    assert ff.file_pattern == file_pattern
    assert ff.path_pattern == path_pattern


def test_keys():

    root_path = ""
    file_pattern = "{a}_{b}_{c}"
    path_pattern = "{ab}_{c}"

    ff = FileFinder(
        root_path=root_path, file_pattern=file_pattern, path_pattern=path_pattern
    )

    expected = set(("a", "b", "c", "ab"))

    assert ff.keys == expected
