import xarray as xr


def select_by_metadata(datalist, **attributes):
    """Select specific data described by metadata.

    Parameters
    ----------
    datalist : DataList
        List of (ds, metadata) pairs.
    **attributes
        Keyword arguments specifying the required variable attributes and their values.
        Use '*' to select any variable that has the attribute.

    Returns
    -------
    out: DataList
        List of (ds, metadata) pairs that has matching metadata.
    """

    selection = []
    for data, meta in datalist:

        if all(
            a in meta and (meta[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            selection.append((data, meta))
    return selection


def remove_by_metadata(datalist, **attributes):
    """Remove specific data described by metadata.

    Parameters
    ----------
    datalist : DataList
        List of (ds, metadata) pairs
    **attributes
        Keyword arguments specifying the required variable attributes and
        their values.

    Returns
    -------
    out : DataList
        List of (ds, metadata) pairs with non-matching metadata.
    """

    selection = []
    for data, meta in datalist:

        if all(
            a in meta and (meta[a] == attributes[a] or attributes[a] == "*")
            for a in attributes
        ):
            pass
        else:
            selection.append((data, meta))
    return selection


def match_data_list(list_a, list_b, select_by=("model", "exp", "ens"), check=True):

    """align two datalists (inner join)

    Parameters
    ----------
    list_a : DataList
        List of (ds, metadata) pairs.
    list_b : DataList
        List of (ds, metadata) pairs.
    select_by : list of str, optional
        Conditions to align lists on.
    check : bool, default: True
        If True checks that only one dataset is found in list_b

    Returns
    -------
    out_a : DataList
        Aligned list of (ds, metadata) pairs.
    out_b : DataList
        Aligned list of (ds, metadata) pairs.
    """

    out_a = list()
    out_b = list()

    for ds_a, meta in list_a:

        attributes = {key: meta[key] for key in select_by}

        # try to find the in list_b
        match = select_by_metadata(list_b, **attributes)

        # make sure only one dataset is found in index_list
        if check and len(match) > 1:
            print(match)
            raise ValueError(meta)

        # an index was found for this dataset
        if match:
            out_a += [[ds_a, meta]]
            out_b += match

    return out_a, out_b


def concat_xarray_with_metadata(
    datalist,
    process=None,
    index={"mod_ens": ("model", "ens")},
    retain=("model", "ens", "ensnumber", "exp", "postprocess", "table", "grid", "varn"),
    set_index=False,
):
    """create xr Dataset with 'ens' and 'model' as multiindex

    Parameters
    ----------
    datalist : DataList
        List of (ds, metadata) pairs.
    process : callable, default: None
        Function to apply for each ds before concatenation.
    index : dict, optional
        Only applies if set_index is True. dict describing how to create a MultiIndex.
    retain : iterable of str, optional
        Which metadata information to assign as non-dimension coordinates.
    set_index : bool, default: False
        If True sets a MultiIndex to the DataArray

    Returns
    -------
    out : xr.DataArray
        Concatenated DataArray

    Notes
    -----
    should be named ``to_dataarray``

    """

    all_ds = list()

    # no longer necessary since we can plot non-coord dimensions
    retain += ("ensi",)
    retain_dict = {r: ("mod_ens", list()) for r in retain}

    for i, (ds, meta) in enumerate(datalist):

        if process is not None:
            ds = process(ds)

        all_ds.append(ds)

        retain_dict["ensi"][1].append(i)
        for r in retain[:-1]:
            retain_dict[r][1].append(meta.get(r, None))

    # concate all data
    out = xr.concat(all_ds, "mod_ens", compat="override", coords="minimal")
    # assign coordinates
    out = out.assign_coords(**retain_dict)

    if set_index:
        index = {"mod_ens": retain}
        # create multiindex
        out = out.set_index(**index)

    return out


def process_datalist(func, datalist, pass_meta=False, **kwargs):
    """loop over a datalist and apply a function

    Parameters
    ----------
    func : callable
        function to apply
    datalist : DataList
        List to apply the function over.
    pass_meta : bool, default: False
        If "meta" should be passed as keyword argument to ``func``.
    **kwargs : extra arguments
        passed to func

    Returns
    -------
    datalist_out : DataList
        List with ``func`` applied to each element.
    """

    datalist_out = list()

    for ds, meta in datalist:

        if pass_meta:
            ds = func(ds, meta=meta, **kwargs)
        else:
            ds = func(ds, **kwargs)

        if len(ds) == 0:
            continue

        datalist_out.append([ds, meta])

    return datalist_out
