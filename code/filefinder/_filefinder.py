from os import path
import re
import pandas as pd
import os
import parse
import copy
import logging
import numpy as np
import glob

from .utils import _find_keys, atoi, natural_keys

logger = logging.getLogger(__name__)

_FILE_FINDER_REPR = """<FileFinder>
path_pattern: '{path_pattern}'
file_pattern: '{file_pattern}'

keys: {repr_keys}
"""


class FileFinder:
    """docstring for FileFinder"""

    def __init__(self, path_pattern, file_pattern):
        super(FileFinder, self).__init__()

        self.path_pattern = path_pattern
        self.file_pattern = file_pattern
        self._full_pattern = path.join(*filter(None, (path_pattern, file_pattern)))

        self.keys_path = _find_keys(self.path_pattern)
        self.keys_file = _find_keys(self.file_pattern)

        self.keys = self.keys_path | self.keys_file

        self._parse_path = parse.compile(self.path_pattern)
        self._parse_file = parse.compile(self.file_pattern)
        self._parse_full = parse.compile(self._full_pattern)

        # self._all_files = None

    def create_path_name(self, **kwargs):

        return self.path_pattern.format(**kwargs)

    def create_file_name(self, qualifier=None, **kwargs):

        return self.file_pattern.format(**kwargs)

    def create_full_name(self, **kwargs):

        path_name = self.create_path_name(**kwargs)
        file_name = self.create_file_name(**kwargs)

        return os.path.join(path_name, file_name)

    def _create_condition_dict(self, keys, **kwargs):

        kwargs_keys = set(kwargs.keys())

        missing_keys = keys - kwargs_keys
        superfluous_keys = kwargs_keys - keys

        # if missing_keys:
        #     msg = "Missing Keyword Arguments: {}".format(", ".join(missing_keys))
        #     raise ValueError(msg)

        if superfluous_keys:
            msg = "Superfluous Keyword Arguments: {}".format(
                ", ".join(superfluous_keys)
            )
            print(msg)

        cond_dict = {key: "*" for key in self.keys}
        cond_dict.update(**kwargs)

        return cond_dict

    def find_paths(self, **kwargs):

        return self._find(
            what="paths",
            name_creator=self.create_path_name,
            parser=self._parse_path,
            keys=self.keys_path,
            **kwargs,
        )

    def find_files(self, **kwargs):

        return self._find(
            what="files",
            name_creator=self.create_full_name,
            parser=self._parse_full,
            keys=self.keys_file,
            **kwargs,
        )

    def _find(self, what, name_creator, parser, keys, **kwargs):

        cond_dict = self._create_condition_dict(keys, **kwargs)

        full_pattern = name_creator(**cond_dict)

        logger.info(f"Looking for {what} with pattern: '{full_pattern}'")

        paths = sorted(glob.glob(full_pattern), key=natural_keys)

        logger.info(f" - Found: '{len(paths)}' {what}")

        suffix = ""
        if what == "paths":
            suffix = "*"

        out = list()
        for pth in paths:
            parsed = parser.parse(pth)
            out.append([pth + suffix] + list(parsed.named.values()))

        keys = ["filename"] + list(parsed.named.keys())

        df = pd.DataFrame(out, columns=keys)
        return FileContainer(df)

    def _get_all_files(self):
        pass

    def __repr__(self):

        repr_keys = ""
        for key in self.keys:
            repr_keys += f"'{key}', "

        msg = _FILE_FINDER_REPR.format(
            path_pattern=self.path_pattern,
            file_pattern=self.file_pattern,
            repr_keys=repr_keys[:-2],
        )

        return msg


class FileContainer:
    """docstring for FileContainer"""

    def __init__(self, df):

        self.df = df

    def __iter__(self):

        for index, element in self.df.iterrows():
            yield element["filename"], element.drop("filename").to_dict()

    def __getitem__(self, key):

        element = self.df.loc[key]

        return element["filename"], element.drop("filename").to_dict()

    def combine_by_key(self, keys=None, sep="."):
        """combine colums"""

        if keys is None:
            keys = list(self.df.columns.drop("filename"))

        return self.df[keys].apply(lambda x: ".".join(x.map(str)), axis=1)

    def search(self, **query):

        ret = copy.copy(self)
        ret.df = self._get_subset(**query)
        return ret

    def _get_subset(self, **query):
        if not query:
            return pd.DataFrame(columns=self.df.columns)
        condition = np.ones(len(self.df), dtype=bool)
        for key, val in query.items():
            if isinstance(val, list):
                condition_i = np.zeros(len(self.df), dtype=bool)
                for val_i in val:
                    condition_i = condition_i | (self.df[key] == val_i)
                condition = condition & condition_i
            elif val is not None:
                condition = condition & (self.df[key] == val)
        query_results = self.df.loc[condition]
        return query_results

    def __repr__(self):

        msg = "<FileContainer>\n"
        return msg + self.df.__repr__()


class FileFinder_orig_post:
    """docstring for FileFinder_raw_post"""

    def __init__(
        self, path_pattern_orig, file_pattern_orig, path_pattern_post, file_pattern_post
    ):

        self._path_pattern_orig = path_pattern_orig
        self._file_pattern_orig = file_pattern_orig
        self._path_pattern_post = path_pattern_post
        self._file_pattern_post = file_pattern_post

        self.orig = FileFinder(
            path_pattern=path_pattern_orig, file_pattern=file_pattern_orig
        )

        self.post = FileFinder(
            path_pattern=path_pattern_post, file_pattern=file_pattern_post
        )


# cmip6_ng = FileFinder(
#     path_pattern="/net/atmos/data/cmip6-ng/{varn}/{timeres}/{grid}/",
#     file_pattern="{varn}_{timeres}_{model}_{scenario}_{ens}_{grid}.nc",
# )

# root = "/net/cfc/landclim1/mathause/projects/IPCC_AR6_CH11/data/"

# cmip6 = FileFinder(
#     path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
#     file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}_{time}.nc",
# )

# cmip6_post = FileFinder(
#     path_pattern="/net/cfc/landclim1/mathause/projects/IPCC_AR6_CH11/data/cmip6/{exp}/{table}/{varn}/{postprocess}",
#     file_pattern="{postprocess}.{varn}.{table}.{model}.{exp}.{ens}..nc",
# )


# cmip5 = FileFinder(
#     path_pattern="/net/atmos/data/cmip5/{exp}/{table}/{varn}/{model}/{ens}",
#     file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{time}.nc",
#     # path_out_pattern=root + "cmip5/{var}",
#     # file_out_pattern="{varn}_{table}_{model}_{exp}_{ens}.nc",
# )


# cmip6_r = FileFinder(
#     path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/r{r}i{i}p{p}f{f}/{grid}/",
#     file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}_{time}.nc",
# )


# cmip6_fx = FileFinder(
#     path_pattern="/net/atmos/data/cmip6/{exp}/{table}/{varn}/{model}/{ens}/{grid}/",
#     file_pattern="{varn}_{table}_{model}_{exp}_{ens}_{grid}.nc",
# )


# merra = FileFinder(
#     path_pattern="/net/exo/landclim/data/dataset/MERRA/20150504/0.5x0.666deg_lat-lon_{res}/original/",
#     file_pattern="merra.{var}.{year}.nc",
# )


# ==================


# import logging

# logger = logging.getLogger()
# handler = logging.StreamHandler()
# formatter = logging.Formatter(
#         '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
# handler.setFormatter(formatter)
# logger.addHandler(handler)
# logger.setLevel(logging.DEBUG)


# tas_files = cmip6.find_paths(table='Amon', varn='tas', exp='historical')

# tas_files = tas_files.filter(ens_number=0)

# for i, tas_file in tas_files.iterrows():

#     fN_in = tas_file.filename
#     fN_out = cmip6_out.create_full_name(**tas_file, qualifier='global_mean')

#     prc.globmean().process(fN_in, fN_out)


# tasmax_files = cmip6.find_paths(table='Amon', varn='tasmax', exp='historical')

# tasmax_files = tasmax_files.filter(ens_number=0)

# for i, tas_file in tasmax_files.iterrows():

#     fN_in = tas_file.filename
#     fN_out = cmip6_out.create_full_name(**tas_file, qualifier=['annual', 'max'])

#     prc.resample(time="A", how='max').process(fN_in, fN_out)

#     fN_in = fN_out

#     fN_out = cmip6_out.create_full_name(**tas_file, qualifier=['annual', 'max', "regid"])

#     prc.regrid_cdo(resolution="2.5", how='conservative').process(fN_in, fN_out)
