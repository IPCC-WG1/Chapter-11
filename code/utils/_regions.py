import geopandas as gp
import numpy as np
import pooch
import regionmask
import xarray as xr
from shapely.geometry import MultiPolygon, Polygon

ORDER_REGIONS_IN_TABLE = [
    "SAH",
    "WAF",
    "CAF",
    "NEAF",
    "SEAF",
    "WSAF",
    "ESAF",
    "MDG",
    "RAR",
    "WSB",
    "ESB",
    "RFE",
    "WCA",
    "ECA",
    "TIB",
    "EAS",
    "ARP",
    "SAS",
    "SEA",
    "NAU",
    "CAU",
    "EAU",
    "SAU",
    "NZ",
    "SCA",
    "CAR",
    "NWS",
    "NSA",
    "NES",
    "SAM",
    "SWS",
    "SES",
    "SSA",
    "GIC",
    "NEU",
    "WCE",
    "EEU",
    "MED",
    "NWN",
    "NEN",
    "WNA",
    "CNA",
    "ENA",
    "NCA",
]


def retrieve_continents():

    file = pooch.retrieve(
        "https://pubs.usgs.gov/of/2006/1187/basemaps/continents/continents.zip",
        known_hash="af0ba524a62ad31deee92a9700fc572088c2b93a39ba66f320677dd8dacaaaaf",
        path="../data/regions/",
    )

    continents_gdf = gp.read_file("zip://" + file)

    return regionmask.from_geopandas(
        continents_gdf, names="CONTINENT", abbrevs="_from_name", name="continent"
    )


def create_mid_lat_arctic_region():
    # for Ed Hawkins; Seneviratne Sonia Isabelle

    # from the arctic cycle up
    arctic = Polygon([[-180, 66], [-180, 90], [180, 90], [180, 66]])

    # as discussed with Sonia
    lat_pole = 55
    lat_eq = 30

    # Northern and Southern mid-latitudes
    mid_lat_north = Polygon(
        [[-180, lat_eq], [-180, lat_pole], [180, lat_pole], [180, lat_eq]]
    )
    mid_lat_south = Polygon(
        [[-180, -lat_eq], [-180, -lat_pole], [180, -lat_pole], [180, -lat_eq]]
    )
    mid_lat = MultiPolygon([mid_lat_north, mid_lat_south])

    # they want
    # - Arctic: with ocean
    # - Mid latitudes: land only
    # so I mask the midlatidues

    p_land110 = regionmask.defined_regions.natural_earth.land_110.polygons[0]

    mid_lat_land = mid_lat.intersection(p_land110.buffer(0))

    mid_lat_arctic = regionmask.Regions(
        [arctic, mid_lat_land],
        name="mid_lat_arctic",
        names=["Arctic", "Mid Latitudes"],
        abbrevs=["Arc", "MidLat"],
    )

    return mid_lat_arctic


class REGIONS:
    """container for regions (so they can be loaded lazily)"""

    def __init__(self):

        self._continents = None
        self._mid_lat_arctic_region = None
        self.ORDER_REGIONS_IN_TABLE = ORDER_REGIONS_IN_TABLE

    @property
    def continents(self):

        if self._continents is None:
            self._continents = retrieve_continents()

        return self._continents

    @property
    def mid_lat_arctic_region(self):
        if self._mid_lat_arctic_region is None:
            self._mid_lat_arctic_region = create_mid_lat_arctic_region()
        return self._mid_lat_arctic_region

    @staticmethod
    def global_mask_3D(da, landmask=None, numbers=None):
        """create 3D mask: global, ocean, land, land w/o Antarctica

        Parameters
        ----------
        da : xr.DataArray or Dataset
            longitude and latitude must be called "lat" and "lon"
        landmask : xr.DataArray
            2D DataArray with a landmask or land fraction. Data must be
            in 0..1. If None uses defined_regions.natural_earth.land_110
            to calculate it.
        numbers : iterable of int
            Four numbers assigned to the 4 created regions. If None uses
            0 through 3.

        Returns
        -------
        3D_mask : xr.DataArray
            Boolean mask of shape 4 x lax x lon.
        """

        if landmask is None:
            landmask = regionmask.defined_regions.natural_earth.land_110.mask_3D(da)
            landmask = landmask.squeeze(drop=True)

        if landmask.max() > 1.0 or landmask.min() < 0.0:
            msg = "landmask must be in the range 0..1. Found values {}..{}"
            msg = msg.format(landmask.min().values, landmask.max().values)
            raise ValueError(msg)

        if numbers is None:
            numbers = np.arange(4)

        mask_3D = list()
        # global mean
        mask_3D.append(xr.ones_like(landmask, dtype=np.bool))
        # ocean mean
        mask_3D.append(1.0 - landmask)
        # land mean
        mask_3D.append(landmask)
        # land mean w/o antarctica
        landmask_wo_ant = landmask
        sel = landmask_wo_ant.lat > -60
        landmask_wo_ant = landmask_wo_ant.where(sel, 0)
        mask_3D.append(landmask_wo_ant)

        mask_3D = xr.concat(mask_3D, dim="region")

        abbrevs = ["global", "ocean", "land", "land_wo_antarctica"]

        names = ["Global", "Ocean", "Land", "Land w/o Antarctica"]

        # add the abbreviations of the regions, update the numbers
        mask_3D = mask_3D.assign_coords(
            **{
                "abbrevs": ("region", abbrevs),
                "region": ("region", numbers),
                "names": ("region", names),
            }
        )

        return mask_3D


regions = REGIONS()
