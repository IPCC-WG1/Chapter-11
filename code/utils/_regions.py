import regionmask
import geopandas as gp
import pooch


class REGIONS:
    """container for regions (so they can be loaded lazily)"""

    def __init__(self):

        self._continents = None

    @property
    def continents(self):

        if self._continents is None:
            file = pooch.retrieve(
                "https://pubs.usgs.gov/of/2006/1187/basemaps/continents/continents.zip",
                known_hash="af0ba524a62ad31deee92a9700fc572088c2b93a39ba66f320677dd8dacaaaaf",
                path="../data/regions/",
            )

            continents_gdf = gp.read_file("zip://" + file)

            self._continents = regionmask.from_geopandas(
                continents_gdf,
                names="CONTINENT",
                abbrevs="_from_name",
                name="continent",
            )

        return self._continents


regions = REGIONS()
