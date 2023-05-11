"""Load various geographic boundaries in Philadelphia."""

import esri2gpd
import geopandas as gpd

from . import DATA_DIR, EPSG


def number_to_string(value):
    return str(int(value))


def get_city_limits():
    """Load the city limits."""

    path = DATA_DIR / "raw" / "City_Limits.geojson"
    return gpd.read_file(path).to_crs(epsg=EPSG)


def get_pa_house_districts():
    """PA House districts in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_PA_House_Districts/FeatureServer/0",
            fields=["house_district"],
        )
        .assign(house_district=lambda df: df.house_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_pa_senate_districts():
    """PA Senate districts in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_PA_Senate_Districts/FeatureServer/0",
            fields=["senate_district"],
        )
        .assign(senate_district=lambda df: df.senate_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_school_catchments():
    """Elementary school catchments in in Philadelphia."""

    return esri2gpd.get(
        "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_School_Catchments/FeatureServer/0",
        fields=["school_name"],
    ).to_crs(epsg=EPSG)


def get_police_districts():
    """Police Districts in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_Police_Districts/FeatureServer/0",
            fields=["police_district"],
        )
        .to_crs(epsg=EPSG)
        .assign(police_district=lambda df: df.police_district.apply(number_to_string))
    )


def get_zip_codes():
    """ZIP Codes in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_ZIP_Codes/FeatureServer/0",
            fields=["zip_code"],
        )
        .to_crs(epsg=EPSG)
        .assign(zip_code=lambda df: df.zip_code.apply(number_to_string))
    )


def get_council_districts():
    """Council Districts in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_Council_Districts/FeatureServer/0/",
            fields=["council_district"],
        )
        .assign(council_district=lambda df: df.council_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_neighborhoods():
    """Neighborhoods in Philadelphia."""

    return esri2gpd.get(
        "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Gun_Violence_Dashboard_Neighborhoods/FeatureServer/0",
        fields=["neighborhood"],
    ).to_crs(epsg=EPSG)
