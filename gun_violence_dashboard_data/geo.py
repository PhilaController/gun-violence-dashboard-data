"""Load various geographic boundaries in Philadelphia."""

import esri2gpd
import geopandas as gpd

from . import EPSG


def number_to_string(value):
    return str(int(value))


def get_city_limits():
    """Load the city limits."""
    return gpd.read_file(
        "https://opendata.arcgis.com/datasets/405ec3da942d4e20869d4e1449a2be48_0.geojson"
    ).to_crs(epsg=EPSG)


def get_pa_house_districts():
    """PA House districts in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/PA_House_Districts/FeatureServer/0",
            fields=["district"],
        )
        .rename(columns={"district": "house_district"})
        .assign(house_district=lambda df: df.house_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_pa_senate_districts():
    """PA Senate districts in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/PA_Senate_Districts/FeatureServer/0",
            fields=["district"],
        )
        .rename(columns={"district": "senate_district"})
        .assign(senate_district=lambda df: df.senate_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_school_catchments():
    """Elementary school catchments in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philadelphia_Elementary_School_Catchments_SY_2019_2020/FeatureServer/0",
            fields=["name"],
        )
        .rename(columns={"name": "school_name"})
        .to_crs(epsg=EPSG)
    )


def get_police_districts():
    """Police Districts in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_District/FeatureServer/0",
            fields=["DIST_NUM"],
        )
        .to_crs(epsg=EPSG)
        .rename(columns={"DIST_NUM": "police_district"})
        .assign(police_district=lambda df: df.police_district.apply(number_to_string))
    )


def get_zip_codes():
    """ZIP Codes in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philadelphia_ZCTA_2018/FeatureServer/0",
            fields=["zip_code"],
        )
        .to_crs(epsg=EPSG)
        .rename(columns={"zip_code": "zip_code"})
        .assign(zip_code=lambda df: df.zip_code.apply(number_to_string))
    )


def get_council_districts():
    """Council Districts in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Council_Districts_2016/FeatureServer/0/",
            fields=["DISTRICT"],
        )
        .rename(columns={"DISTRICT": "council_district"})
        .assign(council_district=lambda df: df.council_district.apply(number_to_string))
        .to_crs(epsg=EPSG)
    )


def get_neighborhoods():
    """Neighborhoods in Philadelphia."""

    return esri2gpd.get(
        "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philly_NTAs/FeatureServer/0",
        fields=["neighborhood"],
    ).to_crs(epsg=EPSG)
