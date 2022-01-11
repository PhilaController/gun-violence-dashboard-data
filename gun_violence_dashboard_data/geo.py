"""Load various geographic boundaries in Philadelphia."""

import esri2gpd
import geopandas as gpd

from . import EPSG


def get_city_limits():
    """Load the city limits."""
    return gpd.read_file(
        "https://opendata.arcgis.com/datasets/405ec3da942d4e20869d4e1449a2be48_0.geojson"
    ).to_crs(epsg=EPSG)


def get_pa_house_districts():
    """Elementary school catchments in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/PA_House_Districts/FeatureServer/0",
            fields=["district"],
        )
        .rename(columns={"district": "house_district"})
        .to_crs(epsg=EPSG)
    )


def get_school_catchments():
    """Elementary school catchments in in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philadelphia_Elementary_School_Catchments_SY_2019_2020/FeatureServer/0",
            fields=["name"],
        )
        .rename(columns={"name": "school"})
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
        .rename(columns={"DIST_NUM": "police"})
    )


def get_zip_codes():
    """ZIP Codes in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philadelphia_ZCTA_2018/FeatureServer/0",
            fields=["zip_code"],
        )
        .to_crs(epsg=EPSG)
        .rename(columns={"zip_code": "zip"})
    )


def get_council_districts():
    """Council Districts in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Council_Districts_2016/FeatureServer/0/",
            fields=["DISTRICT"],
        )
        .rename(columns={"DISTRICT": "council"})
        .to_crs(epsg=EPSG)
    )


def get_neighborhoods():
    """Neighborhoods in Philadelphia."""

    return (
        esri2gpd.get(
            "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Philly_NTAs/FeatureServer/0",
            fields=["neighborhood"],
        )
        .rename(columns={"neighborhood": "hood"})
        .to_crs(epsg=EPSG)
    )
