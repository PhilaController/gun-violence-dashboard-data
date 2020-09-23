import esri2gpd

from . import DATA_DIR, EPSG


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
