import pandas as pd


def replace_missing_geometries(df):
    """
    Utility function to replace missing geometries with empty Point() objects.
    """
    from shapely.geometry import Point

    mask = df.geometry.isnull()
    empty = pd.Series(
        [Point() for i in range(mask.sum())], index=df.loc[mask, "geometry"].index
    )
    df.loc[mask, "geometry"] = empty

    return df
