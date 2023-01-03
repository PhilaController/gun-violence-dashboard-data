"""Utilities for dashboard data processing."""

from typing import Callable

import pandas as pd
from pydantic import BaseModel
from pydantic.main import ModelMetaclass


def validate_data_schema(data_schema: ModelMetaclass) -> Callable:
    """
    This decorator will validate a pandas.DataFrame against the given data_schema.
    Source
    ------
    https://www.inwt-statistics.com/read-blog/pandas-dataframe-validation-with-pydantic-part-2.html
    """

    def Inner(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):  # type: ignore
            res = func(*args, **kwargs)
            if isinstance(res, pd.DataFrame):
                # check result of the function execution against the data_schema
                df_dict = res.to_dict(orient="records")

                # Wrap the data_schema into a helper class for validation
                class ValidationWrap(BaseModel):
                    df_dict: list[data_schema]  # type: ignore

                # Do the validation
                _ = ValidationWrap(df_dict=df_dict)
            else:
                raise TypeError(
                    "Your Function is not returning an object of type pandas.DataFrame."
                )

            # return the function result
            return res

        return wrapper

    return Inner