import re
from pathlib import Path

from setuptools import find_packages, setup

PACKAGE_NAME = "gun_violence_dashboard_data"
HERE = Path(__file__).parent.absolute()


def find_version(*paths: str) -> str:
    with HERE.joinpath(*paths).open("tr") as fp:
        version_file = fp.read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name=PACKAGE_NAME,
    version=find_version(PACKAGE_NAME, "__init__.py"),
    author="Nick Hand",
    maintainer="Nick Hand",
    maintainer_email="nick.hand@phila.gov",
    packages=find_packages(),
    description="Python toolkit for preprocessing data for the City Controller's Gun Violence Dashboard",
    license="MIT",
    python_requires=">=3.7",
    install_requires=[
        "numpy",
        "pandas",
        "geopandas",
        "click",
        "carto2gpd",
        "loguru",
        "simplejson",
        "rtree",
    ],
    entry_points={
        "console_scripts": ["gv_dashboard_data=gun_violence_dashboard_data.core:cli"]
    },
)
