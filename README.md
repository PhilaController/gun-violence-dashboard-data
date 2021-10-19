# gun-violence-dashboard-data

Python toolkit for preprocessing data for the Philadelphia City Controller's Gun Violence Dashboard.

The dashboard is available on the Controller's Office website: https://controller.phila.gov/philadelphia-audits/mapping-gun-violence/#/

## Overview

This packages pulls data from the following data sources, pre-processes them, and makes
them available to be loaded into the gun violence dashboard application.

- [Shooting victims data](https://www.opendataphilly.org/dataset/shooting-victims) from the City of Philadelphia's open data portal Open Data Philly
- [Total homicide count](https://www.phillypolice.com/crime-maps-stats/) from the Philadelphia Police Department's crime statistics website
- [Information on court cases](https://ujsportal.pacourts.us/DocketSheets/MC.aspx) from the web portal for the Unified Judicial System of Pennsylvania

Processed data sources are updated automatically either daily or weekly using two GitHub workflows. The updates are described in more detail in the following sections.

### Daily Update

This runs the following steps:

1. Download a fresh copy of the shooting victims database.
1. For each victim, identify the nearest street block and merge that info into the database.
1. Load database containing information on incidents with court cases.
1. Save the processed shooting victims database.
1. Calculate and save the cumulative daily shooting victims total.
1. Scrape and save the homicide count from the PPD's website.

This script runs every day at about 11:15am.

### Weekly Update

This runs the following steps:

1. Load the latest shooting victims database.
1. For every incident identifier (DC number), scrape information from the PA's Unified Judicial System's portal and save summary info about court cases associated with incidents.

This process is done in parallel using a GitHub workflow — scraping is done for 10 chunks of the database, and then combined into a single database. 

This script runs every week at midnight on Sunday.

## Technical Overview

### Installation

Clone the repository

```bash
git clone https://github.com/PhiladelphiaController/gun-violence-dashboard-data.git
cd gun_violence_dashboard_data
```

And install via `poetry`:

```bash
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -
poetry install
```

### Command Line Interface

The main command-line tool is `gv_dashboard_data`. This handles running the daily and weekly updates.
The help message can be printed using:

```bash
poetry run gv-dashboard-data --help
```


### Main Modules

- [`__main__.py`](./gun_violence_dashboard_data/__main__.py) : The main command line module that defines the "gv-dashboard-data" tool.
- [`courts.py`](./gun_violence_dashboard_data/courts.py): Scrape court information from the PA's Unified Judicial System portal.
- [`geo.py`](./gun_violence_dashboard_data/geo.py): Load various geographic boundaries in Philadelphia.
- [`homicides.py`](./gun_violence_dashboard_data/homicides.py): Scrape the total homicide count from the Philadelphia Police Department's 
Crime Stats website.
- [`shootings.py`](./gun_violence_dashboard_data/shootings.py): Module for downloading and analyzing the shooting victims database.
- [`streets.py`](./gun_violence_dashboard_data/streets.py): Module for calculating shooting hot spots by street block.