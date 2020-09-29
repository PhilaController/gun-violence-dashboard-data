"""The main command line module that defines the "gv_dashboard_data" tool."""
import datetime

import click
import numpy as np
import simplejson as json
from loguru import logger

from . import DATA_DIR
from .courts import CourtInfoByIncident
from .geo import *
from .homicides import PPDHomicideTotal
from .shootings import ShootingVictimsData
from .streets import StreetHotSpots


@click.group()
@click.version_option()
def cli():
    """Processing data for the Controller's Office gun violence dashboard.

    https://controller.phila.gov/philadelphia-audits/mapping-gun-violence/#/
    """
    pass


@cli.command()
@click.option("--debug", is_flag=True)
def save_geojson_layers(debug=False):
    """Save the various geojson layers needed in the dashboard."""

    # ------------------------------------------------
    # Part 1: Hot spot streets
    # -------------------------------------------------
    hotspots = StreetHotSpots(debug=debug)
    hotspots.save()

    # ------------------------------------------------
    # Part 2: Police Districts
    # -------------------------------------------------
    if debug:
        logger.debug("Saving police districts as a GeoJSON file")
    path = DATA_DIR / "processed" / "geo" / "police_districts.geojson"
    get_police_districts().to_crs(epsg=4326).to_file(path, driver="GeoJSON")

    # ------------------------------------------------
    # Part 3: Council Districts
    # -------------------------------------------------
    if debug:
        logger.debug("Saving council districts as a GeoJSON file")
    path = DATA_DIR / "processed" / "geo" / "council_districts.geojson"
    get_council_districts().to_crs(epsg=4326).to_file(path, driver="GeoJSON")

    # ------------------------------------------------
    # Part 4: ZIP Codes
    # -------------------------------------------------
    if debug:
        logger.debug("Saving zip codes as a GeoJSON file")
    path = DATA_DIR / "processed" / "geo" / "zip_codes.geojson"
    get_zip_codes().to_crs(epsg=4326).to_file(path, driver="GeoJSON")


@cli.command()
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
def daily_update(debug=False):
    """Run the daily pre-processing update.

    This runs the following steps:

        1. Download a fresh copy of the shooting victims database.

        2. Merge data for hot spot blocks.

        3. Merge data for court information.

        4. Save the processed shooting victims database.

        5. Save the cumulative daily shooting victims total.

        6. Scrape and save the homicide count from the PPD's website.
    """
    # ---------------------------------------------------
    # Part 1: Main shooting victims data file
    # ---------------------------------------------------
    victims = ShootingVictimsData(debug=debug)
    data = victims.get(fresh=True, update_local=True)

    # Value-added info for hot spots and court info
    hotspots = StreetHotSpots(debug=debug)
    courts = CourtInfoByIncident(debug=debug)

    # Merge in the value-added info
    data = data.pipe(hotspots.merge).pipe(courts.merge)

    # Save victims data to annual files
    victims.save(data)

    # -----------------------------------------------------
    # Part 2: Cumulative daily victim totals
    # -----------------------------------------------------
    victims.save_cumulative_totals(data, update_local=True)

    # ------------------------------------------------------
    # Part 3: Homicide count scraped from PPD
    # ------------------------------------------------------
    homicide_count = PPDHomicideTotal(debug=debug)
    homicide_count.update()

    # Update meta data
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_path = DATA_DIR / "meta.json"

    # save the download time
    meta = {"last_updated": now}
    json.dump(meta, meta_path.open(mode="w"))


@cli.command()
@click.option(
    "--nprocs",
    type=int,
    default=1,
    help="If running in parallel, the total number of processes that will run.",
)
@click.option(
    "--pid",
    type=int,
    default=0,
    help=(
        "If running in parallel, the local process id."
        "This should be between 0 and number of processes."
    ),
)
@click.option(
    "--sleep",
    default=7,
    help="Total waiting time b/w scraping calls (in seconds)",
    type=int,
)
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
@click.option(
    "--sample",
    type=int,
    default=None,
    help="Only run a random sample of incident numbers.",
)
def scrape_courts_portal(nprocs, pid, sleep, debug, sample, dry_run):
    """Scrape courts information from the PA's Unified Judicial System's portal.

    This can be run in parallel by specifying a total
    number of processes and a specific chunk to run.
    """

    # Load the shootings data
    shootings = ShootingVictimsData(debug=debug).get(fresh=False)
    shootings["dc_key"] = shootings["dc_key"].astype(str)

    # Drop duplicates
    shootings = shootings.drop_duplicates(subset=["dc_key"])

    # Sample?
    if sample is not None:
        shootings = shootings.sample(sample)

    # Split
    assert pid < nprocs
    if nprocs > 1:
        shootings_chunk = np.array_split(shootings, nprocs)[pid]
        chunk = pid
    else:
        shootings_chunk = shootings
        chunk = None

    # Scrape courts info
    courts_data = CourtInfoByIncident(debug=debug)
    courts_data.update(shootings_chunk, chunk=chunk, sleep=sleep, dry_run=dry_run)


@cli.command()
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
def finalize_courts_scraping(debug, dry_run):
    """Finalize courts scraping by combining scraping results
    computed in parallel.

    This updates the "scraped_courts_data.json" data file.
    """

    # Load the shootings data
    data_path = DATA_DIR / "raw"
    files = data_path.glob("scraped_courts_data_*.json")

    combined = {}
    for f in sorted(files):
        if debug:
            logger.debug(f"Combining file: '{f}'")
        combined.update(json.load(f.open("r")))

    if not dry_run:
        json.dump(combined, (DATA_DIR / "raw" / "scraped_courts_data.json").open("w"))


if __name__ == "__main__":
    cli(prog_name="gv_dashboard_data")
