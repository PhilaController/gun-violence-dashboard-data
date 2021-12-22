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

    # ------------------------------------------------
    # Part 5: Neighborhoods
    # -------------------------------------------------
    if debug:
        logger.debug("Saving neighborhoods as a GeoJSON file")
    path = DATA_DIR / "processed" / "geo" / "neighborhoods.geojson"
    get_neighborhoods().to_crs(epsg=4326).to_file(path, driver="GeoJSON")


@cli.command()
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
@click.option(
    "--ignore-checks", is_flag=True, help="Whether to ignore any validation checks."
)
@click.option(
    "--homicides-only", is_flag=True, help="Whether to process the Homicide data."
)
@click.option(
    "--shootings-only", is_flag=True, help="Whether to process the shooting data."
)
@click.option(
    "--force-homicide-update",
    is_flag=True,
    help="Whether to force the homicide update.",
)
def daily_update(
    debug=False,
    ignore_checks=False,
    homicides_only=False,
    shootings_only=False,
    force_homicide_update=False,
):
    """Run the daily pre-processing update.

    This runs the following steps:

        1. Download a fresh copy of the shooting victims database.

        2. Merge data for hot spot blocks.

        3. Merge data for court information.

        4. Save the processed shooting victims database.

        5. Save the cumulative daily shooting victims total.

        6. Scrape and save the homicide count from the PPD's website.
    """
    # Do all parts
    process_all = not (homicides_only or shootings_only)

    # Initialize meta
    meta = {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------
    # Part 1: Homicide count scraped from PPD
    # ------------------------------------------------------
    if process_all or homicides_only:

        # Run the update
        homicide_count = PPDHomicideTotal(debug=debug)
        homicide_count.update(force=force_homicide_update)

        # Update the meta
        meta["last_updated_homicides"] = now

    # ---------------------------------------------------
    # Part 2: Main shooting victims data file
    # ---------------------------------------------------
    if process_all or shootings_only:
        victims = ShootingVictimsData(debug=debug, ignore_checks=ignore_checks)
        data = victims.get(fresh=True, update_local=True)

        # Value-added info for hot spots and court info
        hotspots = StreetHotSpots(debug=debug)
        courts = CourtInfoByIncident(debug=debug)

        # Merge in the value-added info
        data = data.pipe(hotspots.merge).pipe(courts.merge)

        # Save victims data to annual files
        victims.save(data)

        # Update the meta
        meta["last_updated_shootings"] = now

    # Update meta data
    meta_path = DATA_DIR / "meta.json"
    existing_meta = json.load(meta_path.open(mode="r"))

    # Remove old key
    if "last_updated" in existing_meta:
        existing_meta.pop("last_updated")

    # Add new info
    existing_meta.update(meta)

    # Save the download time
    json.dump(existing_meta, meta_path.open(mode="w"))

    # -----------------------------------------------------
    # Part 3: Cumulative daily victim totals
    # -----------------------------------------------------
    # victims.save_cumulative_totals(data, update_local=True)


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

    # Format dc_key
    valid = shootings["dc_key"] != ""
    shootings = shootings.loc[valid]
    shootings["dc_key"] = shootings["dc_key"].astype(float).astype(int).astype(str)

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
    courts_data = CourtInfoByIncident(debug=debug, sleep=sleep)
    courts_data.update(shootings_chunk, chunk=chunk, dry_run=dry_run)


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
