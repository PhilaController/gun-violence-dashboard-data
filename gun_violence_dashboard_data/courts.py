import json
import time

import geopandas as gpd
import pandas as pd
from loguru import logger
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from phl_courts_scraper.scrape import IncidentNumberScraper

from . import DATA_DIR


def scrape_courts_portal(sleep=7):
    """Scrape"""

    # Initialize the driver in headless mode
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    # Initialize the scraper
    scraper = IncidentNumberScraper(driver)

    # Load the shootings data
    shootings = gpd.read_file(DATA_DIR / "raw" / "shootings.json")
    shootings["dc_key"] = shootings["dc_key"].astype(str)

    # Load existing courts data
    courts = json.load((DATA_DIR / "raw" / "scraped_courts_data.json").open("r"))
    existing_dc_keys = list(courts.keys())

    # Trim shootings to those without cases
    shootings = shootings.loc[~shootings["dc_key"].isin(existing_dc_keys)]
    shootings = shootings.drop_duplicates(subset=["dc_key"])
    N = len(shootings)
    logger.info(f"Scraping info for {N} shooting incidents")

    # Save new results here
    new_results = {}

    # Loop over shootings and scrape
    try:
        for i in range(N):
            if i % 50 == 0:
                logger.info(i)
            dc_key = shootings.iloc[i]["dc_key"]

            # Some DC keys for OIS are shorter
            if len(dc_key) == 12:

                # Scrape!
                scraping_result = scraper.scrape(dc_key[2:])

                # Save those with new results
                if scraping_result is not None:
                    new_results[dc_key] = scraping_result

                # Sleep!
                time.sleep(sleep)

    except Exception as e:
        logger.info(f"Exception raised: {e}")
    finally:
        logger.info(f"Done scraping: {i} DC keys scraped")
        logger.info(f"  Found {len(new_results)} DC keys with new info")

        # Update and save
        courts.update(new_results)
        json.dump(courts, (DATA_DIR / "raw" / "scraped_courts_data.json").open("w"))


def merge_courts_data(data):
    """Merge courts data."""

    # Load raw courts data
    data = json.load((DATA_DIR / "raw" / "scraped_courts_data.json").open("r"))
    dc_keys = list(data.keys())

    # Check dc keys
    data["dc_key"] = data["dc_key"].astype(str)
    data["has_court_case"] = False
    data.loc[data["dc_key"].isin(dc_keys), "has_court_case"] = True

    return data
