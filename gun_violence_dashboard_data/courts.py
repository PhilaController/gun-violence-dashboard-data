"""Scrape court information from the PA's Unified Judicial System portal."""
from dataclasses import dataclass

import numpy as np
import simplejson as json
from loguru import logger
from phl_courts_scraper.portal import UJSPortalScraper

from . import DATA_DIR


@dataclass
class CourtInfoByIncident:
    """
    Court information for shooting incidents scraped from the
    PA's Unified Judicial System.
    """

    sleep: int = 2
    debug: bool = False

    def __post_init__(self):

        # Initialize the scraper
        self.scraper = UJSPortalScraper(sleep=self.sleep)

    @property
    def path(self):
        return DATA_DIR / "raw" / "scraped_courts_data.json"

    def get(self):
        """Get the shooting victims data, either loading
        the currently downloaded version or a fresh copy."""

        return json.load(self.path.open("r"))

    def merge(self, data):
        """Merge courts data."""

        # Load raw courts data and existing dc keys
        courts = self.get()
        existing_dc_keys = [key for key in courts.keys() if len(courts[key])]

        if self.debug:
            logger.debug("Merging in court case information")

        out = data.copy()
        return out.assign(
            has_court_case=lambda df: np.select(
                [df.dc_key.isin(existing_dc_keys)], [True], default=False
            )
        )

    def update(self, shootings, chunk=None, dry_run=False):
        """Scrape the courts portal."""

        # Get the incident numbers
        incident_numbers = shootings["dc_key"].tolist()

        # Scrape the results
        results = self.scraper.scrape_incident_data(incident_numbers)

        # Save
        if not dry_run:
            if chunk is None:
                filename = "scraped_courts_data.json"
            else:
                filename = f"scraped_courts_data_{chunk}.json"
            json.dump(results, (DATA_DIR / "raw" / filename).open("w"))
