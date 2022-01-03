"""Scrape the total homicide count from the Philadelphia Police Department's 
Crime Stats website."""

from dataclasses import dataclass

import pandas as pd
import requests
from bs4 import BeautifulSoup
from cached_property import cached_property
from loguru import logger

from . import DATA_DIR


@dataclass
class PPDHomicideTotal:
    """Total number of homicides scraped from the Philadelphia Police
    Department's website.

    This provides:
        - Annual totals since 2007 for past years.
        - Year-to-date homicide total for the current year.

    Source
    ------
    https://www.phillypolice.com/crime-maps-stats/
    """

    debug: bool = False

    URL = "https://www.phillypolice.com/crime-maps-stats/"

    def __post_init__(self):
        self.soup = BeautifulSoup(requests.get(self.URL).content, "html.parser")

    @cached_property
    def years(self):
        """The years available on the page. Starts with 2007."""

        return [
            int(td.text)
            for td in self.soup.select("#homicide-stats")[0]
            .find("tr")
            .find_all("th")[1:]
        ]

    @cached_property
    def as_of_date(self):
        """The current "as of" date on the page."""

        date = (
            self.soup.select("#homicide-stats")[0]
            .select("tbody")[0]
            .select_one("td")
            .text.split("\n")[0]
        )
        return pd.to_datetime(date + " 11:59:00")

    @cached_property
    def annual_totals(self):
        """The annual totals for homicides in Philadelphia."""

        # This is for historic data only (doesn't include current year)
        annual_totals = [
            int(td.text)
            for td in self.soup.select("#homicide-stats")[1].find_all("td")[1:]
        ]

        if len(annual_totals) != len(self.years[1:]):
            raise ValueError(
                "Length mismatch between parsed years and annual homicide totals"
            )

        return pd.DataFrame(
            {"year": self.years[1:], "annual": annual_totals}
        ).sort_values("year", ascending=False)

    @cached_property
    def ytd_totals(self):
        """The year-to-date totals for homicides in Philadelphia."""

        # Scrape the table
        table = self.soup.select("#homicide-stats")[0]
        ytd_totals = [table.select("tbody")[0].select(".homicides-count")[0].text]
        ytd_totals += [td.text for td in table.select("tbody")[0].find_all("td")[2:-1]]
        ytd_totals = list(map(int, ytd_totals))

        if len(ytd_totals) != len(self.years):
            raise ValueError("Length mismatch between parsed years and homicides")

        # Return ytd totals, sorted in ascending order
        out = pd.DataFrame({"year": self.years, "ytd": ytd_totals})
        return out.sort_values("year", ascending=False)

    @property
    def path(self):
        return DATA_DIR / "raw" / "homicide_totals_daily.csv"

    def get(self):
        """Get the shooting victims data, either loading
        the currently downloaded version or a fresh copy."""

        # Load the database of daily totals
        df = pd.read_csv(self.path, parse_dates=[0])

        # Make sure it's in ascending order by date
        return df.sort_values("date", ascending=True)

    def update(self, force=False):
        """Update the local data via scraping the PPD website."""

        # Load the database
        database = self.get()

        # Latest database date
        latest_database_date = database.iloc[-1]["date"]

        # Update if we need to
        if force or latest_database_date < self.as_of_date:

            if self.debug:
                logger.debug("Parsing PPD website to update YTD homicides")

            # Merge annual totals (historic) and YTD (current year)
            data = pd.merge(self.annual_totals, self.ytd_totals, on="year", how="outer")

            # Add new row to database
            YTD = self.ytd_totals.iloc[0]["ytd"]
            database.loc[len(database)] = [self.as_of_date, YTD]

            # Sanity check on new total
            new_homicide_total = database.iloc[-1]["total"]
            old_homicide_total = database.iloc[-2]["total"]
            new_year = database.iloc[-1]['date'].year
            old_year = database.iloc[-2]['date'].year
            if not force and new_homicide_total < old_homicide_total and (new_year==old_year):
                raise ValueError(
                    f"New YTD homicide total ({new_homicide_total}) is less than previous YTD total ({old_homicide_total})"
                )

            # Save it
            path = DATA_DIR / "processed" / "homicide_totals.json"
            data.set_index("year").to_json(path, orient="index")

            # Save it
            if self.debug:
                logger.debug("Updating PPD homicides data file")

            # Drop duplicates and save
            database.drop_duplicates(subset=["date"], keep="last").to_csv(
                self.path, index=False
            )
