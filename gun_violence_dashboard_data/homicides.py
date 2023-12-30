"""Scrape the total homicide count from the Philadelphia Police Department's 
Crime Stats website."""

from dataclasses import dataclass
from datetime import date

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver

import pandas as pd
from bs4 import BeautifulSoup
from cached_property import cached_property
from loguru import logger

from . import DATA_DIR


def get_webdriver(debug=False):
    """
    Initialize a selenium web driver with the specified options.

    Parameters
    ----------
    debug: bool
        Whether to use the headless version of Chrome
    """
    # Create the options
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    if not debug:
        options.add_argument("--headless")

    return webdriver.Chrome(options=options)


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
        # Get the driver
        driver = get_webdriver(debug=self.debug)

        # Navigate to the page
        driver.get(self.URL)

        # Wait for the tables to load
        delay = 5  # seconds
        try:
            WebDriverWait(driver, delay).until(
                EC.presence_of_element_located((By.ID, "stats-content"))
            )

            # Get the page source
            self.soup = BeautifulSoup(driver.page_source, "html.parser")

            # Get the two tables on the page
            self.tables = self.soup.select("table")

        except TimeoutException:
            raise ValueError("Page took too long to load")

    @cached_property
    def years(self):
        """The years available on the page. Starts with 2007."""

        # Get the years from both tables and take the unique ones
        years = []
        for table in self.tables:
            years += [
                int(th.text)
                for th in table.select_one("thead").select("th")
                if th.text.startswith("2")
            ]

        return list(sorted(set(years), reverse=True))

    @cached_property
    def as_of_date(self):
        """The current "as of" date on the page."""

        # This will be in the form of "Month name Day"
        date = self.tables[0].select_one("tbody").select_one("tr").select_one("th").text

        # Return a datetime object
        return pd.to_datetime(f"{date} {self.years[0]}" + " 11:59:00")

    @cached_property
    def annual_totals(self):
        """The annual totals for homicides in Philadelphia."""

        # This is for historic data only (doesn't include current year)
        annual_totals = [
            int(td.text)
            for td in self.tables[1].select_one("tbody").select("td")
            if td.text
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

        # Years
        years = [
            int(th.text)
            for th in self.tables[0].select_one("thead").select("th")
            if th.text.startswith("2")
        ]

        # Get the YTD totals
        ytd_totals = []
        for i, td in enumerate(self.tables[0].select_one("tbody").select("td")):
            if i == 0:
                value = td.select_one("div").text
            else:
                value = td.text

            if value:
                ytd_totals.append(int(value))

        if len(ytd_totals) != len(years):
            raise ValueError("Length mismatch between parsed years and YTD homicides")

        # Return ytd totals, sorted in ascending order
        out = pd.DataFrame({"year": years, "ytd": ytd_totals})
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

    def _get_years_from_year_end_section(self):
        return [
            int(th.text)
            for th in self.tables[1].select_one("thead").select("th")
            if th.text.startswith("2")
        ]

    def update(self, force=False):
        """Update the local data via scraping the PPD website."""

        # Check for new year's
        year_end_years = self._get_years_from_year_end_section()
        max_year_end_year = max(year_end_years)

        thisYear = date.today().year
        if thisYear != max_year_end_year + 1:
            raise ValueError(
                f"It seems like we are in a new year {thisYear} but the homicide page hasn't been updated yet"
            )

        # Load the database
        database = self.get()

        # Latest database date
        latest_database_date = database.iloc[-1]["date"]

        # Remove last row
        if self.as_of_date == latest_database_date:
            database = database.drop(index=database.index[-1])

        # Update
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
        new_year = database.iloc[-1]["date"].year
        old_year = database.iloc[-2]["date"].year
        if (
            not force
            and new_homicide_total < old_homicide_total
            and (new_year == old_year)
        ):
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
