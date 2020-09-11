import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from cached_property import cached_property

from . import DATA_DIR


class PPDHomicideScraper:
    """Scrape homicide data from the Philadelphia Police Department's website"""

    URL = "https://www.phillypolice.com/crime-maps-stats/"

    def __init__(self):

        # Parse daily data
        self.historic = pd.read_csv(
            DATA_DIR / "processed" / "homicides_daily.csv", parse_dates=[0]
        ).sort_values("date", ascending=True)

        # Parse the URL
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
    def latest_historic_date(self):
        """The latest data in the saved historic data file."""
        return self.historic.iloc[-1]["date"]

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

        return pd.DataFrame({"year": self.years, "ytd": ytd_totals}).sort_values(
            "year", ascending=False
        )

    def run_update(self):
        """Run the scraping update."""

        # Check if we need to update
        if self.latest_historic_date < self.as_of_date:

            # Merge annual and YTD
            data = pd.merge(self.annual_totals, self.ytd_totals, on="year", how="outer")
            data.set_index("year").to_json(
                DATA_DIR / "processed" / "homicide_totals.json", orient="index"
            )

            # Add new row
            YTD = self.ytd_totals.iloc[0]["ytd"]
            self.historic.loc[len(self.historic)] = [self.as_of_date, YTD]

            # Save it
            self.historic.to_csv(
                DATA_DIR / "processed" / "homicides_daily.csv", index=False
            )

            return True

        return False
