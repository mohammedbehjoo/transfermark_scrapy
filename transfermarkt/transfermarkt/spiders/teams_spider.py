import scrapy
from transfermarkt.items import TeamItem
import pandas as pd
import json
import os

# path of the json file of country name, league names and league urls of the leagues crawled with leagues_spider
leagues_file_path = "/home/mohammed/projects/coding/transfermarkt_scrapy/transfermarkt/transfermarkt/spiders/leagues.json"

# check if the file is not empty
if os.path.exists(leagues_file_path) and os.path.getsize(leagues_file_path) > 0:
    # read the json file and load it to a variable named data
    with open(leagues_file_path, 'r') as f:
        data = json.load(f)
else:
    data = []
    print(f"the leagues file is either missing or empty.")


class TeamsSpiderSpider(scrapy.Spider):
    # count the teams that are scraped
    team_counter = 0

    # start year and end year
    start_year, end_year = 2023, 2025

    name = "teams_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/"]

    def start_requests(self):
        # create variables for country_name, league_name, and league_url
        df = pd.json_normalize(data, 'leagues', ['country_name'])
        country_name_list = []
        league_name_list = []
        base_url_list = []
        for _, row in df.iterrows():
            country_name_list .append(row["country_name"])
            league_name_list .append(row["league_name"])
            base_url_list.append(row["league_url"])

        for season in range(self.start_year, self.end_year):
            for index in range(len(base_url_list)):
                # get country name and league name each time
                country_name = country_name_list[index]
                league_name = league_name_list[index]
                season_num = season
                # add the parameters of each season to the base league_url
                season_url = f"{base_url_list[index]}/plus/?saison_id={season}"
                self.logger.info(f"Requesting URL: {season_url}")
                # add the saison_id to the url of the league_url
                yield scrapy.Request(url=season_url, callback=self.parse, cb_kwargs={"country_name": country_name, "league_name": league_name, "season": season_num})

    def parse(self, response, country_name, league_name, season):
        # print and log the response status and url
        print(f"Response status: {response.status}")
        self.logger.info(f"Response status: {response.status}")
        self.logger.info(f"Response URL: {response.url}")
        if response.status != 200:
            self.logger.error(f"Falied to retrieve data from {response.url}")
            return

        TEAM_SELECTOR = "#yw1 .items tbody tr"  # this is the root items css element

        # create a dictionary for the teams data of each country and league and season
        seasons_data = {
            "country_name": country_name,
            "league_name": league_name,
            "season": season,
            "teams" : []
        }

        print("Total rows found:", len(response.css(TEAM_SELECTOR)))

        for team_row in response.css(TEAM_SELECTOR):
            team_item = {
                "team_name": team_row.css(
                    "td.hauptlink.no-border-links a::text").get(),
                "team_url": "https://www.transfermarkt.com" +
                team_row.css(
                    "td.hauptlink.no-border-links a::attr(href)").get(),
                "squad_size": team_row.css(
                    "td.zentriert:nth-of-type(3) a::text").get(),
                "avg_age": team_row.css(
                    "td.zentriert:nth-of-type(4)::text").get(),
                "foreigners_num": team_row.css(
                    "td.zentriert:nth-of-type(5)::text").get(),
                "avg_market": team_row.css("td.rechts::text").get(),
                "total_market": team_row.css("td.rechts a::text").get()
                # print(
                #     f"Found team {team['team_name']}, Found squad size: {team['squad_size']}, avg age is: {team['avg_age']}, team url is: {team['team_url']}")
                # print(
                #     f"Number of foreigners is: {team['foreigners_num']}, avg market value is: {team['avg_market']}, and total market value is: {team['total_market']}")
            }
            self.team_counter += 1
            self.logger.info(f"team counter is: {self.team_counter}")
            print(f"team counter is: {self.team_counter}")
            seasons_data["teams"].append(team_item)
            # check if there are any teams found
        if seasons_data["teams"]:
            yield seasons_data
        else:
            self.logger.warning(f"No teams found for this league. {league_name}")

        # Yield the data if any teams were found
        # if any(seasons_data[key] for key in seasons_data if key.startswith("seasons_")):
        #     yield seasons_data
        # else:
        #     self.logger.warning(
        #         f"No teams found for this league: {league_name}")
