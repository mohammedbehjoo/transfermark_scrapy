import scrapy
import os
import json
import pandas as pd
from dotenv import load_dotenv



class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = ["https://www.transfermarkt.com"]

    def __init__(self, *args, **kwargs):
        super(TeamDetailsSpider, self).__init__(*args, **kwargs)

        # loading the confing.env file to get the teams.json file
        load_dotenv("config.env")
        # path of the teams.json file
        teams_file_path = os.getenv("TEAMS_FILE_PATH")

        # check if the file is not empty
        if os.path.exists(teams_file_path) and os.path.getsize(teams_file_path) > 0:
            print("teams json file is not empty")
            with open(teams_file_path, "r") as file:
                self.data = json.load(file)
        else:
            self.data = []
            print("teams json file is either missing or empty")

        # create a dataframe from the teams json file
        self.df = pd.json_normalize(
            self.data,
            record_path=["teams"],
            meta=["country_name", "league_name", "season"]
        )

    def start_requests(self):
        # Initialize a list to store all teams' URLs
        self.teams_url_list = []
        for _, row in self.df.iterrows():
            self.teams_url_list.append(row["team_url"])
        print(f"teams url list:\n{self.teams_url_list},len: {len(self.teams_url_list)}")

        # Initialize a dictionary to hold league data
        self.league_data_dict = {}

        for _, row in self.df.iterrows():
            team_url = row["team_url"]
            yield scrapy.Request(url=team_url, callback=self.parse, meta={"row": row})

    def parse(self, response):
        # Logging the page status
        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return
        print(f"parsing:\n{response.url}\n")
        # Retrieve row data passed via meta
        row = response.meta['row']
        league_name = row["league_name"]
        country_name = row["country_name"]
        season = row["season"]

        # Extract the required team data
        TABLE_COUNTRY_POSITION_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__label span.data-header__content a"
        NATIONAL_PLAYERS_NUM_SELECTOR = ".data-header__info-box  div.data-header__details ul.data-header__items li.data-header__label span.data-header__content"
        TEAM_NAME_SELECTOR = ".data-header__headline-wrapper--oswald"
        CURRENT_TRANSFER_RECORD_SELECTOR = ".data-header__content span a"

        position_data = None
        national_players_data = None

        # Extract position data
        for index, element in enumerate(response.css(TABLE_COUNTRY_POSITION_SELECTOR)):
            if index == 1:
                position_data = element.css("::text").get()

        # Extract national players data
        for index, element in enumerate(response.css(NATIONAL_PLAYERS_NUM_SELECTOR)):
            if index == 3:
                national_players_data = element.css("a::text").get()
        
        # Extract team name
        team_name = response.css(TEAM_NAME_SELECTOR).css("::text").get().strip()
        
        # Extract current transfer record
        current_transfer_record = response.css(CURRENT_TRANSFER_RECORD_SELECTOR).css("::text").get()
        
        # Create the team details
        team_detail = {
            "team_name": team_name,
            "table_position": position_data,
            "current_transfer_record": current_transfer_record,
            "national_players_num": national_players_data
        }

        # Create a unique key for the league
        league_key = f"{league_name}_{country_name}_{season}"

        # Check if the league already exists in the dictionary
        if league_key not in self.league_data_dict:
            self.league_data_dict[league_key] = {
                "league_name": league_name,
                "country_name": country_name,
                "season": season,
                "teams": []
            }

        # Append this team to the league's team list
        self.league_data_dict[league_key]["teams"].append(team_detail)

        
        print(f"league_data_dict:\n{self.league_data_dict}\n")
        team_names_from_league_data_dict=[team["team_name"] for league in self.league_data_dict.values() for team in league["teams"]]
        print(f"len team names:{len(team_names_from_league_data_dict)}")
        # Check if all teams for this league are processed
        if len(team_names_from_league_data_dict) == len(self.teams_url_list):
            # Yield the final structured data
            yield self.league_data_dict
        else:
            print("NOOOOO")

