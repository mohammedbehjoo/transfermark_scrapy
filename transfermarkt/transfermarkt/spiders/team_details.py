import scrapy
import os
import json
import pandas as pd

class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = ["https://www.transfermarkt.com"]
    
    def __init__(self, *args, **kwargs):
        super(TeamDetailsSpider, self).__init__(*args, **kwargs)
        # path of the teams.json file
        teams_file_path = "/home/mohammed/projects/coding/transfermarkt_scrapy/transfermarkt/transfermarkt/spiders/teams0.json"
        
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
        
        # get the teams url
        self.teams_url_list = []
        for _, row in self.df.iterrows():
            self.teams_url_list.append(row["team_url"])
        self.teams_url_list = self.teams_url_list[:2]  # Limiting to 2 teams for testing
        
        # Initialize the league data structure
        self.league_data = {
            "league_name": self.df["league_name"].iloc[0] if not self.df.empty else None,
            "country_name": self.df["country_name"].iloc[0] if not self.df.empty else None,
            "season": self.df["season"].iloc[0] if not self.df.empty else None,
            "teams": []
        }
        
        # getting the season id and the stats url
        stats_url_list=[]
        if self.teams_url_list:
            for index,_ in enumerate(self.teams_url_list):
                base_url = self.teams_url_list[index]
                root_url = base_url.split("/")
                season_id = root_url[-1]
                stats_url = "/".join(root_url[:4])+"/leistungsdaten/"
                stats_url = stats_url + \
                    "/".join(root_url[5:7])+"/reldata"+f"/%26{season_id}"+"/plus/1"
                stats_url_list.append(stats_url)

    def start_requests(self):
        print(f"teams url list:\n{self.teams_url_list},\nlen: {len(self.teams_url_list)}")
        for url in self.teams_url_list:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"failed to retrieve data from {response.url}")
            return
        
        # CSS selectors remain the same
        TEAM_SELECTOR = "#tm-main"
        LEAGUE_NAME_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__club"
        TABLE_COUNTRY_POSITION_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__label span.data-header__content a"
        NATIONAL_PLAYERS_NUM_SELECTOR = ".data-header__info-box  div.data-header__details ul.data-header__items li.data-header__label span.data-header__content"
        TEAM_NAME_SELECTOR = ".data-header__headline-wrapper--oswald"
        CURRENT_TRANSFER_RECORD_SELECTOR = ".data-header__content span a"

        # Extract team details
        for index, element in enumerate(response.css(TABLE_COUNTRY_POSITION_SELECTOR)):
            if index == 1:
                position_data = element.css("::text").get()
            if element.css("img"):
                country_data = element.css("img").attrib.get("title")

        for index, element in enumerate(response.css(NATIONAL_PLAYERS_NUM_SELECTOR)):
            if index == 3:
                national_players_data = element.css("a::text").get()

        
        for team_detail_item in response.css(TEAM_SELECTOR):
            team_name = team_detail_item.css(TEAM_NAME_SELECTOR).css("::text").get()
            current_transfer_record = team_detail_item.css(CURRENT_TRANSFER_RECORD_SELECTOR).css("::text").get()
            
            # if there is a team_name, then the following dict will be created.
            if team_name:
                team_detail = {
                    "team_name": team_name,
                    "table_position": position_data,
                    "current_transfer_record": current_transfer_record,
                    "national_players_num": national_players_data,
                    "players": []
                }
                # append the team_detail data to the "teams" key into the league_data dictionary
                self.league_data["teams"].append(team_detail)
                
        # only yield league_data after all teams have been processed
        if len(self.league_data["teams"])==len(self.teams_url_list):
            yield self.league_data
