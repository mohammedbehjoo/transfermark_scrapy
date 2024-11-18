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
    def check_team_exists(data, target_team_name):
        for league_key, league_info in data.items():
            for team in league_info['teams']:
                if team['team_name'] == target_team_name:
                    return True  # Team found
        return False  # Team not found
    
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
            "national_players_num": national_players_data,
            "players":[]
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
                    
        self.league_data_dict[league_key]["teams"].append(team_detail)
        print(f"league_data_dict:\n{self.league_data_dict}\n")
        team_names_league_data=[team["team_name"] for league in self.league_data_dict.values() for team in league["teams"]]
        print(f"len team names:{len(team_names_league_data)}\n")
        print(f"team names:\n{team_names_league_data}\n")
        
        HREF_SELECTOR = "div.tm-tabs a.tm-tab"
        elements = response.css(HREF_SELECTOR)
        if len(elements) > 1:
            detailed_squad_page = "https://www.transfermarkt.com" + \
                elements[1].css("a").attrib.get("href")

            if detailed_squad_page:
                yield response.follow(detailed_squad_page, callback=self.parse_detailed_squad, meta={"team_detail": team_detail,"league_data_dict":self.league_data_dict})
            else:
                # Append this team to the league's team list
                self.league_data_dict[league_key]["teams"].append(team_detail)
                
                
    def parse_detailed_squad(self, response):
        print(f"now detailed squad url: {response.url}\n")
        print(f"hello\n")
        
        team_detail=response.meta["team_detail"]
        league_data_dict=response.meta["league_data_dict"]
        print(f"first league data dict\n{league_data_dict}\n")
        print(f"first team detail detailed sqaud:\n{team_detail},type:\n{type(team_detail)}\n")
        # extraction
        PLAYER_NAME_SELECTOR = ".items tbody td.posrela td.hauptlink a::text"
        raw_names = response.css(PLAYER_NAME_SELECTOR).getall()
        cleaned_names = [name.strip() for name in raw_names if name.strip()]
        
        player_list = []
        for name in cleaned_names:
            player_dict = {
                "player_name": name
            }
            player_list.append(player_dict)
        print(f"players list detailed squad\n{player_list}\n")
        
        # Update team_detail with the player list
        team_detail["players"] = player_list
        # print(f"second team detail\n{team_detail}\n")

        print(f"second league data dict\n{league_data_dict}\n")
        
        print(f"second team detail team name\n{team_detail['team_name']}\n")
        team_name_check=team_detail['team_name']
        print(f"team_name_check\n{team_name_check}\n")
        for key, league_data in league_data_dict.items():
            if any(team['team_name'] == team_name_check for team in league_data['teams']):
                generated_key = f"{league_data['league_name']}_{league_data['country_name']}_{league_data['season']}"
                print(f"Found {team_name_check}. Generated key: {generated_key}")
                if generated_key in league_data_dict:
                    # Remove existing team if it exists
                    league_data_dict[generated_key]["teams"] = [team for team in league_data_dict[generated_key]["teams"] if team['team_name'] != team_detail['team_name']]
                    # Append the updated team_detail to the correct league entry
                    league_data_dict[generated_key]["teams"].append(team_detail)
                else:
                    # If the league doesn't exist, create a new entry
                    league_data_dict[generated_key] = {
                        "league_name": team_detail["league_name"],
                        "country_name": team_detail["country_name"],
                        "season": team_detail["season"],
                        "teams": [team_detail]  # Start with the current team
                    }
        for key, league_data in league_data_dict.items():
            # Find the last team in the teams list
            last_team = league_data['teams'][-1] if league_data['teams'] else None

        if last_team and 'players' in last_team and last_team['players']:
            # Check if there is any player value in the last team's players list
            if last_team['players']:
                print(f"Found players in the last team '{last_team['team_name']}'")
                print(f"yeilding second time\n")
                yield league_data_dict
            else:
                print(f"No players found in the last team '{last_team['team_name']}'")
        # team_to_check=team_detail['team_name']
        # print(f"team to check\n{team_to_check}\n")
        
        # exists = self.check_team_exists(league_data_dict, team_to_check) 

        # key=f"{league_data_dict['league_name']}_{league_data_dict['country_name']}_{league_data_dict['season']}"

        # print(f"key is:\n{key}\n")
        # if exists:
        #     print(f"appending team detail")
        #     league_data_dict[key]["teams"].append(team_detail)
        # team_names_league_data=[team["team_name"] for league in league_data_dict.values() for team in league["teams"]]
        # if len(team_names_league_data) == len(self.teams_url_list):
        #     # Yield the final structured data
        #     print(f"yield\n")
        #     yield self.league_data_dict
        # else:
        #     print("their length is not the same")