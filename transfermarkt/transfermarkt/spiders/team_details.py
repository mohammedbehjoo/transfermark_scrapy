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
    def is_valid_float(self,item):
        self.item=item
        try:
            # Try converting to float, and ensure it remains a valid numeric string without extra characters
            float(item)
            return True
        except ValueError:
            return False
    
    def start_requests(self):
        # Initialize a list to store all teams' URLs
        self.teams_url_list = []
        for _, row in self.df.iterrows():
            self.teams_url_list.append(row["team_url"])
        print(f"teams url list:\n{self.teams_url_list},len: {len(self.teams_url_list)}")

        # get stats url list for the prase_detailed_stats method
        self.stats_url_list = []
        if self.teams_url_list:
            for team_url in self.teams_url_list:
                root_url = team_url.split("/")
                season_id = root_url[-1]
                stats_url = "/".join(root_url[:4])+"/leistungsdaten/"
                stats_url = stats_url + \
                    "/".join(root_url[5:7])+"/reldata" + \
                    f"/%26{season_id}"+"/plus/1"
                self.stats_url_list.append(stats_url)
        print(f"stats url list:\n{self.stats_url_list}\nlen stats URL: {len(self.stats_url_list)}\n")
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
        print(f"starting parse method with URL:\n{response.url}\n")
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
        
        team_names_league_data=[team["team_name"] for league in self.league_data_dict.values() for team in league["teams"]]
        
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
        # logging the page status. if it is not == 200, then it won't proceed.
        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return
        
        print(f"starting parse_detailed_squad method with this URL: {response.url}\n")
        
        team_detail=response.meta["team_detail"]
        league_data_dict=response.meta["league_data_dict"]


        # css selectors
        PLAYER_NAME_SELECTOR = ".items tbody td.posrela td.hauptlink a::text"
        PLAYER_POSITION_SELECTOR = ".items tbody  td.posrela tr td::text"
        PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR = ".items td.zentriert"
        PLAYER_CURRENT_CLUB_SELECTOR = ".items td.zentriert"
        PLAYER_HEIGHT_SELECTOR = ".items td.zentriert"
        MARKET_VALUE_SELECTOR = ".rechts"
        
        # extraction
        # extraction
        raw_names = response.css(PLAYER_NAME_SELECTOR).getall()
        raw_positions = response.css(PLAYER_POSITION_SELECTOR).getall()

        # Clean up the extracted player positions, and names by removing unwanted characters
        cleaned_names = [name.strip() for name in raw_names if name.strip()]
        cleaned_positions = [pos.strip()
                             for pos in raw_positions if pos.strip()]
        
        # extract date of birth elements
        dob_temp_list = []
        dob_elements = response.css(PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR)
        for element in dob_elements:
            text_content = element.css("::text").get(default="empty string")
            if text_content:
                dob_temp_list.append(text_content.strip())
        # only item that is date of birth + age
        dates_only_list = dob_temp_list[1::8]
        # strip and split the raw date of birth nag age. only get the dates.
        dates_only_list = [item.split("(")[0].strip()
                           for item in dates_only_list]

        # market value list
        temp_list = []
        for element in response.css(MARKET_VALUE_SELECTOR):
            temp_list.append(element.css("a::text").get())

        # add to market_value list and process the items to be just digits
        market_value_list = []
        for item in temp_list:
            if item is None:  # if the value is None
                market_value_list.append(str(0))  # Set value to 0 and append
            # Check if item contains at least one digit
            elif any(char.isdigit() for char in item):
                market_value_list.append(item)  # Append item as-is
            # Check for special characters
            elif any(char in "-!@#$%^&*()_+=[]{}|\\:;\"\'<>,.?/~`" for char in item):
                market_value_list.append(str(0))  # Set value to 0 and append

        market_value_list = [item.replace("€", "")
                             for item in market_value_list]
        market_value_list = [float(item.replace("m", ""))*1000000 if "m" in item
                             else float(item.replace("k", ""))*1000 if "k" in item
                             else float(item)
                             for item in market_value_list]

        # get the nationality of players
        nationality_list = []
        for element in response.css(PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR):
            # get the <img> tag with class named flaggenrahmen
            img_tags = element.css("img.flaggenrahmen")
            # check for the number of <img> tags
            if len(img_tags) == 2:
                nation = ", ".join([img.css("::attr(alt)").get()
                                   for img in img_tags])
            elif len(img_tags) == 1:
                nation = img_tags.css("::attr(alt)").get()
            else:
                nation = None
            # check if nationality is not empty and has a value, it will be added to the nationality_list
            if nation:
                nationality_list.append(nation)

        # get the current club of the player
        temp_all_clubs_list = []
        for element in response.css(PLAYER_CURRENT_CLUB_SELECTOR):
            cur_club = element.css("img::attr(alt)").get(
                default="empty string")
            if cur_club:
                temp_all_clubs_list.append(cur_club)

        # current club list gets every thrid element from all the clubs list
        current_club_list = temp_all_clubs_list[3::8]
        # signed from team every seventh element from all the clubs list
        signed_from_list = temp_all_clubs_list[7::8]

        # height of the player
        temp_list = []
        for element in response.css(PLAYER_HEIGHT_SELECTOR):
            height_string = element.css("::text").get()
            if height_string:
                temp_list.append(height_string)
        height_list = temp_list[2::5]
        height_list = [i.replace("m", "").replace(",", "")
                       for i in height_list]
        height_list = [int(item)
                       if any(char.isdigit() for char in item)
                       else 0
                       for item in height_list]

        # foot list of the players
        foot_list = temp_list[3::5]

        # joined date list
        joined_date_list = temp_list[4::5]
        # handle missing dates
        joined_date_list = [item.replace(
            "\u00A0", "00-00-0000") if item else None for item in joined_date_list]
        
        player_list = []
        for i,name in enumerate(cleaned_names):
            # position of each player
            position = cleaned_positions[i] if i < len(
                cleaned_positions) else None
            # date of birth of each player
            date_of_birth = dates_only_list[i] if i < len(
                dates_only_list) else None
            # nationality of each player
            nationality = nationality_list[i] if i < len(
                nationality_list) else None

            # current club of each player
            current_club = current_club_list[i] if i < len(
                current_club_list) else None

            # height of each player
            height = height_list[i] if i < len(height_list) else None

            # joined date of each player
            joined_date = joined_date_list[i] if i < len(
                joined_date_list) else None

            # foot of the player
            foot = foot_list[i] if i < len(foot_list) else None

            # signed from club for each player
            signed_from = signed_from_list[i] if i < len(
                signed_from_list) else None

            # market value of each player
            market_value = market_value_list[i] if i < len(
                market_value_list) else None
            player_dict = {
                "player_name": name,
                "player_position": position.strip() if position else None,
                "date_of_birth": date_of_birth,
                "nationality": nationality,
                "current_club": current_club,
                "height_CM": height,
                "foot": foot,
                "joined_date": joined_date,
                "signed_from": signed_from,
                "market_value": market_value
            }
            player_list.append(player_dict)
        
        # Update team_detail with the player list
        team_detail["players"] = player_list

        team_name_check=team_detail['team_name']
        for key, league_data in league_data_dict.items():
            if any(team['team_name'] == team_name_check for team in league_data['teams']):
                generated_key = f"{league_data['league_name']}_{league_data['country_name']}_{league_data['season']}"
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
        if self.stats_url_list:
            next_url = self.stats_url_list.pop(0)
            yield response.follow(next_url, callback=self.parse_detailed_stats_page, meta={"team_detail": team_detail, "player_list": player_list,"league_data_dict":league_data_dict})
        else:
            yield self.league_data

    def parse_detailed_stats_page(self, response):
        # logging the page status. if it is not == 200, then it won't proceed.
        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return
        
        print(f"starting parse_detailed_stats with this URL:\n{response.url}")
        
        team_detail = response.meta["team_detail"]
        player_list = response.meta["player_list"].copy()
        league_data_dict=response.meta["league_data_dict"]
        # create a player map
        player_map = {player["player_name"]: player for player in player_list}
        
        # css selectors
        PLAYER_NAME_SELECTOR = "table.inline-table td.hauptlink a"
        DETAILS_SELECTOR = ".items td.zentriert"
        MINUTES_PLAYED_SELECTOR = ".rechts"

        # extract names from stats page for the player_map dictionary
        temp_player_names_list = []
        for element in response.css(PLAYER_NAME_SELECTOR):
            player_name_data = element.css("::text").get()
            temp_player_names_list.append(player_name_data)
        cleaned_names = temp_player_names_list[::2]
        cleaned_names = [name.strip() for name in cleaned_names]
        
        # Extract data for age of players
        temp_detail_list = []
        age_list = []
        for element in response.css(DETAILS_SELECTOR):
            age_data = element.css("::text").get(default="empty string")
            if age_data:
                temp_detail_list.append(age_data)
            else:
                print("age data is empty")

        # age list from the temp details list
        age_list = temp_detail_list[1::13]
        age_list = [int(age) for age in age_list]
        
        # in squad list from the temp details list
        in_squad_list = temp_detail_list[3::13]
        # process the in_squad_list to have integer values. if the initial value is not a number, return 0
        in_squad_list = [int(item) if item is not None and any(char.isdigit() for char in item)
                         else 0 if not any(char.isdigit() for char in item) else 0
                         for item in in_squad_list]

        # appearances_list

        appearances_list = temp_detail_list[4::13]
        appearances_list = [int(item) if any(char.isdigit() for char in item)
                            # you can change it to "Not used during this season" later if you want. but now it returns null.
                            else 0
                            for item in appearances_list]

        # number of goals of each player
        goals_list = temp_detail_list[5::13]
        goals_list = [int(item) if any(char.isdigit() for char in item)
                      else 0
                      for item in goals_list]

        # number of assists list of each player
        assists_list = temp_detail_list[6::13]
        assists_list = [int(item) if any(char.isdigit() for char in item)
                        else 0
                        for item in assists_list]

        # number of yellows cards list of each player
        yellow_cards_list = temp_detail_list[7::13]
        yellow_cards_list = [int(item) if any(char.isdigit() for char in item)
                             else 0
                             for item in yellow_cards_list]

        # number of second yellow cards list of each player
        second_yellow_cards_list = temp_detail_list[8::13]
        second_yellow_cards_list = [int(item) if any(char.isdigit() for char in item)
                                    else 0
                                    for item in second_yellow_cards_list]

        # number of red cards list for each player
        red_cards_list = temp_detail_list[9::13]
        red_cards_list = [int(item) if any(char.isdigit() for char in item)
                          else 0
                          for item in red_cards_list]

        # number of times a player was substituted on as a list
        substitutions_on_list = temp_detail_list[10::13]
        substitutions_on_list = [int(item) if any(char.isdigit() for char in item)
                                 else 0
                                 for item in substitutions_on_list]
        # number of time a player was substituted off as a list
        substitutions_off_list = temp_detail_list[11::13]
        substitutions_off_list = [int(item) if any(char.isdigit() for char in item)
                                  else 0
                                  for item in substitutions_off_list]

        # list of PPG(points per game) for each player
        points_per_game_list = temp_detail_list[12::13]
        points_per_game_list = [float(item) if self.is_valid_float(item) else 0 for item in points_per_game_list]

        # minutes played during the season for each player
        temp_minutes_list = []
        for element in response.css(MINUTES_PLAYED_SELECTOR):
            minutes_data = element.css("::text").get()
            temp_minutes_list.append(minutes_data)
        # process and clean the minutes. remove "'" characters. return int value.
        minutes_played_list = []
        for i in temp_minutes_list:
            if any(char.isdigit() for char in i):
                i = i.replace("'", "")
                i = i.split(".")
                i = "".join(i)
                minutes_played_list.append(int(i))
            elif i == "-":
                i = i.replace("-", "0")
                minutes_played_list.append(int(i))
        
        for i, name in enumerate(cleaned_names):
            if name in player_map:
                # add or update the data fields in the corresponding player_dict
                player_map[name]["age"] = age_list[i] if i < len(
                    age_list) else None
                player_map[name]["in_sqaud"] = in_squad_list[i] if i < len(
                    in_squad_list) else None
                player_map[name]["appearance"] = appearances_list[i] if i < len(
                    appearances_list) else None
                player_map[name]["goals"] = goals_list[i] if i < len(
                    goals_list) else None
                player_map[name]["assists"] = assists_list[i] if i < len(
                    assists_list) else None
                player_map[name]["yelow_cards"] = yellow_cards_list[i] if i < len(
                    yellow_cards_list) else None
                player_map[name]["second_yellow_cards"] = second_yellow_cards_list[i] if i < len(
                    second_yellow_cards_list) else None
                player_map[name]["red_cards"] = red_cards_list[i] if i < len(
                    red_cards_list) else None
                player_map[name]["substitutions_on"] = substitutions_on_list[i] if i < len(
                    substitutions_on_list) else None
                player_map[name]["substitutions_off"] = substitutions_off_list[i] if i < len(
                    substitutions_on_list) else None
                player_map[name]["PPG"] = points_per_game_list[i] if i < len(
                    points_per_game_list) else None
                player_map[name]["minutes_played"] = minutes_played_list[i] if i < len(
                    minutes_played_list) else None
        
        team_detail["players"] = list(player_map.values())
        
        team_name_check=team_detail['team_name']
        for key, league_data in league_data_dict.items():
            if any(team['team_name'] == team_name_check for team in league_data['teams']):
                generated_key = f"{league_data['league_name']}_{league_data['country_name']}_{league_data['season']}"
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
        can_yeild=False
        for key, league_data in league_data_dict.items():
            # Find the last team in the teams list
            last_team = league_data['teams'][-1] if league_data['teams'] else None
        if last_team and 'players' in last_team:
            for player in last_team["players"]:
                if "age" in player and player["age"] is not None:
                    can_yeild=True
        if can_yeild:
            print("yielding data")
            yield league_data_dict
