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

        # get the teams url
        self.teams_url_list = []
        for _, row in self.df.iterrows():
            self.teams_url_list.append(row["team_url"])
        # Limiting to 2 teams for testing
        self.teams_url_list = self.teams_url_list[:2]

        # getting the season id and the stats url
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
        print(f"len stats URL: {len(self.stats_url_list)}")

        # Initialize the league data structure
        self.league_data = {
            "league_name": self.df["league_name"].iloc[0] if not self.df.empty else None,
            "country_name": self.df["country_name"].iloc[0] if not self.df.empty else None,
            "season": self.df["season"].iloc[0] if not self.df.empty else None,
            "teams": []
        }

    def start_requests(self):
        print(
            f"teams url list:\n{self.teams_url_list},\nlen: {len(self.teams_url_list)}")
        for url in self.teams_url_list:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # logging the page status. if it is not == 200, then it won't proceed.
        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return

        # CSS selectors remain the same
        TEAM_SELECTOR = "#tm-main"
        LEAGUE_NAME_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__club"
        TABLE_COUNTRY_POSITION_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__label span.data-header__content a"
        NATIONAL_PLAYERS_NUM_SELECTOR = ".data-header__info-box  div.data-header__details ul.data-header__items li.data-header__label span.data-header__content"
        TEAM_NAME_SELECTOR = ".data-header__headline-wrapper--oswald"
        CURRENT_TRANSFER_RECORD_SELECTOR = ".data-header__content span a"

        # Extract team details
        position_data = None
        country_data = None
        national_players_data = None

        for index, element in enumerate(response.css(TABLE_COUNTRY_POSITION_SELECTOR)):
            if index == 1:
                position_data = element.css("::text").get()
            if element.css("img"):
                country_data = element.css("img").attrib.get("title")

        for index, element in enumerate(response.css(NATIONAL_PLAYERS_NUM_SELECTOR)):
            if index == 3:
                national_players_data = element.css("a::text").get()

        for team_detail_item in response.css(TEAM_SELECTOR):
            team_name = team_detail_item.css(
                TEAM_NAME_SELECTOR).css("::text").get().strip()
            current_transfer_record = team_detail_item.css(
                CURRENT_TRANSFER_RECORD_SELECTOR).css("::text").get()

            # if there is a team_name, then the following dict will be created.
            if team_name:
                team_detail = {
                    "team_name": team_name,
                    "table_position": position_data,
                    "current_transfer_record": current_transfer_record,
                    "national_players_num": national_players_data,
                    "players": []  # Initialize players list
                }

                # go to the detailed squad page
                HREF_SELECTOR = "div.tm-tabs a.tm-tab"
                elements = team_detail_item.css(HREF_SELECTOR)
                if len(elements) > 1:
                    detailed_squad_page = "https://www.transfermarkt.com" + \
                        elements[1].css("a").attrib.get("href")

                    if detailed_squad_page:
                        yield response.follow(detailed_squad_page, callback=self.parse_detailed_squad, meta={"team_detail": team_detail})
                else:
                    # If no detailed squad page, append team_detail directly
                    self.league_data["teams"].append(team_detail)

        # Only yield league_data after all teams have been processed
        if len(self.league_data["teams"]) == len(self.teams_url_list):
            yield self.league_data

    def parse_detailed_squad(self, response):
        # logging the page status. if it is not == 200, then it won't proceed.
        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return

        print(f"detailed squad URL:\n{response.url}")

        # retrieve the item passed from the previous parse method
        team_detail = response.meta["team_detail"]

        # css selectors
        PLAYER_NAME_SELECTOR = ".items tbody td.posrela td.hauptlink a::text"
        PLAYER_POSITION_SELECTOR = ".items tbody  td.posrela tr td::text"
        PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR = ".items td.zentriert"
        PLAYER_CURRENT_CLUB_SELECTOR = ".items td.zentriert"
        PLAYER_HEIGHT_SELECTOR = ".items td.zentriert"
        MARKET_VALUE_SELECTOR = ".rechts"

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

        # foot list of the players
        foot_list = temp_list[3::5]

        # joined date list
        joined_list = temp_list[4::5]
        # handle missing dates
        joined_list = [item.replace(
            "\u00A0", "00-00-0000") if item else None for item in joined_list]

        player_list = []
        for i, name in enumerate(cleaned_names):
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
            joined = joined_list[i] if i < len(joined_list) else None

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
                "joined": joined,
                "signed_from": signed_from,
                "market_value": market_value
            }
            player_list.append(player_dict)

        print(f"players list detailed squad\n{player_list}\n")

        # Update team_detail with the player list
        team_detail["players"] = player_list

        if self.stats_url_list:
            next_url = self.stats_url_list.pop(0)
            yield response.follow(next_url, callback=self.parse_detailed_stats_page, meta={"team_detail": team_detail, "player_list": player_list})
        else:
            yield self.league_data

    def parse_detailed_stats_page(self, response):

        if response.status != 200:
            self.logger.error(f"Failed to retrieve data from {response.url}")
            return
        team_detail = response.meta["team_detail"]
        player_list = response.meta["player_list"].copy()

        # create a player map
        player_map = {player["player_name"]: player for player in player_list}
        print(f"\nresponse url\n{response.url}\nplayer map is:\n{player_map}")

        # css selectors
        PLAYER_NAME_SELECTOR = "table.inline-table td.hauptlink a"
        DETAILS_SELECTOR = ".items td.zentriert"

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

        for i, name in enumerate(cleaned_names):
            if name in player_map:
                # add or update the data fields in the corresponding player_dict
                player_map[name]["age"] = age_list[i] if i < len(
                    age_list) else None

        # print(f"player map after age:\n{player_map}")
        team_detail["players"] = list(player_map.values())
        if team_detail not in self.league_data["teams"]:
            self.league_data["teams"].append(team_detail)

        if len(self.league_data["teams"]) == len(self.teams_url_list):
            yield self.league_data
