import scrapy
from transfermarkt.items import TeamDetailsItem


class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/red-bull-salzburg/startseite/verein/409/saison_id/2022"
        # "https://www.transfermarkt.com/fc-paris-saint-germain/startseite/verein/583/saison_id/2022"
    ]
    root_url = start_urls[0].split("/")
    season_id = root_url[-1]
    stats_url = "/".join(root_url[:4])+"/leistungsdaten/"
    stats_url = stats_url + \
        "/".join(root_url[5:7])+"/reldata"+f"/%26{season_id}"+"/plus/1"

    def parse(self, response):
        TEAM_SELECTOR = "#tm-main"
        LEAGUE_NAME_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__club"
        TABLE_COUNTRY_POSITION_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__label span.data-header__content a"
        NATIONAL_PLAYERS_NUM_SELECTOR = ".data-header__info-box  div.data-header__details ul.data-header__items li.data-header__label span.data-header__content"
        TEAM_NAME_SELECTOR = ".data-header__headline-wrapper--oswald"
        CURRENT_TRANSFER_RECORD_SELECTOR = ".data-header__content span a"

        for team_detail_item in response.css(TEAM_SELECTOR):
            team_detail = TeamDetailsItem()
            team_detail["league_name"] = team_detail_item.css(
                LEAGUE_NAME_SELECTOR+" a::text").get()
            elements = team_detail_item.css(
                TABLE_COUNTRY_POSITION_SELECTOR)
            team_detail["table_position"] = elements[1].css("::text").get() if len(
                elements) > 0 else None
            team_detail["country"] = elements[0].css("img").attrib.get("title")
            elements = team_detail_item.css(NATIONAL_PLAYERS_NUM_SELECTOR)
            team_detail["national_players_num"] = elements[3].css(
                "a::text").get()
            team_detail["season"] = int(self.start_urls[0].split("/")[-1])
            team_detail["team_name"] = team_detail_item.css(
                TEAM_NAME_SELECTOR).css("::text").get()
            team_detail["current_transfer_record"] = team_detail_item.css(
                CURRENT_TRANSFER_RECORD_SELECTOR).css("::text").get()

            # let's go to the next page, and get the detailed squad data.
            HREF_SELECTOR = "div.tm-tabs a.tm-tab"
            elements = team_detail_item.css(HREF_SELECTOR)
            detailed_squad_page = "https://www.transfermarkt.com" + \
                elements[1].css("a").attrib.get("href")
            print(f"detailed squad URL:\n{detailed_squad_page}")
            if detailed_squad_page:
                yield response.follow(detailed_squad_page, callback=self.parse_detailed_squad, meta={"team_detail": team_detail})
            else:
                yield team_detail

    def parse_detailed_squad(self, response):
        # retrieve the item passed from the previous parse method
        team_detail = response.meta["team_detail"]

        # extract additional details here
        PLAYER_NAME_SELECTOR = ".items tbody td.posrela td.hauptlink a::text"
        PLAYER_POSITION_SELECTOR = ".items tbody  td.posrela tr td::text"
        PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR = ".items td.zentriert"
        PLAYER_CURRENT_CLUB_SELECTOR = ".items td.zentriert"
        PLAYER_HEIGHT_SELECTOR = ".items td.zentriert"
        MARKET_VALUE_SELECTOR = ".rechts"

        # response of the selectors
        raw_names = response.css(PLAYER_NAME_SELECTOR).getall()
        raw_positions = response.css(PLAYER_POSITION_SELECTOR).getall()

        # Clean up the extracted player positions, and names by removing unwanted characters
        cleaned_names = [nam.strip() for nam in raw_names if nam.strip()]
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

        # list of players
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

            # player details to be added to the player_dict
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

        # add the player_list to the team_detail item
        team_detail["players"] = player_list
        # print the player_list to see all the details that are crawled
        # print(f"extracted players with details: {player_list}")

        # the URL of the detailed stats of the team players
        stsurl = self.stats_url
        print(f"detailed stats URL:\n{stsurl}")

        if stsurl:
            yield response.follow(stsurl, callback=self.parse_detailed_stats_page, meta={"team_detail": team_detail, "player_list": player_list})
        # yield the complete item after accumulating data from the second page
        else:
            yield team_detail

    def parse_detailed_stats_page(self, response):
        # retrieve the team_detail and player_list from meta
        team_detail = response.meta["team_detail"]
        player_list = response.meta["player_list"]

        # create a dictionary mapping player names to their corresponding player_dicts from the player_list
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

        # in squad list from the temp details list
        in_squad_list = temp_detail_list[3::13]
        # process the in_squad_list to have integer values. if the initial value is not a number, return 0
        in_squad_list = [int(item) if item is not None and any(char.isdigit() for char in item)
                         else 0 if not any(char.isdigit() for char in item) else None
                         for item in in_squad_list]

        # appearances_list

        appearances_list = temp_detail_list[4::13]
        appearances_list = [int(item) if any(char.isdigit() for char in item)
                            # you can change it to "Not used during this season" later if you want. but now it returns null.
                            else None
                            for item in appearances_list]

        # number of goals of each player
        goals_list = temp_detail_list[5::13]
        goals_list = [int(item) if any(char.isdigit() for char in item)
                      else None
                      for item in goals_list]

        # number of assists list of each player
        assists_list = temp_detail_list[6::13]
        assists_list = [int(item) if any(char.isdigit() for char in item)
                        else None
                        for item in assists_list]

        # number of yellows cards list of each player
        yellow_cards_list = temp_detail_list[7::13]
        yellow_cards_list = [int(item) if any(char.isdigit() for char in item)
                             else None
                             for item in yellow_cards_list]

        # number of second yellow cards list of each player
        second_yellow_cards_list = temp_detail_list[8::13]
        second_yellow_cards_list = [int(item) if any(char.isdigit() for char in item)
                                    else None
                                    for item in second_yellow_cards_list]

        # number of red cards list for each player
        red_cards_list = temp_detail_list[9::13]
        red_cards_list = [int(item) if any(char.isdigit() for char in item)
                          else None
                          for item in red_cards_list]

        # number of times a player was substituted on as a list
        substitutions_on_list = temp_detail_list[10::13]
        substitutions_on_list = [int(item) if any(char.isdigit() for char in item)
                                 else None
                                 for item in substitutions_on_list]
        # number of time a player was substituted off as a list
        substitutions_off_list = temp_detail_list[11::13]
        substitutions_off_list = [int(item) if any(char.isdigit() for char in item)
                                  else None
                                  for item in substitutions_off_list]

        # list of PPG(points per game) for each player
        points_per_game_list = temp_detail_list[12::13]
        points_per_game_list = [float(item) if any(char.isdigit() for char in item)
                                else None
                                for item in points_per_game_list]

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

        # match extracted data with existing players based on the "player_name" key
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

        # update the team_detail with the modified player_list
        team_detail["players"] = list(player_map.values())

        # yield the final updated team_detail
        yield team_detail
