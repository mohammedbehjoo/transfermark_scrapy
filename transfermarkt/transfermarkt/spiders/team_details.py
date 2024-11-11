import scrapy
from transfermarkt.items import TeamDetailsItem


class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        # "https://www.transfermarkt.com/red-bull-salzburg/startseite/verein/409/saison_id/2022"]
        "https://www.transfermarkt.com/fc-paris-saint-germain/startseite/verein/583/saison_id/2022"]

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

            print(
                f"Found league name: {team_detail['league_name']},position is: {team_detail['table_position']},country is: {team_detail['country']}")
            print(f"National Players: {team_detail['national_players_num']}")

            # let's go to the next page, and get the detailed squad data.
            HREF_SELECTOR = "div.tm-tabs a.tm-tab"
            elements = team_detail_item.css(HREF_SELECTOR)
            next_page = "https://www.transfermarkt.com" + \
                elements[1].css("a").attrib.get("href")
            print(f"next page is: {next_page}")
            if next_page:
                yield response.follow(next_page, callback=self.parse_next, meta={"team_detail": team_detail})
            else:
                yield team_detail

    def parse_next(self, response):
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
        dates_only_list = [item.split("(")[0].strip() for item in dates_only_list]

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
            cur_club = element.css("img::attr(alt)").get(default="empty string")
            if cur_club:
                temp_all_clubs_list.append(cur_club)
                
        # current club list gets every thrid element from all the clubs list
        current_club_list=temp_all_clubs_list[3::8]
        # signed from team every seventh element from all the clubs list
        signed_from_list=temp_all_clubs_list[7::8]

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
        joined_list=[item.replace("\u00A0", "00-00-0000") if item else None for item in joined_list]
        
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

        # yield the complete item after accumulating data from the second page
        yield team_detail
