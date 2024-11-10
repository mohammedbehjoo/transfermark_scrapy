import scrapy
from transfermarkt.items import TeamDetailsItem


class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/red-bull-salzburg/startseite/verein/409/saison_id/2022"]

    def parse(self, response):
        TEAM_SELECTOR = "#tm-main"
        LEAGUE_NAME_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__club"
        TABLE_COUNTRY_POSITION_SELECTOR = ".data-header__box--big div.data-header__club-info span.data-header__label span.data-header__content a"
        NATIONAL_PLAYERS_NUM_SELECTOR = ".data-header__info-box  div.data-header__details ul.data-header__items li.data-header__label span.data-header__content"
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

        # response of the selectors
        raw_names = response.css(PLAYER_NAME_SELECTOR).getall()
        raw_positions = response.css(PLAYER_POSITION_SELECTOR).getall()

        # Clean up the extracted player positions, and names by removing unwanted characters
        cleaned_names = [nam.strip() for nam in raw_names if nam.strip()]
        cleaned_positions = [pos.strip()
                             for pos in raw_positions if pos.strip()]

        # extract date of birth elements
        temp_list = []
        dob_elements = response.css(PLAYER_DATE_OF_BIRTH_NATIONALITY_SELECTOR)[
            1::2]  # Select every second element
        for element in dob_elements:
            text_content = element.css("::text").get()
            if text_content:
                temp_list.append(text_content.strip())
        # only get every second item that is date of birth + age
        dob_list = temp_list[::2]
        # strip and split the raaw date of birth nag age. only get the dates.
        dates_only_list = [item.split("(")[0].strip() for item in dob_list]

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
        temp_club_list = []
        for element in response.css(PLAYER_CURRENT_CLUB_SELECTOR):
            cur_club = element.css("img::attr(alt)").get()
            if cur_club:
                temp_club_list.append(cur_club)
        # current club list gets every thrid element from all the clubs list
        current_club_list = temp_club_list[1::3]

        # height of the player
        temp_list = []
        for element in response.css(PLAYER_HEIGHT_SELECTOR):
            height_string = element.css("::text").get()
            if height_string:
                temp_list.append(height_string)
        print(f"temp list:\n", temp_list)
        height_list = temp_list[2::5]
        height_list = [i.replace("m", "").replace(",", "")
                       for i in height_list]

        # foot list of the players
        foot_list = temp_list[3::5]
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

            # foot of the player
            foot = foot_list[i] if i < len(foot_list) else None
            # player details to be added to the player_dict
            player_dict = {
                "player_name": name,
                "player_position": position.strip() if position else None,
                "date_of_birth": date_of_birth,
                "nationality": nationality,
                "current_club": current_club,
                "height_CM": height,
                "foot": foot
            }

            player_list.append(player_dict)

        # add the player_list to the team_detail item
        team_detail["players"] = player_list
        # print the player_list to see all the details that are crawled
        print(f"extracted players with details: {player_list}")

        # yield the complete item after accumulating data from the second page
        yield team_detail
