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
            # if len(elements)>0:
            #     for i, element in enumerate(elements):
            #         print(f"Element {i}:")
            #         for attr, value in element.attrib.items():
            #             print(f"{attr}: {value}")
            # else:
            #     print("Elements is empty")
            # print("href selector:",team_detail.css(HREF_SELECTOR))

    def parse_next(self, response):
        # retrieve the item passed from the previous parse method
        team_detail = response.meta["team_detail"]

        # extract additional details here
        PLAYER_NAME_SELECTOR = ".items tbody td.posrela td.hauptlink a::text"
        PLAYER_POSITION_SELECTOR = ".items tbody  td.posrela tr td::text"
        PLAYER_DATE_OF_BIRTH_SELECTOR = ".items td.zentriert"
        # response of the selectors
        raw_names = response.css(PLAYER_NAME_SELECTOR).getall()
        raw_positions = response.css(PLAYER_POSITION_SELECTOR).getall()

        # Clean up the extracted player positions, and names by removing unwanted characters
        cleaned_names = [nam.strip() for nam in raw_names if nam.strip()]
        cleaned_positions = [pos.strip()
                             for pos in raw_positions if pos.strip()]

        # extract date of birth elements
        temp_list = []
        dob_elements = response.css(PLAYER_DATE_OF_BIRTH_SELECTOR)[
            1::2]  # Select every second element
        for element in dob_elements:
            text_content = element.css("::text").get()
            if text_content:
                temp_list.append(text_content.strip())
        # only get every second item that is date of birth + age
        dob_list = temp_list[::2]
        # strip and split the raaw date of birth nag age. only get the dates.
        dates_only_list = [item.split("(")[0].strip() for item in dob_list]
        # # Print extracted dates of birth for verification
        # print(f"Extracted dates of birth (every second element): {dob_texts}")
        print(f"daets_only_list is: {dates_only_list}")
        print(f"len dates_only_list is: {len(dates_only_list)}")

        # list of players
        player_list = []
        for i, name in enumerate(cleaned_names):
            position = cleaned_positions[i] if i < len(
                cleaned_positions) else None
            date_of_birth = dates_only_list[i] if i < len(
                dates_only_list) else None
            # print(
            #     f"Player {i + 1} - Name: {name}, Position: {position.strip() if position else 'N/A'}")
            player_dict = {
                "player_name": name,
                "player_position": position.strip() if position else None,
                "date_of_birth": date_of_birth
            }
            player_list.append(player_dict)
        # add the player_list to the team_detail item
        team_detail["players"] = player_list
        print(f"extracted players with details: {player_list}")

        # yield the complete item after accumulating data from the second page
        yield team_detail
