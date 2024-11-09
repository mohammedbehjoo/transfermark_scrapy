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
        PLAYER_POSITION_SELECTOR=".items tbody  td.posrela tr td::text"
        
        # response of the selectors
        raw_names=response.css(PLAYER_NAME_SELECTOR).getall()
        raw_positions = response.css(PLAYER_POSITION_SELECTOR).getall()

        # Clean up the extracted player positions, and names by removing unwanted characters
        cleaned_names=[na.strip() for na in raw_names if na.strip()]
        cleaned_positions = [pos.strip() for pos in raw_positions if pos.strip()]
        print(f"cleaned positions: {cleaned_positions}")
        print(f"cleaned names: {cleaned_names}")
        # list of players
        player_list=[]
        for i,name in enumerate(cleaned_names):
            position=cleaned_positions[i] if i < len(cleaned_positions) else None
            print(f"Player {i + 1} - Name: {name}, Position: {position.strip() if position else 'N/A'}")
            player_dict={
                "player_name": name,
                "player_position": position.strip() if position else None
                # "age":int(age.strip())
            }
            player_list.append(player_dict)
        # add the player_list to the team_detail item
        team_detail["players"] =player_list
        print(f"extracted players with details: {player_list}")

        # yield the complete item after accumulating data from the second page
        yield team_detail
