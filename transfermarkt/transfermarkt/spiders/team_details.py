import scrapy
from transfermarkt.items import TeamDetailsItem


class TeamDetailsSpider(scrapy.Spider):
    name = "team_details"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/red-bull-salzburg/startseite/verein/409/saison_id/2022"]

    def parse(self, response):
        TEAM_SELECTOR = "#tm-main"
        for team_detail_item in response.css(TEAM_SELECTOR):
            team_detail = TeamDetailsItem()
            team_detail["league_name"] = team_detail_item.css(
                ".data-header__box--big div.data-header__club-info span.data-header__club a::text").get()
            print(
                f"Found league name: {team_detail['league_name']}")
        yield team_detail
