import scrapy
from transfermarkt.items import TeamItem


class TeamsSpiderSpider(scrapy.Spider):
    name = "teams_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/A1"]

    def parse(self, response):
        # print the response status
        print(f"Response status: {response.status}")
        TEAM_SELECTOR = "#yw1 .items tbody tr"
        print("Total rows found:", len(response.css(TEAM_SELECTOR)))
        for team_row in response.css(TEAM_SELECTOR):
            team = TeamItem()
            team["team_name"] = team_row.css(
                "td.hauptlink.no-border-links a::text").get()
            team["team_url"] = "https://www.transfermarkt.com" + \
                team_row.css(
                    "td.hauptlink.no-border-links a::attr(href)").get()
            team["squad_size"] = team_row.css(
                "td.zentriert:nth-of-type(3) a::text").get()
            team["avg_age"] = team_row.css(
                "td.zentriert:nth-of-type(4)::text").get()
            team["foreigners_num"]=team_row.css("td.zentriert:nth-of-type(5)::text").get()
            print(
                f"Found team {team['team_name']}, Found squad size: {team['squad_size']}, avg age is: {team['avg_age']}, team url is: {team['team_url']}")
            print(f"Number of foreigners is: {team['foreigners_num']}")
            yield team
