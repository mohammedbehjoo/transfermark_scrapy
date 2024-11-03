import scrapy
from transfermarkt.items import TeamItem

class TeamsSpiderSpider(scrapy.Spider):
    name = "teams_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = ["https://www.transfermarkt.com/bundesliga/startseite/wettbewerb/A1"]

    def parse(self, response):
        # print the response status
        print(f"Response status: {response.status}")
        TEAM_SELECTOR="#yw1 .items tbody tr"
        print("Total rows found:",len(response.css(TEAM_SELECTOR)))
        for team_row in response.css(TEAM_SELECTOR):
            team=TeamItem()
            team["team_name"]=team_row.css("td.hauptlink.no-border-links a::text").get()
            print(f"Found team: {team['team_name']}")
            yield team
