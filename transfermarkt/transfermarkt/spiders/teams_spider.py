import scrapy
from transfermarkt.items import TeamItem
import pandas as pd

# the dataframe for the scraped urls of the leagues crawled with leagues_spider
df = pd.read_json(
    "/home/mohammed/projects/coding/transfermarkt_scrapy/transfermarkt/transfermarkt/spiders/output.json")

# create a list of league urls to iterate over them.
urls_list=[]
for league in df["leagues"]:
    for item in league:
        urls_list.append(item["league_url"])


class TeamsSpiderSpider(scrapy.Spider):
    # count the teams that are scraped
    team_counter=0
    
    name = "teams_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/"]
    
    def start_requests(self) :
        for base_url in urls_list:
            for season in range(2023,2025):
                # add the saison_id to the url of the league_url
                yield scrapy.Request(url=base_url+f"/plus/?saison_id={season}",callback=self.parse)    

    def parse(self, response):
        # print the response status
        print(f"Response status: {response.status}")
        self.logger.info(f"Response status: {response.status}")
        self.logger.info(f"Response URL: {response.url}")
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
            team["foreigners_num"] = team_row.css(
                "td.zentriert:nth-of-type(5)::text").get()
            team["avg_market"] = team_row.css("td.rechts::text").get()
            team["total_market"] = team_row.css("td.rechts a::text").get()
            print(
                f"Found team {team['team_name']}, Found squad size: {team['squad_size']}, avg age is: {team['avg_age']}, team url is: {team['team_url']}")
            print(
                f"Number of foreigners is: {team['foreigners_num']}, avg market value is: {team['avg_market']}, and total market value is: {team['total_market']}")
            self.team_counter+=1
            self.logger.info(f"team counter is: {self.team_counter}")
            print(f"team counter is: {self.team_counter}")
            yield team
