import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from transfermarkt.items import TransfermarktItem
import pandas as pd

country_id_df = pd.DataFrame([{"country_id": 127, "country_name": "Austria"},
                              {"country_id": 19, "country_name": "Belgium"},
                              {"country_id": 26, "country_name": "Brazil"},
                              {"country_id": 9, "country_name": "Argentina"},
                              {"country_id": 39, "country_name": "Denmark"},
                              {"country_id": 189, "country_name": "England"},
                              {"country_id": 50, "country_name": "France"},
                              {"country_id": 40, "country_name": "Germany"},
                              {"country_id": 75, "country_name": "Italy"},
                              {"country_id": 122, "country_name": "Netherlands"},
                              {"country_id": 136, "country_name": "Portugal"},
                              {"country_id": 157, "country_name": "Spain"},
                              {"country_id": 174, "country_name": "Turkiye"}])


class LeaguesSpiderSpider(scrapy.Spider):
    name = "leagues_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = [
        "https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/127"]

    def parse(self, response):
        # Add debug prints to see what we're getting
        print("Total rows found:", len(response.css(".items tbody tr")))
        print("Total links found:", len(response.css(".items .inline-table a")))
        print("*"*100)

        LEAGUE_SELECTOR = ".items .inline-table"

        for league in response.css(LEAGUE_SELECTOR):
            item = TransfermarktItem()
            # Get league info
            item["league_name"] = league.css("td+ td a::text").get()
            item["league_url"] = "https://www.transfermarkt.com" + \
                league.css("a::attr(href)").get()
            # get the number of clubs and players of each league with xpath. since we have to go back in the css hierarchy
            #  xpath for the number of clubs of each league
            item["club_num"] = league.xpath(
                'parent::td/following-sibling::td[@class="zentriert"]/text()').get()
            # xpath for the number of players of each league
            item["player_num"] = league.xpath(
                '../following-sibling::td[@class="zentriert"][2]/text()').get()
            # xpath total value of each league
            item["total_value"] = league.xpath(
                'parent::td/following-sibling::td[@class="rechts hauptlink"]/text()').get()
            yield item
