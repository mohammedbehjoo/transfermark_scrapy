import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from transfermarkt.items import TransfermarktItem


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
            item["league_url"] = "https://www.transfermarkt.com" + league.css("a::attr(href)").get()
            #  xpath for the club numbers of each league
            item["club_num"] = league.xpath('parent::td/following-sibling::td[@class="zentriert"]/text()').get()
            
            yield item
