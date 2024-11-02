import scrapy


class LeaguesSpiderSpider(scrapy.Spider):
    name = "leagues_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = ["https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/127"]

    def parse(self, response):
        LEAGUE_SELECTOR=".inline-table a"
        
