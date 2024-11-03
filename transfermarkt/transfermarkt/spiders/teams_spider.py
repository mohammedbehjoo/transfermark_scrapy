import scrapy


class TeamsSpiderSpider(scrapy.Spider):
    name = "teams_spider"
    allowed_domains = ["www.transfermarkt.com"]
    start_urls = ["http://www.transfermarkt.com/"]

    def parse(self, response):
        pass
