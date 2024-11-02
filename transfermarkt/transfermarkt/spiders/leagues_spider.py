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
        LEAGUE_SELECTOR = ".items .inline-table a"
        LEAGUE_URL_SELECTOR = "::attr(href)"
        for league in response.css(LEAGUE_SELECTOR):
            item = TransfermarktItem()
            item["league_name"]=league.css("::text").get()
            item["league_url"]="https://www.transfermarkt.com" + league.css(LEAGUE_URL_SELECTOR).get()
            yield item
