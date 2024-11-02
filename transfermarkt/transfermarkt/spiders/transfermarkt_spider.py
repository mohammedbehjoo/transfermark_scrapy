import scrapy


class TransfermarktSpiderSpider(scrapy.Spider):
    name = "transfermarkt_spider"
    allowed_domains = ["www.transfermarkt.com"]
    # for instance we work on Austria info for now: 127 is Austria
    start_urls = ["https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/127"]

    def parse(self, response):
        
        pass
