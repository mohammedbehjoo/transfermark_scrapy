# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TransfermarktItem(scrapy.Item):
    league_name = scrapy.Field()
    league_url = scrapy.Field()
