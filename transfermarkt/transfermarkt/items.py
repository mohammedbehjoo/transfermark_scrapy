# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import scrapy.item


class LeagueItem(scrapy.Item):
    league_name = scrapy.Field()
    league_url = scrapy.Field()
    club_num = scrapy.Field()
    player_num = scrapy.Field()
    total_value = scrapy.Field()


class TeamItem(scrapy.Item):
    team_name = scrapy.Field()
    team_url = scrapy.Field()
    squad_size = scrapy.Field()
    avg_age = scrapy.Field()
    foreigners_num = scrapy.Field()
    avg_market = scrapy.Field()
    total_market = scrapy.Field()


class TeamDetailsItem(scrapy.Item):
    league_name = scrapy.Field()
    table_position=scrapy.Field()
    country=scrapy.Field()
    national_players_num=scrapy.Field()
    