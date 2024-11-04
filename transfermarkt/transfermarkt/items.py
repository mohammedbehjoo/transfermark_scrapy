# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LeagueItem(scrapy.Item):
    league_name = scrapy.Field()
    league_url = scrapy.Field()
    club_num=scrapy.Field()
    player_num=scrapy.Field()
    total_value=scrapy.Field()
    
class TeamItem(scrapy.Item):
    team_name=scrapy.Field()
    squad_size=scrapy.Field()
    avg_age=scrapy.Field()
