# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from scrapy.exceptions import DropItem
import logging


class LeaguePipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.league_names = set()
        self.league_urls = set()

        # required fields
        self.required_fields = [
            "league_name",
            "league_url",
            "club_num",
            "player_num",
            "total_value",
        ]

    def process_item(self, item, spider):
        # only process items from the league spider
        if not spider.name == "leagues_spider":
            return item

        self.logger.info(f"Processing item: {item}")

        # check if this a country item
        if "country_name" in item and "leagues" in item:
            valid_leagues = []
            for league in item["leagues"]:
                try:
                    # check if required fields are present
                    '''
                    what it does is that it looks for these required fields in each item.
                    Since each item in scrapy is like a dictionary, so we use the get()
                    method to retrieve the value of each key. the keys here are
                    the required fields. So if the value of each key is not present
                    the item will be dropped.
                    '''
                    for field in self.required_fields:
                        if field not in league or not league[field]:
                            raise DropItem(f"Missing {field} ")

                    # clean league_name
                    league["league_name"] = league["league_name"].strip()
                    if not league["league_name"]:
                        raise DropItem(f"Missing {field}")

                    # check for duplicate league urls
                    if league["league_url"] in self.league_urls:
                        raise DropItem(f"Duplicate league url found: {item}")

                    # if all checks passed, add to valid leagues
                    self.league_names.add(league["league_name"])
                    self.league_urls.add(league["league_url"])
                    valid_leagues.append(league)

                except DropItem as e:
                    self.logger.error(f"Dropped league: {str(e)}")
                    continue

            # update item with valid leagues
            item["leagues"] = valid_leagues
            return item
        else:
            self.logger.error("Item missing country_name or leagues")
            raise DropItem("Invalid item structure")


class TeamPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.team_names = set()
        self.team_urls=set()

    def process_item(self, item, spider):
        if not spider.name == "teams_spider":
            return item

        # clean the team_name, squad_size, avg age, foreigners_num
        item["team_name"] = item["team_name"].strip()
        item["squad_size"] = item["squad_size"].strip()
        item["avg_age"] = item["avg_age"].strip()
        item["foreigners_num"] = item["foreigners_num"].strip()
        
        # check for duplicate team urls
        if item["team_url"] in self.team_urls:
            raise DropItem(f"Duplicate URL found {item}")
        
        return item
