# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import logging

class TransfermarktPipeline:
    def __init__(self):
        self.logger=logging.getLogger(__name__)
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
        self.logger.info(f"Processing item: {item}")
        
        # check if this a country item
        if "country_name" in item and "leagues" in item:
            valid_leagues=[]
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

                    # clean league name
                    league["league_name"]=league["league_name"].strip()
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
            item["leagues"]=valid_leagues
            return item
        else:
            self.logger.error("Item missing country_name or leagues")
            raise DropItem("Invalid item structure")

        