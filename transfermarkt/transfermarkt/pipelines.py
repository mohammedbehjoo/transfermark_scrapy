# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class TransfermarktPipeline:
    def __init__(self):
        self.league_names = set()
        self.league_urls = set()

        # required fields
        self.required_fields = [
            "league_name",
            "league_url",
            "club_num",
            "player_num",
            "total_value"
        ]

    def process_item(self, item, spider):
        # check if required fields are present
        '''
        what it does is that it looks for these required fields in each item.
        Since each item in scrapy is like a dictionary, so we use the get()
        method to retrieve the value of each key. the keys here are
        the required fields. So if the value of each key is not present
        the item will be dropped.
        '''
        for field in self.required_fields:
            if not item.get(field):
                raise DropItem(f"Missing {field} in {item}")

        # check for duplicate league urls
        if item["league_url"] in self.league_urls:
            raise DropItem(f"Duplicate league url found: {item}")

        # Check for newlines before cleaning in the league names
        if "\n" in item["league_name"]:
            raise DropItem(f"Item found with new line in league name: {item}")

        # If no newlines, clean and process the item
        item["league_name"] = item["league_name"].strip()
        if item["league_name"]:  # ensure it's not empty after stripping
            self.league_names.add(item["league_name"])  # add name to the set
            self.league_urls.add(item["league_url"])  # add url to the set
            return item
        else:
            raise DropItem(f"Empty league name after cleaning: {item}")
