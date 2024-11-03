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

    def process_item(self, item, spider):
        # check for duplicate league urls
        if item["league_url"] in self.league_urls:
            raise DropItem(f"Duplicate league url found: {item}")

        # Check for newlines before cleaning in the league names
        if "\n" in item["league_name"]:
            raise DropItem(f"Item found with new line in league name: {item}")
            
        # If no newlines, clean and process the item
        item["league_name"] = item["league_name"].strip()
        if item["league_name"]:  # ensure it's not empty after stripping
            self.league_names.add(item["league_name"]) # add name to the set
            self.league_urls.add(item["league_url"]) # add url to the set
            return item
        else:
            raise DropItem(f"Empty league name after cleaning: {item}")
