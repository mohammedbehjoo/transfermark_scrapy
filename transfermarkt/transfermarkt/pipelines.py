# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import re


class TransfermarktPipeline:
    def __init__(self):
        self.league_names = set()

    def process_item(self, item, spider):
        # clean the league name by removing any whitespace
        item["league_name"] = item["league_name"].strip()
        
        if re.match(r"\n", item["league_name"]):
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.league_names.add(item["league_name"])
            return item
