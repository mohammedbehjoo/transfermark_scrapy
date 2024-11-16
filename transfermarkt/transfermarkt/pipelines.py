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

        # check if this is a country item
        if "country_name" in item and "leagues" in item:
            valid_leagues = []
            for league in item["leagues"]:
                try:
                    # check if required fields are present
                    for field in self.required_fields:
                        if field not in league or not league[field]:
                            raise DropItem(league)

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
        self.team_urls = set()

        # required fields
        self.required_fields = [
            "team_name",
            "team_url",
            "squad_size",
            "avg_age",
            "foreigners_num",
            "avg_market",
            "total_market"
        ]

    def process_item(self, item, spider):
        if not spider.name == "teams_spider":
            return item

        if "country_name" in item and "league_name" in item and "season" in item:
            valid_seasons = []
            for team in item["teams"]:
                try:
                    # check if the fields are not present. return "empty string".
                    for field in self.required_fields:
                        if field not in team or not team[field]:
                            team[field] = "empty string"

                    # clean the team_name, squad_size, avg age, foreigners_num, avg_market, and toal_market
                    team["team_name"] = team["team_name"].strip()
                    team["squad_size"] = team["squad_size"].strip()
                    team["avg_age"] = team["avg_age"].strip()
                    team["foreigners_num"] = team["foreigners_num"].strip()
                    team["avg_market"] = team["avg_market"].strip()
                    team["total_market"] = team["total_market"].strip()

                    # # check for duplicate team urls
                    if team["team_url"] in self.team_urls:
                        raise DropItem(f"Duplicate URL found {team}")

                    valid_seasons.append(team)

                except DropItem as e:
                    self.logger.info(f"Dropped season {str(e)}")
                    continue

            item["teams"] = valid_seasons
            return item
        raise DropItem(
            f"Missing country_name, league_name, or season in item: {item}")


class TeamDetailsPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_item(self, item, spider):
        if not spider.name == "team_details":
            return item
        
        self.logger.info(f"Processing item of team details: {item}")
        return item
        # pass
        # Strip leading/trailing whitespace
        # item["league_name"] = item["league_name"].strip()
        # item["table_position"] = item["table_position"].strip()
        # item["team_name"] = item["team_name"].strip()

        # processing datatypes
        # item["table_position"] = int(item["table_position"])
        # item["national_players_num"] = int(item["national_players_num"])

        # # process the current_transfer_record field
        # item["current_transfer_record"] = item["current_transfer_record"].replace(
        #     "€", "")
        # if "m" in item["current_transfer_record"]:
        #     item["current_transfer_record"] = item["current_transfer_record"].replace(
        #         "m", "")
        #     item["current_transfer_record"] = float(
        #         item["current_transfer_record"])*1000000
        # elif "k" in item["current_transfer_record"]:
        #     item["current_transfer_record"] = item["current_transfer_record"].replace(
        #         "k", "")
        #     item["current_transfer_record"] = float(
        #         item["current_transfer_record"])*1000

        # else:
        #     item["current_transfer_record"] = float(
        #         item["current_transfer_record"])
            

        # self.logger.info(f"Cleaned item: '{item['league_name']}'")

        # return item
