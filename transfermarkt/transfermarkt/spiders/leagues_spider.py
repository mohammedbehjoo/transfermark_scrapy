import scrapy
import pandas as pd

country_id_df = pd.DataFrame([{"country_id": 127, "country_name": "Austria"},
                            #   {"country_id": 19, "country_name": "Belgium"},
                            #   {"country_id": 26, "country_name": "Brazil"},
                              ])


class LeaguesSpiderSpider(scrapy.Spider):
    name = "leagues_spider"
    allowed_domains = ["www.transfermarkt.com"]

    def start_requests(self):
        for _, row in country_id_df.iterrows():
            country_id = row["country_id"]
            country_name = row["country_name"]
            url = f"https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/{country_id}"

            yield scrapy.Request(
                url=url,
                callback=self.parse,
                cb_kwargs={"country_name": country_name}
            )

    def parse(self, response, country_name):
        # Add debug prints to see what we're getting
        print("Total rows found:", len(response.css(".items tbody tr")))
        print("Total links found:", len(response.css(".items .inline-table a")))
        print("*"*100)

        LEAGUE_SELECTOR = ".items .inline-table"
        country_data = {
            "country_name": country_name,
            "leagues": []
        }

        for league in response.css(LEAGUE_SELECTOR):
            league_item = {
                # Get league info
                "league_name": league.css("td+ td a::text").get(),
                "league_url": "https://www.transfermarkt.com" + league.css("a::attr(href)").get(),
                # get the number of clubs and players of each league with xpath. since we have to go back in the css hierarchy
                #  xpath for the number of clubs of each league
                "club_num": league.xpath('parent::td/following-sibling::td[@class="zentriert"]/text()').get(),
                # xpath for the number of players of each league
                "player_num": league.xpath('../following-sibling::td[@class="zentriert"][2]/text()').get(),
                # xpath total value of each league
                "total_value": league.xpath('parent::td/following-sibling::td[@class="rechts hauptlink"]/text()').get()
            }
            country_data["leagues"].append(league_item)
        yield country_data
