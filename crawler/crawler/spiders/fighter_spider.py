import scrapy
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import FighterItem, FightItem, EventItem, FighterFightItem
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader
from ..loaders import BaseLoader, EventLoader, FightLoader

class FighterSpider(scrapy.Spider):
    name = "fighters"
    allowed_domains = ["ufcstats.com", "www.ufcstats.com"]

    career_labels = [
        ("height", "Height"),
        ("weight", "Weight"),
        ("reach", "Reach"),
        ("stance", "Stance"),
        ("dob", "DOB"),
        ("slpm", "SLpM"),
        ("str_acc", "Str. Acc."),
        ("sapm", "SApM"),
        ("str_def", "Str. Def"),
        ("td_avg", "TD Avg."),
        ("td_acc", "TD Acc."),
        ("td_def", "TD Def."),
        ("sub_avg", "Sub. Avg."),
    ]
    event_labels = []
    total_labels = ["knockdowns", "sig_str", "sig_str_percentage", "tot_str", "td", "td_percentage", "sub_attempts",
                    "reversals", "ctrl"]
    sig_str_labels = [ "sig_str_head_landed", "sig_str_head_attempted", "sig_str_body_landed", 
                        "sig_str_body_attempted", "sig_str_leg_landed", "sig_str_leg_attempted", 
                        "distance_landed", "distance_attempted", "clinch_landed", "clinch_attempted", 
                        "ground_landed", "ground_attempted"]

    # Get requests for each event in events page
    # def start_requests(self):
    #     url = "http://www.ufcstats.com/statistics/events/completed?page=all"
    #     yield scrapy.Request(url=url, callback = self.parse)
    
    # Ilia Topuria
    # def start_requests(self):
    #     url = "http://www.ufcstats.com/fighter-details/54f64b5e283b0ce7"
    #     yield scrapy.Request(url=url, callback=self.parse_fighter_profile)

    # Kayla Harrison
    # def start_requests(self):
    #     url = "http://www.ufcstats.com/fighter-details/1af1170ed937cba7"
    #     yield scrapy.Request(url=url, callback=self.parse_fighter_profile)

    # def start_requests(self):
    #     url = "http://www.ufcstats.com/statistics/events/completed"
    #     yield scrapy.Request(url=url, callback=self.parse)

    def start_requests(self):
        url = "http://www.ufcstats.com/event-details/bd92cf5da5413d2a"
        yield scrapy.Request(url=url, callback=self.parse_event)

    def parse(self, response):
        rows = response.xpath('//tr[@class="b-statistics__table-row"]')
        for row in rows:
            link = row.xpath('.//a/@href').get()

            if link:
                event_id = link.strip('/').split("/")[-1]
                yield response.follow(
                    link, 
                    callback=self.parse_event,
                    meta={"event_id": event_id}
                )

    # Get fields for EventItem
    def parse_event(self, response):
        eventLoader = EventLoader(item=EventItem(), response = response)
        eventLoader.add_value('event_id', response.meta.get("event_id"))
        eventLoader.add_xpath('event_name', 'normalize-space(//span[@class="b-content__title-highlight"]/text())')
        eventLoader.add_xpath('event_date', 'normalize-space(//li[contains(., "Date:")]/text()[last()])')
        eventLoader.add_xpath('event_location', 'normalize-space(//li[contains(., "Location:")]/text()[last()])')

        yield eventLoader.load_item()
        # print("EVENT ITEM:")
        # print(dict(event026b4f7049085842Loader.load_item()))

        rows = response.xpath('//tr[contains(@class, "js-fight-details-click")]')
        for row in rows:
            link = row.xpath("./@data-link").get()
            yield response.follow(
                link,
                callback=self.parse_fight,
                meta={
                    "event_id": eventLoader.get_output_value('event_id'),
                    "event_date": eventLoader.get_output_value('event_date'),
                }
            )
    
    # Get feields for FightItem
    def parse_fight(self, response):
        loader = FightLoader(item=FightItem(), response = response)
        header_text = "".join(response.xpath("//i[contains(@class, 'fight-title')]//text()").getall())

        loader.add_value('fight_id', response.url.split("/")[-1])
        loader.add_value('event_id', response.meta.get('event_id'))
        loader.add_value('event_date', response.meta.get('event_date'))
        loader.add_value('weight_class', header_text)
        loader.add_value('is_title_fight', header_text)
        loader.add_value('gender', header_text)

        # Identity
        loader.add_xpath('red_fighter_id', "(//div[@class='b-fight-details__person']//a/@href)[1]")
        loader.add_xpath('blue_fighter_id', "(//div[@class='b-fight-details__person']//a/@href)[2]")
        loader.add_xpath('red_fighter_name', '(//div[@class="b-fight-details__person-text"]//a/text())[1]')
        loader.add_xpath('blue_fighter_name', '(//div[@class="b-fight-details__person-text"]//a/text())[2]')
        loader.add_xpath('red_status', "(//i[contains(@class, 'person-status')])[1]/text()")
        loader.add_xpath('blue_status', "(//i[contains(@class, 'person-status')])[2]/text()")
        loader.add_value('winner_id', None)
        loader.add_value('loser_id', None)
        loader.add_value('result_type', None)
        loader.add_value('winner_color', None)       

        # Time and Rounds
        loader.add_xpath('end_round', "//i[contains(text(),'Round')]/following-sibling::text()")
        loader.add_xpath('end_round_time', "normalize-space(//i[contains(text(),'Time')]/following-sibling::text()[1])")
        loader.add_xpath('rounds_scheduled', "normalize-space(//i[i[contains(text(),'Time format')]]/text()[last()])")
        loader.add_xpath('referee', "//i[contains(text(),'Referee')]/following-sibling::span/text()")
        loader.add_value('time_scheduled', None)
        loader.add_value('total_duration', None)

        # Results
        loader.add_xpath('method_raw', "//i[contains(text(),'Method')]/following-sibling::i/text()")
        loader.add_value('finish_type', None)
        loader.add_value('decision_type', None)

        yield loader.load_item()

    # Extract a fighter stat with xpath
    def get_fighter_stat(self, label, response):
        value = response.xpath(
            f"//li[i[contains(normalize-space(), '{label}')]]"
            "/span[@class='b-list__box-item-value']/text()").get()
        if value:  
            return value.strip()
        value = response.xpath(
            f"normalize-space(//li[i[contains(normalize-space(), '{label}')]]/text()[last()])"
        ).get()
        return value.strip() if value else None

    # Get stats of each fighter
    def parse_fighter_profile(self, response):
        fighterItem = FighterItem()

        # Fill fighter basic info
        fighterItem["fighter_id"] = response.url.split("/")[-1]
        fighterItem["name"] = response.css("span.b-content__title-highlight::text").get(default="").strip()

        for field, display_label in self.career_labels:
            fighterItem[field] = self.get_fighter_stat(display_label, response)

        # Iterate through each row of the fight table and follow link
        rows = response.xpath("//tr[contains(@class,'js-fight-details-click')]")
        for row in rows:
            link = row.xpath("./@data-link").get()
            yield response.follow(
                link,
                callback=self.parse_fight,
                meta={"fighter_id": fighterItem["fighter_id"], "fighter_name": fighterItem["name"]}
            )
        print(dict(fighterItem))
        yield fighterItem

    def parse_fighter_fights(self, response):
        fighterFightItem = FighterFightItem()


     # Extract a fighter's total stats of a fight
    def populate_total_stats(self, fightItem, label, text):
        if label in ("sig_str", "tot_str", "td"):
            parsed = self.post_process(label, text)
            fightItem[f"{label}_landed"] = parsed[0]
            fightItem[f"{label}_attempted"] = parsed[1]
        elif label == "ctrl":
            fightItem[f"{label}_time"] = text
            fightItem[f"{label}_seconds"] = self.convert_seconds(text)
        else:
            fightItem[label] = text

    # Clean strings in format " X of Y"
    def post_process(self, label, value):
        if value is None:
            return (None, None)
        value = str(value).strip()
        if "of" in value:
            try:
                landed, attempted = value.split("of")
                landed = int(landed.strip())
                attempted = int(attempted.strip())
                return (landed, attempted)
            except Exception:
                return (None, None)
            
        return (None, None)



    #     # Totals
    #     total_table_tds = response.xpath("(//table)//td[@class='b-fight-details__table-col']")
    #     for td, label in zip(total_table_tds, self.total_labels):
    #         text = td.xpath(f"./p[{fighterNum}]/text()").get().strip()
    #         self.populate_total_stats(fightItem, label, text)

        


