import scrapy
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import FighterItem, FightItem, EventItem, FighterFightItem
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader
from ..loaders import BaseLoader, EventLoader, FightLoader, FighterLoader, FighterFightLoader
from datetime import datetime
from ..constants import TOTAL_FIELDS, SIG_FIELDS, CUTOFF_TIME, FIGHT_SELECTORS
import psycopg2
import os


class UfcSpider(scrapy.Spider):
    name = "ufc"
    allowed_domains = ["ufcstats.com", "www.ufcstats.com"]

    def __init__(self, *args, **kwargs):
        super(UfcSpider, self).__init__(*args, **kwargs)
        
        self.connection = psycopg2.connect(
            host='localhost',
            user='erickim',
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname='ufc'
        )
        self.cur = self.connection.cursor()

        # Guard against already scraped data
        # self.cur.execute("SELECT event_id FROM events")
        # self.seen_events = {row[0] for row in self.cur.fetchall()}

        # self.cur.execute("SELECT fight_id FROM fights")
        # self.seen_fights = {row[0] for row in self.cur.fetchall()}

        # self.cur.execute("SELECT fighter_id, updated_at FROM fighters")
        # self.fighter_updates = {row[0]: row[1] for row in self.cur.fetchall()}

        # self.cur.execute("SELECT fight_id, fighter_id FROM fighter_fights")
        # self.seen_stats = {(row[0], row[1]) for row in self.cur.fetchall()}

        # self.cur.close()
        # self.connection.close()

    # Get requests for each event in events page
    def start_requests(self):
        url = "http://www.ufcstats.com/statistics/events/completed?page=all"
        yield scrapy.Request(url=url, callback = self.parse)

    def parse(self, response):
        rows = response.xpath('//tr[contains(@class, "b-statistics__table-row")]')[1:]        
        for row in rows:
            link = row.xpath('.//a/@href').get()
            date_str = row.xpath("normalize-space(.//span[@class='b-statistics__date']/text())").get()

            # Check for missing data
            if not link or not date_str:
                continue

            try:
                event_id = link.strip('/').split("/")[-1]
                event_date = datetime.strptime(date_str, '%B %d, %Y')

                if event_date < CUTOFF_TIME:
                    continue
                # if event_id in self.seen_events:
                #     continue

                event_name = row.xpath("normalize-space(.//a/text())").get()

                if event_date <= datetime.now():
                    event_status = "completed"
                else:
                    event_status = "upcoming"
                callback = self.parse_event

                yield response.follow(
                    link,
                    callback=callback,
                    priority=15,
                    meta={
                        "event_id": event_id, 
                        "event_date": date_str, 
                        "event_name": event_name,
                        "event_status": event_status
                    }
                )
            except Exception as e:
                self.logger.error(f"Error parsing row: {e}")

    # Get information for each event (EventItem)
    def parse_event(self, response):
        eventLoader = EventLoader(item=EventItem(), response = response)
        eventLoader.add_value('event_id', response.meta.get("event_id"))
        eventLoader.add_value('event_status', response.meta.get("event_status"))
        eventLoader.add_value('date', response.meta.get("event_date"))
        eventLoader.add_value('name', response.meta.get("event_name"))
        eventLoader.add_xpath('location', 'normalize-space(//li[contains(., "Location:")]/text()[last()])')

        yield eventLoader.load_item()

        # Iterate through each fight in each event page
        rows = response.xpath('//tr[contains(@class, "js-fight-details-click")]')
        for row in rows:
            fighter_links = row.xpath(".//a[contains(@href,'fighter-details')]/@href").getall()           
            fight_link = row.xpath("./@data-link").get()
            weight_class = row.xpath("normalize-space(./td[7]//p/text())").get()

            for link in fighter_links:
                yield response.follow(
                    link,
                    callback=self.parse_fighter,
                    priority=10,
                )

            yield response.follow(
                fight_link,
                callback=self.parse_fight,
                priority=5,
                meta={
                    "event_id": eventLoader.get_output_value('event_id'),
                    "event_date": eventLoader.get_output_value('date'),
                    "fighter_links": fighter_links,
                    "weight_class": weight_class,
                    "event_status": eventLoader.get_output_value('event_status')
                }
            )
    
    # Get information of each fight (FightItem)
    def parse_fight(self, response):
        fight_id = response.url.split("/")[-1]
        # if fight_id in self.seen_fights:
        #     return
    
        fightLoader = FightLoader(item=FightItem(), response = response)
        header_text = "".join(response.xpath("//i[contains(@class, 'fight-title')]//text()").getall())

        fightLoader.add_value('fight_id', fight_id)
        fightLoader.add_value('event_id', response.meta.get('event_id'))
        fightLoader.add_value('event_date', response.meta.get('event_date'))
        fightLoader.add_value('weight_class', response.meta.get('weight_class'))
        fightLoader.add_value('is_title_fight', header_text)
        fightLoader.add_value('gender', header_text)
        fightLoader.add_value('event_status', response.meta.get('event_status'))
        fightLoader.add_xpath('red_fighter_id', "(//div[@class='b-fight-details__person']//a/@href)[1]")
        fightLoader.add_xpath('blue_fighter_id', "(//div[@class='b-fight-details__person']//a/@href)[2]")
        fightLoader.add_xpath('red_fighter_name', '(//div[@class="b-fight-details__person-text"]//a/text())[1]')
        fightLoader.add_xpath('blue_fighter_name', '(//div[@class="b-fight-details__person-text"]//a/text())[2]')

        FIELDS = FIGHT_SELECTORS.keys()
        event_status = fightLoader.get_output_value('event_status')

        if event_status == 'completed':
            for field in FIELDS:
                xpath = FIGHT_SELECTORS.get(field)
                if xpath:
                    fightLoader.add_xpath(field, xpath)
                else:
                    fightLoader.add_value(field, None)
        else:
            for field in FIELDS:
                fightLoader.add_value(field, None)
        
        ffLoaderRed = FighterFightLoader(item=FighterFightItem(), response = response)
        ffLoaderBlue = FighterFightLoader(item=FighterFightItem(), response = response)
        self.parse_fighter_fights(response, ffLoaderRed, ffLoaderBlue, fightLoader)
        
        yield fightLoader.load_item()
        yield ffLoaderRed.load_item()
        yield ffLoaderBlue.load_item()

    def parse_fighter_fights(self, response, ffLoaderRed, ffLoaderBlue, fightLoader):
        fight_id = fightLoader.get_output_value('fight_id')
        red_fighter_id = fightLoader.get_output_value('red_fighter_id')
        blue_fighter_id = fightLoader.get_output_value('blue_fighter_id')
        event_status = fightLoader.get_output_value('event_status')

        # if (fight_id, red_fighter_id) in self.seen_stats:
        #     return
        # if (fight_id, blue_fighter_id) in self.seen_stats:
        #     return

        ffLoaderRed.add_value('fight_id', fight_id)
        ffLoaderRed.add_value('fighter_id', red_fighter_id)
        ffLoaderRed.add_value('opponent_id', blue_fighter_id)
        ffLoaderRed.add_value('event_status', event_status)
        
        ffLoaderBlue.add_value('fight_id', fight_id)
        ffLoaderBlue.add_value('fighter_id', fightLoader.get_output_value('blue_fighter_id'))
        ffLoaderBlue.add_value('opponent_id', fightLoader.get_output_value('red_fighter_id'))
        ffLoaderBlue.add_value('event_status', event_status)
        
        # Total Table
        total_tds = response.xpath("(//table)//td[@class='b-fight-details__table-col']")

        for field, td in zip(TOTAL_FIELDS, total_tds):
            if field is None:
                continue

            if event_status == 'completed':
                red_val = td.xpath("normalize-space(./p[1]/text())").get()
                blue_val = td.xpath("normalize-space(./p[2]/text())").get()

                ffLoaderRed.add_value(field, red_val)
                ffLoaderBlue.add_value(field, blue_val)
            else:
                red_val.add_value(field, None)
                blue_val.add_value(field, None)


        # Significant Strikes Table
        sig_tds = response.xpath("(//table)[3]//td[@class='b-fight-details__table-col']")

        for field, td in zip(SIG_FIELDS, sig_tds):
            if field is None:
                continue
            if event_status == 'completed':
                red_val = td.xpath("normalize-space(./p[1]/text())").get()
                blue_val = td.xpath("normalize-space(./p[2]/text())").get()

                ffLoaderRed.add_value(field, red_val)
                ffLoaderBlue.add_value(field, blue_val)
            else:
                red_val.add_value(field, None)
                blue_val.add_value(field, None)

    # Get attributes of each fighter (FighterItem)
    def parse_fighter(self, response):
        loader = FighterLoader(item=FighterItem(), response = response)

        fighter_id = response.url.split("/")[-1]
        # if fighter_id in self.fighter_updates:
        #     return

        loader.add_value('fighter_id', response.url.split("/")[-1])
        loader.add_xpath('name', "//span[contains(@class,'b-content__title-highlight')]/text()")
        loader.add_xpath('height', "//li[contains(., 'Height:')]/text()[last()]")
        loader.add_xpath('weight', "//li[contains(., 'Weight:')]/text()[last()]")
        loader.add_xpath('reach', "//li[contains(., 'Reach:')]/text()[last()]")
        loader.add_xpath('stance', "//li[contains(., 'STANCE:')]/text()[last()]")
        loader.add_xpath('dob', "//li[contains(., 'DOB:')]/text()[last()]")

        yield loader.load_item()

        


