import scrapy
import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import FighterItem, FightItem, EventItem, FighterFightItem
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader

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
        eventItem = EventItem()
        eventItem["event_id"] = response.meta.get("event_id")
        eventItem["event_name"] = response.xpath('normalize-space(//span[@class="b-content__title-highlight"]/text())').get()
        eventItem["event_date"] = response.xpath('normalize-space(//li[contains(., "Date:")]/text()[last()])').get()
        eventItem["event_location"] = response.xpath('normalize-space(//li[contains(., "Location:")]/text()[last()])').get()

        rows = response.xpath('//tr[contains(@class, "js-fight-details-click")]')
        for row in rows:
            link = row.xpath("./@data-link").get()
            yield response.follow(
                link,
                callback=self.parse_fight,
                meta={
                    "event_id": eventItem["event_id"],
                    "event_date": eventItem["event_date"],
                }
            )
        # print("EVENT ITEM:")
        # print(dict(eventItem))
        # yield eventItem
    
    # Get feields for FightItem
    def parse_fight(self, response):
        fightItem = FightItem()

        # Fighter labels
        red_fighter_name, blue_fighter_name = self.get_fighter_names(response)
        red_fighter_id, blue_fighter_id = self.get_fighter_ids(response)
        result_type, winner_id, loser_id, winner_color = self.get_winner_loser(response, red_fighter_id, blue_fighter_id)
        weight_class = self.get_weight_class(response)
        if "women" in weight_class.lower():
            gender = "Women"
        else:
            gender = "Men"
        title = self.is_title_fight(response)
        event_id = response.meta.get("event_id")
        event_date = response.meta.get("event_date")
        
        # Identity labels
        self.populate_identity(response, fightItem,red_fighter_id, red_fighter_name, 
                               blue_fighter_id, blue_fighter_name, winner_id, loser_id, 
                               result_type, winner_color, weight_class, event_id, event_date, 
                               gender, title)
        
        # Time and Round labels
        self.populate_time_details(fightItem, response)

        # Result labels
        self.populate_result_details(fightItem, result_type, response)


        print("FIGHT ITEM:")
        print(dict(fightItem))
        yield fightItem

    def populate_result_details(self, fightItem, result_type, response):
        method_raw = response.xpath("//i[contains(text(),'Method')]/following-sibling::i/text()").get(default="").strip()
        fightItem["method_raw"] = method_raw
        # 2. Extract detail (e.g., Unanimous, Split, Rear Naked Choke)
        # This is usually in the second <i> or a separate paragraph
        detail_raw = response.xpath("//i[contains(text(),'Method')]/following-sibling::p/text()").get(default="").strip()

        # Normalize for comparison
        method_lower = method_raw.lower()

        # 3. Handle Non-Wins (Draw, NC)
        if result_type == "NC":
            fightItem["finish_type"] = result_type
            fightItem["decision_type"] = None

        elif result_type == "Draw":
            fightItem["finish_type"] = result_type
            fightItem["decision_type"] = self.determine_decision(method_lower)
        
        # 4. Handle Wins
        else:
            if "ko" in method_lower:
                fightItem["finish_type"] = "KO/TKO"
                fightItem["decision_type"] = None
            elif "submission" in method_lower:
                fightItem["finish_type"] = "SUB"
                fightItem["decision_type"] = None
            elif "decision" in method_lower:
                fightItem["finish_type"] = "DEC"
                fightItem["decision_type"] = self.determine_decision(method_lower)
            elif "dq" in method_lower:
                fightItem["finish_type"] = "DQ"
                fightItem["decision_type"] = None
            else:
                fightItem["finish_type"] = "OTHER"
                fightItem["decision_type"] = None

    def determine_decision(self, text):
        if "unanimous" in text:
            return"U-DEC"
        elif "split" in text:
            return "S-DEC"
        elif "majority" in text:
            return "M-DEC"
        else:
            return "OTHER-DEC"
    
    # Convert time of format M:SS to seconds
    def convert_seconds(self, text):
        if not text:
            return None
        mm, ss = text.split(":")
        return int(mm) * 60 + int(ss)

    # Get number of rounds that were scheduled
    def get_scheduled_rounds(self, text):
        rounds = text.split()[0]
        return int(rounds)
    
    # Get scheduled time in seconds
    def get_time_scheduled_seconds(self, text):
        return text * 5 * 60

    # Populate fight result and time details
    def populate_time_details(self, fightItem, response):
        result_extractors = {
            "end_round": ("//i[contains(text(),'Round')]/following-sibling::text()", "int"),
            "end_round_time": ("//i[contains(text(),'Time')]/following-sibling::text()", "text"),
            "rounds_scheduled": ("//i[i[contains(text(),'Time format')]]/text()[last()]", "format"),
            "referee": ("//i[contains(text(),'Referee')]/following-sibling::span/text()", "text"),
        }
        
        for label, (xpath, result_type) in result_extractors.items():
            value = response.xpath(xpath).get(default="").strip()
            if result_type == "int":
                fightItem[label] = int(value) if value else None
            elif result_type == "format":
                fightItem[label] = self.get_scheduled_rounds(value)
            else:
                fightItem[label] = value

        # Calculate time in seconds
        fightItem['end_round_time_seconds'] = self.convert_seconds(fightItem["end_round_time"])
        fightItem['time_scheduled_seconds'] = self.get_time_scheduled_seconds(fightItem['rounds_scheduled'])
        fightItem['total_duration_seconds'] = (fightItem["end_round"] * 300) + fightItem['end_round_time_seconds'] - 300

    # Obtain ids of red and blue fighters
    def get_fighter_ids(self, response):
        fighter_urls = response.xpath("//div[@class='b-fight-details__person']//a/@href").getall()
        fighter_a_id = fighter_urls[0].split("/")[-1]
        fighter_b_id = fighter_urls[1].split("/")[-1]

        return fighter_a_id, fighter_b_id
    
    # Obtain names of red and blue fighters
    def get_fighter_names(self, response):
        fighter_names = response.xpath('//div[@class="b-fight-details__person-text"]//a/text()').getall()
        return fighter_names[0].strip(), fighter_names[1].strip()

    # Identify opponent fighter
    def determine_opponent(self, current_name, red_name, red_id, blue_name, blue_id):
        if current_name == red_name:
            return blue_id, blue_name, 1
        else:
            return red_id, red_name, 2
        
    def get_weight_class(self, response):
        for fight in response.css('i.b-fight-details__fight-title'):
            full_text = "".join(fight.xpath('./text()').getall()).strip()
            match = re.search(r'(?:UFC\s+)?(.+?)\s+(?:Title\s+)?Bout', full_text)
            weight_class = match.group(1) if match else "Unknown"
        return weight_class
    
    def is_title_fight(self, response):
        header_text = "".join(response.xpath("//i[contains(@class, 'fight-title')]//text()").getall()).lower()
        images = response.xpath("//i[contains(@class, 'fight-title')]//img/@src").getall()
        has_title_text = 'title' in header_text
        has_belt_icon = any('belt.png' in img for img in images)
        if (has_title_text or has_belt_icon):
            return 1
        else:
            return 0

    # Fill fightItem with fields
    def populate_identity(self, response, fightItem, red_fighter_id, red_fighter_name, blue_fighter_id, blue_fighter_name, winner_id, loser_id, result_type, winner_color, weight_class, event_id, event_date, gender, title):
        fightItem["fight_id"] = response.url.split("/")[-1]
        fightItem["red_fighter_name"] = red_fighter_name
        fightItem["red_fighter_id"] = red_fighter_id
        fightItem["blue_fighter_name"] = blue_fighter_name
        fightItem["blue_fighter_id"] = blue_fighter_id
        fightItem["winner_id"] = winner_id
        fightItem["loser_id"] = loser_id
        fightItem["result_type"] = result_type
        fightItem["winner_color"] = winner_color
        fightItem["weight_class"] = weight_class
        fightItem["event_id"] = event_id
        fightItem["event_date"] = event_date
        fightItem["gender"] = gender
        fightItem["is_title_fight"] = title

    def get_winner_loser(self, response, red_fighter_id, blue_fighter_id):
        raw = response.xpath("//i[contains(@class, 'b-fight-details__person-status')]/text()").get()
        if not raw:
            return None
        value = raw.strip()
        if value == "W":
            winner_id = red_fighter_id
            loser_id = blue_fighter_id
            result_type = "Win"
            winner_color = "Red"
        if value == "L":
            winner_id = blue_fighter_id
            loser_id = red_fighter_id
            result_type = "Win"
            winner_color = "Blue"
        elif value == "NC":
            winner_id = None
            loser_id = None
            result_type = "NC"
            winner_color = None
        elif value == "D":
            winner_id = None
            loser_id = None
            result_type = "Draw"
            winner_color = None

        return result_type, winner_id, loser_id, winner_color

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

        


