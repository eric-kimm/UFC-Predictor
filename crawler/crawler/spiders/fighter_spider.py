import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import FighterItem, FightItem

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

    # Get requests for pages A-Z
    # def start_requests(self):
    #     for char in "abcdefghijklmnopqrstuvwxyz":
    #         url = f"http://ufcstats.com/statistics/fighters?char={char}&page=all"
    #         yield scrapy.Request(url=url, callback=self.parse_fighter_list)
    
    def start_requests(self):
        url = "http://www.ufcstats.com/fighter-details/54f64b5e283b0ce7"
        yield scrapy.Request(url=url, callback=self.parse_fighter_profile)
        
    # Parse table of fighters in each page
    def parse_fighter_list(self, response):
        rows = response.css("tr.b-statistics__table-row")
        for row in rows:
            link = row.css("a::attr(href)").get()

            # follow the link to the fighter's profile
            if link:
                yield response.follow(
                    link, 
                    callback=self.parse_fighter_profile,
                )
        # If next page exists
        next_page = response.css("a.b-link_style_black.a-pagination__link::attr(href)").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_fighter_list)

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
                callback=self.parse_fights,
                meta={"fighter_id": fighterItem["fighter_id"], "fighter_name": fighterItem["name"]}
            )
        print(dict(fighterItem))
        yield fighterItem

    def extract_scheduled_rounds(self, text):
        rounds = text.split()[0]
        return int(rounds)
    
    def convert_rounds_seconds(self, text):
        rounds = self.extract_scheduled_rounds(text)
        return rounds * 5 * 60

    # Populate fight result details
    def populate_result_details(self, fightItem, response):
        result_extractors = {
            "method": ("//i[contains(text(),'Method')]/following-sibling::i/text()", "text"),
            "round": ("//i[contains(text(),'Round')]/following-sibling::text()", "int"),
            "time": ("//i[contains(text(),'Time')]/following-sibling::text()", "text"),
            "time_seconds": ("//i[contains(text(),'Time')]/following-sibling::text()", "seconds"),
            "scheduled_rounds": ("//i[i[contains(text(),'Time format')]]/text()[last()]", "format"),
            "scheduled_time_seconds": ("//i[i[contains(text(),'Time format')]]/text()[last()]", "format seconds"),
            "referee": ("//i[contains(text(),'Referee')]/following-sibling::span/text()", "text"),
        }
        
        for label, (xpath, result_type) in result_extractors.items():
            value = response.xpath(xpath).get(default="").strip()
            
            if result_type == "int":
                fightItem[label] = int(value) if value else None
            elif result_type == "seconds":
                fightItem[label] = self.convert_seconds(value)
            elif result_type == "format":
                fightItem[label] = self.extract_scheduled_rounds(value)
            elif result_type == "format seconds":
                fightItem[label] = self.convert_rounds_seconds(value)
            else:
                fightItem[label] = value
        
    # Convert time of format M:SS to seconds
    def convert_seconds(self, text):
        if not text:
            return None
        mm, ss = text.split(":")
        return int(mm) * 60 + int(ss)

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

    # Fill fightItem with identity fields
    def populate_identity(self, fightItem, response, current_id, current_name, opponent_id, opponent_name):
        fightItem["fight_id"] = response.url.split("/")[-1]
        fightItem["fighter_name"] = current_name
        fightItem["fighter_id"] = current_id
        fightItem["opponent_name"] = opponent_name
        fightItem["opponent_id"] = opponent_id

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
    
    # Get fight details of each fighter
    def parse_fights(self, response):
        fightItem = FightItem()

        current_fighter_id = response.meta.get("fighter_id")
        current_fighter_name = response.meta.get("fighter_name")

        fighter_red_name, fighter_blue_name = self.get_fighter_names(response)
        fighter_red_id, fighter_blue_id = self.get_fighter_ids(response)

        opponent_id, opponent_name, fighterNum = self.determine_opponent(
            current_fighter_name, fighter_red_name, fighter_red_id,
            fighter_blue_name, fighter_blue_id
        )

        # Identity
        self.populate_identity(fightItem, response, current_fighter_id,
                                 current_fighter_name, opponent_id, opponent_name)

        # Results
        self.populate_result_details(fightItem, response)

        # Totals
        total_table_tds = response.xpath("(//table)//td[@class='b-fight-details__table-col']")
        for td, label in zip(total_table_tds, self.total_labels):
            text = td.xpath(f"./p[{fighterNum}]/text()").get().strip()
            self.populate_total_stats(fightItem, label, text)

        # Significant Strikes

        
        print(dict(fightItem))
        yield fightItem

        


