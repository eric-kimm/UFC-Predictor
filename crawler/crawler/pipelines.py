# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import psycopg2
from datetime import datetime
import re
from crawler.items import FighterItem, FightItem, EventItem, FighterFightItem
import logging

logger = logging.getLogger(__name__)

class CleanItemPipeline:
    def process_item(self, item, spider):
        if not isinstance(item,  FighterItem):
            return item
        # Clean height
        def parse_height(h):
            if not h:
                return None
            m = re.match(r"(\d+)'[\s]*([\d\"]+)", h)
            if not m:
                return None
            feet = int(m.group(1))
            inches = int(m.group(2).replace('"', '').strip())
            return feet * 12 + inches  # return total inches

        # Clean weight
        def parse_weight(w):
            if not w:
                return None
            m = re.search(r"(\d+)", w)
            return int(m.group(1)) if m else None

        # Clean reach
        def parse_reach(r):
            if not r:
                return None
            m = re.search(r"(\d+)", r)
            return int(m.group(1)) if m else None

        # Convert into percent
        def parse_percent(p):
            if not p:
                return None
            if "%" in p:
                return float(p.replace("%", "")) / 100
            return float(p)

        # Convert to float
        def parse_float(v):
            if not v:
                return None
            try:
                return float(v)
            except:
                return None

        # Convert DOB to YYYY-MM-DD
        def parse_dob(date_str):
            if not date_str:
                return None
            try:
                dt = datetime.strptime(date_str, "%b %d, %Y")
                return dt.date()
            except:
                return None

        # Apply the cleaning
        item["height"]  = parse_height(item.get("height"))
        item["weight"]  = parse_weight(item.get("weight"))
        item["reach"]   = parse_reach(item.get("reach"))
        item["dob"]     = parse_dob(item.get("dob"))

        item["slpm"]    = parse_float(item.get("slpm"))
        item["sapm"]    = parse_float(item.get("sapm"))
        item["sub_avg"] = parse_float(item.get("sub_avg"))
        item["td_avg"]  = parse_float(item.get("td_avg"))

        item["str_acc"] = parse_percent(item.get("str_acc"))
        item["str_def"] = parse_percent(item.get("str_def"))
        item["td_acc"]  = parse_percent(item.get("td_acc"))
        item["td_def"]  = parse_percent(item.get("td_def"))

        return item


class FightIdentityPipeline:
    def process_item(self, item, spider):
        if not isinstance(item,  FightItem):
            return item
        
        item['winner_id'] = None
        item['loser_id'] = None
        item['winner_color'] = None

        red_status = item.get('red_status', '').strip()
        
        if red_status == 'W':
            item['winner_id'] = item['red_fighter_id']
            item['loser_id'] = item['blue_fighter_id']
            item['result_type'] = 'Win'
            item['winner_color'] = 'Red'
        elif red_status == 'L':
            item['winner_id'] = item['blue_fighter_id']
            item['loser_id'] = item['red_fighter_id']
            item['result_type'] = 'Win'
            item['winner_color'] = 'Blue'
        elif red_status == 'D':
            item['result_type'] = 'Draw'
        else:
            item['result_type'] = 'NC'

        item['is_title_fight'] = 1 if item.get('is_title_fight') else 0
        
        return item
    
class FightTimePipeline:
    def process_item(self, item, spider):
        if not isinstance(item, FightItem):
            return item
        end_rnd = item.get('end_round')
        end_rnd_time = item.get('end_round_time')
        sched_rnd = item.get('rounds_scheduled')

        if sched_rnd:
            item['time_scheduled'] = sched_rnd * 5 * 60

        if all(v is not None for v in [end_rnd, end_rnd_time]):
            item['total_duration'] = ((end_rnd - 1) * 300) + end_rnd_time

        # logger.info("ITEM: %s", dict(item))
        return item
    
class FightResultsPipeline:
    def process_item(self, item, spider):
        if not isinstance(item, FightItem):
            return item
        
        def determine_decision(text):
            if "unanimous" in text:
                return"U-DEC"
            elif "split" in text:
                return "S-DEC"
            elif "majority" in text:
                return "M-DEC"
            else:
                return "OTHER-DEC"
            
        item['decision_type'] = None
        res = item.get('result_type')
        method_lower = item.get('method_raw').lower()
        if res == "NC":
            item["finish_type"] = res
        elif res == "Draw":
            item["finish_type"] = res
            item["decision_type"] = determine_decision(method_lower)
        else:
            if "ko" in method_lower:
                item["finish_type"] = "KO/TKO"
            elif "submission" in method_lower:
                item["finish_type"] = "SUB"
            elif "decision" in method_lower:
                item["finish_type"] = "DEC"
                item["decision_type"] = determine_decision(method_lower)
            elif "dq" in method_lower:
                item["finish_type"] = "DQ"
            else:
                item["finish_type"] = "OTHER"

        print(dict(item))
        return item
        

# class PostgresPipeline:
#     def open_spider(self, spider):
#         hostname = 'localhost'
#         username = 'erickim'
#         password = os.getenv("POSTGRES_PASSWORD")
#         database = 'ufc'

#         self.connection = psycopg2.connect(
#             host=hostname,
#             user=username,
#             password=password,
#             dbname=database
#         )
#         self.cur = self.connection.cursor()
    
#     def close_spider(self, spider):
#         self.connection.commit()
#         self.cur.close()
#         self.connection.close()

#     def process_item(self, item, spider):
#         self.cur.execute("""
#             INSERT INTO fighters (
#                 fighter_id, name, height, weight, reach, stance, dob,
#                 slpm, str_acc, sapm, str_def,
#                 td_avg, td_acc, td_def, sub_avg
#             ) VALUES (
#                 %(fighter_id)s, %(name)s, %(height)s, %(weight)s, %(reach)s, %(stance)s, %(dob)s,
#                 %(slpm)s, %(str_acc)s, %(sapm)s, %(str_def)s,
#                 %(td_avg)s, %(td_acc)s, %(td_def)s, %(sub_avg)s
#             )
#             ON CONFLICT (fighter_id) DO NOTHING;
#         """, dict(item))
#         print("INSERTED ITEM:", dict(item))
#         return item
