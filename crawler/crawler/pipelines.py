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

class CleanItemPipeline:
    def process_item(self, item, spider):
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

class PostgresPipeline:
    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'erickim'
        password = os.getenv("POSTGRES_PASSWORD")
        database = 'ufc'

        self.connection = psycopg2.connect(
            host=hostname,
            user=username,
            password=password,
            dbname=database
        )
        self.cur = self.connection.cursor()
    
    def close_spider(self, spider):
        self.connection.commit()
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        self.cur.execute("""
            INSERT INTO fighters (
                fighter_id, name, height, weight, reach, stance, dob,
                slpm, str_acc, sapm, str_def,
                td_avg, td_acc, td_def, sub_avg
            ) VALUES (
                %(fighter_id)s, %(name)s, %(height)s, %(weight)s, %(reach)s, %(stance)s, %(dob)s,
                %(slpm)s, %(str_acc)s, %(sapm)s, %(str_def)s,
                %(td_avg)s, %(td_acc)s, %(td_def)s, %(sub_avg)s
            )
            ON CONFLICT (fighter_id) DO NOTHING;
        """, dict(item))
        print("INSERTED ITEM:", dict(item))
        return item
