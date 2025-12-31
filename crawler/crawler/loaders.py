# loaders.py
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader
import re

def clean_na(text):
    if text and text.strip().upper() in ["N/A", "--", ""]:
        return None
    return text.strip() if text else None

def extract_id(url):
    return url.split("/")[-1] if url else None

def parse_weight(text):
    if not text: return "Unknown"
    text = " ".join(text.split()) 
    match = re.search(r'(?:UFC\s+)?(?:Interim\s+)?(.+?)(?:\s+Title)?\s+Bout', text, re.IGNORECASE)
  
    if match:
        return match.group(1).strip()
    return "Unknown"

def check_title(text):
    if "title" in text.lower():
        return 1
    else:
        return 0
    
def determine_gender(text):
    return "Women" if "women" in text.lower() else "Men"

def extract_scheduled_rounds(url):
    return int(url.split()[0])
    
def convert_seconds(text):
    if not text:
      return None
    mm, ss = text.split(":")
    return int(mm) * 60 + int(ss)

class BaseLoader(ItemLoader):
    default_output_processor = TakeFirst()
    default_input_processor = MapCompose(clean_na)

class EventLoader(BaseLoader):
    pass

class FightLoader(BaseLoader):
    red_fighter_id_in = MapCompose(extract_id)
    blue_fighter_id_in = MapCompose(extract_id)
    red_fighter_name_in = MapCompose(clean_na)
    blue_fighter_name_in = MapCompose(clean_na)
    weight_class_in = MapCompose(clean_na, parse_weight)
    red_status_in = MapCompose(clean_na)
    blue_status_in = MapCompose(clean_na)

    is_title_fight_in = MapCompose(check_title)
    gender_in = MapCompose(clean_na, determine_gender)
    end_round_in = MapCompose(clean_na, int)
    end_round_time_in = MapCompose(clean_na, convert_seconds)
    rounds_scheduled_in = MapCompose(clean_na, extract_scheduled_rounds)
    referee_in = MapCompose(clean_na)

    method_raw_in = MapCompose(clean_na)

    