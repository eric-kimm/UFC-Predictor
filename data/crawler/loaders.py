# loaders.py
from itemloaders.processors import MapCompose, TakeFirst
from scrapy.loader import ItemLoader
import re

def clean_na(text):
    if not text:
        return None
    if text and text.strip().upper() in ["N/A", "--", ""]:
        return None
    return text.strip() if text else None

def extract_id(url):
    return url.split("/")[-1] if url else None

def check_title(text):
    if "title" in text.lower():
        return True
    else:
        return False
    
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

    is_title_fight_in = MapCompose(check_title)
    gender_in = MapCompose(clean_na, determine_gender)
    end_round_in = MapCompose(clean_na, int)
    end_round_time_in = MapCompose(clean_na, convert_seconds)
    rounds_scheduled_in = MapCompose(clean_na, extract_scheduled_rounds)

class FighterLoader(BaseLoader):
    pass

class FighterFightLoader(BaseLoader):
    ctrl_time_in = MapCompose(clean_na, convert_seconds)
    pass
