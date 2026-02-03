# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


from .constants import RAW_DATA_MAP
import json

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import os
import psycopg2
from datetime import datetime
import re
from crawler.items import FighterItem, FightItem, EventItem, FighterFightItem
import logging
from datetime import date

# Log scraped events
class EventLoggingPipeline:
    def __init__(self):
        self.logger = logging.getLogger('EventLogger')
    def process_item(self, item, spider):
        if item.__class__.__name__ == 'EventItem':
            self.logger.info(dict(item))
        return item

# Log scraped fighter items
class FighterLoggingPipeline:
    def __init__(self):
        self.logger = logging.getLogger('FighterLogger')
    def process_item(self, item, spider):
        if item.__class__.__name__ == 'FighterItem':
            self.logger.info(dict(item))
        return item
    
# Log scraped fight items
class FightLoggingPipeline:
    def __init__(self):
        self.logger = logging.getLogger('FightLogger')
    def process_item(self, item, spider):
        if item.__class__.__name__ == 'FightItem':
            self.logger.info(dict(item))
        return item

# Log scraped fighter fights
class FighterFightLoggingPipeline:
    def __init__(self):
        self.logger = logging.getLogger('FFLogger')
    def process_item(self, item, spider):
        if item.__class__.__name__ == 'FighterFightItem':
            readable_item = json.dumps(dict(item), indent=4)
            self.logger.info(readable_item)
        return item

# Clean fighter attributes
class FighterProcessorPipeline:
    def process_item(self, item, spider):
        if not isinstance(item,  FighterItem):
            return item
    
        def parse_height(h):
            if not h:
                return None
            m = re.match(r"(\d+)'[\s]*([\d\"]+)", h)
            if not m:
                return None
            feet = int(m.group(1))
            inches = int(m.group(2).replace('"', '').strip())
            return feet * 12 + inches

        def parse_weight(w):
            if not w:
                return None
            m = re.search(r"(\d+)", w)
            return int(m.group(1)) if m else None

        def parse_reach(r):
            if not r:
                return None
            m = re.search(r"(\d+)", r)
            return int(m.group(1)) if m else None
            
        item['height'] = parse_height(item.get('height'))
        item['weight'] = parse_weight(item.get('weight'))
        item['reach'] = parse_reach(item.get('reach'))
        item['stance'] = item.get('stance')
        item['dob'] = item.get('dob')

        return item
        
    
# Convert dates into datetime objects
class DateFormattingPipeline:
    def process_item(self, item, spider):
        if not isinstance(item, (EventItem, FightItem, FighterItem)):
            return item
        def format_date(date_str):
            if not date_str or "--" in date_str:
                return None
            
            # Try the long format (Events)
            try:
                return datetime.strptime(date_str, '%B %d, %Y').strftime('%Y-%m-%d')
            except ValueError:
                pass
            
            # Try the short format (Fighters)
            try:
                return datetime.strptime(date_str, '%b %d, %Y').strftime('%Y-%m-%d')
            except ValueError:
                return None
        
        if 'event_date' in item:
            item['event_date'] = format_date(item['event_date'])
        elif 'date' in item:
            item['date'] = format_date(item['date'])
        elif 'dob' in item:
            item['dob'] = format_date(item['dob'])

        print(dict(item))
        return item

# Determine fight winners and losers
class FightProcessorPipeline:
    def process_item(self, item, spider):
        if not isinstance(item,  FightItem):
            return item
        if item.get('event_status') == 'upcoming':
            return item   
        
        item = self.handle_results(item)
        item = self.handle_time(item)
        item = self.handle_winners_and_losers(item)

        return item
        
    def handle_winners_and_losers(self, item):
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
        
        item['referee'] = item.get('referee')
        return item
        
    def handle_time(self, item):
        end_rnd = item.get('end_round')
        end_rnd_time = item.get('end_round_time')
        sched_rnd = item.get('rounds_scheduled')

        if sched_rnd:
            item['time_scheduled'] = sched_rnd * 5 * 60

        if all(v is not None for v in [end_rnd, end_rnd_time]):
            item['total_duration'] = ((end_rnd - 1) * 300) + end_rnd_time

        return item
        
    def handle_results(self, item):
        item['decision_type'] = None
        res = item.get('result_type')
        method_lower = item.get('method_raw').lower()
        if res == "NC":
            item["finish_type"] = res
        elif res == "Draw":
            item["finish_type"] = res
            item["decision_type"] = self.determine_decision(method_lower)
        else:
            if "ko" in method_lower:
                item["finish_type"] = "KO/TKO"
            elif "submission" in method_lower:
                item["finish_type"] = "SUB"
            elif "decision" in method_lower:
                item["finish_type"] = "DEC"
                item["decision_type"] = self.determine_decision(method_lower)
            elif "dq" in method_lower:
                item["finish_type"] = "DQ"
            else:
                item["finish_type"] = "OTHER"

        return item

    def determine_decision(self, text):
        if "unanimous" in text:
            return"U-DEC"
        elif "split" in text:
            return "S-DEC"
        elif "majority" in text:
            return "M-DEC"
        else:
            return "OTHER-DEC"

class FightUpcomingProcessorPipeline:
    def process_item(self, item, spider):
        if not isinstance(item, FightItem):
            return item
        if item.get('event_status') == 'completed':
            return item
        item['red_status'] = None
        item['blue_status'] = None
        item['winner_id'] = None
        item['loser_id'] = None
        item['winner_color'] = None
        item['result_type'] = None
        item['end_round'] = None
        item['end_round_time'] = None
        item['rounds_scheduled'] = None
        item['referee'] = None
        item['time_scheduled'] = None
        item['total_duration'] = None
        item['method_raw'] = None
        item['finish_type'] = None
        item['decision_type'] = None

        return item
    
# Clean fighter fight stats
class FighterFightProcessorPipeline:
    def process_item(self, item, spider):
        if not isinstance(item, FighterFightItem):
            return item
        if item.get('event_status') == 'upcoming':
            return item
        
        item = self.handle_raw_values(item)
        item = self.convert_to_numerics(item)

        return item
        
    def handle_raw_values(self, item):         
        for raw_key, (landed_key, attempted_key) in RAW_DATA_MAP.items():
            raw_val = item.get(raw_key)
            landed, attempted = self.split(raw_val)
            
            item[landed_key] = landed
            item[attempted_key] = attempted

        return item
    
    def convert_to_numerics(self, item):
        integer_fields = [
            'knockdowns',
            'tot_str_landed', 'tot_str_attempted',
            'td_landed', 'td_attempted',
            'sub_attempts', 'reversals',
            'sig_str_landed',
            'head_str_landed', 'head_str_attempted',
            'body_str_landed', 'body_str_attempted',
            'leg_str_landed', 'leg_str_attempted',
            'distance_str_landed', 'distance_str_attempted',
            'clinch_str_landed', 'clinch_str_attempted',
            'ground_str_landed', 'ground_str_attempted'
        ]
        
        for field in integer_fields:
            value = item.get(field)
            if value is not None:
                try:
                    item[field] = int(float(value)) if isinstance(value, str) else int(value)
                except (ValueError, TypeError):
                    self.logger.warning(f"Could not convert {field}={value} to integer")
        
        return item

    def split(self, value):
        if value is None:
            return (None, None)
        value = str(value).strip().lower()
        if "of" in value:
            try:
                parts = value.split("of")
                landed = int(parts[0].strip())
                attempted = int(parts[1].strip())
                return (landed, attempted)
            except (ValueError, IndexError):
                return (None, None)
        return (None, None)   
        
class FighterFightUpcomingPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_item(self, item, spider):
        if not isinstance(item, FighterFightItem):
            return item
        if item.get('event_status') == 'completed':
            return item
        item['knockdowns'] = None 
        item['sub_attempts'] = None 
        item['reversals'] = None 
        item['ctrl_time'] = None 
        item['tot_str_landed'] = None 
        item['tot_str_attempted'] = None 
        item['tot_str_raw'] = None 
        item['td_landed'] = None 
        item['td_attempted'] = None 
        item['td_raw'] = None 
        item['sig_str_landed'] = None
        item['sig_str_attempted'] = None
        item['sig_str_raw'] = None 
        item['head_str_landed'] = None 
        item['head_str_attempted'] = None 
        item['head_str_raw'] = None
        item['body_str_landed'] = None
        item['body_str_attempted'] = None 
        item['body_str_raw'] = None 
        item['leg_str_landed'] = None
        item['leg_str_attempted'] = None 
        item['leg_str_raw'] = None 
        item['distance_str_landed'] = None 
        item['distance_str_attempted'] = None 
        item['distance_str_raw'] = None 
        item['clinch_str_landed'] = None 
        item['clinch_str_attempted'] = None 
        item['clinch_str_raw'] = None 
        item['ground_str_landed'] = None 
        item['ground_str_attempted'] = None
        item['ground_str_raw'] = None

        return item

# Validate item fields
class ValidationPipeline:
    """Validates all item types for required fields and data integrity."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def process_item(self, item, spider):
        item_type = item.__class__.__name__
        
        if isinstance(item, FighterItem):
            self._validate_fighter(item)
        elif isinstance(item, FightItem):
            self._validate_fight(item)
        elif isinstance(item, EventItem):
            self._validate_event(item)
        elif isinstance(item, FighterFightItem):
            self._validate_fighter_fight(item)
        
        return item
    
    def _validate_fighter(self, item):
        """Validate FighterItem fields."""
        required = ['fighter_id', 'name']
        self._check_required_fields(item, required, 'FighterItem')
        
        # Validate fighter_id is non-empty
        if item.get('fighter_id') == '':
            raise ValueError(f"FighterItem: fighter_id cannot be empty")
        
        # Validate numeric fields if present
        numeric_fields = ['height', 'weight', 'reach']
        for field in numeric_fields:
            if item.get(field) is not None:
                if not isinstance(item[field], (int, float)):
                    raise ValueError(f"FighterItem: {field} must be numeric, got {type(item[field])}")
        
        # Validate date format if present
        if item.get('dob') is not None:
            if not self._is_valid_date(item['dob']):
                raise ValueError(f"FighterItem: dob '{item['dob']}' is not in YYYY-MM-DD format")
    
    def _validate_fight(self, item):
        """Validate FightItem fields."""
        required = ['fight_id', 'event_id', 'red_fighter_id', 'blue_fighter_id']
        self._check_required_fields(item, required, 'FightItem')
        
        # Validate result_type
        valid_results = ['Win', 'Draw', 'NC']
        if item.get('result_type') is not None:
            if item.get('result_type') not in valid_results:
                raise ValueError(f"FightItem: result_type '{item['result_type']}' must be one of {valid_results}")
        
        # Validate finish_type if present
        valid_finish_types = ['KO/TKO', 'SUB', 'DEC', 'DQ', 'NC', 'Draw', 'OTHER']
        if item.get('finish_type') is not None:
            if item['finish_type'] not in valid_finish_types:
                raise ValueError(f"FightItem: finish_type '{item['finish_type']}' must be one of {valid_finish_types}")
        
        # Validate decision_type if present
        valid_decision_types = ['U-DEC', 'M-DEC', 'S-DEC', 'OTHER-DEC', None]
        if item.get('decision_type') not in valid_decision_types:
            raise ValueError(f"FightItem: decision_type '{item['decision_type']}' must be one of {valid_decision_types}")
        
        # Validate winner/loser IDs for Win result_type
        if item.get('result_type') == 'Win':
            if item.get('winner_id') is None:
                raise ValueError(f"FightItem: winner_id is required when result_type is 'Win'")
            if item.get('loser_id') is None:
                raise ValueError(f"FightItem: loser_id is required when result_type is 'Win'")
        
        # Validate round numbers if present
        if item.get('end_round') is not None:
            if not isinstance(item['end_round'], int) or item['end_round'] < 1:
                raise ValueError(f"FightItem: end_round must be a positive integer")
        
        # Validate round time if present
        if item.get('end_round_time') is not None:
            if not isinstance(item['end_round_time'], int) or item['end_round_time'] < 0:
                raise ValueError(f"FightItem: end_round_time must be a non-negative integer")
        
        # Validate event_date format
        if item.get('event_date') is not None:
            if not self._is_valid_date(item['event_date']):
                raise ValueError(f"FightItem: event_date '{item['event_date']}' is not in YYYY-MM-DD format")
    
    def _validate_event(self, item):
        """Validate EventItem fields."""
        required = ['event_id', 'name']
        self._check_required_fields(item, required, 'EventItem')
        
        # Validate event_id
        if item.get('event_id') == '':
            raise ValueError(f"EventItem: event_id cannot be empty")
        
        # Validate status if present
        valid_statuses = ['completed', 'upcoming', 'cancelled']
        if item.get('event_status') is not None:
            if item['event_status'] not in valid_statuses:
                self.logger.warning(f"EventItem: status '{item['event_status']}' is not a recognized value")
        
        # Validate date format if present
        if item.get('date') is not None:
            if not self._is_valid_date(item['date']):
                raise ValueError(f"EventItem: date '{item['date']}' is not in YYYY-MM-DD format")
    
    def _validate_fighter_fight(self, item):
        """Validate FighterFightItem fields."""
        required = ['fight_id', 'fighter_id', 'opponent_id']
        self._check_required_fields(item, required, 'FighterFightItem')
        
        # Validate fighter_id and opponent_id are different
        if item.get('fighter_id') == item.get('opponent_id'):
            raise ValueError(f"FighterFightItem: fighter_id and opponent_id cannot be the same")
        
        # Validate numeric fields
        numeric_fields = ['knockdowns', 'tot_str_landed', 'tot_str_attempted', 
                         'td_landed', 'td_attempted', 'sub_attempts', 
                         'reversals', 'ctrl_time', 'sig_str_landed', 
                         'head_str_landed', 'head_str_attempted', 'body_str_landed',
                         'body_str_attempted', 'leg_str_landed', 'leg_str_attempted',
                         'distance_str_landed', 'distance_str_attempted', 
                         'clinch_str_landed', 'clinch_str_attempted',
                         'ground_str_landed', 'ground_str_attempted']
        for field in numeric_fields:
            if item.get(field) is not None:
                if not isinstance(item[field], (int, float)):
                    raise ValueError(f"FighterFightItem: {field} must be numeric, got {type(item[field])}")
    
    def _check_required_fields(self, item, required_fields, item_type):
        """Check that all required fields are present and non-empty."""
        for field in required_fields:
            if field not in item or item[field] is None or item[field] == '':
                raise ValueError(f"{item_type}: required field '{field}' is missing or empty")
    
    def _is_valid_date(self, date_str):
        """Check if date string is in YYYY-MM-DD format."""
        if not isinstance(date_str, str):
            return False
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False


# Store to postgres
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
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        item_dict = ItemAdapter(item).asdict()

        try:
            if isinstance(item, EventItem):
                self.insert_event(item_dict)
            elif isinstance(item, FighterItem):
                self.insert_fighter(item_dict)
            elif isinstance(item, FightItem):
                self.insert_fight(item_dict)
            elif isinstance(item, FighterFightItem):
                self.insert_fighter_stats(item_dict)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            spider.logger.error("---------------- SQL ERROR ----------------")
            spider.logger.error(f"Type: {type(item).__name__}")
            spider.logger.error(f"Error: {e}")
            spider.logger.error(f"Data: {item_dict}")
            spider.logger.error("-------------------------------------------")

        return item
    
    def insert_event(self, data):
        query = """
            INSERT INTO events (event_id, name, date, event_status, location)
            VALUES (%(event_id)s, %(name)s, %(date)s, %(event_status)s, %(location)s)
            ON CONFLICT (event_id) DO UPDATE SET
                name = EXCLUDED.name,
                date = EXCLUDED.date,
                event_status = EXCLUDED.event_status,
                location = EXCLUDED.location,
                updated_at = CURRENT_TIMESTAMP;
        """
        self.cur.execute(query, data)

    def insert_fighter(self, data):
        query = """
            INSERT INTO fighters (fighter_id, name, stance, dob, height, weight, reach)
            VALUES (%(fighter_id)s, %(name)s, %(stance)s, %(dob)s, %(height)s, %(weight)s, %(reach)s)
            ON CONFLICT (fighter_id) DO UPDATE SET
                name = EXCLUDED.name,
                stance = EXCLUDED.stance,
                dob = EXCLUDED.dob,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                reach  = EXCLUDED.reach,
                updated_at = CURRENT_TIMESTAMP;
        """
        self.cur.execute(query, data)
    
    def insert_fight(self, data):
        query = """
            INSERT INTO fights (
                fight_id, event_id, event_date, weight_class, gender, is_title_fight,
                red_fighter_id, blue_fighter_id, red_fighter_name, blue_fighter_name,
                red_status, blue_status, result_type, winner_id, loser_id, winner_color,
                end_round, end_round_time, total_duration, rounds_scheduled, time_scheduled,
                method_raw, finish_type, decision_type, referee, event_status
            ) VALUES (
                %(fight_id)s, %(event_id)s, %(event_date)s, %(weight_class)s, %(gender)s, %(is_title_fight)s,
                %(red_fighter_id)s, %(blue_fighter_id)s, %(red_fighter_name)s, %(blue_fighter_name)s,
                %(red_status)s, %(blue_status)s, %(result_type)s, %(winner_id)s, %(loser_id)s, %(winner_color)s,
                %(end_round)s, %(end_round_time)s, %(total_duration)s, %(rounds_scheduled)s, %(time_scheduled)s,
                %(method_raw)s, %(finish_type)s, %(decision_type)s, %(referee)s, %(event_status)s
            )
            ON CONFLICT (fight_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                event_date = EXCLUDED.event_date,
                weight_class = EXCLUDED.weight_class,
                gender = EXCLUDED.gender,
                is_title_fight = EXCLUDED.is_title_fight,
                red_fighter_id = EXCLUDED.red_fighter_id,
                blue_fighter_id = EXCLUDED.blue_fighter_id,
                red_fighter_name = EXCLUDED.red_fighter_name,
                blue_fighter_name = EXCLUDED.blue_fighter_name,
                red_status = EXCLUDED.red_status,
                blue_status = EXCLUDED.blue_status,
                result_type = EXCLUDED.result_type,
                winner_id = EXCLUDED.winner_id,
                loser_id = EXCLUDED.loser_id,
                winner_color = EXCLUDED.winner_color,
                end_round = EXCLUDED.end_round,
                end_round_time = EXCLUDED.end_round_time,
                total_duration = EXCLUDED.total_duration,
                rounds_scheduled = EXCLUDED.rounds_scheduled,
                time_scheduled = EXCLUDED.time_scheduled,
                method_raw = EXCLUDED.method_raw,
                finish_type = EXCLUDED.finish_type,
                decision_type = EXCLUDED.decision_type,
                referee = EXCLUDED.referee,
                event_status = EXCLUDED.event_status,
                updated_at = CURRENT_TIMESTAMP;

        """
        self.cur.execute(query, data)

    def insert_fighter_stats(self, data):
        query = """
            INSERT INTO fighter_fights (
                fight_id, fighter_id, opponent_id,
                knockdowns, sub_attempts, reversals, ctrl_time,
                tot_str_landed, tot_str_attempted, tot_str_raw,
                td_landed, td_attempted, td_raw,
                sig_str_landed, sig_str_attempted, sig_str_raw,
                head_str_landed, head_str_attempted, head_str_raw,
                body_str_landed, body_str_attempted, body_str_raw,
                leg_str_landed, leg_str_attempted, leg_str_raw,
                distance_str_landed, distance_str_attempted, distance_str_raw,
                clinch_str_landed, clinch_str_attempted, clinch_str_raw,
                ground_str_landed, ground_str_attempted, ground_str_raw, event_status
            ) VALUES (
                %(fight_id)s, %(fighter_id)s, %(opponent_id)s,
                %(knockdowns)s, %(sub_attempts)s, %(reversals)s, %(ctrl_time)s,
                %(tot_str_landed)s, %(tot_str_attempted)s, %(tot_str_raw)s,
                %(td_landed)s, %(td_attempted)s, %(td_raw)s,
                %(sig_str_landed)s, %(sig_str_attempted)s, %(sig_str_raw)s,
                %(head_str_landed)s, %(head_str_attempted)s, %(head_str_raw)s,
                %(body_str_landed)s, %(body_str_attempted)s, %(body_str_raw)s,
                %(leg_str_landed)s, %(leg_str_attempted)s, %(leg_str_raw)s,
                %(distance_str_landed)s, %(distance_str_attempted)s, %(distance_str_raw)s,
                %(clinch_str_landed)s, %(clinch_str_attempted)s, %(clinch_str_raw)s,
                %(ground_str_landed)s, %(ground_str_attempted)s, %(ground_str_raw)s, %(event_status)s
            )
            ON CONFLICT (fight_id, fighter_id) DO UPDATE SET
                opponent_id = EXCLUDED.opponent_id,
                knockdowns = EXCLUDED.knockdowns,
                sub_attempts = EXCLUDED.sub_attempts,
                reversals = EXCLUDED.reversals,
                ctrl_time = EXCLUDED.ctrl_time,
                tot_str_landed = EXCLUDED.tot_str_landed,
                tot_str_attempted = EXCLUDED.tot_str_attempted,
                tot_str_raw = EXCLUDED.tot_str_raw,
                td_landed = EXCLUDED.td_landed,
                td_attempted = EXCLUDED.td_attempted,
                td_raw = EXCLUDED.td_raw,
                sig_str_landed = EXCLUDED.sig_str_landed,
                sig_str_attempted = EXCLUDED.sig_str_attempted,
                sig_str_raw = EXCLUDED.sig_str_raw,
                head_str_landed = EXCLUDED.head_str_landed,
                head_str_attempted = EXCLUDED.head_str_attempted,
                head_str_raw = EXCLUDED.head_str_raw,
                body_str_landed = EXCLUDED.body_str_landed,
                body_str_attempted = EXCLUDED.body_str_attempted,
                body_str_raw = EXCLUDED.body_str_raw,
                leg_str_landed = EXCLUDED.leg_str_landed,
                leg_str_attempted = EXCLUDED.leg_str_attempted,
                leg_str_raw = EXCLUDED.leg_str_raw,
                distance_str_landed = EXCLUDED.distance_str_landed,
                distance_str_attempted = EXCLUDED.distance_str_attempted,
                distance_str_raw = EXCLUDED.distance_str_raw,
                clinch_str_landed = EXCLUDED.clinch_str_landed,
                clinch_str_attempted = EXCLUDED.clinch_str_attempted,
                clinch_str_raw = EXCLUDED.clinch_str_raw,
                ground_str_landed = EXCLUDED.ground_str_landed,
                ground_str_attempted = EXCLUDED.ground_str_attempted,
                ground_str_raw = EXCLUDED.ground_str_raw,
                event_status = EXCLUDED.event_status,
                updated_at = CURRENT_TIMESTAMP;
        """
        self.cur.execute(query, data)
