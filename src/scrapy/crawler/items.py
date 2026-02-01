# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class EventItem(scrapy.Item):
    event_id = scrapy.Field()
    name = scrapy.Field()
    date = scrapy.Field()
    event_status = scrapy.Field()   # Completed, upcoming
    location = scrapy.Field()

class FightItem(scrapy.Item):
    # Identity Fields
    fight_id = scrapy.Field()
    event_id = scrapy.Field()
    event_date = scrapy.Field()
    weight_class = scrapy.Field()   
    gender = scrapy.Field()
    is_title_fight = scrapy.Field()
    event_status = scrapy.Field()

    # Fighters
    red_fighter_name = scrapy.Field()
    red_fighter_id = scrapy.Field()
    blue_fighter_name = scrapy.Field()
    blue_fighter_id = scrapy.Field()

    # Outcome
    red_status = scrapy.Field(default=None)
    blue_status = scrapy.Field(default=None)
    result_type = scrapy.Field(default=None) # Win, Draw, NC
    winner_id = scrapy.Field(default=None)
    loser_id = scrapy.Field(default=None)
    winner_color = scrapy.Field(default=None)

    # Time and Rounds
    end_round = scrapy.Field(default=None)
    end_round_time = scrapy.Field(default=None)
    total_duration = scrapy.Field(default=None)
    rounds_scheduled = scrapy.Field(default=None)
    time_scheduled = scrapy.Field(default=None)

    # Results
    method_raw = scrapy.Field(default=None)     ##
    finish_type = scrapy.Field(default=None)    ##    # KO/TKO | SUB | DEC | DQ | NC | Draw
    decision_type = scrapy.Field(default=None)  ##    # U-DEC | M-DEC | S-DEC | None

    # Context
    referee = scrapy.Field(default=None)

class FighterFightItem(scrapy.Item):
    #Fight Identity
    fight_id = scrapy.Field()

    #Fighter Identity
    fighter_id = scrapy.Field()
    opponent_id = scrapy.Field()
    event_status = scrapy.Field()

    # Totals
    knockdowns = scrapy.Field(default=None)
    tot_str_raw = scrapy.Field(default=None)
    tot_str_landed = scrapy.Field(default=None)
    tot_str_attempted = scrapy.Field(default=None)
    td_raw = scrapy.Field(default=None)
    td_landed = scrapy.Field(default=None)
    td_attempted = scrapy.Field(default=None)
    sub_attempts = scrapy.Field(default=None)
    reversals = scrapy.Field(default=None)
    ctrl_time = scrapy.Field(default=None)

    # Significant strikes
    sig_str_raw = scrapy.Field(default=None)
    sig_str_landed = scrapy.Field(default=None)
    sig_str_attempted = scrapy.Field(default=None)
    head_str_raw = scrapy.Field(default=None)
    head_str_landed = scrapy.Field(default=None)
    head_str_attempted = scrapy.Field(default=None)
    body_str_raw = scrapy.Field(default=None)
    body_str_landed = scrapy.Field(default=None)
    body_str_attempted = scrapy.Field(default=None)
    leg_str_raw = scrapy.Field(default=None)
    leg_str_landed = scrapy.Field(default=None)
    leg_str_attempted = scrapy.Field(default=None)
    distance_str_raw = scrapy.Field(default=None)
    distance_str_landed = scrapy.Field(default=None)
    distance_str_attempted = scrapy.Field(default=None)
    clinch_str_raw = scrapy.Field(default=None)
    clinch_str_landed = scrapy.Field(default=None)
    clinch_str_attempted = scrapy.Field(default=None)
    ground_str_raw = scrapy.Field(default=None)
    ground_str_landed = scrapy.Field(default=None)
    ground_str_attempted = scrapy.Field(default=None)

class FighterItem(scrapy.Item):
    fighter_id = scrapy.Field()    
    name = scrapy.Field()
    height = scrapy.Field(default=None)
    weight = scrapy.Field(default=None)
    reach = scrapy.Field(default=None)
    stance = scrapy.Field(default=None)
    dob = scrapy.Field(default=None)
    # record -> derive later