# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class EventItem(scrapy.Item):
    event_id = scrapy.Field()
    name = scrapy.Field()
    date = scrapy.Field()
    status = scrapy.Field()   # Completed, upcoming
    location = scrapy.Field()

class FightItem(scrapy.Item):
    # Identity Fields
    fight_id = scrapy.Field()
    event_id = scrapy.Field()
    event_date = scrapy.Field()
    weight_class = scrapy.Field()   
    gender = scrapy.Field()
    is_title_fight = scrapy.Field()

    # Fighters
    red_fighter_name = scrapy.Field()
    red_fighter_id = scrapy.Field()
    blue_fighter_name = scrapy.Field()
    blue_fighter_id = scrapy.Field()

    # Outcome
    red_status = scrapy.Field()
    blue_status = scrapy.Field()
    result_type = scrapy.Field() # Win, Draw, NC
    winner_id = scrapy.Field()
    loser_id = scrapy.Field()
    winner_color = scrapy.Field()

    # Time and Rounds
    end_round = scrapy.Field()
    end_round_time = scrapy.Field()
    total_duration = scrapy.Field()
    rounds_scheduled = scrapy.Field()
    time_scheduled = scrapy.Field()

    # Results
    method_raw = scrapy.Field()     ##
    finish_type = scrapy.Field()    ##    # KO/TKO | SUB | DEC | DQ | NC | Draw
    decision_type = scrapy.Field()  ##    # U-DEC | M-DEC | S-DEC | None

    # Context
    referee = scrapy.Field()

class FighterFightItem(scrapy.Item):
    #Fight Identity
    fight_id = scrapy.Field()

    #Fighter Identity
    fighter_id = scrapy.Field()
    opponent_id = scrapy.Field()

    # Totals
    knockdowns = scrapy.Field()
    tot_str_raw = scrapy.Field()
    tot_str_landed = scrapy.Field()
    tot_str_attempted = scrapy.Field()
    td_raw = scrapy.Field()
    td_landed = scrapy.Field()
    td_attempted = scrapy.Field()
    sub_attempts = scrapy.Field()
    reversals = scrapy.Field()
    ctrl_time = scrapy.Field()

    # Significant strikes
    sig_str_raw = scrapy.Field()
    sig_str_landed = scrapy.Field()
    sig_str_attempted = scrapy.Field()
    head_str_raw = scrapy.Field()
    head_str_landed = scrapy.Field()
    head_str_attempted = scrapy.Field()
    body_str_raw = scrapy.Field()
    body_str_landed = scrapy.Field()
    body_str_attempted = scrapy.Field()
    leg_str_raw = scrapy.Field()
    leg_str_landed = scrapy.Field()
    leg_str_attempted = scrapy.Field()
    distance_str_raw = scrapy.Field()
    distance_str_landed = scrapy.Field()
    distance_str_attempted = scrapy.Field()
    clinch_str_raw = scrapy.Field()
    clinch_str_landed = scrapy.Field()
    clinch_str_attempted = scrapy.Field()
    ground_str_raw = scrapy.Field()
    ground_str_landed = scrapy.Field()
    ground_str_attempted = scrapy.Field()

class FighterItem(scrapy.Item):
    fighter_id = scrapy.Field()    
    name = scrapy.Field()
    height = scrapy.Field()
    weight = scrapy.Field()
    reach = scrapy.Field()
    stance = scrapy.Field()
    dob = scrapy.Field()
    # record -> derive later