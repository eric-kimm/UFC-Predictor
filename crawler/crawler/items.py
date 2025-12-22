# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class FighterItem(scrapy.Item):
    fighter_id = scrapy.Field()    
    name = scrapy.Field()
    height = scrapy.Field()
    weight = scrapy.Field()
    reach = scrapy.Field()
    stance = scrapy.Field()
    dob = scrapy.Field()


    ### Remove Later ###
    slpm = scrapy.Field()
    str_acc = scrapy.Field()
    sapm = scrapy.Field()
    str_def = scrapy.Field()

    td_avg = scrapy.Field()
    td_acc = scrapy.Field()
    td_def = scrapy.Field()
    sub_avg = scrapy.Field()

class FightItem(scrapy.Item):
    # Identity fields
    # fight_id = scrapy.Field()
    # fighter_name = scrapy.Field()
    # fighter_id = scrapy.Field()
    # opponent_name = scrapy.Field()
    # opponent_id = scrapy.Field()

    # Identity Fields
    fight_id = scrapy.Field()
    red_fighter_name = scrapy.Field()
    red_fighter_id = scrapy.Field()
    blue_fighter_name = scrapy.Field()
    blue_fighter_id = scrapy.Field()
    winner_id = scrapy.Field()
    loser_id = scrapy.Field()

    # Results
    result_type = scrapy.Field() # Win/Loss, Draw, NC
    method = scrapy.Field()
    round = scrapy.Field()
    time = scrapy.Field()
    time_seconds = scrapy.Field()
    scheduled_rounds = scrapy.Field()
    scheduled_time_seconds = scrapy.Field()
    referee = scrapy.Field()

class EventItem(scrapy.Item):
    #Event
    event_id = scrapy.Field()
    event_name = scrapy.Field()
    event_date = scrapy.Field()
    weight_class = scrapy.Field()

class FighterFightItem(scrapy.Item):
    #Fight Identity
    fight_id = scrapy.Field()

    #Fighter Identity
    fighter_id = scrapy.Field()
    opponent_id = scrapy.Field()

    # Totals
    knockdowns = scrapy.Field()
    sig_str_landed = scrapy.Field()
    sig_str_attempted = scrapy.Field()
    sig_str_percentage = scrapy.Field()
    tot_str_landed = scrapy.Field()
    tot_str_attempted = scrapy.Field()
    td_landed = scrapy.Field()
    td_attempted = scrapy.Field()
    td_percentage = scrapy.Field()
    sub_attempts = scrapy.Field()
    reversals = scrapy.Field()
    ctrl_time = scrapy.Field()
    ctrl_seconds = scrapy.Field()

    # Significant strikes
    sig_str_head_landed = scrapy.Field()
    sig_str_head_attempted = scrapy.Field()
    sig_str_body_landed = scrapy.Field()
    sig_str_body_attempted = scrapy.Field()
    sig_str_leg_landed = scrapy.Field()
    sig_str_leg_attempted = scrapy.Field()
    distance_landed = scrapy.Field()
    distance_attempted = scrapy.Field()
    clinch_landed = scrapy.Field()
    clinch_attempted = scrapy.Field()
    ground_landed = scrapy.Field()
    ground_attempted = scrapy.Field()

