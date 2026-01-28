# constants.py

from datetime import datetime

CUTOFF_TIME = datetime(2001, 1, 1)

TOTAL_FIELDS = [
    'knockdowns', 
    None,            # Skip significant strikes
    None,            # Skip significant strike percentage
    'tot_str_raw', 
    'td_raw', 
    None,           # Skip percentages
    'sub_attempts', 
    'reversals', 
    'ctrl_time'
]

SIG_FIELDS = [
    'sig_str_raw', 
    None,       # Skip percentages
    'head_str_raw', 
    'body_str_raw', 
    'leg_str_raw', 
    'distance_str_raw', 
    'clinch_str_raw', 
    'ground_str_raw'
]

RAW_DATA_MAP = {
    'sig_str_raw': ('sig_str_landed', 'sig_str_attempted'),
    'tot_str_raw': ('tot_str_landed', 'tot_str_attempted'),
    'td_raw':      ('td_landed', 'td_attempted'),
    'head_str_raw': ('head_str_landed', 'head_str_attempted'),
    'body_str_raw': ('body_str_landed', 'body_str_attempted'),
    'leg_str_raw':  ('leg_str_landed', 'leg_str_attempted'),
    'distance_str_raw': ('distance_str_landed', 'distance_str_attempted'),
    'clinch_str_raw': ('clinch_str_landed', 'clinch_str_attempted'),
    'ground_str_raw': ('ground_str_landed', 'ground_str_attempted')
}