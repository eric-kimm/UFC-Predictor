import os
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

def connect_to_postgres():
  password = os.getenv("POSTGRES_PASSWORD")
  conn_string = f"postgresql://erickim:{password}@localhost:5432/ufc"
  return create_engine(conn_string)

def get_fighter_fights_table(engine):
  query1 = "SELECT * FROM fighter_fights;"
  fighter_fights = pd.read_sql(query1, con=engine)
  return fighter_fights

def get_fights_table(engine):
  query2 = "SELECT * FROM fights;"
  fights = pd.read_sql(query2, con=engine)
  fights = fights.drop(['event_status'], axis = 1)
  return fights

def merge_tables(fighter_fights, fights):
  df = pd.merge(fights, fighter_fights, on='fight_id')
  df = df.sort_values(by=['fight_id', 'event_id'])

  drop_cols = [
      "event_id",
      "updated_at_x",
      "updated_at_y",
      "gender",
      "red_fighter_name",
      "blue_fighter_name",
      "red_status",
      "blue_status",
      "winner_id",
      "loser_id",
      "result_type",
      "end_round_time",
      "time_scheduled",
      "method_raw"
  ]

  df = df.drop(drop_cols, axis = 1)

  df.insert(2,'fighter_id', df.pop("fighter_id"))
  df.insert(3,'opponent_id', df.pop("opponent_id"))

  print(df.shape)
  print(df.columns)
  
  return df

def create_absorb_receive_columns(df):
  df['sig_str_absorbed'] = (
      df
      .groupby('fight_id')['sig_str_landed']
      .transform(lambda x: x.iloc[::-1].values)
  )
  df['sig_str_received'] = (
      df
      .groupby('fight_id')['sig_str_attempted']
      .transform(lambda x: x.iloc[::-1].values)
  )
  df['td_absorbed'] = (
      df
      .groupby('fight_id')['td_landed']
      .transform(lambda x: x.iloc[::-1].values)
  )
  df['td_received'] = (
      df
      .groupby('fight_id')['td_attempted']
      .transform(lambda x: x.iloc[::-1].values)
  )

  return df

def calculate_weighted_moving_averages(df):
  wanted = df.columns.to_list()
  unwanted = ['fight_id', 'event_date', 'fighter_id', 'opponent_id', 'weight_class',
            'is_title_fight', 'red_fighter_id', 'blue_fighter_id', 'winner_color',
            'end_round','rounds_scheduled', 'finish_type', 'total_duration',
            'decision_type', 'referee','tot_str_raw', 'td_raw', 'sig_str_raw', 
            'head_str_raw', 'body_str_raw', 'leg_str_raw','distance_str_raw',
            'clinch_str_raw','ground_str_raw','updated_at', 'event_status'
            ] 
            
  cols = [c for c in wanted if c not in unwanted]
  cols.append('total_duration')

  df = df.sort_values(['fighter_id', 'event_date', 'fight_id'])
  avg = (
    df.groupby("fighter_id")[cols]
    .ewm(span=5)
    .mean()
    .groupby(level=0)
    .shift(1)
    .reset_index(level=0, drop=True)
  )
  avg.columns = [f"avg_{c}" for c in cols]
  df = df.join(avg)
    
  final_cols = [
      "fighter_id",
      "fight_id",
      "event_date",
      "weight_class",
      "is_title_fight",
      "winner_color",
      "red_fighter_id",
      "blue_fighter_id",
      "end_round",
      "total_duration",
      "rounds_scheduled",
      "finish_type",
      "decision_type",
      "referee",
      "event_status"
  ] + [
      f"avg_{c}" for c in cols
  ]

  df = df[final_cols]

  return df

def calculate_rates(df):
  # General rates
  df['w_SLpM'] = df['avg_sig_str_landed'] / (df['avg_total_duration'] / 60)
  df['w_SApM'] = df['avg_sig_str_absorbed'] / (df['avg_total_duration'] / 60)
  df['w_StrAcc'] = np.where(
    df['avg_sig_str_attempted'] > 0, df['avg_sig_str_landed'] / df['avg_sig_str_attempted'],
    np.nan
  )
  df['w_StrDef'] = np.where(
    df['avg_sig_str_received'] > 0, (df['avg_sig_str_received'] - df['avg_sig_str_absorbed']) / df['avg_sig_str_received'],
    np.nan
  )
  df['w_TDavg'] = (df['avg_td_landed'] / (df['avg_total_duration'] / 900))
  df['w_TDacc'] = np.where(
    df['avg_td_attempted'] > 0, df['avg_td_landed'] / df['avg_td_attempted'],
    np.nan
  ) 
  df['w_TDdef'] = np.where(
    df['avg_td_received'] > 0, (df['avg_td_received'] - df['avg_td_absorbed']) / df['avg_td_received'],
    np.nan
  )
  df['w_SubAvg'] = (df['avg_sub_attempts'] / (df['avg_total_duration'] / 900))

  # Additional rates
  specs = ['head', 'body', 'leg', 'distance', 'clinch', 'ground']

  landed_cols = [f'avg_{c}_str_landed' for c in specs]
  attempted_cols = [f'avg_{c}_str_attempted' for c in specs]

  ratios = df[landed_cols].div(df['avg_sig_str_landed'], axis=0)
  ratios.columns = [f'w_{c}_ratio' for c in specs]

  attempted = df[attempted_cols].replace(0, np.nan)
  acc = df[landed_cols].div(attempted.to_numpy())
  acc.columns = [f'w_{c}_acc' for c in specs]

  df = df.join(ratios).join(acc)
  df['w_knockdown_avg'] = df['avg_knockdowns'] / (df['avg_total_duration'] / 900)
  df['w_reversal_avg'] = df['avg_reversals'] / (df['avg_total_duration'] / 900)
  df['w_ctrl_time_pct'] = df['avg_ctrl_time'] / df['avg_total_duration']
  df['w_str_eff'] = np.where(
    df['avg_tot_str_landed'] > 0, df['avg_sig_str_landed'] / df['avg_tot_str_landed'], 
    np.nan
  )

  df = calculate_delta_rates(df)

  return df

def calculate_delta_rates(df):
  cols = ['SLpM', 'SApM', 'StrAcc', 'StrDef', 'TDavg', 'TDdef', 'TDacc', 'SubAvg', 'head_ratio', 'head_acc', 'body_ratio', 'body_acc', 
        'leg_ratio', 'leg_acc', 'distance_ratio', 'distance_acc', 'clinch_ratio', 'clinch_acc', 'ground_ratio', 'ground_acc', 
        'knockdown_avg', 'reversal_avg', 'ctrl_time_pct', 'str_eff']

  w_cols = [f'w_{c}' for c in cols]
  opp = df.groupby('fight_id')[w_cols].transform(lambda x: x.iloc[::-1].values)
  opp.columns = [f'opp_{c}' for c in w_cols]
  df = df.join(opp)

  deltas = pd.DataFrame(
    df[w_cols].to_numpy() - opp.to_numpy(),
    columns=[f'delta_{c}' for c in cols],
    index=df.index,
  )
  df = df.join(deltas)

  df['net_str_eff'] = (df['w_SLpM'] - df['w_SApM']) - (df['opp_w_SLpM'] - df['opp_w_SApM'])

  df = drop_avg_cols(df, cols)

  return df

def drop_avg_cols(df, cols):
  drop_cols = [
    'avg_knockdowns',
    'avg_sub_attempts',
    'avg_reversals',
    'avg_ctrl_time',
    'avg_tot_str_landed',
    'avg_tot_str_attempted',
    'avg_td_landed',
    'avg_td_attempted',
    'avg_sig_str_landed',
    'avg_sig_str_attempted',
    'avg_head_str_landed',
    'avg_head_str_attempted',
    'avg_body_str_landed',
    'avg_body_str_attempted',
    'avg_leg_str_landed',
    'avg_leg_str_attempted',
    'avg_distance_str_landed',
    'avg_distance_str_attempted',
    'avg_clinch_str_landed',
    'avg_clinch_str_attempted',
    'avg_ground_str_landed',
    'avg_ground_str_attempted',
    'avg_sig_str_absorbed',
    'avg_sig_str_received',
    'avg_td_absorbed',
    'avg_td_received',
    'avg_total_duration'
  ]

  additional = [f'opp_w_{c}' for c in cols]

  drop_cols.extend(additional)

  df = df.drop(drop_cols, axis=1)

  return df

def get_fighters_table(engine):
  query = """ 
    SELECT * FROM fighters;
  """

  fighters = pd.read_sql(query, con=engine)

  return fighters

def merge_fighters_to_main_df(df, fighters):
  df = df.merge(fighters[['fighter_id', 'name', 'height', 'reach', 'stance', 'dob']], on='fighter_id', how='left')

  return df

def handle_NaNs(df):
  cols = ['height', 'reach']
  group_medians = df.groupby('weight_class')[cols].transform('median')
  df[cols] = df[cols].fillna(group_medians)

  df['stance'] = df['stance'].fillna('Orthodox')

  is_dob_null = df['dob'].isnull()
  has_any_null_in_group = is_dob_null.groupby(df['fight_id']).transform('any')
  df = df[~has_any_null_in_group]
  
  return df

def encode_categorical_columns(df):
  df['weight_class'] = df['weight_class'].str.replace("'", "", regex=False).str.replace(" ", "_", regex=False)
  df['finish_type'] = df['finish_type'].str.replace("/", "_", regex=False)
  df['decision_type'] = df['decision_type'].str.replace("-", "_", regex=False)
  df['stance'] = df['stance'].str.replace(" ", "_", regex=False)

  df = pd.get_dummies(df, columns=['weight_class', 'finish_type', 'decision_type', 'stance'])

  print("COLUMNS:", df.columns)

  df = df[
    ~df['fight_id'].isin(
      df.loc[
          df[
              [
                'finish_type_DQ',
                'finish_type_Draw',
                'finish_type_NC',
                'decision_type_OTHER_DEC',
                'stance_Sideways',
                'stance_Open_Stance'
              ]
          ].any(axis=1),
          'fight_id'
      ]
    )
  ]

  # drop_cols = ['finish_type_DQ', 'finish_type_Draw', 'finish_type_NC', 'decision_type_OTHER_DEC', 'stance_Sideways', 'stance_Open_Stance']

  # df = df.drop(drop_cols, axis=1)

  return df

def calculate_current_win_streak(df):
  df['fighter_color'] = np.where(
      df['fighter_id'] == df['red_fighter_id'], 
      'Red',
      'Blue'
  )

  df = df.sort_values(by=['fighter_id', 'event_date'])

  df['is_win'] = (df['fighter_color'] == df['winner_color']).astype(int)

  df['streak_group'] = (
      df['is_win'] != df.groupby('fighter_id')['is_win'].shift()
  ).groupby(df['fighter_id']).cumsum()

  df['running_streak'] = (
      df.groupby(['fighter_id', 'is_win', 'streak_group']).cumcount() + 1
  )
  df['tmp_win_streak'] = np.where(df['is_win'] == 1, df['running_streak'], 0)

  df['win_streak'] = df.groupby('fighter_id')['tmp_win_streak'].shift(fill_value=0)

  df.drop(columns=['is_win', 'streak_group', 'running_streak', 'tmp_win_streak'], inplace=True)

  return df

def calculate_current_lose_streak(df):
  df['is_loss'] = (df['fighter_color'] != df['winner_color']).astype(int)

  df['streak_group'] = (
      df['is_loss'] != df.groupby('fighter_id')['is_loss'].shift()
  ).groupby(df['fighter_id']).cumsum()

  df['running_streak'] = (
      df.groupby(['fighter_id', 'is_loss', 'streak_group']).cumcount() + 1
  )
  df['tmp_loss_streak'] = np.where(df['is_loss'] == 1, df['running_streak'], 0)

  df['lose_streak'] = df.groupby('fighter_id')['tmp_loss_streak'].shift(fill_value=0)

  df = df.drop(columns=['is_loss', 'streak_group', 'running_streak', 'tmp_loss_streak'])

  return df

def calculate_longest_win_streak(df):
  df = df.sort_values(['fighter_id', 'event_date'])

  df['is_win'] = df['fighter_color'] == df['winner_color']
  streak_group = df['is_win'].ne(df.groupby('fighter_id')['is_win'].shift()).groupby(df['fighter_id']).cumsum()
  win_streak = df.groupby(['fighter_id', streak_group])['is_win'].cumsum().where(df['is_win'], 0)
  longest_to_date = win_streak.groupby(df['fighter_id']).cummax()
  df['longest_win_streak'] = longest_to_date.groupby(df['fighter_id']).shift(1, fill_value=0)
  df = df.drop(columns=['is_win'])

  return df


def calculating_win_by_columns(df):
  cols = [
    'finish_type_KO_TKO',
    'finish_type_SUB', 
    'decision_type_M_DEC', 
    'decision_type_S_DEC', 
    'decision_type_U_DEC'
  ]

  df = df.sort_values(by=['fighter_id', 'event_date'])

  win_mask = (df['winner_color'] == df['fighter_color']).astype(int)
  win_signal = df[cols].mul(win_mask, axis=0)
  wins_by = win_signal.groupby(df['fighter_id']).cumsum()
  wins_by = wins_by.groupby(df['fighter_id']).shift(1, fill_value=0)
  wins_by.columns = [f"wins_by_{col.split('_type_')[1]}" for col in cols]
  df = df.join(wins_by)

  df = df.drop(columns=['finish_type_DEC','finish_type_KO_TKO', 
        'finish_type_SUB', 'decision_type_M_DEC', 'decision_type_S_DEC', 'decision_type_U_DEC'])

  return df

def calculate_age(df):
  def calculate(row):
      return row['event_date'].year - row['dob'].year - (
          (row['event_date'].month, row['event_date'].day) < 
          (row['dob'].month, row['dob'].day)
      )

  df['age'] = df.apply(calculate, axis=1)
  df = df.drop(columns=['dob'])

  return df

def add_is_debut_feature(df):
  df = df.sort_values(by=['event_date', 'fight_id'])
  df['is_debut'] = (
      df.groupby('fighter_id').cumcount() == 0
  ).astype(int)

  return df

def convert_data_to_wide_format(df):
  shared_cols = [
    'fight_id', 'event_date', 'is_title_fight', 'winner_color', 'end_round', 
    'total_duration', 'rounds_scheduled', 'referee', 'event_status', 'weight_class_Bantamweight',
    'weight_class_Catch_Weight', 'weight_class_Featherweight',
    'weight_class_Flyweight', 'weight_class_Heavyweight',
    'weight_class_Light_Heavyweight', 'weight_class_Lightweight',
    'weight_class_Middleweight', 'weight_class_Welterweight',
    'weight_class_Womens_Bantamweight', 'weight_class_Womens_Featherweight',
    'weight_class_Womens_Flyweight', 'weight_class_Womens_Strawweight', 
    'delta_TDavg', 'delta_TDdef', 'delta_TDacc', 'delta_SubAvg', 'net_str_eff',
    'delta_head_ratio', 'delta_head_acc', 'delta_body_ratio', 'delta_body_acc', 'delta_leg_ratio', 'delta_leg_acc',
    'delta_knockdown_avg', 'delta_reversal_avg', 'delta_ctrl_time_pct', 
    'delta_SLpM', 'delta_SApM', 'delta_StrDef', 'delta_StrAcc', 'delta_str_eff',
  ]

  fighter_cols = [
      'fighter_id','w_SLpM', 'w_StrAcc', 'w_SApM', 
      'w_StrDef', 'w_TDavg', 'w_TDacc', 'w_TDdef', 'w_SubAvg', 'w_knockdown_avg', 'w_reversal_avg', 'w_ctrl_time_pct', 
      'w_str_eff','name', 'height', 'reach', 'stance_Orthodox','stance_Southpaw', 'stance_Switch',
      'fighter_color', 'win_streak','lose_streak', 'longest_win_streak', 'wins_by_KO_TKO', 'wins_by_SUB',
      'wins_by_M_DEC', 'wins_by_S_DEC', 'wins_by_U_DEC', 'age', 'is_debut'
  ]

  red_df = df[df['fighter_color'] == 'Red'][['fight_id'] + fighter_cols]
  blue_df = df[df['fighter_color'] == 'Blue'][['fight_id'] + fighter_cols]

  red_df = red_df.add_prefix('R_').rename(columns={'R_fight_id': 'fight_id'})
  blue_df = blue_df.add_prefix('B_').rename(columns={'B_fight_id': 'fight_id'})

  shared_df = df[shared_cols].drop_duplicates(subset=['fight_id'])

  df = shared_df.merge(red_df, on='fight_id').merge(blue_df, on='fight_id')

  return df

def create_fighter_attribute_deltas(df):
  df['delta_age'] = df['R_age'] - df['B_age']
  df['delta_height'] = df['R_height'] - df['B_height']
  df['delta_reach'] = df['R_reach'] - df['B_reach']

  return df

def clean_up_for_training(df):
  df = df.sort_values(by=['event_date', 'fight_id'])

  cols = ['fight_id', 'event_date', 'total_duration', 'end_round', 'referee', 'R_name', 'B_name', 'R_fighter_color', 'B_fighter_color' , 'R_fighter_id', 'B_fighter_id']
  df = df.drop(cols, axis=1)
  df = df.rename(columns={'winner_color': 'winner', 'rounds_scheduled': 'no_of_rounds'})
  df.columns = df.columns.str.lower()

  return df

def save_data_as_parquet(df):
  df.to_parquet("data.parquet", engine="pyarrow", compression="snappy", index=False)

def print_table_descending(df):
  print(df.shape)
  print(df.columns)

  df = df.sort_values(by=['event_date', 'fight_id'], ascending=False)
  print(df.head(10))


def main():
  conn_engine = connect_to_postgres()

  fighter_fights_table = get_fighter_fights_table(conn_engine)
  fights_table = get_fights_table(conn_engine)
  fighters_table = get_fighters_table(conn_engine)

  df = merge_tables(fighter_fights_table, fights_table)
  df = create_absorb_receive_columns(df)
  df = calculate_weighted_moving_averages(df)
  df = calculate_rates(df)
  df = merge_fighters_to_main_df(df, fighters_table)
  df = handle_NaNs(df)
  df = encode_categorical_columns(df)
  df = calculate_current_win_streak(df)
  df = calculate_current_lose_streak(df)
  df = calculate_longest_win_streak(df)
  df = calculating_win_by_columns(df)
  df = calculate_age(df)
  df = add_is_debut_feature(df)
  df = convert_data_to_wide_format(df)
  df = create_fighter_attribute_deltas(df)

  print_table_descending(df)
  df = clean_up_for_training(df)

  print(df.head(10))
  save_data_as_parquet(df)


if __name__ == "__main__":
  main()



