import psycopg2
import os

conn = psycopg2.connect(
    dbname="ufc",
    user="erickim",
    password=os.getenv("POSTGRES_PASSWORD"),
    host="localhost",
    port=5432
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id VARCHAR(50) PRIMARY KEY,
        name TEXT NOT NULL,
        date DATE NOT NULL,
        status TEXT,
        location TEXT,
        updated_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS fighters (
        fighter_id VARCHAR(50) PRIMARY KEY,
        name        TEXT NOT NULL,
        height      INTEGER,   -- inches
        weight      INTEGER,   -- lbs
        reach       INTEGER,   -- inches
        stance      TEXT,
        dob         DATE,
        updated_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS fights (
        -- Unique Identity
        fight_id        VARCHAR(50) PRIMARY KEY,
        event_id        VARCHAR(50) NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
        event_date      DATE NOT NULL,
        weight_class    TEXT,
        gender          VARCHAR(10), -- Men/Women
        is_title_fight  BOOLEAN DEFAULT FALSE,

        -- Fighter Links (Foreign Keys to the fighters table)
        red_fighter_id  VARCHAR(50) NOT NULL REFERENCES fighters(fighter_id),
        blue_fighter_id VARCHAR(50) NOT NULL REFERENCES fighters(fighter_id),
        red_fighter_name  TEXT,
        blue_fighter_name TEXT,

        -- Outcome Data
        red_status      VARCHAR(20), -- Win, Loss, Draw, NC
        blue_status     VARCHAR(20),
        result_type     VARCHAR(20), -- KO/TKO, SUB, U-DEC, etc.
        winner_id       VARCHAR(50),
        loser_id        VARCHAR(50),
        winner_color    VARCHAR(10), -- Red or Blue

        -- Timing and Rounds
        end_round       INTEGER,
        end_round_time  INTEGER,     -- Seconds
        total_duration  INTEGER,     -- Seconds
        rounds_scheduled INTEGER,
        time_scheduled  INTEGER,     -- Seconds

        -- Result Specifics
        method_raw      TEXT,
        finish_type     VARCHAR(50), -- KO/TKO" | "SUBMISSION" | "DEC"
        decision_type   VARCHAR(50), -- "U-DEC" | "M-DEC" | "S-DEC"
        referee         TEXT,

        -- Metadata
        updated_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS fighter_fights (
        -- Composite Primary Key: A fighter can only have one set of stats per fight
        fight_id            VARCHAR(50) NOT NULL REFERENCES fights(fight_id) ON DELETE CASCADE,
        fighter_id          VARCHAR(50) NOT NULL REFERENCES fighters(fighter_id) ON DELETE CASCADE,
        
        -- Context
        opponent_id         VARCHAR(50) REFERENCES fighters(fighter_id),

        -- General Performance
        knockdowns          INTEGER DEFAULT 0,
        sub_attempts        INTEGER DEFAULT 0,
        reversals           INTEGER DEFAULT 0,
        ctrl_time           INTEGER, -- Store as total seconds (e.g., 145)

        -- Total Strikes
        tot_str_landed      INTEGER DEFAULT 0,
        tot_str_attempted   INTEGER DEFAULT 0,
        tot_str_raw         TEXT,    -- Original string like "45 of 100"

        -- Takedowns
        td_landed           INTEGER DEFAULT 0,
        td_attempted        INTEGER DEFAULT 0,
        td_raw              TEXT,

        -- Significant Strikes
        sig_str_landed      INTEGER DEFAULT 0,
        sig_str_attempted   INTEGER DEFAULT 0,
        sig_str_raw         TEXT,

        -- Significant Strikes by Target
        head_str_landed     INTEGER DEFAULT 0,
        head_str_attempted  INTEGER DEFAULT 0,
        head_str_raw        TEXT,
        body_str_landed     INTEGER DEFAULT 0,
        body_str_attempted  INTEGER DEFAULT 0,
        body_str_raw        TEXT,
        leg_str_landed      INTEGER DEFAULT 0,
        leg_str_attempted   INTEGER DEFAULT 0,
        leg_str_raw         TEXT,

        -- Significant Strikes by Position
        distance_str_landed    INTEGER DEFAULT 0,
        distance_str_attempted INTEGER DEFAULT 0,
        distance_str_raw       TEXT,
        clinch_str_landed      INTEGER DEFAULT 0,
        clinch_str_attempted   INTEGER DEFAULT 0,
        clinch_str_raw         TEXT,
        ground_str_landed      INTEGER DEFAULT 0,
        ground_str_attempted   INTEGER DEFAULT 0,
        ground_str_raw         TEXT,

        -- Metadata
        updated_at          TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

        -- Constraint to ensure we don't duplicate a fighter's stats for the same fight
        PRIMARY KEY (fight_id, fighter_id)
    );    
           
""")


conn.commit()
cur.close()
conn.close()
