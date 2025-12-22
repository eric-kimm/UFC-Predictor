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
    CREATE TABLE IF NOT EXISTS fighters (
        id SERIAL PRIMARY KEY,
        fighter_id TEXT UNIQUE,

        -- Core profile info
        name        TEXT NOT NULL,
        stance      TEXT,
        dob         DATE,

        -- Physical attributes (cleaned to integers)
        height      INTEGER,   -- inches
        weight      INTEGER,   -- lbs
        reach       INTEGER,   -- inches

        -- Striking statistics
        slpm        REAL,      -- significant strikes landed per minute
        str_acc     REAL,      -- accuracy as decimal (e.g., 0.48)
        sapm        REAL,      -- significant strikes absorbed per minute
        str_def     REAL,      -- defense percentage as decimal

        -- Grappling statistics
        td_avg      REAL,
        td_acc      REAL,
        td_def      REAL,
        sub_avg     REAL
    );
""")

cur.execute("""
    CREATE TABLE IF NOT EXISTS fighter_history (
        id SERIAL PRIMARY KEY,

        -- Identity fields
        fight_id TEXT NOT NULL,
        fighter_id TEXT NOT NULL,
        opponent_id TEXT NOT NULL,

        -- Event metadata
        event_name TEXT,
        event_date DATE NOT NULL,
        weight_class TEXT,

        -- Result
        result TEXT,             -- win, loss, draw, nc
        method TEXT,             -- KO/TKO, SUB, DEC, etc
        round INTEGER,
        time TEXT,               -- "4:21"
        time_seconds INTEGER,    -- normalized round time in seconds
        scheduled_rounds INTEGER, -- 3 or 5
        scheduled_time_seconds INTEGER, -- 900 or 1500
        referee TEXT,

        -- Overall striking/grappling
        sig_str_landed INTEGER,
        sig_str_attempted INTEGER,
        tot_str_landed INTEGER,
        tot_str_attempted INTEGER,
        td_landed INTEGER,
        td_attempted INTEGER,
        sub_attempts INTEGER,
        passes INTEGER,
        reversals INTEGER,

        -- Control time
        ctrl_time TEXT,          -- "3:12"
        ctrl_seconds INTEGER,    -- normalized

        -- Significant strike breakdown
        sig_str_head_landed INTEGER,
        sig_str_head_attempted INTEGER,
        sig_str_body_landed INTEGER,
        sig_str_body_attempted INTEGER,
        sig_str_leg_landed INTEGER,
        sig_str_leg_attempted INTEGER,

        -- Position breakdown
        distance_landed INTEGER,
        distance_attempted INTEGER,
        clinch_landed INTEGER,
        clinch_attempted INTEGER,
        ground_landed INTEGER,
        ground_attempted INTEGER
    );
""")

conn.commit()
cur.close()
conn.close()
