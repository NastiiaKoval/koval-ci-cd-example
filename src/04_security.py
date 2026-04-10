# 04_apply_security.py

# ── Configuration ─────────────────────────────────────
CATALOG       = spark.conf.get("my.catalog")  # dbr_dev / dbr_prod
SCHEMA_BRONZE = spark.conf.get("my.schema_bronze")        
SCHEMA_GOLD   = spark.conf.get("my.schema_gold")

def fqn_table(table):
    """Fully qualified for all table name."""
    return f"{CATALOG}.{SCHEMA_BRONZE}.{table}"

def fqn(func):
    """Fully qualified function name."""
    return f"{CATALOG}.{SCHEMA_GOLD}.{func}"


# ── RLS ───────────────────────────────────────────────
# PART 1 — Date-range row filter (recent data only)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn('rls_recent_date_filter')}(event_date DATE)
RETURNS BOOLEAN
RETURN
    CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts') 
        THEN TRUE
        ELSE event_date >= DATE_SUB(CURRENT_DATE(), 2)
    END
""")

spark.sql(f"""
ALTER TABLE {fqn_table('gold_daily_actions')}
SET ROW FILTER {fqn('rls_recent_date_filter')} ON (event_date)
""")

print("✓ RLS applied: date-range filter on gold_daily_actions")

# ── CLS ───────────────────────────────────────────────
# CLS 1: Mask user_id
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn('cls_mask_user_id')}(user_id STRING)
RETURNS STRING
RETURN
    CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts') 
        THEN user_id
        ELSE CONCAT('MASKED_', SHA2(user_id, 256))
    END
""")

spark.sql(f"""
ALTER TABLE {fqn_table('dlt_silver_events')}
ALTER COLUMN user_id SET MASK {fqn('cls_mask_user_id')}
""")

spark.sql(f"""
ALTER TABLE {fqn_table('gold_user_sessions')}
ALTER COLUMN user_id SET MASK {fqn('cls_mask_user_id')}
""")

print("✓ CLS 1 applied: user_id masked on silver & gold_user_sessions")


# CLS 2: Mask event_id
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn('cls_mask_event_id')}(event_id STRING)
RETURNS STRING
COMMENT 'CLS: redacts internal event_id for non-engineers'
RETURN
    CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')         
        THEN event_id
        ELSE 'REDACTED'
    END
""")

spark.sql(f"""
ALTER TABLE {fqn_table('dlt_silver_events')}
ALTER COLUMN event_id SET MASK {fqn('cls_mask_event_id')}
""")

print("✓ CLS 2 applied: event_id masked on silver")

# CLS 3: Mask URI
spark.sql(f"""
CREATE OR REPLACE FUNCTION {fqn('cls_mask_uri')}(uri STRING)
RETURNS STRING
COMMENT 'CLS: strips URI path for non-analyst users'
RETURN
    CASE
        WHEN IS_ACCOUNT_GROUP_MEMBER('senior_analysts')   THEN uri
        ELSE CONCAT(PARSE_URL(uri, 'HOST'), '/***')
    END
""")

spark.sql(f"""
ALTER TABLE {fqn_table('dlt_silver_events')}
ALTER COLUMN uri SET MASK {fqn('cls_mask_uri')}
""")

print("✓ CLS 3 applied: uri masked on silver")