import dlt
from pyspark.sql.functions import (
    col, count, min, max, sum, when,
    window, unix_timestamp, countDistinct,
    current_timestamp
)

# ─────────────────────────────────────────
# GOLD TABLE 1 — user sessions
#
# Groups events into 30-minute windows
# per user. A session is "active" if it
# contains 3 or more events.
#
# This is a useful business metric:
# how engaged was each user in a session?
# ─────────────────────────────────────────
@dlt.table(
    name    = "gold_user_sessions",
    comment = "Gold: user sessions aggregated in 30-minute windows.",
    table_properties = {
        "quality"                          : "gold",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact"   : "true",
    }
)
def gold_user_sessions():
    return (
        dlt.read("dlt_silver_events")
        .groupBy(
            "user_id",
            "platform",
            window(col("event_timestamp"), "30 minutes").alias("session_window")
        )
        .agg(
            count("*").alias("event_count"),
            countDistinct("event_type").alias("unique_actions"),
            min("event_timestamp").alias("session_start"),
            max("event_timestamp").alias("session_end"),

            # count specific high-value actions
            sum(when(col("event_type") == "purchase",    1).otherwise(0)).alias("purchase_count"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
        )
        .withColumn(
            "session_duration_seconds",
            unix_timestamp("session_end") - unix_timestamp("session_start")
        )
        .withColumn(
            "session_status",
            when(col("event_count") >= 3, "active").otherwise("passive")
        )
        .select(
            "user_id",
            "platform",
            "session_status",
            col("session_window.start").alias("session_start"),
            col("session_window.end").alias("session_end"),
            "session_duration_seconds",
            "event_count",
            "unique_actions",
            "purchase_count",
            "add_to_cart_count",
        )
    )


# ─────────────────────────────────────────
# GOLD TABLE 2 — daily action summary
#
# Per day + action type: how many events,
# how many unique users, and what share of
# total daily events does this action type
# represent.
#
# Useful for trend analysis:
# is purchase rate growing over time?
# ─────────────────────────────────────────
@dlt.table(
    name    = "gold_daily_actions",
    comment = "Gold: daily breakdown of event types with unique user counts.",
    table_properties = {
        "quality"                          : "gold",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact"   : "true",
    }
)
def gold_daily_actions():
    silver = dlt.read("dlt_silver_events")

    # total events per day — needed for share calculation
    daily_totals = (
        silver
        .groupBy("event_date")
        .agg(count("*").alias("total_daily_events"))
    )

    return (
        silver
        .groupBy("event_date", "event_type")
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
        )
        # join totals to compute share
        .join(daily_totals, on="event_date", how="left")
        .withColumn(
            "action_share_pct",
            (col("event_count") / col("total_daily_events") * 100).cast("decimal(5,2)")
        )
        .select(
            "event_date",
            "event_type",
            "event_count",
            "unique_users",
            "total_daily_events",
            "action_share_pct",
        )
        .orderBy("event_date", "event_type")
    )