import dlt
from pyspark.sql.functions import col, sha2, concat_ws, to_date

# ─────────────────────────────────────────
# SILVER — clean, deduplicated events
# ─────────────────────────────────────────
@dlt.table(
    name    = "dlt_silver_events",
    comment = "Silver: cleaned, deduplicated events. One row per unique event.",
    table_properties = {
        "quality"                          : "silver",
        "delta.enableChangeDataFeed"       : "true",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact"   : "true",
    }
)
@dlt.expect_or_drop(
    "valid_action",
    "event_type IN ('click','pageview','scroll','add_to_cart','purchase','search')"
)
@dlt.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect_or_drop("uri_not_null",      "uri IS NOT NULL")

def silver_events():
    return (
        dlt.read("bronze_events")
        .withColumnRenamed("action", "event_type")

        .withColumn("event_date", to_date(col("event_timestamp")))

        # row_hash — used for deduplication
        # if the same event arrives twice (e.g. pipeline rerun),
        # the hash will be identical and dropDuplicates removes it
        .withColumn(
            "row_hash",
            sha2(concat_ws("|",
                col("event_id"),
                col("user_id"),
                col("event_timestamp"),
                col("event_type"),
                col("uri")
            ), 256)
        )
        .dropDuplicates(["row_hash"])

        .select(
            "event_id",
            "user_id",
            "event_type",
            "event_timestamp",
            "event_date",
            "uri",
            "platform",
            "file_index",
            "row_hash",
            "source_filename",
            "ingestion_timestamp",
            "load_date",
        )
    )