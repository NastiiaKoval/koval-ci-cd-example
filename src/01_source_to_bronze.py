import dlt
from pyspark.sql.functions import col, current_timestamp, current_date, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, TimestampType
)

# ── Constants ──────────────────
RAW_PATH = spark.conf.get("my.raw_path")


EVENTS_SCHEMA = StructType([
    StructField("user_id",         StringType(),    True),
    StructField("platform",        StringType(),    True),
    StructField("event_date",      LongType(),      True),
    StructField("action",          StringType(),    True),
    StructField("uri",             StringType(),    True),
    StructField("event_id",        StringType(),    True),
    StructField("file_index",      IntegerType(),   True),
    StructField("generation_time", TimestampType(), True),
])

# ── raw_events ────────────────────
@dlt.table(
    name    = "raw_events",
    comment = "Raw JSON events from /raw/incremental/ via Auto Loader.",
    table_properties = {
        "quality"                          : "raw",
        "delta.autoOptimize.optimizeWrite" : "true",
    }
)
def raw_events():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",         "json")
        # .option("cloudFiles.schemaLocation", CHECKPOINT_DLT)
        .option("multiLine",                 "true")
        .option("cloudFiles.maxFilesPerTrigger", 20)
        .schema(EVENTS_SCHEMA)
        .load(RAW_PATH)
        .withColumn("source_filename", col("_metadata.file_path"))

    )

# ── bronze_events ─────────────────────────────────────────────────────────────
@dlt.table(
    name    = "bronze_events",
    comment = "Bronze: raw events + metadata. Invalid rows dropped via expectations.",
    table_properties = {
        "quality"                          : "bronze",
        "delta.autoOptimize.optimizeWrite" : "true",
        "delta.autoOptimize.autoCompact"   : "true",
    }
)
@dlt.expect_or_drop("user_id_not_null",  "user_id IS NOT NULL")
@dlt.expect_or_drop("action_not_null",   "action IS NOT NULL")
@dlt.expect_or_drop("event_date_exists", "event_date IS NOT NULL")

def bronze_events():
    return (
        dlt.read_stream("raw_events")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("load_date",           current_date())
        .withColumn("event_timestamp",     from_unixtime(col("event_date")).cast("timestamp"))
    )