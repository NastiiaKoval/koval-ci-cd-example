from pyspark.sql.types import *
from pyspark.sql import Row
import uuid
import random
from datetime import datetime, timedelta

def generate_json_files(
    start_idx: int,
    end_idx: int,
    target_dir: str,
    start_date=datetime(2024, 1, 1)
):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("event_date", LongType(), True),
        StructField("action", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("file_index", IntegerType(), True),
        StructField("generation_time", TimestampType(), True)
    ])

    actions = ["click", "pageview", "scroll", "add_to_cart", "purchase", "search"]
    uris = ["https://databricks.com/", "https://databricks.com/product", ...]

    for file_idx in range(start_idx, end_idx):
        events = []
        num_events = random.randint(1, 5)
        file_time = start_date + timedelta(hours=random.randint(0, 24*90))

        for i in range(num_events):
            ts = file_time + timedelta(seconds=random.randint(0, 300))
            event = {
                "user_id": str(uuid.uuid4()),
                "platform": random.choice(["web", "mobile", None]),
                "event_date": int(ts.timestamp()),
                "action": random.choice(actions),
                "uri": random.choice(uris),
                "event_id": str(uuid.uuid4()) if random.random() > 0.7 else None,
                "file_index": file_idx,
                "generation_time": datetime.now()
            }
            events.append(event)

        df_batch = spark.createDataFrame([Row(**event) for event in events], schema=schema)
        file_path = f"{target_dir}events_batch_{file_idx:04d}.json"
        df_batch.write.mode("overwrite").json(file_path)
        print(f"File created: {file_idx:04d}")

    # print(f"Generation completed: {start_idx} → {end_idx-1}")

# Automatic determination of the next index
bronze_table = "koval_bronze.bronze_events_stream"

try:
    # if table exist 
    max_idx_df = spark.sql(f"""
        SELECT COALESCE(MAX(file_index), -1) AS max_file_index 
        FROM {bronze_table}
    """)
    max_file_index = max_idx_df.collect()[0]["max_file_index"]
except Exception:
    # if the table dosn't exist or is empty
    max_file_index = -1

start_idx = max_file_index + 1
num_new_files = 100
end_idx = start_idx + num_new_files

print(f"Detected max file_index in bronze: {max_file_index}")
print(f"Generating next {num_new_files} files: {start_idx} → {end_idx-1}")

target_dir = spark.conf.get("my.raw_path")

# Run
generate_json_files(start_idx, end_idx, target_dir)

print("Auto-generation Done!")