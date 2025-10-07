import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import os

from aliyun.log import LogClient
import requests

# ================= CONFIG =================
# SLS config
ENDPOINT = os.environ.get("ENDPOINT")   # replace with your SLS endpoint
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
ACCESS_KEY_SECRET = os.environ.get("ACCESS_KEY_SECRET")
PROJECT = os.environ.get("PROJECT")
LOGSTORE = os.environ.get("LOGSTORE")

# Vector config
VECTOR_HTTP = os.environ.get("VECTOR_HTTP", "http://127.0.0.1:8686")

# Batch settings
BATCH_SIZE = 20      # how many log groups per batch
BATCH_TIMEOUT = 2.0  # max seconds to wait before flushing

# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# Init client
log_client = LogClient(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET)

def forward_to_vector(batch):
    """Send a batch of log groups to Vector via HTTP"""
    try:
        r = requests.post(VECTOR_HTTP, json=batch, timeout=5)
        if r.status_code != 200:
            logging.error(f"Vector rejected logs: {r.status_code} {r.text}")
        else:
            logging.info(f"Sent {len(batch)} log groups to Vector")
    except Exception as e:
        logging.error(f"Failed to send logs to Vector: {e}")

def handle_shard(shard_id: int):
    logging.info(f"Worker started for shard {shard_id}")
    # Start from the latest cursor (skip old logs)
    cursor = log_client.get_cursor(PROJECT, LOGSTORE, shard_id, "end").cursor

    batch = []
    last_flush = time.time()

    while True:
        try:
            res = log_client.pull_logs(PROJECT, LOGSTORE, shard_id, cursor, 100)
            cursor = res.get_next_cursor()

            logs = res.get_loggroup_json_list()
            if logs:
                batch.extend(logs)

            # flush conditions: batch full or timeout
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_TIMEOUT:
                if batch:
                    forward_to_vector(batch)
                    batch.clear()
                    last_flush = time.time()

            if not logs:
                time.sleep(1)

        except Exception as e:
            logging.error(f"Error in shard {shard_id}: {e}")
            time.sleep(2)

def main():
    # discover shards
    shards = log_client.list_shards(PROJECT, LOGSTORE).shards
    shard_ids = [s["shardID"] for s in shards]
    logging.info(f"Starting workers for shards: {shard_ids}")

    with ThreadPoolExecutor(max_workers=len(shard_ids)) as executor:
        for sid in shard_ids:
            executor.submit(handle_shard, sid)

        # keep main alive
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            logging.info("Stopping workers...")


if __name__ == "__main__":
    main()