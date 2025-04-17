import asyncio
import time
import json
import redis

# Redis connection
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

MERGE_WINDOW_MS = 15000
# IDLE_FINALIZE_SECONDS = 3

# Store current merging state
merger_state = {
    "current": None,
    "last_merge_time": 0,
    "base_timestamp": None  # Timestamp to persist across merges
}

def get_uncleaned_blerbs():
    """Retrieve all unprocessed blerbs from Redis."""
    raw_list = redis_client.lrange("transcripts:uncleaned", 0, -1)
    return [json.loads(raw) for raw in raw_list]

def update_cleaned_line(entry, key_timestamp):
    """Write or update the cleaned transcript entry using a stable key."""
    print(f"✅ Updating cleaned line for timestamp: {key_timestamp} with entry: {entry}")
    redis_client.set(f"transcripts:cleaned:{key_timestamp}", json.dumps(entry))

def merge_blerbs(blerbs):
    for blerb in blerbs:
        current = merger_state["current"]

        if not current:
            # New merge thread begins
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])
            continue

        # Compare with current
        same_speaker = blerb["player_id"] == current["player_id"]
        close_in_time = blerb["start_timestamp"] - current["start_timestamp"] <= MERGE_WINDOW_MS

        if same_speaker and close_in_time:
            current["text"] += " " + blerb["text"]
            update_cleaned_line(current, merger_state["base_timestamp"])
        else:
            # Start new thread, but don’t “finalize” the old one
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])


def clear_uncleaned_queue():
    """Clear all processed blerbs from the uncleaned queue."""
    redis_client.delete("transcripts:uncleaned")

async def run_merger_loop():
    """Continuously process new transcript chunks and progressively merge them."""
    print("🌀 Merger service started.")

    while True:
        start = time.perf_counter()

        blerbs = get_uncleaned_blerbs()
        count = len(blerbs)

        if count > 0:
            print(f"\n📥 Found {count} blerb(s) to process.")
            merge_blerbs(blerbs)
            clear_uncleaned_queue()

        duration = round((time.perf_counter() - start) * 1000)

        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(run_merger_loop())
