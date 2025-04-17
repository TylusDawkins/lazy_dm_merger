import asyncio
import time
import json
import redis

# Connect to Redis with string decoding enabled
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# Time window in milliseconds within which two messages from the same speaker will be merged
MERGE_WINDOW_MS = 8000
# Time to wait before finalizing an unfinished message due to inactivity (in seconds)
IDLE_FINALIZE_SECONDS = 5

# Persistent state across merge loop iterations
merger_state = {
    "current": None,            # Currently merging blerb
    "last_merge_time": 0       # Timestamp of last merge update
}

def get_uncleaned_blerbs():
    """Retrieve all unprocessed blerbs from Redis."""
    raw_list = redis_client.lrange("transcripts:uncleaned", 0, -1)
    return [json.loads(raw) for raw in raw_list]

def merge_blerbs(blerbs):
    """Merge a batch of blerbs based on speaker and timing proximity."""
    for blerb in blerbs:
        current = merger_state["current"]

        if not current:
            # Set first blerb as current if nothing is being merged yet
            merger_state["current"] = blerb
            merger_state["last_merge_time"] = time.time()
            continue

        # Check if same speaker and within merge window
        same_speaker = blerb["player_id"] == current["player_id"]
        close_in_time = blerb["start_timestamp"] - current["start_timestamp"] <= MERGE_WINDOW_MS

        if same_speaker and close_in_time:
            # Merge current text with new blerb's text
            current["text"] += " " + blerb["text"]
            merger_state["last_merge_time"] = time.time()
        else:
            # Finalize current message and switch to new one
            redis_client.rpush("transcripts:cleaned", json.dumps(current))
            print(f"✅ Finalized merge: {current['text']}")
            merger_state["current"] = blerb
            merger_state["last_merge_time"] = time.time()

def finalize_if_idle():
    """Finalize a lingering blerb if idle too long without new merges."""
    current = merger_state["current"]
    if current:
        now = time.time()
        idle_time = now - merger_state["last_merge_time"]
        if idle_time > IDLE_FINALIZE_SECONDS:
            redis_client.rpush("transcripts:cleaned", json.dumps(current))
            print(f"🕒 Idle timeout. Finalizing lingering line: {current['text']}")
            merger_state["current"] = None
            merger_state["last_merge_time"] = 0

def clear_uncleaned_queue():
    """Clear the uncleaned Redis list after processing."""
    redis_client.delete("transcripts:uncleaned")

async def run_merger_loop():
    """Main asynchronous loop that merges incoming blerbs in real time."""
    print("🌀 Merger service started.")

    while True:
        start = time.perf_counter()

        blerbs = get_uncleaned_blerbs()
        count = len(blerbs)

        if count > 0:
            print(f"\n📥 Found {count} blerb(s) to process.")
            merge_blerbs(blerbs)
            clear_uncleaned_queue()
        else:
            finalize_if_idle()

        duration = round((time.perf_counter() - start) * 1000)
        print(f"🔁 Loop cycle completed in {duration}ms")

        await asyncio.sleep(0.5)  # Yield to event loop and wait for next cycle

if __name__ == "__main__":
    asyncio.run(run_merger_loop())
