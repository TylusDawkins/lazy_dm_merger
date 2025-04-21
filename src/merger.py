import asyncio
import time
import json
import redis
from cleaner_tagger_wrapper import tag_line, clean_text

# Redis connection
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

MERGE_WINDOW_MS = 15000  # 15 seconds of silence to finalize a thread

# Store current merging state
merger_state = {
    "current": None,
    "last_merge_time": 0,
    "base_timestamp": None
}


def get_uncleaned_blerbs():
    """Retrieve all unprocessed blerbs from Redis."""
    raw_list = redis_client.lrange("transcripts:uncleaned", 0, -1)
    return [json.loads(raw) for raw in raw_list]


def update_cleaned_line(entry, key_timestamp):
    """Write or update the cleaned transcript entry using a stable key."""
    print(f"✅ Updating cleaned line for timestamp: {key_timestamp} with entry: {entry}")
    redis_client.set(f"transcripts:cleaned:{key_timestamp}", json.dumps(entry))
    merger_state["last_merge_time"] = time.time()


def finalize_current_merge():
    """Finalize the current active merge thread with cleaning + tagging."""
    current = merger_state["current"]
    if not current:
        return

    print("🔚 FINALIZING MERGE THREAD (manual or timeout)...")
    try:
        tag = tag_line(current["text"])
        cleaned = clean_text(current["text"])
        current["tag"] = tag
        current["cleaned_text"] = cleaned
        redis_client.set(f"transcripts:cleaned:{merger_state['base_timestamp']}", json.dumps(current))
        print(f"🏷️ Final Tag: {tag}")
        print(f"🧽 Final Cleaned: {cleaned}")
    except Exception as e:
        print(f"❌ Error tagging/cleaning final merge: {e}")

    # Reset merger state
    merger_state["current"] = None
    merger_state["base_timestamp"] = None
    merger_state["last_merge_time"] = 0


def merge_blerbs(blerbs):
    for blerb in blerbs:
        current = merger_state["current"]

        if not current:
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])
            continue

        now = time.time()
        time_since_last = (now - merger_state["last_merge_time"]) * 1000  # in ms
        same_speaker = blerb["player_id"] == current["player_id"]
        close_in_time = time_since_last <= MERGE_WINDOW_MS

        if same_speaker and close_in_time:
            print(f"🔄 MERGING: Adding blerb from {blerb['start_timestamp']} into {merger_state['base_timestamp']}")
            current["text"] += " " + blerb["text"]
            update_cleaned_line(current, merger_state["base_timestamp"])
        else:
            finalize_current_merge()
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])


def clear_uncleaned_queue():
    redis_client.delete("transcripts:uncleaned")


async def run_merger_loop():
    print("🌀 Merger service started.")

    while True:
        start = time.perf_counter()
        blerbs = get_uncleaned_blerbs()
        count = len(blerbs)
        time_since_last_merge = (time.time() - merger_state["last_merge_time"]) * 1000

        if count > 0:
            merge_blerbs(blerbs)
            clear_uncleaned_queue()
        elif time_since_last_merge > MERGE_WINDOW_MS:
            finalize_current_merge()

        duration = round((time.perf_counter() - start) * 1000)
        await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(run_merger_loop())
