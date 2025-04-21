import time
import json
import redis

# Constants
NUM_BLERBS = 1000
MERGE_WINDOW_MS = 15000
TEST_KEY_PREFIX = "transcripts:testing"

# Setup test Redis connection
redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# Keep test merger state local
merger_state = {
    "current": None,
    "base_timestamp": None
}

def generate_test_blerbs():
    """Create a list of blerbs from two alternating speakers over 20s."""
    base = int(time.time() * 1000)
    return [
        {
            "player_id": f"player_{i % 2 + 1}",
            "start_timestamp": base + (i % 20) * 1000,
            "text": f"Test message {i}"
        }
        for i in range(NUM_BLERBS)
    ]

def update_cleaned_line(entry, key_timestamp):
    """Store line using stable test key."""
    redis_client.set(f"{TEST_KEY_PREFIX}:{key_timestamp}", json.dumps(entry))

def clear_test_data():
    """Delete all keys used during test."""
    for key in redis_client.keys(f"{TEST_KEY_PREFIX}:*"):
        redis_client.delete(key)

def merge_blerbs(blerbs):
    """Run the actual merging logic on the test data."""
    for blerb in blerbs:
        current = merger_state["current"]
        if not current:
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])
            continue

        same_speaker = blerb["player_id"] == current["player_id"]
        close_in_time = blerb["start_timestamp"] - current["start_timestamp"] <= MERGE_WINDOW_MS

        if same_speaker and close_in_time:
            current["text"] += " " + blerb["text"]
            update_cleaned_line(current, merger_state["base_timestamp"])
        else:
            update_cleaned_line(current, merger_state["base_timestamp"])
            merger_state["current"] = blerb
            merger_state["base_timestamp"] = blerb["start_timestamp"]
            update_cleaned_line(blerb, merger_state["base_timestamp"])

# Run the benchmark
if __name__ == "__main__":
    blerbs = generate_test_blerbs()

    start = time.perf_counter()
    merge_blerbs(blerbs)
    end = time.perf_counter()

    print("\n--- Benchmark Results ---")
    print(f"Processed: {NUM_BLERBS} blerbs")
    print(f"Total time: {(end - start)*1000:.2f} ms")
    print(f"Avg time per blerb: {((end - start)*1000) / NUM_BLERBS:.4f} ms")

    clear_test_data()
