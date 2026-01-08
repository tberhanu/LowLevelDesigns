import threading
import time
from collections import defaultdict, deque


# ============================================================
# SLIDING WINDOW RATE LIMITER
# ============================================================

class SlidingWindowRateLimiter:
    """
    Sliding-window rate limiter using deques and per-key locks.
    
    Tracks request timestamps in a sliding window. Removes old timestamps
    outside the window and allows requests if count < limit.
    """

    def __init__(self, default_max_requests: int, default_window_seconds: float):
        """
        default_max_requests: requests allowed per window
        default_window_seconds: time window in seconds
        """
        self.default_max = int(default_max_requests)
        self.default_window = float(default_window_seconds)

        # Per-key storage: deque of request timestamps
        self.store = defaultdict(deque)
        
        # Per-key locks for thread safety
        self.locks = defaultdict(threading.Lock)

        # Per-key limits (optional override of defaults)
        self.limits = {}

    def set_limit(self, key: str, max_requests: int, window_seconds: float):
        """Override default limits for a specific key."""
        self.limits[key] = (int(max_requests), float(window_seconds))

    def _get_limit(self, key: str):
        """Get limit for key, falling back to defaults."""
        return self.limits.get(key, (self.default_max, self.default_window))

    def allow_request(self, key: str) -> bool:
        """
        Check if request is allowed for the given key.
        Returns True if allowed, False if rate limit exceeded.
        """
        now = time.monotonic()
        max_requests, window_seconds = self._get_limit(key)

        lock = self.locks[key]
        with lock:
            q = self.store[key]
            boundary = now - window_seconds

            # Remove expired timestamps outside the window
            while q and q[0] <= boundary:
                q.popleft()

            # Allow if under limit
            if len(q) < max_requests:
                q.append(now)
                return True
            return False

    def get_usage(self, key: str) -> tuple[float, float]:
        """
        Returns (current_count, time_until_reset).
        current_count: number of requests in current window
        time_until_reset: seconds until oldest request expires
        """
        now = time.monotonic()
        max_requests, window_seconds = self._get_limit(key)

        lock = self.locks[key]
        with lock:
            q = self.store[key]
            boundary = now - window_seconds

            # Cleanup expired timestamps
            while q and q[0] <= boundary:
                q.popleft()

            count = len(q)

            if not q:
                return (0, 0.0)

            oldest = q[0]
            ttl = max(0.0, (oldest + window_seconds) - now)
            return (count, ttl)


# ============================================================
# TEST HARNESS
# ============================================================

def worker_thread(limiter: SlidingWindowRateLimiter, key: str, attempts: int, pause: float, results: list):
    """Worker thread that makes requests to the limiter."""
    for i in range(attempts):
        allowed = limiter.allow_request(key)
        usage = limiter.get_usage(key)
        results.append((threading.get_ident(), i, allowed, usage))
        time.sleep(pause)


def main():
    limiter = SlidingWindowRateLimiter(default_max_requests=5, default_window_seconds=2.0)
    base_key = "user:test"

    results = []
    threads = []

    # Create 3 threads with different keys
    for i in range(3):
        key = base_key + f":sliding:{i}"
        print(f"Thread {i}: {key}")
        t = threading.Thread(
            target=worker_thread,
            args=(limiter, key, 6, (0.25 / (i + 3)), results)
        )
        t.start()
        threads.append(t)

    # Wait for completion
    for t in threads:
        t.join()

    # Print results
    print("\n=== SLIDING WINDOW RESULTS ===")
    for tid, attempt, allowed, usage in results:
        print(
            f"Thread {tid} | attempt {attempt} | "
            f"{'ALLOWED' if allowed else 'REJECTED'} | usage={usage}"
        )


if __name__ == "__main__":
    main()
