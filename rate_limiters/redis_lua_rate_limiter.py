import time
import uuid
import redis


class SlidingWindowRateLimiter:
    """
    Sliding-window rate limiter using Redis sorted sets + Lua script.
    """

    LUA_SCRIPT = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window_ms = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])

    -- Remove entries outside the window
    redis.call("ZREMRANGEBYSCORE", key, 0, now - window_ms)

    -- Current count in the window
    local count = redis.call("ZCARD", key)

    if count >= limit then
        return {0, count}  -- not allowed
    end

    -- Add current request with unique member
    local member = ARGV[4]
    redis.call("ZADD", key, now, member)

    -- Optional: set TTL to avoid key buildup
    redis.call("PEXPIRE", key, window_ms)

    count = count + 1
    return {1, count}  -- allowed
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        prefix: str = "rate:",
        max_requests: int = 10,
        window_seconds: int = 60,
    ):
        self.redis = redis_client
        self.prefix = prefix
        self.max_requests = max_requests
        self.window_ms = window_seconds * 1000
        # Registers the Lua script with Redis. 
        # This compiles and caches the script on the Redis server, returning a script object.
        self._lua = self.redis.register_script(self.LUA_SCRIPT)

    def _key(self, identifier: str) -> str:
        return f"{self.prefix}{identifier}"

    def allow_request(self, identifier: str) -> tuple[bool, int]:
        """
        identifier — A unique identifier (e.g., user ID, IP address) to rate limit
        Returns (allowed: bool, current_count: int)
        """
        now_ms = int(time.time() * 1000)
        member = f"{now_ms}-{uuid.uuid4()}"
        # print("member:", member)
        """
        KEYS[1] — Redis key to track requests for an identifier
        ARGV[1] — Current timestamp in milliseconds (now)
        ARGV[2] — Time window in milliseconds (window_ms)
        ARGV[3] — Request limit (limit)
        ARGV[4] — Unique member identifier for this request
        ***Atomicity in action via Lua script***
        """
        result = self._lua(
            keys=[self._key(identifier)],
            args=[now_ms, self.window_ms, self.max_requests, member],
        )
        allowed = bool(result[0])
        count = int(result[1])
        return allowed, count


def main():
    # Configure Redis client
    r = redis.Redis(host="localhost", port=6379, db=0)

    limiter = SlidingWindowRateLimiter(
        redis_client=r,
        prefix="rate:user:",  # key pattern
        max_requests=5,       # e.g. 5 requests
        window_seconds=10,    # per 10 seconds
    )

    # user_id = "tess"

    print("Sending 20 requests quickly with limit=5/10s...")
    for i in range(20):
        if i % 3 == 0:
            user_id = "tess3"
        else:
            user_id = "tess"
        
        allowed, count = limiter.allow_request(user_id)
        print(f"Request {i+1}: allowed={allowed}, count_in_window={count}, user_id={user_id}")
        time.sleep(0.5)  # just for demo pacing


if __name__ == "__main__":
    main()
