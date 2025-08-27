# adapters/redis_backend.py
import redis
from .interfaces import ResultBackend


class RedisBackend:
    def __init__(self, redis_url: str, decode_responses=True):
        self._r = redis.from_url(redis_url, decode_responses=decode_responses)

    def set_fields(self, job_id, mapping, ttl_seconds=None):
        key = f"dtq:result:{job_id}"
        self._r.hset(key, mapping=mapping)
        if ttl_seconds:
            self._r.expire(key, ttl_seconds)

    def get_all(self, job_id):
        data = self._r.hgetall(f"dtq:result:{job_id}")
        return data or None


def build_backend(url: str) -> ResultBackend:
    # (You could branch on scheme; for now assume redis://)
    return RedisBackend(url)
