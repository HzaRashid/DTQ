import importlib
import json
import logging
import pkgutil
import time
import uuid
from threading import RLock
from typing import Callable, Iterable, TypeVar

from adapters.backend import build_backend
from adapters.broker import build_broker
from adapters.interfaces import BrokerConsumer, BrokerPublisher, ResultBackend

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


class DTQ:
    """
    Pass broker_url / backend_url, we build adapters.
    """

    def __init__(
        self,
        *,
        exchange: str = "dtq",
        default_queue: str = "default",
        broker_url: str | None = None,
        backend_url: str | None = None,
        publisher: BrokerPublisher | None = None,
        consumer: BrokerConsumer | None = None,
        result_backend: ResultBackend | None = None,
        result_ttl_seconds: int = 24 * 3600,
    ):
        self.exchange = exchange
        self.default_queue = default_queue
        # Build or use supplied adapters
        if not (publisher and consumer):
            if not broker_url:
                raise ValueError(
                    "broker_url required if publisher/consumer not provided"
                )
            publisher, consumer = build_broker(broker_url)
        if not result_backend:
            if not backend_url:
                raise ValueError("backend_url required if result_backend not provided")
            result_backend = build_backend(backend_url)

        self._pub: BrokerPublisher = publisher
        self._con: BrokerConsumer = consumer
        self._backend: ResultBackend = result_backend
        self._tasks: dict[str, Callable] = {}
        self._lock = RLock()
        self._result_ttl = result_ttl_seconds

        # Store URLs for worker access
        self._broker_url = broker_url
        self._backend_url = backend_url

    # ---- task registry ----
    def task(self, name: str | None = None):
        def deco(fn: F) -> F:
            tname = name or fn.__name__
            with self._lock:
                if tname in self._tasks and self._tasks[tname] is not fn:
                    raise ValueError(
                        f"Task already registered under a different function: {tname}"
                    )
                self._tasks[tname] = fn
            # Attach discoverable metadata on the function itself
            setattr(fn, "_dtq_task_name", tname)
            return fn

        return deco

    def get_task(self, name: str):
        print(self._tasks)
        return self._tasks.get(name)

    # ---- producer API ----
    def enqueue(self, task, *args, queue: str | None = None, **kwargs) -> str:
        queue = queue or self.default_queue
        job_id = str(uuid.uuid4())
        payload = {
            "id": job_id,
            "task": task._dtq_task_name,
            "args": list(args),
            "kwargs": kwargs,
            "retry_count": 0,  # Track attempts
            "max_retries": 3,  # Max retry limit
            "backoff_delay": 60,  # Delay between retries (seconds)
            "first_failure_time": None,  # Track when first failure occurred
        }
        body = json.dumps(payload).encode()
        self._pub.declare_exchange_and_queue(
            self.exchange, f"dtq.{queue}", routing_key=queue
        )
        self._pub.publish(
            self.exchange,
            routing_key=queue,
            body=body,
            headers={"content-type": "application/json"},
        )
        return job_id

    # ---- results API ----
    def set_status(self, job_id: str, **fields):
        self._backend.set_fields(job_id, mapping=fields, ttl_seconds=self._result_ttl)

    def get_result(self, job_id: str):
        try:
            return self._backend.get_all(job_id) or None
        except Exception as e:
            # Backend unavailable, return None to indicate no result
            logger.warning(f"Backend error in get_result for job {job_id}: {e}")
            return None

    def get_status(self, job_id: str) -> str:
        try:
            rec = self.get_result(job_id) or {}
            return rec.get("status", "PENDING")
        except Exception as e:
            # Backend unavailable, return default status
            logger.warning(f"Backend error in get_status for job {job_id}: {e}")
            return "PENDING"

    # ---- task autodiscovery (Celery-style) ----
    def autodiscover_tasks(
        self, packages: Iterable[str], *, related_name: str = "tasks"
    ):
        seen = set()

        def _import(modpath: str):
            if modpath in seen:
                return
            seen.add(modpath)
            try:
                importlib.import_module(modpath)
            except ModuleNotFoundError:
                pass

        for base in packages:
            _import(f"{base}.{related_name}")
            try:
                pkg = importlib.import_module(base)
            except ModuleNotFoundError:
                continue
            if not hasattr(pkg, "__path__"):
                continue
            for _, name, ispkg in pkgutil.walk_packages(
                pkg.__path__, prefix=pkg.__name__ + "."
            ):
                if ispkg:
                    _import(f"{name}.{related_name}")

    # ---- worker helper: start consuming ----
    def start_worker(self, queue: str, on_message):
        # Ensure queue exists
        self._pub.declare_exchange_and_queue(
            self.exchange, f"dtq.{queue}", routing_key=queue
        )
        self._con.consume(queue=f"dtq.{queue}", on_message=on_message)

    # ---- worker helper methods ----
    def _declare_queue(self, queue: str):
        """Declare a queue for worker consumption"""
        self._pub.declare_exchange_and_queue(
            self.exchange, f"dtq.{queue}", routing_key=queue
        )
        
    def retry_from_dlq(self, job_id: str, queue: str | None = None) -> dict:
        """
        Manually retry a job from the dead letter queue
        
        Args:
            job_id: The job ID to retry
            queue: Queue name (defaults to default_queue)
            
        Returns:
            dict: Status of the retry operation
        """
        queue = queue or self.default_queue
        full_queue_name = f"dtq.{queue}"
        
        try:
            # Consume the specific message from DLQ using the consumer
            messages = self._con.consume_from_dlq(
                self.exchange, full_queue_name, job_id=job_id, max_messages=1
            )
            
            if not messages:
                return {
                    "success": False,
                    "error": f"Job {job_id} not found in DLQ for queue {queue}",
                    "job_id": job_id,
                }
            
            payload, delivery_tag = messages[0]
            
            # Reset retry metadata for manual retry
            payload["retry_count"] = 0
            payload["max_retries"] = 3  # Reset to default
            payload["manual_retry"] = True
            payload["manual_retry_time"] = time.time()
            payload["original_failure_time"] = payload.get("first_failure_time")
            
            # Remove failure metadata
            payload.pop("first_failure_time", None)
            payload.pop("error", None)
            payload.pop("traceback", None)
            
            # Republish to original queue
            body = json.dumps(payload).encode()
            self._pub.publish(
                self.exchange,
                routing_key=queue,
                body=body,
                headers={"content-type": "application/json"},
            )
            
            logger.info(f"[DTQ] Successfully retried job {job_id} from DLQ")
            
            return {
                "success": True,
                "message": f"Job {job_id} successfully retried from DLQ",
                "job_id": job_id,
                "queue": queue,
                "task": payload.get("task"),
                "retry_count_reset": True,
            }
            
        except Exception as e:
            logger.error(f"[DTQ] Failed to retry job {job_id} from DLQ: {e}")
            return {"success": False, "error": str(e), "job_id": job_id}
    
    def list_dlq_messages(self, queue: str | None = None, limit: int = 20) -> dict:
        """
        List messages in the dead letter queue for inspection
        
        Args:
            queue: Queue name (defaults to default_queue)
            limit: Maximum number of messages to return
            
        Returns:
            dict: DLQ status and messages
        """
        queue = queue or self.default_queue
        full_queue_name = f"dtq.{queue}"
        
        try:
            message_count = self._con.get_dlq_message_count(
                self.exchange, full_queue_name
            )
            messages = self._con.peek_dlq_messages(
                self.exchange, full_queue_name, limit=limit
            )
            
            return {
                "success": True,
                "queue": queue,
                "dlq_name": f"{full_queue_name}.dlq",
                "message_count": message_count,
                "messages": messages,
                "truncated": len(messages) >= limit and message_count > limit,
            }
            
        except Exception as e:
            logger.error(f"[DTQ] Failed to list DLQ messages: {e}")
            return {"success": False, "error": str(e), "queue": queue}

    def purge_dlq(self, queue: str | None = None, older_than_days: int = 30) -> dict:
        """
        Purge old messages from dead letter queue

        Args:
            queue: Queue name (defaults to default_queue)
            older_than_days: Remove messages older than this many days

        Returns:
            dict: Purging operation result
        """
        queue = queue or self.default_queue
        full_queue_name = f"dtq.{queue}"

        try:
            # This would require implementing message filtering by timestamp
            # For now, return a placeholder implementation
            logger.warning(f"[DTQ] DLQ purging not fully implemented yet")

            return {
                "success": False,
                "message": "DLQ purging not yet implemented",
                "queue": queue,
            }

        except Exception as e:
            logger.error(f"[DTQ] Failed to purge DLQ: {e}")
            return {"success": False, "error": str(e), "queue": queue}

    @property
    def _conn(self):
        """Get the broker connection for worker access"""
        return self._pub._conn

    @property
    def _redis(self):
        """Get the Redis client for worker access"""
        return self._backend._r
