import json, uuid, importlib, pkgutil, os
from typing import Iterable, Optional, Any
from adapters.interfaces import BrokerPublisher, BrokerConsumer, ResultBackend
from adapters.broker import build_broker
from adapters.backend import build_backend
from typing import Callable, TypeVar
from functools import wraps
from threading import RLock

F = TypeVar('F', bound=Callable) 

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
        result_ttl_seconds: int = 24*3600,
    ):
        self.exchange = exchange
        self.default_queue = default_queue
        # Build or use supplied adapters
        if not (publisher and consumer):
            if not broker_url:
                raise ValueError("broker_url required if publisher/consumer not provided")
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
                    raise ValueError(f"Task already registered under a different function: {tname}")
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
        payload = {"id": job_id, "task": task._dtq_task_name,
                   "args": list(args), "kwargs": kwargs}
        body = json.dumps(payload).encode()
        self._pub.declare_exchange_and_queue(self.exchange, f"dtq.{queue}", routing_key=queue)
        self._pub.publish(self.exchange, routing_key=queue, body=body, headers={"content-type":"application/json"})
        return job_id

    # ---- results API ----
    def set_status(self, job_id: str, **fields):
        self._backend.set_fields(job_id, mapping=fields, ttl_seconds=self._result_ttl)
    def get_result(self, job_id: str):
        return self._backend.get_all(job_id) or None
    def get_status(self, job_id: str) -> str:
        rec = self.get_result(job_id) or {}
        return rec.get("status", "PENDING")

    # ---- task autodiscovery (Celery-style) ----
    def autodiscover_tasks(self, packages: Iterable[str], *, related_name: str = "tasks"):
        seen = set()
        def _import(modpath: str):
            if modpath in seen: return
            seen.add(modpath)
            try: importlib.import_module(modpath)
            except ModuleNotFoundError: pass
        for base in packages:
            _import(f"{base}.{related_name}")
            try: pkg = importlib.import_module(base)
            except ModuleNotFoundError: continue
            if not hasattr(pkg, "__path__"): continue
            for _, name, ispkg in pkgutil.walk_packages(pkg.__path__, prefix=pkg.__name__ + "."):
                if ispkg: _import(f"{name}.{related_name}")

    # ---- worker helper: start consuming ----
    def start_worker(self, queue: str, on_message):
        # Ensure queue exists
        self._pub.declare_exchange_and_queue(self.exchange, f"dtq.{queue}", routing_key=queue)
        self._con.consume(queue=f"dtq.{queue}", on_message=on_message)
    
    # ---- worker helper methods ----
    def _declare_queue(self, queue: str):
        """Declare a queue for worker consumption"""
        self._pub.declare_exchange_and_queue(self.exchange, f"dtq.{queue}", routing_key=queue)
    
    @property
    def _conn(self):
        """Get the broker connection for worker access"""
        return self._pub._conn
    
    @property
    def _redis(self):
        """Get the Redis client for worker access"""
        return self._backend._r
