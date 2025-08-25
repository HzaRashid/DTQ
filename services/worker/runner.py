import json
import time
import traceback
import logging

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self, dtq, queue="default", prefetch=1, result_ttl_seconds=86400):
        """
        A single DTQ worker that consumes jobs from RabbitMQ and writes results to Redis.

        :param dtq: The DTQ instance (tasks + broker/backend clients)
        :param queue: Queue name to consume from (without "dtq." prefix)
        :param prefetch: Max unacked messages per worker
        :param result_ttl_seconds: TTL for job results in Redis
        """
        self.dtq = dtq
        self.queue = queue
        self.prefetch = prefetch
        self.result_ttl = result_ttl_seconds

        # Use the same connection as dtq (or create a new one if you prefer isolation)
        self._conn = dtq._conn
        self._ch = self._conn.channel()
        self._ch.basic_qos(prefetch_count=prefetch)

        # Ensure the queue exists/bound
        dtq._declare_queue(queue)

        self._ch.basic_consume(
            queue=f"dtq.{queue}",
            on_message_callback=self._handle_message,
            auto_ack=False,
        )

    def _result_key(self, job_id: str) -> str:
        return f"dtq:result:{job_id}"

    def _set_status(self, job_id: str, **fields):
        key = self._result_key(job_id)
        self.dtq._redis.hset(key, mapping=fields)
        self.dtq._redis.expire(key, self.result_ttl)

    def _handle_message(self, ch, method, properties, body):
        started_at = time.time()
        payload = None
        job_id = None
        try:
            payload = json.loads(body.decode())
            job_id   = payload["id"]
            taskname = payload["task"]
            args     = payload.get("args", [])
            kwargs   = payload.get("kwargs", {})

            logger.info(f"[worker] Received job {job_id} ({taskname})")

            self._set_status(job_id, status="RUNNING", started_at=str(started_at), task=taskname)

            fn = self.dtq.get_task(taskname)
            if fn is None:
                # print()
                raise ValueError(f"Task not registered: {taskname}, {self.dtq._tasks}")
                
            result = fn(*args, **kwargs)

            finished_at = time.time()
            self._set_status(
                job_id,
                status="SUCCEEDED",
                finished_at=str(finished_at),
                duration=str(finished_at - started_at),
                result=json.dumps(result, default=str),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"[worker] Job {job_id} succeeded")

        except Exception as e:
            finished_at = time.time()
            tb = traceback.format_exc()
            if job_id:
                self._set_status(
                    job_id,
                    status="FAILED",
                    finished_at=str(finished_at),
                    error=str(e),
                    traceback=tb,
                )
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Ack to avoid poison loop
            logger.error(f"[worker] Job {job_id} failed: {e}\n{tb}")

    def start(self):
        logger.info(f"[worker] Listening on dtq.{self.queue} (prefetch={self.prefetch})")
        self._ch.start_consuming()
