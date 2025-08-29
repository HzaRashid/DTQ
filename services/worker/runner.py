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

        # Declare the main queue first (like app.py does)
        self.dtq._pub.declare_exchange_and_queue(
            self.dtq.exchange, f"dtq.{queue}", routing_key=queue
        )
        
        # Then setup retry policies for the queue
        from adapters.policy import RMQPolicy
        policy_manager = RMQPolicy()
        policy_manager.setup_retry_infrastructure(f"dtq.{queue}")

        self._ch.basic_consume(
            queue=f"dtq.{queue}",
            on_message_callback=self._handle_message,
            auto_ack=False,
        )

    def _publish_message(self, payload: dict, routing_key: str):
        """Wrapper function for publishing messages"""
        body = json.dumps(payload).encode()
        self.dtq._pub.publish(self.dtq.exchange, routing_key=routing_key, body=body, 
                             headers={"content-type": "application/json"})

    def _republish_with_retry(self, payload: dict, retry_count: int):
        """Republish a failed message for retry"""
        payload["retry_count"] = retry_count
        
        if retry_count == 1:
            # First failure - record the time
            payload["first_failure_time"] = time.time()
        
        logger.info(f"[worker] Republishing job {payload['id']} for retry {retry_count}")
        
        # Publish to retry queue which will delay then route back to original queue
        body = json.dumps(payload).encode()
        self.dtq._pub.publish_to_retry(self.dtq.exchange, f"dtq.{self.queue}", body)

    def _move_to_dead_letter_queue(self, payload: dict):
        """Move exhausted message to dead letter queue"""
        logger.warning(f"[worker] Moving job {payload['id']} to dead letter queue after {payload.get('max_retries', 3)} retries")
        
        body = json.dumps(payload).encode()
        self.dtq._pub.publish_to_dlq(self.dtq.exchange, f"dtq.{self.queue}", body)


    def _handle_message(self, ch, method, properties, body):
        started_at = time.time()
        payload = None
        job_id = None
        
        try:
            payload = self._get_payload(body)
            job_id = payload["id"]
            taskname = payload["task"]
            args = payload.get("args", [])
            kwargs = payload.get("kwargs", {})
            
            # Get retry metadata
            retry_count = payload.get("retry_count", 0)
            max_retries = payload.get("max_retries", 3)


            logger.info(f"[worker] Received job {job_id} ({taskname}) - retry {retry_count}/{max_retries}")

            self._set_status(job_id, status="RUNNING", started_at=str(started_at), 
                           task=taskname, retry_count=retry_count)

            fn = self.dtq.get_task(taskname)
            if fn is None:
                raise ValueError(f"Task not registered: {taskname}, {self.dtq._tasks}")
                
            result = fn(*args, **kwargs)

            finished_at = time.time()
            self._set_status(
                job_id,
                status="SUCCEEDED",
                finished_at=str(finished_at),
                duration=str(finished_at - started_at),
                result=json.dumps(result, default=str),
                retry_count=retry_count
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"[worker] Job {job_id} succeeded")

        except Exception as e:
            finished_at = time.time()
            tb = traceback.format_exc()
            
            if job_id and payload:
                retry_count = payload.get("retry_count", 0)
                max_retries = payload.get("max_retries", 3)
                
                if retry_count < max_retries:
                    # Still have retries left - republish to retry queue
                    self._republish_with_retry(payload, retry_count + 1)
                else:
                    # Exhausted all retries - move to dead letter queue
                    self._move_to_dead_letter_queue(payload)
                    
                self._set_status(
                    job_id,
                    status="FAILED",
                    finished_at=str(finished_at),
                    error=str(e),
                    traceback=tb,
                    retry_count=retry_count + 1
                )
            
            ch.basic_ack(delivery_tag=method.delivery_tag)  # Always ack to prevent poison pill
            logger.error(f"[worker] Job {job_id} failed (retry {retry_count}/{max_retries}): {e}")

    def _add_retry_metadata(self, payload: dict):
        """Add retry metadata to payload"""
        # Add retry metadata if missing (for backward compatibility)
        if "retry_count" not in payload:
            payload["retry_count"] = 0
        if "max_retries" not in payload:
            payload["max_retries"] = 3
        if "backoff_delay" not in payload:
            payload["backoff_delay"] = 60
        if "first_failure_time" not in payload:
            payload["first_failure_time"] = None

    def start(self):
        logger.info(f"[worker] Listening on dtq.{self.queue} (prefetch={self.prefetch})")
        self._ch.start_consuming()

    def _set_status(self, job_id: str, **fields):
        self.dtq.set_status(job_id, **fields)

    def _get_payload(self, body: bytes):
        payload = json.loads(body.decode())
        self._add_retry_metadata(payload)
        return payload
        
