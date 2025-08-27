# adapters/rabbitmq.py
import pika
from .interfaces import BrokerPublisher, BrokerConsumer


class PikaPublisher:
    def __init__(self, rmq_url: str):
        self._conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        self._ch = self._conn.channel()

    def declare_exchange_and_queue(self, exchange: str, queue: str, routing_key: str):
        self._ch.exchange_declare(exchange=exchange, exchange_type="direct", durable=True)
        self._ch.queue_declare(queue=queue, durable=True)
        self._ch.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

    def declare_retry_infrastructure(self, exchange: str, queue: str):
        """Declare retry and dead letter queues for a given queue"""
        retry_queue = f"{queue}.retry"
        dlq = f"{queue}.dlq"
        dlx = f"{exchange}.dlx"
        
        # Dead letter exchange for failed messages
        self._ch.exchange_declare(dlx, "direct", durable=True)
        
        # Bind original queue to dead letter exchange for retry->original routing
        self._ch.queue_bind(queue=queue, exchange=dlx, routing_key=queue)
        
        # Retry queue with message TTL (1 minute delay)
        self._ch.queue_declare(retry_queue, durable=True, arguments={
            'x-message-ttl': 60000,  # 1 minute TTL before re-delivery
            'x-dead-letter-exchange': dlx,  # Route to DLX after TTL
            'x-dead-letter-routing-key': queue  # Back to original queue
        })
        
        # Dead letter queue with longer TTL (7 days for manual inspection)
        self._ch.queue_declare(dlq, durable=True, arguments={
            'x-message-ttl': 604800000  # 7 days TTL
        })
        
        # Bind DLQ to DLX for exhausted retries
        self._ch.queue_bind(queue=dlq, exchange=dlx, routing_key=f"{queue}.exhausted")

    def publish(self, exchange: str, routing_key: str, body: bytes, headers=None):
        props = pika.BasicProperties(content_type='application/json', delivery_mode=2, headers=headers or {})
        self._ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=props)

    def publish_to_retry(self, exchange: str, queue: str, body: bytes, headers=None):
        """Publish message to retry queue"""
        retry_queue = f"{queue}.retry"
        dlx = f"{exchange}.dlx"
        
        # Ensure retry infrastructure exists
        self.declare_retry_infrastructure(exchange, queue)
        
        props = pika.BasicProperties(content_type='application/json', delivery_mode=2, headers=headers or {})
        self._ch.basic_publish(exchange=dlx, routing_key=retry_queue, body=body, properties=props)

    def publish_to_dlq(self, exchange: str, queue: str, body: bytes, headers=None):
        """Publish exhausted message to dead letter queue"""
        dlq = f"{queue}.dlq"
        dlx = f"{exchange}.dlx"
        
        # Ensure retry infrastructure exists
        self.declare_retry_infrastructure(exchange, queue)
        
        props = pika.BasicProperties(content_type='application/json', delivery_mode=2, headers=headers or {})
        self._ch.basic_publish(exchange=dlx, routing_key=f"{queue}.exhausted", body=body, properties=props)


class PikaConsumer:
    def __init__(self, rmq_url: str):
        self._conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        self._ch = self._conn.channel()

    def consume(self, queue: str, on_message, prefetch: int = 1):
        self._ch.basic_qos(prefetch_count=prefetch)
        self._ch.basic_consume(queue=queue, on_message_callback=lambda ch, method, props, body: on_message(body, props.headers or {}), auto_ack=False)
        self._ch.start_consuming()

    def consume_from_dlq(self, exchange: str, queue: str, job_id: str = None, max_messages: int = 1) -> list:
        """
        Consume messages from dead letter queue
        Returns list of (payload, delivery_tag) tuples
        """
        import json
        import logging
        logger = logging.getLogger(__name__)
        
        dlq = f"{queue}.dlq"
        
        # Ensure DLQ exists (publisher declares it, but consumer needs to check)
        try:
            self._ch.queue_declare(dlq, passive=True)
        except Exception:
            logger.warning(f"DLQ {dlq} does not exist yet")
            return []
        
        messages = []
        
        def dlq_callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode())
                
                # If specific job_id requested, only return matching messages
                if job_id and payload.get("id") != job_id:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    return
                    
                messages.append((payload, method.delivery_tag))
                
                # Stop consuming after we get enough messages
                if len(messages) >= max_messages:
                    ch.stop_consuming()
                    
            except Exception as e:
                logger.error(f"Error processing DLQ message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Create a temporary consumer
        temp_ch = self._conn.channel()
        temp_ch.basic_qos(prefetch_count=1)
        temp_ch.basic_consume(queue=dlq, on_message_callback=dlq_callback, auto_ack=False)
        
        # Consume until we get the messages we need or timeout
        try:
            temp_ch.start_consuming()
        except Exception:
            pass  # Expected when we call stop_consuming()
        finally:
            try:
                temp_ch.close()
            except Exception:
                pass
        
        return messages

    def get_dlq_message_count(self, exchange: str, queue: str) -> int:
        """Get the number of messages in the dead letter queue"""
        dlq = f"{queue}.dlq"
        
        try:
            queue_info = self._ch.queue_declare(queue=dlq, passive=True)
            return queue_info.method.message_count
        except Exception:
            return 0

    def peek_dlq_messages(self, exchange: str, queue: str, limit: int = 10) -> list:
        """
        Peek at messages in DLQ without consuming them
        Returns list of payloads for inspection
        """
        import json
        import logging
        logger = logging.getLogger(__name__)
        
        dlq = f"{queue}.dlq"
        
        try:
            self._ch.queue_declare(dlq, passive=True)
        except Exception:
            return []
        
        messages = []
        
        def peek_callback(ch, method, properties, body):
            try:
                payload = json.loads(body.decode())
                messages.append({
                    'job_id': payload.get('id'),
                    'task': payload.get('task'),
                    'retry_count': payload.get('retry_count', 0),
                    'error': payload.get('error', 'Unknown'),
                    'failed_at': payload.get('first_failure_time')
                })
                
                if len(messages) >= limit:
                    ch.stop_consuming()
                    
            except Exception as e:
                logger.error(f"Error peeking DLQ message: {e}")
        
        # Create a temporary consumer that immediately nacks to put messages back
        temp_ch = self._conn.channel()
        temp_ch.basic_qos(prefetch_count=1)
        
        def peek_and_nack(ch, method, properties, body):
            peek_callback(ch, method, properties, body)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        temp_ch.basic_consume(queue=dlq, on_message_callback=peek_and_nack, auto_ack=False)
        
        try:
            temp_ch.start_consuming()
        except Exception:
            pass
        finally:
            try:
                temp_ch.close()
            except Exception:
                pass
        
        return messages


# ---- Tiny adapter registry / factories ----
def build_broker(url: str) -> tuple[BrokerPublisher, BrokerConsumer]:
    # (You could parse scheme here; for now assume AMQP/RabbitMQ)
    pub = PikaPublisher(url)
    con = PikaConsumer(url)
    return pub, con