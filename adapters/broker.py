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

    def publish(self, exchange: str, routing_key: str, body: bytes, headers=None):
        props = pika.BasicProperties(content_type='application/json', delivery_mode=2, headers=headers or {})
        self._ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=props)


class PikaConsumer:
    def __init__(self, rmq_url: str):
        self._conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        self._ch = self._conn.channel()

    def consume(self, queue: str, on_message, prefetch: int = 1):
        self._ch.basic_qos(prefetch_count=prefetch)
        self._ch.basic_consume(queue=queue, on_message_callback=lambda ch, method, props, body: on_message(body, props.headers or {}), auto_ack=False)
        self._ch.start_consuming()


# ---- Tiny adapter registry / factories ----
def build_broker(url: str) -> tuple[BrokerPublisher, BrokerConsumer]:
    # (You could parse scheme here; for now assume AMQP/RabbitMQ)
    pub = PikaPublisher(url)
    con = PikaConsumer(url)
    return pub, con