import os
from dtq.app import DTQ

dtq = DTQ(
    exchange=os.getenv("DTQ_EXCHANGE", "dtq"),
    default_queue=os.getenv("DTQ_DEFAULT_QUEUE", "default"),
    broker_url=os.getenv("RMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
    backend_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
)

# Autodiscover tasks right here
PACKS = [p.strip() for p in os.getenv("DTQ_PACKAGES", "infra").split(",") if p.strip()]
dtq.autodiscover_tasks(PACKS, related_name=os.getenv("DTQ_RELATED_NAME","tasks"))
