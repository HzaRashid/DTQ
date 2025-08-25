from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
import os
from .app import DTQ

app = FastAPI(title="DTQ Monitor", version="1.0.0")

# Initialize DTQ
dtq = DTQ(
    exchange=os.getenv("DTQ_EXCHANGE", "dtq"),
    default_queue=os.getenv("DTQ_DEFAULT_QUEUE", "default"),
    broker_url=os.getenv("RMQ_URL", "amqp://guest:guest@localhost:5672/%2F"),
    backend_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
)

@app.get("/", response_class=HTMLResponse)
async def root():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>DTQ Monitor</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .status { padding: 10px; margin: 10px 0; border-radius: 5px; }
            .healthy { background-color: #d4edda; color: #155724; }
            .unhealthy { background-color: #f8d7da; color: #721c24; }
            .info { background-color: #d1ecf1; color: #0c5460; }
        </style>
    </head>
    <body>
        <h1>DTQ Task Queue Monitor</h1>
        <div class="status info">
            <h3>System Status</h3>
            <p>Exchange: <strong>dtq</strong></p>
            <p>Default Queue: <strong>default</strong></p>
        </div>
        <div class="status healthy">
            <h3>Quick Actions</h3>
            <p><a href="/health">Health Check</a></p>
            <p><a href="/tasks">View Tasks</a></p>
            <p><a href="/stats">Queue Statistics</a></p>
        </div>
    </body>
    </html>
    """

@app.get("/health")
async def health_check():
    try:
        # Try to connect to RabbitMQ
        import pika
        connection = pika.BlockingConnection(
            pika.URLParameters(os.getenv("RMQ_URL", "amqp://guest:guest@localhost:5672/%2F"))
        )
        connection.close()
        
        # Try to connect to Redis
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
        r.ping()
        
        return {"status": "healthy", "message": "All services are running"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/tasks")
async def list_tasks():
    return {
        "registered_tasks": list(dtq._tasks.keys()),
        "task_count": len(dtq._tasks)
    }

@app.get("/stats")
async def queue_stats():
    try:
        import pika
        connection = pika.BlockingConnection(
            pika.URLParameters(os.getenv("RMQ_URL", "amqp://guest:guest@localhost:5672/%2F"))
        )
        channel = connection.channel()
        
        # Get queue info
        queue_info = channel.queue_declare(queue="dtq.default", passive=True)
        
        connection.close()
        
        return {
            "queue_name": "dtq.default",
            "message_count": queue_info.method.message_count,
            "consumer_count": queue_info.method.consumer_count
        }
    except Exception as e:
        return {"error": str(e)}

