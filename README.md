# DTQ - Distributed Task Queue

This repo is for educational purposes to explore RabbitMQ, Redis, and distributed task queues. Not production-ready.


## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.8+

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/HzaRashid/DTQ.git
   cd DTQ
   ```

2. **Start the services**
   ```bash
   docker-compose up -d
   ```
   This starts RabbitMQ and Redis services.

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the test script**
   ```bash
   python test_docker.py
   ```

### Basic Usage

```python
from dtq.app import DTQ

# Initialize DTQ
dtq = DTQ(
    broker_url="amqp://guest:guest@localhost:5672/%2F",
    backend_url="redis://localhost:6379/0"
)

# Define a task
@dtq.task()
def add_numbers(a, b):
    return a + b

# Enqueue a job
job_id = dtq.enqueue(add_numbers, 5, 3)

# Check status
status = dtq.get_status(job_id)

# Get result
result = dtq.get_result(job_id)
```
