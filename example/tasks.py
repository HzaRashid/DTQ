from .dtq_app import dtq

@dtq.task()
def add(a, b):
    return a + b

@dtq.task(name="slow.echo")
def echo(msg, delay=1):
    import time
    time.sleep(delay)
    return {"echo": msg, "delay": delay}

@dtq.task("failing_task")
def failing_task():
    """A task that always fails for demonstration"""
    raise Exception("This task always fails!")

@dtq.task("retry_task") 
def retry_task(attempt_number: int):
    """A task that fails on first two attempts, succeeds on third"""
    import os
    if attempt_number < os.getenv("DTQ_MAX_RETRIES", 3):
        raise Exception(f"Task failed on attempt {attempt_number}")
    return f"Task succeeded on attempt {attempt_number}!"