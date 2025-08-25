from .dtq_app import dtq

@dtq.task()
def add(a, b):
    return a + b

@dtq.task(name="slow.echo")
def echo(msg, delay=1):
    import time
    time.sleep(delay)
    return {"echo": msg, "delay": delay}
