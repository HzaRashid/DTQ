#!/usr/bin/env python3
"""
Test script for DTQ with Docker services
"""
import os
import time
from example.dtq_app import dtq
from example.tasks import add, echo
print(hasattr(echo, "_dtq_task_name"))
def test_basic_functionality():
    print("Testing DTQ basic functionality...")
    
    # Test enqueueing tasks
    print("Enqueueing tasks...")
    job1 = dtq.enqueue(add, 5, 3)
    job2 = dtq.enqueue(echo, "Hello from Docker!", delay=1)
    
    print(f"Job 1 (add): {job1}")
    print(f"Job 2 (echo): {job2}")

    print(f"Job 1 status: {dtq.get_status(job1)}")
    print(f"Job 2 status: {dtq.get_status(job2)}")
    
    # Wait a bit for processing
    print("Waiting for tasks to be processed...")
    time.sleep(3)
    
    # Check results
    print("\nChecking results...")
    
    print(f"Job 1 result: {dtq.get_result(job1)}")
    print(f"Job 2 result: {dtq.get_result(job2)}")
    
    # Test direct execution
    print("\nTesting direct execution...")
    direct_result1 = add(5, 3)
    direct_result2 = echo("Direct execution", delay=0)
    
    print(f"Direct add result: {direct_result1}")
    print(f"Direct echo result: {direct_result2}")

def test_error_handling():
    print("Testing error handling...")
    # ... error handling tests ...

def test_concurrent_processing():
    print("Testing concurrent processing...")
    # ... concurrent processing tests ...

def test_performance():
    print("Testing performance...")
    # ... performance tests ...

def test_system_health():
    print("Testing system health...")
    # ... health check tests ...

if __name__ == "__main__":
    # Set environment to use Docker services
    os.environ["DTQ_USE_MOCK"] = "false"
    os.environ["RMQ_URL"] = "amqp://guest:guest@localhost:5672/%2F"
    os.environ["REDIS_URL"] = "redis://localhost:6379/0"
    
    test_basic_functionality()
    print("\n" + "="*50 + "\n")
    
    test_error_handling()
    print("\n" + "="*50 + "\n")
    
    test_concurrent_processing()
    print("\n" + "="*50 + "\n")
    
    test_performance()
    print("\n" + "="*50 + "\n")
    
    test_system_health()
