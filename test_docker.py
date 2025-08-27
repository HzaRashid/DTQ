#!/usr/bin/env python3
"""
Test script for DTQ with Docker services
"""

import json
import os
import time

from example.dtq_app import dtq
from example.tasks import add, echo


def test_basic_functionality():
    print("Testing DTQ basic functionality...")

    # Test enqueueing tasks
    jobs = [
        (add, {"a": 5, "b": 3}),
        (echo, {"msg": "Hello from Docker!", "delay": 1}),
        (echo, {"msg": "Hello from Docker!", "delay": 1}),
        (echo, {"msg": "Hello from Docker!", "delay": 1}),
        (echo, {"msg": "Hello from Docker!", "delay": 1}),
    ]
    jobs_ids = []

    print("Enqueueing tasks...")  # Non-blocking, prints immediately
    for i, job in enumerate(jobs):
        job_id = dtq.enqueue(job[0], **job[1])
        print(
            f"taskname: {job[0]._dtq_task_name}, id: {job_id}, status: {dtq.get_status(job_id)}"
        )
        jobs_ids.append(job_id)

    # Wait for processing with polling
    print("Waiting for tasks to be processed...")
    wait_for_completion(jobs_ids, timeout=len(jobs) + 1)

    # Check results
    print("\nChecking results...")
    for i, job_id in enumerate(jobs_ids):
        result_str = dtq.get_result(job_id)
        result = json.loads(result_str) if isinstance(result_str, str) else result_str
        status = dtq.get_status(job_id)
        print(f"Job {job_id} status: {status}, result: {result}")

        # Verify expected results
        if i == 0:  # add task
            try:
                result_value = int(result["result"])
                assert result_value == 8, f"Expected 8, got {result_value}"
            except (ValueError, TypeError, KeyError) as e:
                print(f"Error parsing result for add task: {e}")
                assert False, f"Could not parse result: {result}"
        else:  # echo tasks
            assert "Hello from Docker!" in str(result["result"]), (
                f"Expected echo message in {result['result']}"
            )

    # Test direct execution (blocking)
    print("\nTesting direct execution...")
    for job in jobs:
        direct_result = job[0](**job[1])
        print(f"Direct {job[0]._dtq_task_name} result: {direct_result}")


def test_error_handling():
    print("Testing error handling...")

    try:
        # Test with invalid task arguments
        print("Testing invalid arguments...")
        job_id = dtq.enqueue(add, a=5)  # Missing 'b' parameter
        wait_for_completion([job_id], timeout=5)

        result = dtq.get_result(job_id)
        status = dtq.get_status(job_id)
        print(f"Invalid args job {job_id} - Status: {status}, Result: {result}")

        # Should handle error gracefully
        assert status in ["failed", "error"], f"Expected error status, got {status}"

    except Exception as e:
        print(f"Error handling test caught exception: {e}")

    try:
        # Test getting result for non-existent job
        print("Testing non-existent job...")
        fake_result = dtq.get_result("non-existent-job-id")
        print(f"Non-existent job result: {fake_result}")

    except Exception as e:
        print(f"Non-existent job test caught exception: {e}")


def test_concurrent_processing():
    print("Testing concurrent processing...")

    # Enqueue multiple tasks quickly
    num_tasks = 10
    job_ids = []

    print(f"Enqueueing {num_tasks} concurrent tasks...")
    start_time = time.time()

    for i in range(num_tasks):
        job_id = dtq.enqueue(echo, msg=f"Concurrent task {i}", delay=0.5)
        job_ids.append(job_id)

    enqueue_time = time.time() - start_time
    print(f"Enqueued {num_tasks} tasks in {enqueue_time:.2f} seconds")

    # Wait for all to complete
    print("Waiting for concurrent tasks to complete...")
    start_wait = time.time()
    wait_for_completion(job_ids, timeout=15)
    total_time = time.time() - start_wait

    print(f"All {num_tasks} tasks completed in {total_time:.2f} seconds")

    # Verify all completed successfully
    completed_count = 0
    for job_id in job_ids:
        status = dtq.get_status(job_id)
        # print(status)
        if status == "SUCCEEDED":
            completed_count += 1

    print(f"Successfully completed: {completed_count}/{num_tasks} tasks")
    assert completed_count == num_tasks, (
        f"Expected {num_tasks} completed, got {completed_count}"
    )


def test_performance():
    print("Testing performance...")

    # Test enqueue performance
    num_tasks = 50
    print(f"Testing enqueue performance with {num_tasks} tasks...")

    start_time = time.time()
    job_ids = []

    for i in range(num_tasks):
        job_id = dtq.enqueue(add, a=i, b=i + 1)
        job_ids.append(job_id)

    enqueue_time = time.time() - start_time
    enqueue_rate = num_tasks / enqueue_time

    print(f"Enqueue rate: {enqueue_rate:.1f} tasks/second")

    # Test processing performance
    print("Measuring processing performance...")
    start_process = time.time()
    wait_for_completion(job_ids, timeout=30)
    process_time = time.time() - start_process

    process_rate = num_tasks / process_time
    print(f"Processing rate: {process_rate:.1f} tasks/second")

    # Verify results
    correct_results = 0
    for i, job_id in enumerate(job_ids):
        result_str = dtq.get_result(job_id)
        result = json.loads(result_str) if isinstance(result_str, str) else result_str

        try:
            result_value = int(result["result"])
            expected = i + (i + 1)
            if result_value == expected:
                correct_results += 1
        except (ValueError, TypeError, KeyError) as e:
            print(f"Error parsing result for job {job_id}: {e}")

    print(f"Correct results: {correct_results}/{num_tasks}")
    assert correct_results == num_tasks, (
        f"Expected {num_tasks} correct results, got {correct_results}"
    )


def test_system_health():
    print("Testing system health...")

    try:
        # Test basic connectivity
        print("Testing basic connectivity...")
        test_job_id = dtq.enqueue(echo, msg="Health check", delay=0.1)

        if test_job_id:
            print("✓ Successfully enqueued health check task")
        else:
            print("✗ Failed to enqueue health check task")
            return

        # Wait for completion
        wait_for_completion([test_job_id], timeout=5)

        # Check if task completed
        status = dtq.get_status(test_job_id)
        result = dtq.get_result(test_job_id)

        if status == "SUCCEEDED":
            print("✓ Health check task completed successfully")
            try:
                result_str = dtq.get_result(test_job_id)
                result = (
                    json.loads(result_str)
                    if isinstance(result_str, str)
                    else result_str
                )
                print(f"✓ Result: {result['result']}")
            except (ValueError, TypeError, KeyError) as e:
                print(f"✓ Result: {result_str} (could not parse: {e})")
        else:
            print(f"✗ Health check task failed with status: {status}")

        # Test queue stats (if available)
        try:
            # This might not be implemented in your DTQ, but worth testing
            print("Testing queue statistics...")
            # stats = dtq.get_queue_stats()  # Uncomment if this method exists
            # print(f"Queue stats: {stats}")
        except AttributeError:
            print("Queue statistics not available")

    except Exception as e:
        print(f"✗ System health check failed: {e}")


def wait_for_completion(job_ids, timeout=10, poll_interval=0.5):
    """
    Wait for all jobs to complete or timeout
    """
    start_time = time.time()
    remaining_jobs = set(job_ids)

    while remaining_jobs and (time.time() - start_time) < timeout:
        completed_jobs = set()

        for job_id in remaining_jobs:
            status = dtq.get_status(job_id)
            if status in ["SUCCEEDED", "failed", "error"]:
                completed_jobs.add(job_id)

        remaining_jobs -= completed_jobs

        if remaining_jobs:
            time.sleep(poll_interval)

    if remaining_jobs:
        print(
            f"Warning: {len(remaining_jobs)} jobs did not complete within {timeout} seconds"
        )

    return len(remaining_jobs) == 0


if __name__ == "__main__":
    # Set environment to use Docker services
    os.environ["DTQ_USE_MOCK"] = "false"
    os.environ["RMQ_URL"] = "amqp://guest:guest@localhost:5672/%2F"
    os.environ["REDIS_URL"] = "redis://localhost:6379/0"

    print("Starting DTQ Docker Tests")
    print("=" * 60)

    try:
        test_basic_functionality()
        print("\n" + "=" * 50 + "\n")

        test_error_handling()
        print("\n" + "=" * 50 + "\n")

        test_concurrent_processing()
        print("\n" + "=" * 50 + "\n")

        test_performance()
        print("\n" + "=" * 50 + "\n")

        test_system_health()

        print("\n" + "=" * 60)
        print("OK")

    except KeyboardInterrupt:
        print("\nTests interrupted by user")
    except Exception as e:
        print(f"\nTest suite failed with error: {e}")
        import traceback

        traceback.print_exc()
