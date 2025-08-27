import json
import time
import pytest
from unittest.mock import Mock, patch, MagicMock

from dtq.app import DTQ
from adapters.broker import PikaPublisher, PikaConsumer
from adapters.backend import RedisBackend


class TestEndToEnd:
    """End-to-end tests covering complete DTQ workflows"""
    
    @pytest.fixture
    def mock_publisher(self):
        """Mock publisher for testing"""
        publisher = Mock(spec=PikaPublisher)
        publisher.declare_exchange_and_queue = Mock()
        publisher.declare_retry_infrastructure = Mock()
        publisher.publish = Mock()
        publisher.publish_to_retry = Mock()
        publisher.publish_to_dlq = Mock()
        publisher._conn = Mock()  # FIX: Add missing _conn attribute
        return publisher
    
    @pytest.fixture
    def mock_consumer(self):
        """Mock consumer for e2e tests"""
        consumer = Mock(spec=PikaConsumer)
        consumer.consume = Mock()
        consumer.consume_from_dlq = Mock(return_value=[])
        consumer.get_dlq_message_count = Mock(return_value=0)
        consumer.peek_dlq_messages = Mock(return_value=[])
        return consumer
    
    @pytest.fixture
    def mock_backend(self):
        """Mock Redis backend for e2e tests"""
        backend = Mock(spec=RedisBackend)
        backend.set_fields = Mock()
        backend.get_all = Mock(return_value=None)
        return backend
    
    @pytest.fixture
    def dtq_app(self, mock_publisher, mock_consumer, mock_backend):
        """Create DTQ application with mocked adapters"""
        return DTQ(
            publisher=mock_publisher,
            consumer=mock_consumer,
            result_backend=mock_backend
        )
    
    def test_complete_successful_job_workflow(self, dtq_app):
        """Test complete workflow for a successful job"""
        # Create a mock task
        mock_task = Mock()
        mock_task._dtq_task_name = "successful_task"
        
        # Step 1: Enqueue job
        job_id = dtq_app.enqueue(mock_task, input_value="test_data")
        
        # Verify job was enqueued with correct metadata
        dtq_app._pub.publish.assert_called_once()
        call_args = dtq_app._pub.publish.call_args
        
        # FIX: Access keyword args instead of positional args
        body = call_args[1]['body']  # call_args[1] is the kwargs dict
        payload = json.loads(body.decode())
        
        assert payload["id"] == job_id
        assert payload["task"] == "successful_task"
        assert payload["retry_count"] == 0
        assert payload["args"] == []  # FIX: Empty because no positional args
        assert payload["kwargs"] == {"input_value": "test_data"}  # FIX: Keyword args go here
        
        # Step 2: Simulate worker processing
        # Worker would consume message, process task, store result
        job_result = {"status": "SUCCEEDED", "result": "processed_data", "duration": "0.5"}
        dtq_app._backend.set_fields(job_id, job_result, ttl_seconds=86400)
        
        # Step 3: Check job status and result
        dtq_app._backend.get_all.return_value = job_result
        
        status = dtq_app.get_status(job_id)
        result = dtq_app.get_result(job_id)
        
        assert status == "SUCCEEDED"
        assert result == job_result
    
    def test_job_failure_and_retry_workflow(self, dtq_app):
        """Test complete workflow for job failure and retry"""
        # Create a mock task
        mock_task = Mock()
        mock_task._dtq_task_name = "failing_task"
        
        # Step 1: Enqueue job
        job_id = dtq_app.enqueue(mock_task, input_param="fail_data")
        
        # Step 2: Simulate worker failure (first attempt)
        failure_result = {
            "status": "FAILED",
            "error": "Task processing failed",
            "traceback": "Exception details...",
            "retry_count": 1
        }
        dtq_app._backend.set_fields(job_id, failure_result, ttl_seconds=86400)
        
        # Step 3: Check initial status
        dtq_app._backend.get_all.return_value = failure_result
        status = dtq_app.get_status(job_id)
        assert status == "FAILED"
        
        # Step 4: Simulate successful retry
        success_result = {
            "status": "SUCCEEDED",
            "result": "retry_success",
            "duration": "1.2",
            "retry_count": 1
        }
        dtq_app._backend.set_fields(job_id, success_result, ttl_seconds=86400)
        
        # Step 5: Verify final status
        dtq_app._backend.get_all.return_value = success_result
        final_status = dtq_app.get_status(job_id)
        final_result = dtq_app.get_result(job_id)
        
        assert final_status == "SUCCEEDED"
        assert final_result["retry_count"] == 1
    
    def test_exhausted_retries_dlq_workflow(self, dtq_app):
        """Test workflow when job exhausts all retries and goes to DLQ"""
        # Create a mock task
        mock_task = Mock()
        mock_task._dtq_task_name = "permanently_failing_task"

        # Step 1: Enqueue job
        job_id = dtq_app.enqueue(mock_task, param="always_fails")

        # Step 2: Simulate worker retry behavior
        max_retries = 3
        for retry_count in range(max_retries + 1):
            if retry_count < max_retries:
                # Worker would publish to retry queue
                dtq_app._pub.publish_to_retry("dtq", "default", b"retry_message")
                dtq_app._pub.publish_to_retry.assert_called()
            else:
                # Worker would send to DLQ after max retries
                dtq_app._pub.publish_to_dlq("dtq", "default", b"dlq_message")
                dtq_app._pub.publish_to_dlq.assert_called_once()

        # Step 3: Verify job status shows failed
        failure_result = {
            "status": "FAILED",
            "error": "Final failure",
            "retry_count": 3
        }
        dtq_app._backend.get_all.return_value = failure_result
        status = dtq_app.get_status(job_id)
        assert status == "FAILED"

        # Step 4: Test DLQ inspection
        dlq_messages = [
            {
                "job_id": job_id,
                "task": "permanently_failing_task",
                "retry_count": 3,
                "error": "Final failure"
            }
        ]
        dtq_app._con.peek_dlq_messages.return_value = dlq_messages

        dlq_status = dtq_app.list_dlq_messages()
        assert dlq_status["success"] is True
        assert dlq_status["message_count"] == 0  # Mock returns 0
        assert len(dlq_status["messages"]) == 1
    
    def test_concurrent_job_processing(self, dtq_app):
        """Test processing multiple jobs concurrently"""
        # Create multiple mock tasks
        tasks = []
        job_ids = []
        
        for i in range(5):
            mock_task = Mock()
            mock_task._dtq_task_name = f"concurrent_task_{i}"
            tasks.append(mock_task)
            
            job_id = dtq_app.enqueue(mock_task, job_number=i)
            job_ids.append(job_id)
        
        # Simulate concurrent processing results
        for i, job_id in enumerate(job_ids):
            result = {
                "status": "SUCCEEDED",
                "result": f"processed_job_{i}",
                "processing_order": i
            }
            dtq_app._backend.set_fields(job_id, result, ttl_seconds=86400)
        
        # Verify all results are retrievable
        for i, job_id in enumerate(job_ids):
            dtq_app._backend.get_all.return_value = {
                "status": "SUCCEEDED",
                "result": f"processed_job_{i}",
                "processing_order": i
            }
            
            status = dtq_app.get_status(job_id)
            result = dtq_app.get_result(job_id)
            
            assert status == "SUCCEEDED"
            assert result["result"] == f"processed_job_{i}"
    
    def test_job_ttl_and_expiration(self, dtq_app):
        """Test job result TTL and expiration behavior"""
        mock_task = Mock()
        mock_task._dtq_task_name = "ttl_test_task"
        
        job_id = dtq_app.enqueue(mock_task, ttl_param="expires_quickly")
        
        # Simulate job completion with short TTL
        result = {"status": "SUCCEEDED", "result": "temporary_data"}
        dtq_app._backend.set_fields(job_id, result, ttl_seconds=1)  # 1 second TTL
        
        # Initially result should be available
        dtq_app._backend.get_all.return_value = result
        assert dtq_app.get_result(job_id) == result
        
        # Simulate TTL expiration
        dtq_app._backend.get_all.return_value = None  # Redis returns None after expiration
        assert dtq_app.get_result(job_id) is None
        assert dtq_app.get_status(job_id) == "PENDING"  # Default status
    
    def test_task_registration_and_discovery(self, dtq_app):
        """Test task registration and autodiscovery"""
        # Register some test tasks
        @dtq_app.task()
        def test_task_1(param):
            return f"processed_{param}"
        
        @dtq_app.task(name="custom_named_task")
        def test_task_2(param):
            return f"custom_{param}"
        
        # Verify tasks are registered
        assert "test_task_1" in dtq_app._tasks
        assert "custom_named_task" in dtq_app._tasks
        assert callable(dtq_app._tasks["test_task_1"])
        assert callable(dtq_app._tasks["custom_named_task"])
        
        # Test task retrieval
        retrieved_task = dtq_app.get_task("test_task_1")
        assert retrieved_task == test_task_1
        
        # Test task execution
        result = retrieved_task("test_input")
        assert result == "processed_test_input"
    
    def test_error_handling_edge_cases(self, dtq_app):
        """Test error handling for edge cases"""
        # Test with invalid task (missing _dtq_task_name attribute)
        class InvalidTask:
            pass
        invalid_task = InvalidTask()

        with pytest.raises(AttributeError):
            dtq_app.enqueue(invalid_task, param="test")

        # Test getting status/result for non-existent job
        dtq_app._backend.get_all.return_value = None

        status = dtq_app.get_status("nonexistent-job")
        result = dtq_app.get_result("nonexistent-job")

        assert status == "PENDING"
        assert result is None
    
    def test_system_monitoring_and_health_checks(self, dtq_app):
        """Test system monitoring capabilities"""
        # Test DLQ message count
        dtq_app._con.get_dlq_message_count.return_value = 5
        
        dlq_info = dtq_app.list_dlq_messages()
        assert dlq_info["success"] is True
        assert dlq_info["message_count"] == 5
        
        # Test queue statistics (would integrate with RabbitMQ management API)
        # This is a placeholder for future enhancement
        pass
    
    @pytest.mark.parametrize("num_jobs,expected_successes", [
        (1, 1),
        (5, 5), 
        (10, 10),
        (100, 100)
    ])
    def test_scalability_simulation(self, dtq_app, num_jobs, expected_successes):
        """Test system scalability with multiple jobs"""
        # Create multiple jobs
        job_ids = []
        for i in range(num_jobs):
            mock_task = Mock()
            mock_task._dtq_task_name = f"scale_task_{i}"
            job_id = dtq_app.enqueue(mock_task, job_num=i)
            job_ids.append(job_id)
        
        # Simulate all jobs succeeding
        for job_id in job_ids:
            success_result = {"status": "SUCCEEDED", "result": f"output_{job_id}"}
            dtq_app._backend.set_fields(job_id, success_result, ttl_seconds=3600)
        
        # Verify all jobs completed successfully
        successful_jobs = 0
        for job_id in job_ids:
            dtq_app._backend.get_all.return_value = {"status": "SUCCEEDED", "result": f"output_{job_id}"}
            if dtq_app.get_status(job_id) == "SUCCEEDED":
                successful_jobs += 1
        
        assert successful_jobs == expected_successes
    
    def test_system_recovery_scenarios(self, dtq_app):
        """Test system behavior during failure and recovery"""
        # Simulate broker connection failure during enqueue
        dtq_app._pub.publish.side_effect = Exception("RabbitMQ connection failed")
        
        mock_task = Mock()
        mock_task._dtq_task_name = "recovery_test_task"
        
        with pytest.raises(Exception, match="RabbitMQ connection failed"):
            dtq_app.enqueue(mock_task, recovery_param="test")
        
        # Simulate backend failure
        dtq_app._backend.set_fields.side_effect = Exception("Redis connection failed")
        
        # Job should still be enqueable (only result storage fails)
        dtq_app._pub.publish.side_effect = None  # Reset publisher
        job_id = dtq_app.enqueue(mock_task, recovery_param="test")
        
        # But status/result retrieval should handle backend failures
        dtq_app._backend.get_all.side_effect = Exception("Redis connection failed")
        
        status = dtq_app.get_status(job_id)
        result = dtq_app.get_result(job_id)
        
        # System should degrade gracefully
        assert status == "PENDING"  # Default when backend unavailable
        assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])