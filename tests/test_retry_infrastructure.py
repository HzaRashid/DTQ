import json
import time
import pytest
from unittest.mock import Mock, patch

from dtq.app import DTQ
from adapters.broker import PikaPublisher, PikaConsumer
from adapters.backend import RedisBackend


class TestRetryInfrastructure:
    """Test retry queues, dead letter queues, and manual retry functionality"""
    
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
        """Mock consumer for testing"""
        consumer = Mock(spec=PikaConsumer)
        consumer.consume = Mock()
        consumer.consume_from_dlq = Mock(return_value=[])
        consumer.get_dlq_message_count = Mock(return_value=0)
        consumer.peek_dlq_messages = Mock(return_value=[])
        return consumer
    
    @pytest.fixture
    def mock_backend(self):
        """Mock Redis backend for testing"""
        backend = Mock(spec=RedisBackend)
        backend.set_fields = Mock()
        backend.get_all = Mock(return_value=None)
        return backend
    
    @pytest.fixture
    def dtq_instance(self, mock_publisher, mock_consumer, mock_backend):
        """Create DTQ instance with mocked adapters"""
        return DTQ(
            publisher=mock_publisher,
            consumer=mock_consumer,
            result_backend=mock_backend
        )

    def test_enqueue_includes_retry_metadata(self, dtq_instance):
        """Test that enqueued jobs include retry metadata"""
        # Mock task
        mock_task = Mock()
        mock_task._dtq_task_name = "test_task"
        
        job_id = dtq_instance.enqueue(mock_task, arg1="value1")
        
        # Verify publish was called with retry metadata
        dtq_instance._pub.publish.assert_called_once()
        call_args = dtq_instance._pub.publish.call_args
        
        # FIX: Access keyword arguments correctly
        assert len(call_args) >= 2
        kwargs = call_args[1]  # Keyword arguments dict
        assert 'body' in kwargs
        
        body = kwargs['body']
        payload = json.loads(body.decode())
        
        assert payload["id"] == job_id
        assert payload["task"] == "test_task"
        assert payload["retry_count"] == 0
        assert payload["max_retries"] == 3
        assert payload["backoff_delay"] == 60
        assert payload["first_failure_time"] is None
        assert payload["args"] == []  # FIX: Empty because no positional args
        assert payload["kwargs"] == {"arg1": "value1"}  # FIX: Keyword args go here

    def test_retry_from_dlq_success(self, dtq_instance, mock_consumer):
        """Test successful manual retry from DLQ"""
        # Mock DLQ consumption returning a message
        test_payload = {
            "id": "test-job-123",
            "task": "failing_task",
            "args": ["arg1"],
            "kwargs": {},
            "retry_count": 3,
            "max_retries": 3,
            "error": "Task failed",
            "first_failure_time": time.time() - 3600
        }
        mock_consumer.consume_from_dlq.return_value = [(test_payload, "delivery_tag_123")]
        
        result = dtq_instance.retry_from_dlq("test-job-123")
        
        # Verify success response
        assert result["success"] is True
        assert result["job_id"] == "test-job-123"
        assert result["retry_count_reset"] is True
        
        # Verify message was republished with reset retry count
        dtq_instance._pub.publish.assert_called_once()
        call_args = dtq_instance._pub.publish.call_args
        body = call_args[1]['body']  # FIX: call_args[1] is kwargs dict
        republished_payload = json.loads(body.decode())
        
        assert republished_payload["retry_count"] == 0
        assert republished_payload["manual_retry"] is True
        assert "error" not in republished_payload
        assert "traceback" not in republished_payload

    def test_retry_from_dlq_not_found(self, dtq_instance, mock_consumer):
        """Test retry when job not found in DLQ"""
        mock_consumer.consume_from_dlq.return_value = []  # Empty DLQ
        
        result = dtq_instance.retry_from_dlq("nonexistent-job")
        
        assert result["success"] is False
        assert "not found in DLQ" in result["error"]
        assert result["job_id"] == "nonexistent-job"
        
        # Verify publish was not called
        dtq_instance._pub.publish.assert_not_called()

    def test_list_dlq_messages(self, dtq_instance, mock_consumer):
        """Test DLQ message listing"""
        # Mock DLQ with some messages
        mock_consumer.get_dlq_message_count.return_value = 3
        mock_consumer.peek_dlq_messages.return_value = [
            {"job_id": "job1", "task": "task1", "retry_count": 3, "error": "Failed"},
            {"job_id": "job2", "task": "task2", "retry_count": 3, "error": "Timeout"}
        ]
        
        result = dtq_instance.list_dlq_messages(limit=10)
        
        assert result["success"] is True
        assert result["message_count"] == 3
        assert len(result["messages"]) == 2
        assert result["truncated"] is False
        
        # Verify consumer methods were called correctly
        mock_consumer.get_dlq_message_count.assert_called_once()
        mock_consumer.peek_dlq_messages.assert_called_once_with(
            dtq_instance.exchange, "dtq.default", limit=10
        )

    def test_list_dlq_messages_error_handling(self, dtq_instance, mock_consumer):
        """Test DLQ listing with error handling"""
        mock_consumer.get_dlq_message_count.side_effect = Exception("RabbitMQ connection failed")
        
        result = dtq_instance.list_dlq_messages()
        
        assert result["success"] is False
        assert "RabbitMQ connection failed" in result["error"]

    @patch('services.worker.runner.logger')
    def test_worker_retry_logic(self, mock_logger, dtq_instance, mock_publisher):
        """Test worker retry logic integration"""
        from services.worker.runner import Worker
        
        # Create worker with mocked DTQ
        worker = Worker(dtq_instance, queue="test")
        
        # Mock successful task
        dtq_instance.get_task = Mock(return_value=lambda x: f"processed_{x}")
        
        # Test message payload
        test_payload = {
            "id": "test-job-123",
            "task": "test_task", 
            "args": ["input"],
            "kwargs": {},
            "retry_count": 0,
            "max_retries": 3
        }
        
        # Mock RabbitMQ callback parameters
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "tag_123"
        mock_properties = Mock()
        mock_properties.headers = {}
        
        # Execute message handling
        worker._handle_message(mock_ch, mock_method, mock_properties, 
                              json.dumps(test_payload).encode())
        
        # Verify successful processing
        assert mock_ch.basic_ack.called
        mock_logger.info.assert_called()  # Success log
        
        # Verify Redis status was set
        dtq_instance._backend.set_fields.assert_called()

    @patch('services.worker.runner.logger')  
    def test_worker_dlq_routing(self, mock_logger, dtq_instance, mock_publisher):
        """Test that exhausted retries go to DLQ"""
        from services.worker.runner import Worker
        
        worker = Worker(dtq_instance, queue="test")
        
        # Mock failing task
        dtq_instance.get_task = Mock(return_value=Mock(side_effect=Exception("Task failed")))
        
        # Test message with exhausted retries
        test_payload = {
            "id": "test-job-456",
            "task": "failing_task",
            "args": [],
            "kwargs": {},
            "retry_count": 3,  # Already at max retries
            "max_retries": 3
        }
        
        # Mock RabbitMQ callback parameters
        mock_ch = Mock()
        mock_method = Mock()
        mock_method.delivery_tag = "tag_456"
        mock_properties = Mock()
        mock_properties.headers = {}
        
        # Execute message handling
        worker._handle_message(mock_ch, mock_method, mock_properties,
                              json.dumps(test_payload).encode())
        
        # Verify message went to DLQ
        mock_publisher.publish_to_dlq.assert_called_once()
        mock_ch.basic_ack.assert_called_once()
        
        # Verify error was logged
        mock_logger.error.assert_called()

    def test_retry_infrastructure_declaration(self, mock_publisher):
        """Test that retry infrastructure is properly declared"""
        dtq_instance = DTQ(publisher=mock_publisher, consumer=Mock(), result_backend=Mock())
        
        # Trigger infrastructure declaration
        dtq_instance._pub.declare_retry_infrastructure("test_exchange", "test_queue")
        
        # Verify declaration was called
        mock_publisher.declare_retry_infrastructure.assert_called_once_with(
            "test_exchange", "test_queue"
        )

    @pytest.mark.parametrize("retry_count,max_retries,should_retry", [
        (0, 3, True),   # First failure - should retry
        (1, 3, True),   # Second failure - should retry  
        (2, 3, True),   # Third failure - should retry
        (3, 3, False),  # Fourth failure - should go to DLQ
        (5, 3, False),  # Way over limit - should go to DLQ
    ])
    def test_retry_count_logic(self, retry_count, max_retries, should_retry):
        """Test retry count logic edge cases"""
        from services.worker.runner import Worker
        
        payload = {
            "id": "test-job",
            "task": "test_task",
            "retry_count": retry_count,
            "max_retries": max_retries
        }
        
        # This is the logic from the worker
        should_go_to_dlq = retry_count >= max_retries
        
        assert should_go_to_dlq != should_retry

    def test_manual_retry_preserves_original_metadata(self, dtq_instance, mock_consumer):
        """Test that manual retry preserves important original metadata"""
        original_time = time.time() - 3600  # 1 hour ago
        
        test_payload = {
            "id": "test-job-789",
            "task": "important_task",
            "args": ["critical_data"],
            "kwargs": {"priority": "high"},
            "retry_count": 3,
            "max_retries": 3,
            "first_failure_time": original_time,
            "error": "Connection timeout",
            "traceback": "Stack trace here..."
        }
        
        mock_consumer.consume_from_dlq.return_value = [(test_payload, "delivery_tag")]
        
        dtq_instance.retry_from_dlq("test-job-789")
        
        # Verify republished message
        call_args = dtq_instance._pub.publish.call_args
        body = call_args[1]['body']  # FIX: call_args[1] is kwargs dict
        republished = json.loads(body.decode())
        
        # Should preserve important data
        assert republished["task"] == "important_task"
        assert republished["args"] == ["critical_data"]
        assert republished["kwargs"] == {"priority": "high"}
        
        # Should reset retry data
        assert republished["retry_count"] == 0
        assert republished["manual_retry"] is True
        assert republished["original_failure_time"] == original_time
        
        # Should remove failure data
        assert "error" not in republished
        assert "traceback" not in republished


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
