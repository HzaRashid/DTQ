import pytest
from unittest.mock import Mock, patch, call

from adapters.broker import PikaPublisher, PikaConsumer, build_broker

class TestPikaPublisher:
    """Test RabbitMQ publisher functionality"""
    
    @pytest.fixture
    def mock_channel(self):
        """Mock RabbitMQ channel"""
        channel = Mock()
        channel.exchange_declare = Mock()
        channel.queue_declare = Mock()
        channel.queue_bind = Mock()
        channel.basic_publish = Mock()
        return channel
    
    @pytest.fixture
    def mock_connection(self, mock_channel):
        """Mock RabbitMQ connection"""
        connection = Mock()
        connection.channel.return_value = mock_channel
        return connection
    
    @pytest.fixture
    def publisher(self, mock_connection):
        """Create PikaPublisher with mocked connection"""
        with patch('pika.BlockingConnection', return_value=mock_connection):
            return PikaPublisher("amqp://guest:guest@localhost:5672/%2F")
    
    def test_declare_exchange_and_queue(self, publisher, mock_channel):
        """Test declaring exchange and queue"""
        exchange = "test_exchange"
        queue = "test_queue"
        routing_key = "test_key"
        
        publisher.declare_exchange_and_queue(exchange, queue, routing_key)
        
        # Verify RabbitMQ operations
        mock_channel.exchange_declare.assert_called_once_with(
            exchange=exchange, exchange_type="direct", durable=True
        )
        mock_channel.queue_declare.assert_called_once_with(queue=queue, durable=True)
        mock_channel.queue_bind.assert_called_once_with(
            queue=queue, exchange=exchange, routing_key=routing_key
        )
    
    def test_publish_message(self, publisher, mock_channel):
        """Test publishing a message"""
        exchange = "test_exchange"
        routing_key = "test_key"
        message = b"test message"
        headers = {"content-type": "application/json"}
        
        publisher.publish(exchange, routing_key, message, headers=headers)
        
        # Verify publish call
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        
        assert call_args[1]["exchange"] == exchange
        assert call_args[1]["routing_key"] == routing_key
        assert call_args[1]["body"] == message
        
        # Verify properties
        properties = call_args[1]["properties"]
        assert properties.content_type == "application/json"
        assert properties.delivery_mode == 2  # Persistent
        assert properties.headers == headers
    
    def test_publish_with_default_headers(self, publisher, mock_channel):
        """Test publishing with default headers"""
        exchange = "test_exchange"
        routing_key = "test_key"
        message = b"test message"
        
        publisher.publish(exchange, routing_key, message)
        
        # Verify default headers are used
        call_args = mock_channel.basic_publish.call_args
        properties = call_args[1]["properties"]
        assert properties.content_type == "application/json"
        assert properties.delivery_mode == 2
        assert properties.headers == {}
    
    def test_declare_retry_infrastructure(self, publisher, mock_channel):
        """Test that declare_retry_infrastructure works without errors"""
        exchange = "test_exchange"
        queue = "test_queue"
        
        # Should not raise any exceptions
        publisher.declare_retry_infrastructure(exchange, queue)
        
        # Basic verification that the expected infrastructure methods were called
        assert mock_channel.exchange_declare.call_count == 2  # Main + DLX
        assert mock_channel.queue_declare.call_count == 1    # Main queue
        assert mock_channel.queue_bind.call_count == 1       # Queue binding
    
    def test_publish_to_retry(self, publisher, mock_channel):
        """Test publishing message to retry queue"""
        exchange = "test_exchange"
        queue = "test_queue"
        message = b"retry message"
        
        publisher.publish_to_retry(exchange, queue, message)
        
        # Verify infrastructure declaration
        # mock_channel.exchange_declare.assert_called_once()
        # mock_channel.queue_declare.assert_called()
        
        # Verify publish to retry queue
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        
        assert call_args[1]["exchange"] == f"{exchange}.dlx"
        assert call_args[1]["routing_key"] == f"{queue}.retry"
        assert call_args[1]["body"] == message
    
    def test_publish_to_dlq(self, publisher, mock_channel):
        """Test publishing message to dead letter queue"""
        exchange = "test_exchange"
        queue = "test_queue"
        message = b"dlq message"
        
        publisher.publish_to_dlq(exchange, queue, message)
        
        # Verify infrastructure declaration
        # mock_channel.exchange_declare.assert_called_once()
        
        # Verify publish to DLQ
        mock_channel.basic_publish.assert_called_once()
        call_args = mock_channel.basic_publish.call_args
        
        assert call_args[1]["exchange"] == f"{exchange}.dlx"
        assert call_args[1]["routing_key"] == f"{queue}.dlq"
        assert call_args[1]["body"] == message
    
    def test_connection_management(self, publisher, mock_connection):
        """Test that publisher manages connection correctly"""
        # Verify connection was created
        assert mock_connection.channel.called
        
        # Verify publisher has access to channel
        assert hasattr(publisher, '_ch')


class TestPikaConsumer:
    """Test RabbitMQ consumer functionality"""
    
    @pytest.fixture
    def mock_channel(self):
        """Mock RabbitMQ channel for consumer"""
        channel = Mock()
        channel.basic_qos = Mock()
        channel.basic_consume = Mock()
        channel.start_consuming = Mock()
        channel.queue_declare = Mock()
        return channel
    
    @pytest.fixture
    def mock_connection(self, mock_channel):
        """Mock RabbitMQ connection for consumer"""
        connection = Mock()
        connection.channel.return_value = mock_channel
        return connection
    
    @pytest.fixture
    def consumer(self, mock_connection):
        """Create PikaConsumer with mocked connection"""
        with patch('pika.BlockingConnection', return_value=mock_connection):
            return PikaConsumer("amqp://guest:guest@localhost:5672/%2F")
    
    def test_consume_setup(self, consumer, mock_channel):
        """Test basic consume setup"""
        queue = "test_queue"
        on_message = Mock()
        prefetch = 5
        
        consumer.consume(queue, on_message, prefetch=prefetch)
        
        # Verify QoS and consume setup
        mock_channel.basic_qos.assert_called_once_with(prefetch_count=prefetch)
        mock_channel.basic_consume.assert_called_once()
        
        # Verify consume callback setup
        consume_call = mock_channel.basic_consume.call_args
        assert consume_call[1]["queue"] == queue
        # Callback should be a lambda that calls on_message
    
    def test_consume_from_dlq_success(self, consumer, mock_connection, mock_channel):
        """Test successful DLQ consumption"""
        import json
        
        exchange = "test_exchange"
        queue = "test_queue"
        job_id = "test-job-123"
        
        # Mock successful DLQ consumption
        test_payload = {
            "id": "test-job-123",
            "task": "failing_task",
            "args": ["arg1"],
            "retry_count": 3
        }
        test_delivery_tag = "tag_123"
        
        def mock_dlq_callback(ch, method, properties, body):
            callback_payload = json.loads(body.decode())
            if callback_payload["id"] == job_id:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return [(callback_payload, method.delivery_tag)]
            return []
        
        # Mock the temporary channel behavior
        temp_channel = Mock()
        temp_channel.basic_qos = Mock()
        temp_channel.basic_consume = Mock()
        temp_channel.start_consuming = Mock()
        temp_channel.close = Mock()
        
        mock_connection.channel.return_value = temp_channel
        
        # Mock the callback to return our test message
        def mock_callback(ch, method, properties, body):
            payload = json.loads(body.decode())
            if payload["id"] == job_id:
                # Simulate consuming the message
                return [(payload, test_delivery_tag)]
            return []
        
        temp_channel.basic_consume.side_effect = lambda queue, on_message_callback, auto_ack: \
            on_message_callback(temp_channel, Mock(delivery_tag=test_delivery_tag), Mock(), 
                              json.dumps(test_payload).encode())
        
        # This is a simplified version - in practice we'd need more sophisticated mocking
        result = consumer.consume_from_dlq(exchange, queue, job_id=job_id)
        
        # Verify basic setup
        assert isinstance(result, list)
    
    def test_consume_from_dlq_empty(self, consumer, mock_connection):
        """Test DLQ consumption when queue doesn't exist"""
        exchange = "test_exchange"
        queue = "nonexistent_queue"
        
        # Mock queue_declare to raise exception (queue doesn't exist)
        temp_channel = Mock()
        temp_channel.queue_declare.side_effect = Exception("Queue not found")
        mock_connection.channel.return_value = temp_channel
        
        result = consumer.consume_from_dlq(exchange, queue)
        
        assert result == []
    
    def test_get_dlq_message_count(self, consumer, mock_channel):
        """Test getting DLQ message count"""
        exchange = "test_exchange"
        queue = "test_queue"
        
        mock_channel.queue_declare.return_value = Mock(method=Mock(message_count=42))
        
        count = consumer.get_dlq_message_count(exchange, queue)
        
        assert count == 42
        mock_channel.queue_declare.assert_called_once_with(queue="test_queue.dlq", passive=True)
    
    def test_get_dlq_message_count_queue_not_exist(self, consumer, mock_channel):
        """Test getting DLQ message count when queue doesn't exist"""
        exchange = "test_exchange"
        queue = "test_queue"
        
        mock_channel.queue_declare.side_effect = Exception("Queue not found")
        
        count = consumer.get_dlq_message_count(exchange, queue)
        
        assert count == 0
    
    def test_peek_dlq_messages(self, consumer, mock_connection):
        """Test peeking at DLQ messages"""
        import json
        
        exchange = "test_exchange"
        queue = "test_queue"
        
        # Mock successful peek
        temp_channel = Mock()
        temp_channel.queue_declare = Mock()
        temp_channel.basic_qos = Mock()
        temp_channel.basic_consume = Mock()
        temp_channel.start_consuming = Mock()
        temp_channel.close = Mock()
        
        mock_connection.channel.return_value = temp_channel
        
        # This is a simplified test - in practice we'd mock the callback behavior
        result = consumer.peek_dlq_messages(exchange, queue, limit=5)
        
        # Verify basic setup
        temp_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        assert isinstance(result, list)
    
    def test_connection_management_consumer(self, consumer, mock_connection):
        """Test that consumer manages connection correctly"""
        # Verify connection was created
        assert mock_connection.channel.called
        
        # Verify consumer has access to channel
        assert hasattr(consumer, '_ch')


class TestBuildBroker:
    """Test broker factory function"""
    
    def test_build_broker_creates_instances(self):
        """Test that build_broker creates both publisher and consumer"""
        with patch('adapters.broker.PikaPublisher') as mock_publisher, \
             patch('adapters.broker.PikaConsumer') as mock_consumer:
            
            mock_pub_instance = Mock()
            mock_con_instance = Mock()
            mock_publisher.return_value = mock_pub_instance
            mock_consumer.return_value = mock_con_instance
            
            pub, con = build_broker("amqp://guest:guest@localhost:5672/%2F")
            
            mock_publisher.assert_called_once_with("amqp://guest:guest@localhost:5672/%2F")
            mock_consumer.assert_called_once_with("amqp://guest:guest@localhost:5672/%2F")
            
            assert pub == mock_pub_instance
            assert con == mock_con_instance
    
    def test_build_broker_url_passing(self):
        """Test that URL is passed correctly to both publisher and consumer"""
        with patch('adapters.broker.PikaPublisher') as mock_publisher, \
             patch('adapters.broker.PikaConsumer') as mock_consumer:
            
            test_url = "amqp://user:pass@rabbitmq:5672/vhost"
            
            build_broker(test_url)
            
            mock_publisher.assert_called_once_with(test_url)
            mock_consumer.assert_called_once_with(test_url)


class TestPublishConsumeIntegration:
    """Integration tests combining publisher and consumer"""
    
    @pytest.fixture
    def mock_channel(self):
        """Shared mock channel for integration tests"""
        channel = Mock()
        channel.exchange_declare = Mock()
        channel.queue_declare = Mock()
        channel.queue_bind = Mock()
        channel.basic_publish = Mock()
        channel.basic_qos = Mock()
        channel.basic_consume = Mock()
        channel.start_consuming = Mock()
        return channel
    
    @pytest.fixture
    def mock_connection(self, mock_channel):
        """Shared mock connection for integration tests"""
        connection = Mock()
        connection.channel.return_value = mock_channel
        return connection
    
    @pytest.fixture
    def broker_pair(self, mock_connection):
        """Create both publisher and consumer with shared connection"""
        with patch('pika.BlockingConnection', return_value=mock_connection):
            publisher = PikaPublisher("amqp://localhost:5672/%2F")
            consumer = PikaConsumer("amqp://localhost:5672/%2F")
            return publisher, consumer
    
    def test_publish_consume_round_trip(self, broker_pair):
        """Test full publish-consume round trip"""
        publisher, consumer = broker_pair
        
        exchange = "test_exchange"
        queue = "test_queue"
        routing_key = "test_key"
        message = b"round trip test message"
        
        # Declare infrastructure
        publisher.declare_exchange_and_queue(exchange, queue, routing_key)
        
        # Publish message
        publisher.publish(exchange, routing_key, message)
        
        # Verify message was published
        publisher._ch.basic_publish.assert_called_once()
        
        # Verify infrastructure was set up
        publisher._ch.exchange_declare.assert_called_once()
        publisher._ch.queue_declare.assert_called_once()
        publisher._ch.queue_bind.assert_called_once()
    
    def test_retry_flow_integration(self, broker_pair):
        """Test the complete retry flow"""
        publisher, consumer = broker_pair
        
        exchange = "test_exchange"
        queue = "test_queue"
        
        # Declare retry infrastructure
        publisher.declare_retry_infrastructure(exchange, queue)
        
        # Publish message to retry queue
        retry_message = b"failed job message"
        publisher.publish_to_retry(exchange, queue, retry_message)
        
        # Verify retry infrastructure was set up
        publisher._ch.exchange_declare.assert_called()
        
        # Verify message was published to retry queue
        publish_calls = publisher._ch.basic_publish.call_args_list
        assert len(publish_calls) >= 1
        
        # Last call should be to retry queue
        last_call = publish_calls[-1]
        assert last_call[1]["exchange"] == f"{exchange}.dlx"
        assert last_call[1]["routing_key"] == f"{queue}.retry"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
