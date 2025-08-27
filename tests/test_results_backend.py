
from unittest.mock import Mock, patch

import pytest

from adapters.backend import RedisBackend, build_backend


class TestRedisBackend:
    """Test Redis backend for result storage and retrieval"""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Mock Redis client for testing"""
        client = Mock()
        client.hset = Mock()
        client.expire = Mock()
        client.hgetall = Mock()
        client.delete = Mock()
        client.exists = Mock()
        client.ttl = Mock()
        return client
    
    @pytest.fixture
    def backend(self, mock_redis_client):
        """Create RedisBackend with mocked client"""
        with patch('redis.from_url', return_value=mock_redis_client):
            return RedisBackend("redis://localhost:6379/0")
    
    def test_set_fields_without_ttl(self, backend, mock_redis_client):
        """Test setting fields without TTL"""
        job_id = "test-job-123"
        data = {"status": "RUNNING", "started_at": "1234567890"}
        
        backend.set_fields(job_id, data)
        
        # Verify Redis operations
        expected_key = "dtq:result:test-job-123"
        mock_redis_client.hset.assert_called_once_with(expected_key, mapping=data)
        mock_redis_client.expire.assert_not_called()
    
    def test_set_fields_with_ttl(self, backend, mock_redis_client):
        """Test setting fields with TTL"""
        job_id = "test-job-456"
        data = {"status": "SUCCEEDED", "result": "42"}
        ttl_seconds = 3600
        
        backend.set_fields(job_id, data, ttl_seconds=ttl_seconds)
        
        # Verify Redis operations
        expected_key = "dtq:result:test-job-456"
        mock_redis_client.hset.assert_called_once_with(expected_key, mapping=data)
        mock_redis_client.expire.assert_called_once_with(expected_key, ttl_seconds)
    
    def test_get_all_existing_job(self, backend, mock_redis_client):
        """Test retrieving existing job data"""
        job_id = "test-job-789"
        expected_data = {"status": "SUCCEEDED", "result": "42", "duration": "5.2"}
        
        mock_redis_client.hgetall.return_value = expected_data
        
        result = backend.get_all(job_id)
        
        expected_key = "dtq:result:test-job-789"
        mock_redis_client.hgetall.assert_called_once_with(expected_key)
        assert result == expected_data
    
    def test_get_all_nonexistent_job(self, backend, mock_redis_client):
        """Test retrieving non-existent job returns None"""
        job_id = "nonexistent-job"
        
        mock_redis_client.hgetall.return_value = {}  # Redis returns empty dict for non-existent keys
        
        result = backend.get_all(job_id)
        
        assert result is None
    
    def test_key_format_consistency(self, backend, mock_redis_client):
        """Test that all operations use consistent key format"""
        job_id = "consistent-test-job"
        data = {"status": "RUNNING"}
        
        backend.set_fields(job_id, data)
        backend.get_all(job_id)
        
        expected_key = "dtq:result:consistent-test-job"
        
        # Both operations should use the same key format
        assert mock_redis_client.hset.call_args[0][0] == expected_key
        assert mock_redis_client.hgetall.call_args[0][0] == expected_key
    
    def test_ttl_behavior_with_real_redis(self):
        """Integration test with real Redis (requires Redis running)"""
        pytest.importorskip("redis")
        
        # This test would require a real Redis instance
        # For now, we'll skip it in CI environments
        pytest.skip("Requires Redis server for integration testing")
        
        # Example of how this would work:
        # backend = RedisBackend("redis://localhost:6379/0")
        # job_id = "ttl-test-job"
        # 
        # backend.set_fields(job_id, {"status": "TEST"}, ttl_seconds=2)
        # assert backend.get_all(job_id) is not None
        # time.sleep(3)
        # assert backend.get_all(job_id) is None
    
    def test_large_result_storage(self, backend, mock_redis_client):
        """Test storing large result data"""
        job_id = "large-result-job"
        
        # Simulate a large result (e.g., ML model output, large dataset)
        large_result = {
            "status": "SUCCEEDED",
            "result": {
                "model_output": [0.1] * 10000,  # Large array
                "metadata": {"confidence": 0.95, "model_version": "v2.1"},
                "processing_time": 45.2
            },
            "large_debug_info": "x" * 5000  # Large debug string
        }
        
        backend.set_fields(job_id, large_result, ttl_seconds=3600)
        
        # Verify it was stored (Redis handles large values automatically)
        mock_redis_client.hset.assert_called_once()
        call_args = mock_redis_client.hset.call_args
        assert call_args[1]["mapping"] == large_result
    
    def test_special_characters_in_data(self, backend, mock_redis_client):
        """Test handling of special characters and data types"""
        job_id = "special-chars-job"
        
        # Test various data types and special characters
        special_data = {
            "status": "FAILED",
            "error": "Unicode error: éñüîôü",
            "traceback": "File 'test.py', line 42: invalid syntax\n  def test():\n    return 'hello'",
            "numbers": [1, 2.5, -10],
            "boolean": True,
            "none_value": None,
            "special_chars": "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        }
        
        backend.set_fields(job_id, special_data)
        
        # Verify it was stored correctly
        call_args = mock_redis_client.hset.call_args
        stored_data = call_args[1]["mapping"]
        
        # Redis should handle all these data types
        assert stored_data["error"] == "Unicode error: éñüîôü"
        assert "invalid syntax" in stored_data["traceback"]
        assert stored_data["numbers"] == [1, 2.5, -10]
        assert stored_data["boolean"] is True
    
    def test_concurrent_access_simulation(self, backend, mock_redis_client):
        """Test behavior under concurrent access patterns"""
        job_id = "concurrent-job"
        
        # Simulate multiple workers updating the same job
        updates = [
            {"status": "RUNNING", "started_at": "1000"},
            {"status": "RUNNING", "progress": "50%"},
            {"status": "SUCCEEDED", "result": "final_result", "finished_at": "2000"}
        ]
        
        for update in updates:
            backend.set_fields(job_id, update)
        
        # Verify all updates were made (Redis HSET merges fields)
        assert mock_redis_client.hset.call_count == 3
        
        # Final read should get merged data
        mock_redis_client.hgetall.return_value = {
            "status": "SUCCEEDED",
            "started_at": "1000", 
            "progress": "50%",
            "result": "final_result",
            "finished_at": "2000"
        }
        
        result = backend.get_all(job_id)
        assert result["status"] == "SUCCEEDED"
        assert result["result"] == "final_result"
    
    def test_memory_efficiency(self, backend, mock_redis_client):
        """Test memory usage patterns"""
        job_ids = [f"memory-test-{i}" for i in range(100)]
        
        # Store many small jobs
        for job_id in job_ids:
            backend.set_fields(job_id, {"status": "PENDING"}, ttl_seconds=300)
        
        # Verify Redis operations were called efficiently
        assert mock_redis_client.hset.call_count == 100
        assert mock_redis_client.expire.call_count == 100
        
        # Verify key naming pattern
        hset_calls = mock_redis_client.hset.call_args_list
        for i, call in enumerate(hset_calls):
            expected_key = f"dtq:result:memory-test-{i}"
            assert call[0][0] == expected_key


class TestBuildBackend:
    """Test backend factory function"""
    
    def test_build_backend_creates_redis_backend(self):
        """Test that build_backend creates RedisBackend instance"""
        with patch('adapters.backend.RedisBackend') as mock_redis_backend:
            mock_instance = Mock()
            mock_redis_backend.return_value = mock_instance
            
            result = build_backend("redis://localhost:6379/0")
            
            mock_redis_backend.assert_called_once_with("redis://localhost:6379/0")
            assert result == mock_instance
    
    def test_build_backend_passes_url_correctly(self):
        """Test that URL is passed correctly to RedisBackend"""
        with patch('adapters.backend.RedisBackend') as mock_redis_backend:
            test_url = "redis://user:pass@host:port/db"
            
            build_backend(test_url)
            
            mock_redis_backend.assert_called_once_with(test_url)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


