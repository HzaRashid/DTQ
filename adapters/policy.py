import requests
import json
import os
import logging

logger = logging.getLogger(__name__)


class RMQPolicy:
    def __init__(self, broker_mgmt_url=None):
        self.main_exchange = os.getenv("DTQ_EXCHANGE", "dtq")
        self.dl_exchange = os.getenv("DTQ_DL_EXCHANGE", f"{self.main_exchange}.dlx")
        self.default_queue = os.getenv("DTQ_DEFAULT_QUEUE", "default")
        self.broker_mgmt_url = broker_mgmt_url or os.getenv("BROKER_MGMT_URL", "http://guest:guest@localhost:15672")
        
        # Remove trailing slash and ensure proper format
        if self.broker_mgmt_url.endswith('/'):
            self.broker_mgmt_url = self.broker_mgmt_url.rstrip('/')
        
        # Configuration for retry behavior
        self.retry_delay_seconds = int(os.getenv("DTQ_RETRY_DELAY", "60"))  # 1 minute delay
        self.max_retry_attempts = int(os.getenv("DTQ_MAX_RETRIES", "3"))

    def _make_request(self, method, endpoint, data=None):
        """Helper method to make HTTP requests to RabbitMQ management API"""
        url = f"{self.broker_mgmt_url}/api{endpoint}"
        headers = {"Content-Type": "application/json"}
        
        try:
            if method.upper() == "PUT":
                response = requests.put(url, headers=headers, json=data)
            elif method.upper() == "GET":
                response = requests.get(url, headers=headers)
            elif method.upper() == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json() if response.content else {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to {method} {endpoint}: {e}")
            raise

    # adapters/policy.py - Enhanced version
    def create_unified_dlq_policy(self, queue_name: str):
        """Unified DLQ policy that handles both caught and uncaught failures"""
        policy_name = f"unified-dlq-{queue_name}"
        
        policy_data = {
            "pattern": f"^{queue_name}$",  # Match main queue
            "apply-to": "queues",
            "definition": {
                "dead-letter-exchange": self.dl_exchange,
                "dead-letter-routing-key": f"{queue_name}.dlq"  # Consistent routing key
            },
            "priority": 1
        }
        return self._make_request("PUT", f"/policies/%2F/{policy_name}", policy_data)

    def create_retry_policy(self, queue_name: str):
        """Retry policy for retry queues"""
        policy_name = f"retry-{queue_name}"
        
        policy_data = {
            "pattern": f"^{queue_name}\\.retry$",  # Match retry queues
            "apply-to": "queues", 
            "definition": {
                "message-ttl": self.retry_delay_seconds * 1000,
                "dead-letter-exchange": self.main_exchange,
                "dead-letter-routing-key": queue_name  # Back to main queue
            },
            "priority": 1
        }
        return self._make_request("PUT", f"/policies/%2F/{policy_name}", policy_data)

    def create_dlq_policy(self, queue_name: str):
        """DLQ policy with monitoring and cleanup"""
        policy_name = f"dlq-{queue_name}"
        seven_days = 7 * 24 * 60 * 60 * 1000
        policy_data = {
            "pattern": f"^{queue_name}\\.dlq$",  # Match DLQ
            "apply-to": "queues",
            "definition": {
                "message-ttl": seven_days,  # 7 days
                "expires": seven_days,  # Auto-delete empty queue
                "max-length": 10000,    # Prevent unbounded growth
                "overflow": "reject-publish"    # Reject when full
            },
            "priority": 1
        }
        return self._make_request("PUT", f"/policies/%2F/{policy_name}", policy_data)

    def setup_retry_infrastructure(self, queue_name: str):
        """Setup complete retry infrastructure for a queue using policies"""
        try:
            # Create all necessary policies
            self.create_unified_dlq_policy(queue_name)
            self.create_retry_policy(queue_name)
            self.create_dlq_policy(queue_name)
            
            logger.info(f"Successfully setup retry infrastructure for {queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to setup retry infrastructure for {queue_name}: {e}")
            return False

    def delete_policies(self, queue_name: str):
        """Clean up policies for a queue"""
        policies = [
            f"unified-dlq-{queue_name}",  # Updated to match new policy name
            f"retry-{queue_name}", 
            f"dlq-{queue_name}"
        ]
        
        for policy in policies:
            try:
                endpoint = f"/policies/%2F/{policy}"
                self._make_request("DELETE", endpoint)
                logger.info(f"Deleted policy {policy}")
            except Exception as e:
                logger.warning(f"Failed to delete policy {policy}: {e}")

    def list_policies(self):
        """List all current policies"""
        try:
            return self._make_request("GET", "/policies/%2F")
        except Exception as e:
            logger.error(f"Failed to list policies: {e}")
            return {}

    def get_policy(self, policy_name: str):
        """Get a specific policy"""
        try:
            return self._make_request("GET", f"/policies/%2F/{policy_name}")
        except Exception as e:
            logger.error(f"Failed to get policy {policy_name}: {e}")
            return None