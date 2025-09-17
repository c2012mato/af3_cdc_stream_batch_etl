"""
Custom Airflow hook for Debezium connector management.
"""

from typing import Dict, Any, List, Optional
import requests
import json
import logging
import time
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class DebeziumHook(BaseHook):
    """Hook for interacting with Debezium Connect."""
    
    conn_name_attr = "debezium_conn_id"
    default_conn_name = "debezium_default"
    conn_type = "debezium"
    hook_name = "Debezium"
    
    def __init__(self, debezium_conn_id: str = default_conn_name):
        super().__init__()
        self.debezium_conn_id = debezium_conn_id
        self.base_url = None
        self._session = None
    
    def get_connection(self):
        """Get connection configuration."""
        if self.base_url is None:
            conn = BaseHook.get_connection(self.debezium_conn_id)
            self.base_url = f"http://{conn.host}:{conn.port or 8083}"
        return self.base_url
    
    @property
    def session(self):
        """Get HTTP session."""
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update({
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            })
        return self._session
    
    def test_connection(self) -> bool:
        """Test connection to Debezium Connect."""
        try:
            response = self.session.get(f"{self.get_connection()}/")
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def list_connectors(self) -> List[str]:
        """List all connectors."""
        try:
            response = self.session.get(f"{self.get_connection()}/connectors")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list connectors: {e}")
            return []
    
    def get_connector_config(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """Get connector configuration."""
        try:
            response = self.session.get(f"{self.get_connection()}/connectors/{connector_name}/config")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get connector config for {connector_name}: {e}")
            return None
    
    def get_connector_status(self, connector_name: str) -> Optional[Dict[str, Any]]:
        """Get connector status."""
        try:
            response = self.session.get(f"{self.get_connection()}/connectors/{connector_name}/status")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get connector status for {connector_name}: {e}")
            return None
    
    def create_connector(self, connector_name: str, connector_config: Dict[str, Any]) -> bool:
        """Create a new connector."""
        try:
            payload = {
                "name": connector_name,
                "config": connector_config
            }
            
            response = self.session.post(
                f"{self.get_connection()}/connectors",
                data=json.dumps(payload)
            )
            response.raise_for_status()
            
            logger.info(f"Created connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to create connector {connector_name}: {e}")
            return False
    
    def update_connector(self, connector_name: str, connector_config: Dict[str, Any]) -> bool:
        """Update connector configuration."""
        try:
            response = self.session.put(
                f"{self.get_connection()}/connectors/{connector_name}/config",
                data=json.dumps(connector_config)
            )
            response.raise_for_status()
            
            logger.info(f"Updated connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to update connector {connector_name}: {e}")
            return False
    
    def delete_connector(self, connector_name: str) -> bool:
        """Delete a connector."""
        try:
            response = self.session.delete(f"{self.get_connection()}/connectors/{connector_name}")
            response.raise_for_status()
            
            logger.info(f"Deleted connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to delete connector {connector_name}: {e}")
            return False
    
    def restart_connector(self, connector_name: str) -> bool:
        """Restart a connector."""
        try:
            response = self.session.post(f"{self.get_connection()}/connectors/{connector_name}/restart")
            response.raise_for_status()
            
            logger.info(f"Restarted connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to restart connector {connector_name}: {e}")
            return False
    
    def pause_connector(self, connector_name: str) -> bool:
        """Pause a connector."""
        try:
            response = self.session.put(f"{self.get_connection()}/connectors/{connector_name}/pause")
            response.raise_for_status()
            
            logger.info(f"Paused connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to pause connector {connector_name}: {e}")
            return False
    
    def resume_connector(self, connector_name: str) -> bool:
        """Resume a connector."""
        try:
            response = self.session.put(f"{self.get_connection()}/connectors/{connector_name}/resume")
            response.raise_for_status()
            
            logger.info(f"Resumed connector: {connector_name}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to resume connector {connector_name}: {e}")
            return False
    
    def wait_for_connector_status(self, connector_name: str, expected_status: str, 
                                timeout: int = 300, check_interval: int = 10) -> bool:
        """Wait for connector to reach expected status."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            status = self.get_connector_status(connector_name)
            if status and status.get('connector', {}).get('state') == expected_status:
                logger.info(f"Connector {connector_name} reached status {expected_status}")
                return True
            
            logger.debug(f"Waiting for connector {connector_name} to reach {expected_status}, "
                        f"current status: {status.get('connector', {}).get('state') if status else 'unknown'}")
            time.sleep(check_interval)
        
        logger.error(f"Timeout waiting for connector {connector_name} to reach {expected_status}")
        return False
    
    def get_postgres_connector_config(self, 
                                    connector_name: str,
                                    database_hostname: str,
                                    database_port: int,
                                    database_user: str,
                                    database_password: str,
                                    database_dbname: str,
                                    schema_include_list: str = "etl_demo",
                                    table_include_list: str = "etl_demo.customers,etl_demo.products,etl_demo.orders,etl_demo.order_items") -> Dict[str, Any]:
        """Get standard PostgreSQL connector configuration."""
        return {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": database_hostname,
            "database.port": str(database_port),
            "database.user": database_user,
            "database.password": database_password,
            "database.dbname": database_dbname,
            "database.server.name": connector_name,
            "schema.include.list": schema_include_list,
            "table.include.list": table_include_list,
            "plugin.name": "pgoutput",
            "slot.name": f"{connector_name}_slot",
            "publication.name": f"{connector_name}_publication",
            "transforms": "route",
            "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
            "transforms.route.replacement": "etl_cdc_events"
        }