import boto3
import psycopg2
import socket
import time
import logging
import json
import os
from typing import Dict, List, Optional, Tuple
from botocore.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DSQLConnectionManager:
    def __init__(self, config_file=None, endpoints=None):
        """
        Initialize the DSQL Connection Manager
        
        Args:
            config_file: Path to a JSON configuration file (optional)
            endpoints: List of endpoint dictionaries (optional)
        """
        self.endpoints = endpoints or []
        self.connection_settings = {}
        
        # Load configuration from file if provided
        if config_file and os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                if 'endpoints' in config:
                    self.endpoints = config['endpoints']
                if 'connection_settings' in config:
                    self.connection_settings = config['connection_settings']
        
        # If no endpoints provided, try to load from dsql_config.json
        if not self.endpoints:
            default_config_path = os.path.join(os.path.dirname(__file__), 'dsql_config.json')
            if os.path.exists(default_config_path):
                try:
                    with open(default_config_path, 'r') as f:
                        config = json.load(f)
                        if 'endpoints' in config:
                            self.endpoints = config['endpoints']
                        if 'connection_settings' in config and not self.connection_settings:
                            self.connection_settings = config['connection_settings']
                except Exception as e:
                    logger.error(f"Error loading default config: {str(e)}")
                    
        # If still no endpoints, raise an error
        if not self.endpoints:
            raise ValueError("No endpoints provided. Please specify endpoints via constructor parameter, config file, or ensure dsql_config.json exists.")
        
        logger.info(f"Initialized with {len(self.endpoints)} endpoints")
        
    def get_connection(self, database, user, connect_timeout=5):
        """
        Get a database connection to the best available endpoint
        
        Args:
            database: Database name
            user: IAM username/role
            connect_timeout: Connection timeout in seconds
            
        Returns:
            psycopg2 connection object
        """
        # Check health and measure latency for all endpoints
        self._check_endpoints()
        
        # Filter for healthy endpoints
        healthy_endpoints = [e for e in self.endpoints if e.get("is_healthy", False)]
        
        if not healthy_endpoints:
            logger.warning("No healthy endpoints found, will try all endpoints")
            target_endpoints = self.endpoints
        else:
            # Sort healthy endpoints by latency
            target_endpoints = sorted(healthy_endpoints, key=lambda e: e.get("latency", float('inf')))
            logger.info(f"Found {len(healthy_endpoints)} healthy endpoints")
        
        # Try connecting to each endpoint in order
        last_exception = None
        for endpoint in target_endpoints:
            try:
                logger.info(f"Attempting connection to {endpoint['hostname']} in {endpoint['region']}")
                
                # Generate auth token using the DSQL client
                token = self._generate_auth_token(
                    endpoint["hostname"],
                    endpoint["region"]
                )
                
                # Prepare connection parameters
                conn_params = {
                    "host": endpoint["hostname"],
                    "port": endpoint.get("port", 5432),
                    "database": database,
                    "user": user,
                    "password": token,
                    "connect_timeout": connect_timeout,
                    "sslmode": 'require'  # DSQL requires SSL
                }
                
                # Add any additional connection settings from config
                for key, value in self.connection_settings.items():
                    if key not in conn_params and key != "connect_timeout":
                        conn_params[key] = value
                
                # Connect using the token
                conn = psycopg2.connect(**conn_params)
                logger.info(f"Successfully connected to {endpoint['hostname']} in {endpoint['region']}")
                return conn
            except Exception as e:
                logger.error(f"Failed to connect to {endpoint['hostname']}: {str(e)}")
                last_exception = e
        
        # If we get here, all connection attempts failed
        error_msg = "Failed to connect to any endpoint"
        if last_exception:
            error_msg += f": {str(last_exception)}"
        raise Exception(error_msg)
    
    def _generate_auth_token(self, hostname, region):
        """
        Generate an authentication token for DSQL using the DSQL client
        
        Args:
            hostname: The DSQL hostname
            region: The AWS region
            
        Returns:
            Authentication token
        """
        try:
            # Create a DSQL client with the specified region
            logger.info(f"Creating DSQL client for region {region}")
            
            # Get the caller identity for debugging
            sts_client = boto3.client('sts')
            caller_identity = sts_client.get_caller_identity()
            logger.info(f"Current AWS identity: {caller_identity}")
            
            # Create DSQL client
            dsql_client = boto3.client("dsql", region_name=region)
            
            # Generate the auth token using the DSQL-specific method
            logger.info(f"Generating DSQL admin auth token for {hostname} in {region}")
            token = dsql_client.generate_db_connect_admin_auth_token(hostname, region)
            
            # Log a portion of the token for debugging (don't log the full token for security)
            token_preview = token[:10] + "..." + token[-10:] if len(token) > 20 else "token too short"
            logger.info(f"Generated token preview: {token_preview}")
            
            return token
        except Exception as e:
            logger.error(f"Error generating auth token: {str(e)}")
            raise
    
    def _check_endpoints(self):
        """Check health and measure latency for all endpoints"""
        for endpoint in self.endpoints:
            latency, is_healthy = self._measure_endpoint(
                endpoint["hostname"], 
                endpoint.get("port", 5432)
            )
            endpoint["latency"] = latency
            endpoint["is_healthy"] = is_healthy
            
            status = "healthy" if is_healthy else "unhealthy"
            logger.info(f"Endpoint {endpoint['hostname']} is {status} with latency {latency:.2f}ms")
    
    def _measure_endpoint(self, hostname, port=5432) -> Tuple[float, bool]:
        """
        Measure latency and check health of an endpoint
        
        Args:
            hostname: Hostname to check
            port: Port to check
            
        Returns:
            Tuple of (latency in ms, is_healthy boolean)
        """
        try:
            # Try to resolve the hostname first
            ip_address = socket.gethostbyname(hostname)
            
            # Measure connection time
            start_time = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((ip_address, port))
            s.close()
            
            latency = (time.time() - start_time) * 1000  # Convert to milliseconds
            return latency, True
        except socket.gaierror:
            logger.error(f"Could not resolve hostname: {hostname}")
            return float('inf'), False
        except (socket.timeout, ConnectionRefusedError) as e:
            logger.error(f"Connection error to {hostname}:{port}: {str(e)}")
            return float('inf'), False
        except Exception as e:
            logger.error(f"Unexpected error checking {hostname}:{port}: {str(e)}")
            return float('inf'), False
