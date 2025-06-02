#!/usr/bin/env python3
"""
DSQL Connection Manager with Route 53 Health Checks and Client-Side Latency Routing

This implementation combines:
1. Client-side latency measurement for optimal endpoint selection
2. Route 53 health checks for reliable health monitoring
3. Automatic failover between endpoints
"""

import json
import logging
import socket
import time
import boto3
import psycopg2
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dsql_hybrid_manager')


class DSQLHybridConnectionManager:
    """
    Connection manager for Amazon Aurora DSQL endpoints with Route 53 health checks
    and client-side latency measurement.
    """
    
    def __init__(
        self,
        endpoints: Optional[List[Dict[str, Any]]] = None,
        config_file: Optional[str] = None,
        health_check_ttl: int = 60,
        latency_test_timeout: float = 2.0,
        latency_test_retries: int = 3,
        connection_timeout: int = 5
    ):
        """
        Initialize the DSQL Hybrid Connection Manager.
        
        Args:
            endpoints: Optional list of endpoint dictionaries
            config_file: Optional path to a JSON configuration file
            health_check_ttl: Time in seconds to cache health check results
            latency_test_timeout: Timeout in seconds for latency tests
            latency_test_retries: Number of retries for latency tests
            connection_timeout: Timeout in seconds for database connections
        """
        self.health_check_ttl = health_check_ttl
        self.latency_test_timeout = latency_test_timeout
        self.latency_test_retries = latency_test_retries
        self.connection_timeout = connection_timeout
        self.health_check_cache = {}  # Cache for health check results
        
        # Load configuration
        if config_file:
            self._load_config(config_file)
        elif endpoints:
            self.endpoints = endpoints
            self.connection_settings = {
                "connect_timeout": connection_timeout,
                "application_name": "dsql-hybrid-router",
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 3
            }
        else:
            raise ValueError("Either endpoints or config_file must be provided")
        
        # Initialize AWS clients
        self.route53 = boto3.client('route53')
        
        # Initialize DSQL client if available
        try:
            self.dsql = boto3.client('dsql')
            self.dsql_available = True
        except Exception:
            logger.warning("DSQL client not available in boto3. Using simulated auth tokens.")
            self.dsql_available = False
        
        logger.info(f"Initialized DSQL Hybrid Connection Manager with {len(self.endpoints)} endpoints")
    
    def _load_config(self, config_file: str) -> None:
        """
        Load configuration from a JSON file.
        
        Args:
            config_file: Path to the JSON configuration file
        """
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            self.endpoints = config.get('endpoints', [])
            self.connection_settings = config.get('connection_settings', {})
            
            if not self.endpoints:
                raise ValueError("No endpoints found in configuration file")
                
            logger.info(f"Loaded configuration from {config_file}")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def check_route53_health(self, health_check_id: str) -> bool:
        """
        Check endpoint health using Route 53 health checks with caching.
        
        Args:
            health_check_id: Route 53 health check ID
            
        Returns:
            bool: True if the endpoint is healthy, False otherwise
        """
        now = time.time()
        
        # Check cache first
        if health_check_id in self.health_check_cache:
            cached_result, timestamp = self.health_check_cache[health_check_id]
            if now - timestamp < self.health_check_ttl:
                return cached_result
        
        try:
            response = self.route53.get_health_check_status(
                HealthCheckId=health_check_id
            )
            
            # Extract status from the response
            observations = response.get('HealthCheckObservations', [])
            if not observations:
                logger.warning(f"No observations found for health check {health_check_id}")
                return False
            
            # Check if any observation reports success
            success_count = 0
            for obs in observations:
                status = obs.get('StatusReport', {}).get('Status', '')
                # Check if status starts with "Success" (case insensitive)
                if status.lower().startswith('success'):
                    success_count += 1
            
            # Consider healthy if at least one observation is successful
            is_healthy = success_count > 0
            
            # Log detailed information
            logger.info(f"Route 53 health check {health_check_id}: {success_count}/{len(observations)} healthy observations")
            
            # Cache the result
            self.health_check_cache[health_check_id] = (is_healthy, now)
            
            logger.info(f"Route 53 health check {health_check_id}: {'Healthy' if is_healthy else 'Unhealthy'}")
            return is_healthy
            
        except Exception as e:
            logger.error(f"Error checking Route 53 health status for {health_check_id}: {e}")
            return False
    
    def check_direct_health(self, endpoint: Dict[str, Any]) -> bool:
        """
        Check endpoint health using direct TCP connection.
        
        Args:
            endpoint: Endpoint dictionary
            
        Returns:
            bool: True if the endpoint is healthy, False otherwise
        """
        try:
            hostname = endpoint['hostname']
            port = endpoint['port']
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self.latency_test_timeout)
            sock.connect((hostname, port))
            sock.close()
            
            logger.debug(f"Direct health check for {hostname}: Healthy")
            return True
        except Exception as e:
            logger.debug(f"Direct health check for {endpoint['hostname']}: Unhealthy - {e}")
            return False
    
    def is_endpoint_healthy(self, endpoint: Dict[str, Any]) -> bool:
        """
        Check if an endpoint is healthy using Route 53 health checks if available,
        falling back to direct TCP checks.
        
        Args:
            endpoint: Endpoint dictionary
            
        Returns:
            bool: True if the endpoint is healthy, False otherwise
        """
        # If the endpoint has a health check ID, use Route 53
        if 'health_check_id' in endpoint and endpoint['health_check_id']:
            return self.check_route53_health(endpoint['health_check_id'])
        
        # Otherwise, fall back to a direct TCP check
        return self.check_direct_health(endpoint)
    
    def measure_latency(self, endpoint: Dict[str, Any]) -> float:
        """
        Measure connection latency to an endpoint with multiple retries.
        
        Args:
            endpoint: Endpoint dictionary
            
        Returns:
            float: Average latency in seconds, or float('inf') if all connections fail
        """
        hostname = endpoint['hostname']
        port = endpoint['port']
        latencies = []
        
        for i in range(self.latency_test_retries):
            try:
                start_time = time.time()
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.latency_test_timeout)
                sock.connect((hostname, port))
                sock.close()
                
                latency = time.time() - start_time
                latencies.append(latency)
                logger.debug(f"Latency to {hostname} (attempt {i+1}): {latency:.6f}s")
                
            except Exception as e:
                logger.debug(f"Failed to measure latency to {hostname} (attempt {i+1}): {e}")
        
        # Calculate average latency if we have any successful measurements
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            logger.debug(f"Average latency to {hostname}: {avg_latency:.6f}s")
            return avg_latency
        else:
            logger.debug(f"All latency measurements to {hostname} failed")
            return float('inf')  # Infinite latency for failed connections
    
    def get_healthy_endpoints(self) -> List[Dict[str, Any]]:
        """
        Get a list of healthy endpoints.
        
        Returns:
            List of healthy endpoint dictionaries
        """
        healthy_endpoints = []
        
        for endpoint in self.endpoints:
            if self.is_endpoint_healthy(endpoint):
                healthy_endpoints.append(endpoint)
        
        logger.info(f"Found {len(healthy_endpoints)} healthy endpoints out of {len(self.endpoints)}")
        return healthy_endpoints
    
    def get_best_endpoint(self) -> Optional[Dict[str, Any]]:
        """
        Get the best endpoint based on health and latency.
        
        Returns:
            The best endpoint dictionary, or None if no healthy endpoints
        """
        healthy_endpoints = self.get_healthy_endpoints()
        
        if not healthy_endpoints:
            logger.warning("No healthy endpoints available")
            return None
        
        # Measure latency for all healthy endpoints
        for endpoint in healthy_endpoints:
            endpoint['latency'] = self.measure_latency(endpoint)
        
        # Sort by latency first, then by priority if specified
        healthy_endpoints.sort(
            key=lambda x: (
                x['latency'],
                x.get('priority', 999)
            )
        )
        
        best_endpoint = healthy_endpoints[0]
        
        # Log information about all healthy endpoints for better visibility
        logger.info(f"Endpoint latency comparison:")
        for i, endpoint in enumerate(healthy_endpoints):
            logger.info(f"  {i+1}. {endpoint['hostname']} - "
                      f"Latency: {endpoint['latency']:.6f}s, "
                      f"Priority: {endpoint.get('priority', 'N/A')}, "
                      f"Region: {endpoint['region']}")
        
        logger.info(f"Selected best endpoint: {best_endpoint['hostname']} "
                   f"(latency: {best_endpoint['latency']:.6f}s, "
                   f"priority: {best_endpoint.get('priority', 'N/A')})")
        
        return best_endpoint
    
    def generate_auth_token(self, cluster_id: str, region: str) -> str:
        """
        Generate an authentication token for DSQL.
        
        Args:
            cluster_id: DSQL cluster ID
            region: AWS region
            
        Returns:
            Authentication token
        """
        if self.dsql_available:
            try:
                # Use the DSQL client to generate an auth token
                dsql_client = boto3.client('dsql', region_name=region)
                # Construct the hostname from the cluster ID and region
                hostname = f"{cluster_id}.dsql.{region}.on.aws"
                logger.info(f"Generating DSQL admin auth token for {hostname} in {region}")
                # Use the correct method name that matches dsql_connection_manager.py
                auth_token = dsql_client.generate_db_connect_admin_auth_token(hostname, region)
                # Log a portion of the token for debugging (don't log the full token for security)
                token_preview = auth_token[:10] + "..." + auth_token[-10:] if len(auth_token) > 20 else "token too short"
                logger.info(f"Generated token preview: {token_preview}")
                return auth_token
            except Exception as e:
                logger.error(f"Error generating auth token: {e}")
                raise
        else:
            # For testing, return a placeholder
            logger.info(f"Generating simulated auth token for cluster {cluster_id} in {region}")
            return f"test-auth-token-{cluster_id}-{region}"
    
    def get_connection(self, database: str, username: str) -> psycopg2.extensions.connection:
        """
        Get a connection to the best available DSQL endpoint.
        
        Args:
            database: Database name
            username: Username for authentication
            
        Returns:
            psycopg2 connection object
            
        Raises:
            Exception: If no connection could be established
        """
        # Get healthy endpoints sorted by latency
        healthy_endpoints = self.get_healthy_endpoints()
        
        if not healthy_endpoints:
            raise Exception("No healthy DSQL endpoints available")
        
        # Measure latency for all healthy endpoints
        for endpoint in healthy_endpoints:
            endpoint['latency'] = self.measure_latency(endpoint)
        
        # Sort by latency first, then by priority if specified
        healthy_endpoints.sort(
            key=lambda x: (
                x['latency'],
                x.get('priority', 999)
            )
        )
        
        # Try endpoints in order
        errors = []
        for endpoint in healthy_endpoints:
            try:
                # Generate auth token for this specific cluster and region
                auth_token = self.generate_auth_token(
                    cluster_id=endpoint['cluster_id'],
                    region=endpoint['region']
                )
                
                # Prepare connection parameters
                conn_params = {
                    'host': endpoint['hostname'],
                    'port': endpoint['port'],
                    'database': database,
                    'user': username,
                    'password': auth_token,
                    **self.connection_settings
                }
                
                # Connect using the original hostname to ensure TLS validation works
                logger.info(f"Connecting to {endpoint['hostname']} "
                           f"(latency: {endpoint['latency']:.6f}s, "
                           f"region: {endpoint['region']}, "
                           f"priority: {endpoint.get('priority', 'N/A')})")
                
                # Establish the connection
                conn = psycopg2.connect(**conn_params)
                
                logger.info(f"Successfully connected to {endpoint['hostname']}")
                return conn
                
            except Exception as e:
                error_msg = f"Failed to connect to {endpoint['hostname']}: {e}"
                logger.warning(error_msg)
                errors.append(error_msg)
                continue
                
        # If we get here, all connections failed
        error_details = "\n".join(errors)
        raise Exception(f"Failed to connect to any DSQL endpoint:\n{error_details}")


def create_route53_health_check(endpoint):
    """
    Create a Route 53 health check for a DSQL endpoint.
    
    Args:
        endpoint: Endpoint dictionary
        
    Returns:
        str: Health check ID
    """
    try:
        route53 = boto3.client('route53')
        caller_reference = f"dsql-{endpoint['cluster_id']}-{int(time.time())}"
        
        response = route53.create_health_check(
            CallerReference=caller_reference,
            HealthCheckConfig={
                'Port': endpoint['port'],
                'Type': 'TCP',
                'FullyQualifiedDomainName': endpoint['hostname'],
                'RequestInterval': 30,
                'FailureThreshold': 3
            }
        )
        
        health_check_id = response['HealthCheck']['Id']
        
        # Add a name tag to the health check
        route53.change_tags_for_resource(
            ResourceType='healthcheck',
            ResourceId=health_check_id,
            AddTags=[
                {
                    'Key': 'Name',
                    'Value': f"DSQL-{endpoint['cluster_id']}"
                }
            ]
        )
        
        logger.info(f"Created Route 53 health check {health_check_id} for {endpoint['hostname']}")
        return health_check_id
        
    except Exception as e:
        logger.error(f"Error creating Route 53 health check: {e}")
        raise


def setup_endpoints_with_health_checks(config_file):
    """
    Set up Route 53 health checks for all endpoints in a configuration file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load configuration
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Create health checks for each endpoint
        for endpoint in config['endpoints']:
            if 'health_check_id' not in endpoint or not endpoint['health_check_id']:
                health_check_id = create_route53_health_check(endpoint)
                endpoint['health_check_id'] = health_check_id
        
        # Save updated configuration
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Updated configuration file {config_file} with health check IDs")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up health checks: {e}")
        return False


if __name__ == "__main__":
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='DSQL Hybrid Connection Manager')
    parser.add_argument('--config', default='dsql_config.json',
                        help='Path to configuration file')
    parser.add_argument('--setup', action='store_true',
                        help='Set up Route 53 health checks for endpoints')
    parser.add_argument('--test', action='store_true',
                        help='Test connection to the best endpoint')
    parser.add_argument('--database', default='postgres',
                        help='Database name for test connection')
    parser.add_argument('--user', default='admin',
                        help='Username for test connection')
    
    args = parser.parse_args()
    
    if args.setup:
        setup_endpoints_with_health_checks(args.config)
    
    if args.test:
        manager = DSQLHybridConnectionManager(config_file=args.config)
        best_endpoint = manager.get_best_endpoint()
        
        if best_endpoint:
            print(f"Best endpoint: {best_endpoint['hostname']} "
                  f"(latency: {best_endpoint['latency']:.6f}s, "
                  f"priority: {best_endpoint.get('priority', 'N/A')})")
            
            try:
                conn = manager.get_connection(args.database, args.user)
                print("Connection successful!")
                
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    result = cursor.fetchone()
                    print(f"Database version: {result[0]}")
                
                conn.close()
                print("Connection closed")
                
            except Exception as e:
                print(f"Connection failed: {e}")
        else:
            print("No healthy endpoints found")
