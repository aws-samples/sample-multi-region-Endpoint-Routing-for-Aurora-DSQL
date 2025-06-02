#!/usr/bin/env python3
"""
Test failover between DSQL endpoints using Route 53 health checks
"""
import argparse
import logging
import time
import json
import boto3
from hybrid_failover_approach import DSQLHybridConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_test_query(conn, query="SELECT version();", iterations=1):
    """
    Run a test query and measure execution time
    
    Args:
        conn: Database connection
        query: SQL query to execute
        iterations: Number of times to execute the query
        
    Returns:
        Average execution time in milliseconds
    """
    total_time = 0
    
    with conn.cursor() as cursor:
        for i in range(iterations):
            logger.info(f"Running query iteration {i+1}/{iterations}")
            
            start_time = time.time()
            cursor.execute(query)
            result = cursor.fetchone()
            end_time = time.time()
            
            execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
            total_time += execution_time
            
            logger.info(f"Result: {result}")
            logger.info(f"Query execution time: {execution_time:.2f}ms")
    
    avg_time = total_time / iterations
    logger.info(f"Average query execution time over {iterations} iterations: {avg_time:.2f}ms")
    
    return avg_time, result

def get_connection_info(manager, database, user, query="SELECT version();"):
    """
    Get connection to the best available endpoint and return connection info
    
    Args:
        manager: DSQLHybridConnectionManager instance
        database: Database name
        user: Database user
        query: SQL query to execute
        
    Returns:
        Tuple of (hostname, result, execution_time)
    """
    try:
        # Get the best endpoint first to see what would be selected
        best_endpoint = manager.get_best_endpoint()
        if best_endpoint:
            logger.info(f"Best endpoint selected: {best_endpoint['hostname']} (latency: {best_endpoint['latency']:.6f}s)")
            logger.info(f"Health check ID: {best_endpoint.get('health_check_id', 'N/A')}")
        else:
            logger.warning("No healthy endpoints found")
            return None, None, None
        
        # Get connection
        conn = manager.get_connection(database, user)
        
        # Get connection info
        hostname = conn.info.host
        logger.info(f"Connected to: {hostname}")
        
        # Run the test query
        execution_time, result = run_test_query(conn, query)
        
        # Close connection
        conn.close()
        logger.info("Connection closed")
        
        return hostname, result, execution_time
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return None, None, None

def simulate_health_check_failure(health_check_id):
    """
    Simulate failure of a Route 53 health check by disabling it
    
    Args:
        health_check_id: Route 53 health check ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        route53 = boto3.client('route53')
        
        # Update the health check to be disabled
        route53.update_health_check(
            HealthCheckId=health_check_id,
            Disabled=True
        )
        
        logger.info(f"Disabled health check {health_check_id} to simulate failure")
        return True
        
    except Exception as e:
        logger.error(f"Error simulating health check failure: {e}")
        return False

def restore_health_check(health_check_id):
    """
    Restore a Route 53 health check to its original state
    
    Args:
        health_check_id: Route 53 health check ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        route53 = boto3.client('route53')
        
        # Update the health check to be enabled
        route53.update_health_check(
            HealthCheckId=health_check_id,
            Disabled=False
        )
        
        logger.info(f"Re-enabled health check {health_check_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error restoring health check: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Test DSQL Route 53 Failover')
    parser.add_argument('--config', default='dsql_config_with_healthchecks.json', help='Configuration file')
    parser.add_argument('--database', default='postgres', help='Database name')
    parser.add_argument('--user', default='admin', help='Database user')
    parser.add_argument('--query', default='SELECT version();', help='SQL query to execute')
    args = parser.parse_args()
    
    # Step 1: Test normal connection
    logger.info("\n=== STEP 1: Testing connection under normal conditions ===")
    manager = DSQLHybridConnectionManager(config_file=args.config)
    primary_hostname, primary_result, primary_time = get_connection_info(
        manager, args.database, args.user, args.query
    )
    
    if not primary_hostname:
        logger.error("Initial connection test failed. Exiting.")
        return
    
    # Find the health check ID for the primary endpoint
    with open(args.config, 'r') as f:
        config = json.load(f)
    
    primary_health_check_id = None
    for endpoint in config['endpoints']:
        if endpoint['hostname'] == primary_hostname:
            primary_health_check_id = endpoint.get('health_check_id')
            break
    
    if not primary_health_check_id:
        logger.error(f"No health check ID found for {primary_hostname}. Exiting.")
        return
    
    # Step 2: Simulate failure of the primary endpoint's health check
    logger.info(f"\n=== STEP 2: Simulating failure of the primary endpoint's health check: {primary_health_check_id} ===")
    if not simulate_health_check_failure(primary_health_check_id):
        logger.error("Failed to simulate health check failure. Exiting.")
        return
    
    # Wait for the health check to propagate
    logger.info("Waiting 60 seconds for health check status to propagate...")
    time.sleep(60)
    
    try:
        # Step 3: Test connection again, which should use the failover endpoint
        logger.info("\n=== STEP 3: Testing connection with primary endpoint health check failure ===")
        manager = DSQLHybridConnectionManager(config_file=args.config)
        failover_hostname, failover_result, failover_time = get_connection_info(
            manager, args.database, args.user, args.query
        )
        
        if not failover_hostname:
            logger.error("Failover connection test failed.")
        elif failover_hostname == primary_hostname:
            logger.warning("Failover did not occur. Still using the same endpoint.")
        else:
            logger.info(f"Failover successful! Switched from {primary_hostname} to {failover_hostname}")
    
    finally:
        # Step 4: Restore the original health check configuration
        logger.info(f"\n=== STEP 4: Restoring original health check configuration ===")
        restore_health_check(primary_health_check_id)
        
        # Wait for the health check to propagate
        logger.info("Waiting 60 seconds for health check status to propagate...")
        time.sleep(60)
        
        # Step 5: Test connection again to verify we can connect to the primary endpoint
        logger.info("\n=== STEP 5: Testing connection after restoring health check ===")
        manager = DSQLHybridConnectionManager(config_file=args.config)
        restored_hostname, restored_result, restored_time = get_connection_info(
            manager, args.database, args.user, args.query
        )
    
    # Summary
    logger.info("\n=== ROUTE 53 FAILOVER TEST SUMMARY ===")
    logger.info(f"Primary endpoint: {primary_hostname}")
    logger.info(f"Failover endpoint: {failover_hostname if 'failover_hostname' in locals() else 'N/A'}")
    logger.info(f"Restored endpoint: {restored_hostname if 'restored_hostname' in locals() else 'N/A'}")
    
    if 'failover_hostname' in locals() and primary_hostname != failover_hostname:
        logger.info("RESULT: Route 53 failover test SUCCESSFUL!")
    else:
        logger.info("RESULT: Route 53 failover test INCONCLUSIVE.")

if __name__ == "__main__":
    main()
