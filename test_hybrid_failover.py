#!/usr/bin/env python3
"""
Test failover between DSQL endpoints using the hybrid approach
"""
import argparse
import logging
import time
import json
import os
import shutil
from hybrid_failover_approach import DSQLHybridConnectionManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backup_config(config_file):
    """Create a backup of the configuration file"""
    backup_file = f"{config_file}.backup"
    shutil.copy2(config_file, backup_file)
    logger.info(f"Created backup of configuration at {backup_file}")
    return backup_file

def restore_config(backup_file, config_file):
    """Restore the configuration from backup"""
    shutil.copy2(backup_file, config_file)
    logger.info(f"Restored configuration from {backup_file}")
    os.remove(backup_file)
    logger.info(f"Removed backup file {backup_file}")

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
        else:
            logger.warning("No healthy endpoints found")
        
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

def main():
    parser = argparse.ArgumentParser(description='Test DSQL Hybrid Failover')
    parser.add_argument('--config', default='dsql_config.json', help='Configuration file')
    parser.add_argument('--database', default='postgres', help='Database name')
    parser.add_argument('--user', default='admin', help='Database user')
    parser.add_argument('--query', default='SELECT version();', help='SQL query to execute')
    args = parser.parse_args()
    
    config_file = args.config
    
    # Step 1: Backup the configuration
    backup_file = backup_config(config_file)
    
    try:
        # Step 2: Test normal connection
        logger.info("\n=== STEP 1: Testing connection under normal conditions ===")
        manager = DSQLHybridConnectionManager(config_file=config_file)
        primary_hostname, primary_result, primary_time = get_connection_info(
            manager, args.database, args.user, args.query
        )
        
        if not primary_hostname:
            logger.error("Initial connection test failed. Exiting.")
            return
        
        # Step 3: Modify the configuration to simulate failure of the primary endpoint
        logger.info(f"\n=== STEP 2: Simulating failure of the primary endpoint: {primary_hostname} ===")
        
        # Load the configuration
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Find and modify the primary endpoint
        for endpoint in config['endpoints']:
            if endpoint['hostname'] == primary_hostname:
                # Save original values
                original_hostname = endpoint['hostname']
                
                # Modify the hostname to cause a connection failure
                endpoint['hostname'] = 'invalid-hostname-for-failover-test.example.com'
                logger.info(f"Modified endpoint hostname from {original_hostname} to {endpoint['hostname']}")
                break
        
        # Save the modified configuration
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Step 4: Test connection again, which should use the failover endpoint
        logger.info("\n=== STEP 3: Testing connection with primary endpoint failure ===")
        manager = DSQLHybridConnectionManager(config_file=config_file)
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
        # Step 5: Restore the original configuration
        logger.info("\n=== STEP 4: Restoring original configuration ===")
        restore_config(backup_file, config_file)
        
        # Step 6: Test connection again to verify we can connect to the primary endpoint
        logger.info("\n=== STEP 5: Testing connection after restoring configuration ===")
        manager = DSQLHybridConnectionManager(config_file=config_file)
        restored_hostname, restored_result, restored_time = get_connection_info(
            manager, args.database, args.user, args.query
        )
    
    # Summary
    logger.info("\n=== FAILOVER TEST SUMMARY ===")
    logger.info(f"Primary endpoint: {primary_hostname}")
    logger.info(f"Failover endpoint: {failover_hostname}")
    logger.info(f"Restored endpoint: {restored_hostname}")
    
    if primary_hostname != failover_hostname:
        logger.info("RESULT: Failover test SUCCESSFUL!")
    else:
        logger.info("RESULT: Failover test INCONCLUSIVE.")

if __name__ == "__main__":
    main()
