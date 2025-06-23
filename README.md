# DSQL Connection Manager

A Python library implementing client-based routing for Amazon Aurora DSQL endpoints with automatic selection of the lowest-latency endpoint and failover capabilities.

## Features

- **Client-Based Routing**: Routes database connections to the optimal endpoint from the client side
- **Automatic Endpoint Selection**: Connects to the DSQL endpoint with the lowest latency
- **Failover Support**: Automatically tries alternative endpoints if the primary endpoint is unavailable
- **Health Checking**: Monitors endpoint health and availability using direct TCP checks or Route 53 health checks
- **IAM Authentication**: Uses AWS IAM authentication with DSQL admin auth tokens
- **Simple API**: Provides a clean interface for establishing database connections

## Prerequisites

- Python 3.8+
- AWS credentials with DSQL access permissions
- Network access to DSQL endpoints
- boto3 version 1.37.24 or higher (includes DSQL support)
- For Route 53 health checks: permissions to create and manage Route 53 health checks

## Installation

1. Clone this repository
   ```bash
   git clone https://github.com/aws-samples/sample-multi-region-Endpoint-Routing-for-Aurora-DSQL.git;
   cd dsql-routing
   ```

2. Create a virtual environment
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

## Authentication

This library uses the AWS SDK's default credential provider chain for authentication. Before running the code, ensure you have configured your AWS credentials using the AWS Documentation: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

Make sure your credentials have the necessary permissions to access DSQL services. You can follow the documentaion on how to set it up: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/security-iam.html

## Usage

### Basic Usage

```python
from dsql_connection_manager import DSQLConnectionManager

# Initialize the connection manager
manager = DSQLConnectionManager()

# Get a connection to the best available endpoint
conn = manager.get_connection("postgres", "admin")

# Use the connection
with conn.cursor() as cursor:
    cursor.execute("SELECT version();")
    result = cursor.fetchone()
    print(f"Database version: {result[0]}")

# Close the connection
conn.close()
```

### Custom Endpoints

You can specify your own DSQL endpoints:

```python
endpoints = [
    {
        "cluster_id": "<your-cluster-id-1>",
        "region": "us-east-1",
        "hostname": "<your-cluster-id-1>.dsql.us-east-1.on.aws",
        "port": 5432,
        "priority": 1  # Lower priority number means higher preference
    },
    {
        "cluster_id": "<your-cluster-id-2>",
        "region": "us-west-2",
        "hostname": "<your-cluster-id-2>.dsql.us-west-2.on.aws",
        "port": 5432,
        "priority": 2
    }
]

# Create an instance of the connection manager with custom endpoints
manager = DSQLConnectionManager(endpoints=endpoints)
```

The `priority` field is optional. When provided, it can influence endpoint selection if latencies are similar. Lower priority numbers indicate higher preference.

### Configuration File

You can also load endpoints from a JSON configuration file:

```python
manager = DSQLConnectionManager(config_file="dsql_config_with_healthchecks.json")
```

The repository includes a sample configuration file `dsql_config_with_healthchecks.json` with the following structure:

```json
{
  "endpoints": [
    {
      "cluster_id": "<your-cluster-id-1>",
      "region": "us-east-1",
      "hostname": "<your-cluster-id-1>.dsql.us-east-1.on.aws",
      "port": 5432,
      "priority": 1,
      "health_check_id": "<health-check-id-1>"
    },
    {
      "cluster_id": "<your-cluster-id-2>",
      "region": "us-east-2",
      "hostname": "<your-cluster-id-2>.dsql.us-east-2.on.aws",
      "port": 5432,
      "priority": 2,
      "health_check_id": "<health-check-id-2>"
    }
  ],
  "connection_settings": {
    "connect_timeout": 5,
    "application_name": "dsql-hybrid-router",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 3
  }
}
```

You can modify this file to include your own DSQL endpoints and connection settings.

## Advanced Usage with Route 53 Health Checks

The library provides an enhanced approach that uses AWS Route 53 health checks for more reliable health monitoring.

### Setting Up Route 53 Health Checks

1. Create a configuration file with your DSQL endpoints (similar to `dsql_config_with_healthchecks.json`)

2. Run the setup command to create Route 53 health checks for your endpoints:
   ```bash
   python hybrid_failover_approach.py --setup --config dsql_config_with_healthchecks.json
   ```

3. This will create health checks in Route 53 and update your configuration file with the health check IDs:
   ```json
   {
     "endpoints": [
       {
         "cluster_id": "<your-cluster-id-1>",
         "region": "us-east-1",
         "hostname": "<your-cluster-id-1>.dsql.us-east-1.on.aws",
         "port": 5432,
         "priority": 1,
         "health_check_id": "<generated-health-check-id-1>"
       },
       {
         "cluster_id": "<your-cluster-id-2>",
         "region": "us-west-2",
         "hostname": "<your-cluster-id-2>.dsql.us-west-2.on.aws",
         "port": 5432,
         "priority": 2,
         "health_check_id": "<generated-health-check-id-2>"
       }
     ],
     "connection_settings": {
       "connect_timeout": 5,
       "application_name": "dsql-hybrid-router",
       "keepalives": 1,
       "keepalives_idle": 30,
       "keepalives_interval": 10,
       "keepalives_count": 3
     }
   }
   ```

4. Use the hybrid approach with your updated configuration:
   ```python
   from hybrid_failover_approach import DSQLHybridConnectionManager
   
   # Initialize with Route 53 health checks
   manager = DSQLHybridConnectionManager(config_file="dsql_config_with_healthchecks.json")
   
   # Get a connection to the best available endpoint
   conn = manager.get_connection("postgres", "admin")
   ```

## Testing Connectivity and Failover

### Basic Connectivity Test

Test basic connectivity to your DSQL endpoints:

```bash
python hybrid_failover_approach.py --test --config dsql_config_with_healthchecks.json --database postgres --user admin
```


### Testing Failover with Route 53 Health Checks

Test failover using Route 53 health checks:

```bash
python test_route53_failover.py --config dsql_config_with_healthchecks.json --database postgres --user admin
```

This will:
1. Connect to the best available endpoint
2. Disable the Route 53 health check for that endpoint
3. Wait for the health check status to propagate
4. Verify that the connection fails over to another endpoint
5. Re-enable the health check
6. Verify that connections can be made to the restored endpoint

## Monitoring and Debugging

The library provides detailed logging about endpoint health, latency, and connection attempts:

```
Endpoint latency comparison:
  1. <your-cluster-id-1>.dsql.us-east-1.on.aws - Latency: 0.055231s, Priority: 1, Region: us-east-1
  2. <your-cluster-id-2>.dsql.us-east-2.on.aws - Latency: 0.058111s, Priority: 2, Region: us-east-2
Selected best endpoint: <your-cluster-id-1>.dsql.us-east-1.on.aws (latency: 0.055231s, priority: 1)
```

You can adjust the logging level in your application:

```python
import logging
logging.basicConfig(level=logging.INFO)  # or logging.DEBUG for more details
```

## Customizing Health Check Behavior

You can customize the health check behavior when initializing the hybrid connection manager:

```python
from hybrid_failover_approach import DSQLHybridConnectionManager

manager = DSQLHybridConnectionManager(
    config_file="dsql_config_with_healthchecks.json",
    health_check_ttl=60,  # Cache health check results for 60 seconds
    latency_test_timeout=2.0,  # Timeout for latency tests in seconds
    latency_test_retries=3,  # Number of retries for latency tests
    connection_timeout=5  # Timeout for database connections in seconds
)
```

## Important Notes

- This library requires boto3 version 1.37.24 or higher, which includes support for the DSQL client.
- The DSQL endpoints must be accessible from the machine running this code.
- Your AWS credentials must have permissions to generate DSQL admin auth tokens.
- For Route 53 health checks, your credentials must have permissions to create and manage health checks.
- For production use, consider implementing additional error handling and retry logic.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
