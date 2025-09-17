#!/bin/bash

# ETL System Setup Script
set -e

echo "=== ETL System Setup ==="

# Create directories if they don't exist
mkdir -p logs
mkdir -p data
mkdir -p temp

# Copy environment file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "Please review and update .env file with your configuration"
fi

# Function to check if a service is ready
wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=30
    local attempt=1

    echo "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if nc -z $host $port 2>/dev/null; then
            echo "$service_name is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to start within expected time"
    return 1
}

# Start Docker services
echo "Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
wait_for_service "PostgreSQL" localhost 5432
wait_for_service "Redis" localhost 6379
wait_for_service "Kafka" localhost 9092
wait_for_service "Debezium" localhost 8083
wait_for_service "Airflow" localhost 8080

echo "All services are ready!"

# Initialize Airflow database
echo "Initializing Airflow..."
docker-compose exec airflow-webserver airflow db init || true

# Create Airflow admin user
echo "Creating Airflow admin user..."
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Setup Debezium connector
echo "Setting up Debezium connector..."
sleep 10  # Give Debezium some time to fully start

# Create the connector configuration
CONNECTOR_CONFIG='{
  "name": "etl-postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "etl_user",
    "database.password": "etl_password",
    "database.dbname": "etl_db",
    "database.server.name": "etl-postgres-connector",
    "schema.include.list": "etl_demo",
    "table.include.list": "etl_demo.customers,etl_demo.products,etl_demo.orders,etl_demo.order_items",
    "plugin.name": "pgoutput",
    "slot.name": "etl_postgres_connector_slot",
    "publication.name": "etl_postgres_connector_publication",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
    "transforms.route.replacement": "etl_cdc_events"
  }
}'

# Try to create the connector
curl -X POST \
  -H "Content-Type: application/json" \
  -d "$CONNECTOR_CONFIG" \
  http://localhost:8083/connectors || echo "Connector may already exist"

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Services:"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - Debezium Connect: http://localhost:8083"
echo "  - PostgreSQL: localhost:5432 (etl_user/etl_password)"
echo "  - Redis: localhost:6379"
echo "  - Kafka: localhost:9092"
echo ""
echo "Next steps:"
echo "1. Access Airflow UI and enable the DAGs"
echo "2. Run the data generator to create test data"
echo "3. Monitor the CDC and stream processing"
echo ""
echo "To stop all services: docker-compose down"
echo "To view logs: docker-compose logs -f [service-name]"