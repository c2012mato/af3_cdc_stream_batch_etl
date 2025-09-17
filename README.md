# AF3 CDC Stream Batch ETL

A comprehensive ETL (Extract, Transform, Load) system built with Airflow 3.0.0, Kafka, Debezium, PostgreSQL, and Redis. This system demonstrates modern data engineering practices including Change Data Capture (CDC), real-time stream processing, and batch analytics.

## 🏗️ Architecture

### Stack Components
- **Airflow 3.0.0**: Orchestration, DAGs, tasks, hooks, and best practices
- **Kafka**: Message streaming and event processing
- **Zookeeper**: Kafka cluster coordination
- **Debezium**: Change Data Capture (CDC) from PostgreSQL
- **PostgreSQL**: Primary database with logical replication
- **Redis**: Caching and real-time data storage
- **Docker/Compose**: Containerized deployment

### ETL Components
1. **CDC Processor** (`cdc_processor.py`): Handles CRUD operations, caching, and metrics
2. **Batch Processor** (`batch_processor.py`): 60-second intervals, customer segmentation, product analytics, revenue analysis
3. **Stream Processor** (`stream_processor.py`): Real-time windowing, anomaly detection, alerts, performance monitoring
4. **Data Generator** (`data_generator.py`): Realistic, continuous, configurable mock data generation

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 4GB of available RAM
- Ports 5432, 6379, 8080, 8083, 9092 available

### 1. Clone and Setup
```bash
git clone <repository-url>
cd af3_cdc_stream_batch_etl
./setup.sh
```

### 2. Access Services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Debezium Connect**: http://localhost:8083
- **PostgreSQL**: localhost:5432 (etl_user/etl_password)
- **Redis**: localhost:6379
- **Kafka**: localhost:9092

### 3. Enable DAGs in Airflow
1. Access Airflow UI
2. Enable the following DAGs:
   - `etl_main_processing`: Main orchestration
   - `etl_data_generation`: Continuous data generation
   - `etl_batch_processing`: Daily batch analytics

## 📁 Project Structure

```
af3_cdc_stream_batch_etl/
├── dags/                          # Airflow DAGs
│   ├── etl_main_dag.py           # Main processing orchestration
│   ├── etl_data_generation_dag.py # Data generation
│   └── etl_batch_processing_dag.py # Batch processing
├── src/                           # Source code
│   ├── config/                    # Configuration management
│   ├── processors/                # ETL processors
│   │   ├── data_generator.py     # Mock data generation
│   │   ├── cdc_processor.py      # CDC event processing
│   │   ├── batch_processor.py    # Batch analytics
│   │   └── stream_processor.py   # Real-time processing
│   ├── hooks/                     # Airflow hooks
│   │   └── debezium_hook.py      # Debezium integration
│   ├── operators/                 # Airflow operators
│   │   └── etl_operators.py      # Custom ETL operators
│   └── utils/                     # Utility functions
├── sql/                           # Database schemas
├── plugins/                       # Airflow plugins
├── logs/                          # Application logs
├── docker-compose.yml            # Service definitions
├── Dockerfile.airflow            # Airflow container
├── requirements.txt              # Python dependencies
├── setup.sh                      # Setup script
└── README.md                     # This file
```

## 🔄 Data Flow

### 1. Data Generation
- Continuous mock data creation (customers, products, orders)
- Configurable rates and patterns
- Realistic business scenarios

### 2. Change Data Capture (CDC)
- Debezium captures PostgreSQL changes
- Events published to Kafka topics
- Real-time data synchronization

### 3. Stream Processing
- Real-time event processing
- Sliding time windows
- Anomaly detection
- Performance monitoring
- Automated alerting

### 4. Batch Processing
- Customer segmentation (VIP, Regular, New, Inactive)
- Product analytics (sales, revenue, inventory)
- Revenue summaries and reporting
- Historical trend analysis

## 📊 Features

### CDC Processing
- ✅ Real-time change capture from PostgreSQL
- ✅ Event caching with Redis
- ✅ Metrics collection and monitoring
- ✅ CRUD operation tracking
- ✅ Automated error handling

### Batch Processing
- ✅ Customer segmentation based on purchase behavior
- ✅ Product performance analytics
- ✅ Revenue trend analysis
- ✅ Configurable processing intervals
- ✅ Historical data processing

### Stream Processing
- ✅ Sliding time windows (configurable)
- ✅ Real-time anomaly detection
- ✅ Performance monitoring
- ✅ Automated alerting system
- ✅ Metrics aggregation

### Data Generation
- ✅ Realistic customer profiles
- ✅ Product catalogs with categories
- ✅ Order simulation with seasonality
- ✅ Continuous data generation
- ✅ Configurable data volumes

## 🔧 Configuration

### Environment Variables
Key configuration options in `.env` file:

```bash
# Database
POSTGRES_HOST=postgres
POSTGRES_DB=etl_db
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=etl_password

# Processing
BATCH_INTERVAL_SECONDS=60
STREAM_WINDOW_SECONDS=30
ANOMALY_THRESHOLD=2.0
DATA_GENERATOR_INTERVAL=5

# Monitoring
LOG_LEVEL=INFO
ENABLE_PROMETHEUS=true
```

### Scaling Configuration
- Kafka partitions and replication
- Airflow worker scaling
- Database connection pooling
- Redis clustering options

## 📈 Monitoring

### Health Checks
- Service availability monitoring
- Database connectivity
- Kafka topic health
- Debezium connector status

### Metrics Collection
- Processing throughput
- Error rates and patterns
- Resource utilization
- Business KPIs

### Alerting
- Anomaly detection alerts
- System health notifications
- Performance degradation warnings
- Custom business rules

## 🛠️ Development

### Running Individual Components

#### Data Generator
```bash
cd src/processors
python data_generator.py --mode continuous --duration 30
```

#### CDC Processor
```bash
cd src/processors
python cdc_processor.py --log-level DEBUG
```

#### Batch Processor
```bash
cd src/processors
python batch_processor.py --mode single
```

#### Stream Processor
```bash
cd src/processors
python stream_processor.py --log-level DEBUG
```

### Testing
```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run tests
pytest tests/
```

### Code Quality
```bash
# Format code
black src/

# Lint code
flake8 src/

# Type checking
mypy src/
```

## 📋 Use Cases

### Business Intelligence
- Customer lifetime value analysis
- Product performance tracking
- Revenue trend analysis
- Market basket analysis

### Real-time Operations
- Inventory monitoring
- Fraud detection
- Performance alerting
- Operational dashboards

### Data Engineering
- Change data capture patterns
- Stream processing architectures
- Batch processing optimization
- Data quality monitoring

## 🔍 Troubleshooting

### Common Issues

#### Services Not Starting
```bash
# Check service status
docker-compose ps

# View service logs
docker-compose logs -f [service-name]

# Restart services
docker-compose restart
```

#### Debezium Connector Issues
```bash
# Check connector status
curl http://localhost:8083/connectors/etl-postgres-connector/status

# Restart connector
curl -X POST http://localhost:8083/connectors/etl-postgres-connector/restart
```

#### Database Connection Issues
```bash
# Test database connection
docker-compose exec postgres psql -U etl_user -d etl_db -c "SELECT 1;"

# Check database logs
docker-compose logs postgres
```

### Performance Tuning
- Kafka consumer group configuration
- Batch processing intervals
- Database connection pooling
- Memory allocation for JVM services

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Apache Airflow community
- Debezium project
- Kafka ecosystem
- PostgreSQL community
- Redis community