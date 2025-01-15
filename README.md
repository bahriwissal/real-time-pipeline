# Scalable Data Pipeline for Real-Time User Data Processing

## Project Overview
This project implements a scalable data pipeline to process real-time user data. By leveraging modern big data technologies, the pipeline ensures efficient orchestration, streaming, distributed processing, and high-performance storage of data. 

## Architecture
The global architecture of this project is structured around the following key steps:

1. **Data Generation**:
   - We use the `randomuser.me` API to generate random user data.

2. **Orchestration and Ingestion**:
   - A Directed Acyclic Graph (DAG) in Apache Airflow orchestrates the data retrieval process and stores the data in a PostgreSQL database.

3. **Data Streaming**:
   - The data is streamed via Apache Kafka, utilizing Apache Zookeeper for cluster management.
   - Additionally, Confluent components such as Control Center and Schema Registry are used for monitoring and schema management.

4. **Distributed Processing**:
   - The streamed data is sent to a Spark Master, which distributes the workload among Spark Workers to perform parallel data processing.

5. **Final Storage**:
   - Processed data is stored in a Cassandra database for high-performance, large-scale storage and future exploitation.

## Key Features
- **Orchestration**: Automated data retrieval and task management with Apache Airflow.
- **Streaming**: Real-time data streaming using Kafka.
- **Distributed Processing**: Efficient data processing with Apache Spark.
- **Scalable Storage**: High-performance storage with Cassandra, optimized for large-scale datasets.
- **Containerization**: All workflows are containerized using Docker for easy deployment and scalability.

## Technologies Used
- **Apache Airflow**: Workflow orchestration.
- **PostgreSQL**: Relational database for intermediate storage.
- **Apache Kafka**: Real-time data streaming platform.
- **Apache Spark**: Distributed data processing engine.
- **Cassandra**: NoSQL database for processed data storage.
- **Docker**: Containerization for consistent and portable deployment.
- **Apache Zookeeper**: Cluster management for Kafka.
- **Confluent Schema Registry**: Schema management for Kafka topics.
- **Confluent Control Center**: Kafka monitoring and administration.

## Installation and Setup
1. **Clone the Repository**:
   ```bash
   git clone git@github.com:bahriwissal/real-time-pipeline.git
   cd real-time-pipeline
   ```

2. **Set Up Docker**:
   - Install Docker if not already installed.
   - Build and start the Docker containers:
     ```bash
     docker-compose up --build
     ```

3. **Configure Airflow**:
   - Access the Airflow UI at `http://localhost:8080` and trigger the DAG for data retrieval.

4. **Stream Data**:
   - Ensure Kafka and Zookeeper are running within the Docker environment.

5. **Process Data**:
   - Verify that the Spark cluster is operational and processing streamed data.

6. **Access Processed Data**:
   - Query the processed data from Cassandra using the Cassandra Query Language (CQL).


Feel free to contribute to this project by submitting pull requests or opening issues!
