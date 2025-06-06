# Invoice Anomaly Detection Pipeline (Spark Streaming)

This project implements a real-time data processing pipeline using Apache Spark Streaming to detect anomalous invoices. The system leverages both **KMeans** and **BisectingKMeans** clustering algorithms as alternative anomaly detection strategies. The objective is to enable future comparative evaluation of their performance via **A/B testing**. Additionally, the pipeline identifies malformed invoices and monitors cancellation patterns over time.

> This project is an assignment for the course "Big Data" at the Master in Artificial Intelligence Research at the Universidad Internacional Menéndez Pelayo (UIMP).

---

## Execution Instructions

### 1. Compilation

Compile the application using the following command:

```bash
sbt clean assembly
```

### 2. Kafka Setup

Create the required Kafka topics:

```bash
scripts/create_kafka_topics.sh
```

### 3. Model Training (Offline)

Execute the model training routine:

```bash
./start_training.sh
```

### 4. Launch Streaming Pipeline

Start the streaming pipeline:

```bash
./start_pipeline.sh
```

### 5. Simulate Data Feed (Optional)

To simulate a stream of invoice data:

```bash
./productiondata.sh
```

---

## Repository Structure

```txt
.
├── build.sbt                      # SBT build configuration
├── start_training.sh              # Trains KMeans and BisectingKMeans models
├── start_pipeline.sh              # Launches the real-time streaming pipeline
├── productiondata.sh              # Kafka producer for simulating invoice streams
├── clustering/                    # Output directory for KMeans model
├── clustering_bisect/             # Output directory for BisectingKMeans model
├── threshold                      # Anomaly threshold for KMeans
├── threshold_bisect               # Anomaly threshold for BisectingKMeans
├── logs/                          # Detected anomalies, errors, and cancellations for production data
├── scripts/                       # Kafka topic management and utility scripts
├── src/main/scala/es/dmr/uimp/
│   ├── clustering/                # Offline training classes
│   ├── realtime/                  # Streaming pipeline logic
│   └── simulation/                # Kafka producer implementation
└── src/main/resources/            # CSV datasets for training and testing
```

---

## Environment and Dependencies

- **Apache Spark** 2.0.0
- **Scala** 2.11
- **Kafka + Zookeeper** (locally available)

Required Kafka topics:

- `purchases`
- `facturas_erroneas`
- `cancelaciones`
- `anomalias_kmeans`
- `anomalias_bisect_kmeans`

These topics can be created using the provided script:

```bash
scripts/create_kafka_topics.sh
```

---

## Input Data Description

The source data consists of individual purchase lines extracted from `online_retail.csv`. Each purchase record contains the following fields:

- **InvoiceNo**: Unique identifier for the invoice (string)
- **StockCode**: Product/item code (string)
- **Description**: Description of the product (string)
- **Quantity**: Number of units purchased (integer)
- **InvoiceDate**: Date and time of the transaction (timestamp)
- **UnitPrice**: Price per unit of product (float)
- **CustomerID**: Unique identifier for the customer (string or integer)
- **Country**: Country of the customer (string)

Multiple purchase lines sharing the same `InvoiceNo` are aggregated to reconstruct a complete invoice. For anomaly detection, the following features are computed per invoice and used for clustering:

- Average unit price
- Minimum unit price
- Maximum unit price
- Hour of transaction
- Total quantity purchased

---

## Architecture

The pipeline consists of the following logical layers:

1. **Ingestion Layer**

   - Kafka topic `purchases` feeds raw CSV lines
   - Consumed using Spark Streaming (20s batch interval)

2. **Parsing and Validation**

   - Converts CSV lines into structured `Purchase` objects
   - Routes malformed records to the error handler

3. **Stateful Invoice Assembly**

   - Maintains state using `updateStateByKey` keyed by `InvoiceNo`
   - Merges purchase lines until invoice expires (40s of inactivity)

4. **Invoice Finalization**

   - Validates fields; classifies as valid or erroneous
   - Discards incomplete or malformed invoices

5. **Detection and Publishing**

   - Valid invoices are featurized and evaluated for anomaly
   - Outputs published to appropriate Kafka topics

---

## System Workflow

### Offline Training (`start_training.sh`)

- Trains both clustering models using preprocessed CSV data from `training.csv`.
- Saves models and anomaly thresholds to designated directories and files (`clustering/`, `clustering_bisect/`, `threshold`, `threshold_bisect`).

### Real-Time Streaming Pipeline (`start_pipeline.sh`)

1. Consumes data from the Kafka topic `purchases`.
2. Parses and validates purchase records.
3. Maintains invoice state across time using `updateStateByKey`.
4. On expiration, finalizes invoice aggregation and performs the following checks:

   - Identifies and reports malformed invoices
   - Detects cancellations (invoices starting with 'C')
   - Performs anomaly detection using both models

5. Publishes results to respective Kafka topics.

---

## Design Decisions

### Stateful Invoice Tracking

To reconstruct invoices from individual purchases, the system maintains state per `InvoiceNo` using `updateStateByKey`. Each state retains all the purchases associated with a given invoice and tracks inactivity through a tick counter.

### SLA-Adherent Expiration Logic

The system is designed to comply with the data source's service-level agreement (SLA), which guarantees that no more than 40 seconds will elapse between purchase lines of the same invoice. Consequently, the pipeline processes and expires invoices after two consecutive 20-second Spark micro-batches without updates.

### Invoice Completion and Classification

Once an invoice's state expires, the system attempts to validate and classify it:

- Invoices with missing fields (such as customer ID, date, or country) or parse errors are flagged as erroneous.
- Invoices that meet all structural requirements are converted into feature vectors and passed to the anomaly detection phase.

### Clustering Models

Both KMeans and BisectingKMeans models are trained offline. The optimal number of clusters is selected via the Elbow method, using a cutoff ratio of 0.7 between consecutive errors. The resulting models and anomaly thresholds are persisted for use during streaming.

### Anomaly Detection

For each valid invoice, the system calculates its distance to the nearest cluster centroid. An invoice is deemed anomalous if this distance exceeds a threshold derived from the training data (specifically, the 2000th-largest distance).

### Output Channels

The results of each detection step are published to dedicated Kafka topics. While both clustering methods are executed in parallel, the goal of this separation is to facilitate future A/B testing and comparative evaluation—not currently implemented in the pipeline.

---

## Detection Modules

### Erroneous Invoices

Invoices are flagged as erroneous if any of the following conditions are met:

- Missing required fields (e.g., customer ID, date, country)
- Empty set of purchase lines
- Parsing failures

### Cancellations

Invoices with IDs starting with 'C' are counted within a sliding time window of 8 minutes, updated every minute. The resulting counts are published to the `cancelaciones` topic.

### Anomaly Detection

Invoices are transformed into feature vectors. An invoice is considered anomalous if its distance to the closest cluster center exceeds the predefined threshold. Results are published to:

- `anomalias_kmeans` (KMeans)
- `anomalias_bisect_kmeans` (BisectingKMeans)

---

## Kafka Topics Overview

| Topic                     | Description                                    |
| ------------------------- | ---------------------------------------------- |
| `purchases`               | Input stream of raw invoice lines (CSV format) |
| `facturas_erroneas`       | Invoices with structural or parsing errors     |
| `cancelaciones`           | Rolling count of cancelled invoices            |
| `anomalias_kmeans`        | Anomalies identified using KMeans              |
| `anomalias_bisect_kmeans` | Anomalies identified using BisectingKMeans     |

To reset all Kafka topics:

```bash
scripts/delete_kafka_topics.sh
scripts/create_kafka_topics.sh
```
