#!/bin/bash

set -e

TRAINING_DATA="src/main/resources/training.csv"

echo "Building project..."
sbt clean assembly

echo "Training KMeans..."
spark-submit \
    --class es.dmr.uimp.clustering.KMeansClusterInvoices \
    target/scala-2.11/anomalyDetection-assembly-1.0.jar \
    $TRAINING_DATA clustering threshold

echo "Training BisectionKMeans..."
spark-submit \
    --class es.dmr.uimp.clustering.BisectingKMeansClusterInvoices \
    target/scala-2.11/anomalyDetection-assembly-1.0.jar \
    $TRAINING_DATA clustering_bisect threshold_bisect
