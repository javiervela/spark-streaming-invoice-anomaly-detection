#!/bin/bash

spark-submit --class es.dmr.uimp.realtime.InvoicePipeline --master local[4] target/scala-2.11/anomalyDetection-assembly-1.0.jar \
 ./clustering ./threshold ./clustering_bisect ./threshold_bisect localhost:2181 pipeline purchases 2 localhost:9092
