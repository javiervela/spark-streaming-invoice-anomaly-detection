#!/bin/bash

for topic in facturas_erroneas cancelaciones anomalias_kmeans anomalias_bisect_kmeans; do
    echo -e "\n===== Messages from topic: $topic =====\n"
    timeout 5s bin/kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning
done
