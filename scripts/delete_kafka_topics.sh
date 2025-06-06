for topic in facturas_erroneas cancelaciones anomalias_kmeans anomalias_bisect_kmeans; do
    bin/kafka-topics.sh --delete --topic "$topic" --zookeeper localhost:2181
done
