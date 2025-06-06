for topic in purchases facturas_erroneas cancelaciones anomalias_kmeans anomalias_bisect_kmeans; do
    bin/kafka-topics.sh --create --topic "$topic" --partitions 1 --replication-factor 1 --zookeeper localhost:2181
done

bin/kafka-topics.sh --list --zookeeper localhost:2181
