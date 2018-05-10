docker run -it --rm --net 262-network --link kafka spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $TOPIC --from-beginning;
