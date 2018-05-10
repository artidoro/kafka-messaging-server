if [ -z "$1" ]; then

	echo "Usage: sh spawn_producer.sh topic_name";

else

	docker run -it --rm --net 262-network --link kafka spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic $1;

fi
