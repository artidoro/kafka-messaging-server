if [ -z "$1" ]; then

	echo "Usage: sh spawn_consumer.sh topic_name"; 
	
else

	docker run -it --rm --net 262-network --link kafka spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $1 --from-beginning;

fi
