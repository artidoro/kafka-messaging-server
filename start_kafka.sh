## Cleaning up environment
docker stop $(docker ps -aq) && docker rm $(docker ps -aq) \
docker network rm 262-network \
docker volume rm db

docker network create --subnet=172.18.0.0/16 262-network && \

docker volume create --name db \

docker run --rm -it -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=kafka --env ADVERTISED_PORT=9092 --env KAFKA_LOG4J_ROOT_LOGLEVEL=DEBUG --net 262-network --hostname kafka --ip='172.18.0.22' --name kafka spotify/kafka
