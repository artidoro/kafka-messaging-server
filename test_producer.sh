docker build -f Dockerfile.test_producer -t test_producer . && \

docker run -it --rm --net 262-network --add-host kafka:172.18.0.22 --name test_producer test_producer
