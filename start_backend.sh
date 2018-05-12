docker build -f Dockerfile.backend -t backend . && \

docker run --rm -it --net 262-network --name backend backend
