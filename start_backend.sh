docker build -f Dockerfile.backend -t backend . && \

docker run --rm -it -v db:/usr/src/app/db --net 262-network --name backend backend
