docker build -f Dockerfile.client -t client . && \

docker run -it --rm --net 262-network --add-host frontend:172.18.0.10 client
