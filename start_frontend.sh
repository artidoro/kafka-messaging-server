docker build -f Dockerfile.frontend -t frontend . && \

docker run --rm -it --net 262-network --ip='172.18.0.10' --name frontend frontend
