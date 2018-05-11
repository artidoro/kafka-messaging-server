import socket
import sys

## starting code for frontend server should come here

server_port = sys.argv[1]

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('', int(server_port)))
server_socket.listen(5)

while True:
    client_sock, address = server_socket.accept()
