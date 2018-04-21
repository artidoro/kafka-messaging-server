# server.py

import socket
import struct
import sys
import threading
import os

import server_data
import server_receive
import server_send

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

# define header length, version code and op codes
HEADER_LEN = 6
version = b'\x01'

# opcode associations; note that these opcodes will be sent by the client
opcodes = {
    b'\x10': server_receive.create_request,
    b'\x20': server_receive.login_request,
    b'\x30': server_receive.logout_request,
    b'\x40': server_receive.delete_request,
    b'\x50': server_receive.list_request,
    b'\x60': server_receive.retrieve_request,
    b'\x70': server_receive.send_request,
}


def message_handler(sock, lock):
    """
    Function that listens on socket connection and handles messages.

    Upon reception of a message the message header is parsed and unpacked and the message is redirected to the
    appropriate function handler. The function checks that the message is using the correct version of the protocol.
    We only support version 1. If the connection is down the function will close the current socket and log the user
    out.

    :param sock: socket object
    :param lock: threading.lock
    :return:
    """
    while True:
        # Retrieve header data
        try:
            header, payload = handle_messages.recv_message(sock)
        except:
            print("A user has been disconnected.")
            server_receive.logout_user(lock)
            sock.close()
            return

        payload_version = header[0]
        payload_size = header[1]
        opcode = header[2]

        # Version does not match: for now, log user out!
        if payload_version != version:
            print("Version number did not match. The user has been disconnected.")
            server_receive.logout_user(lock)
            sock.close()
            return

        # Try to send packet to correct handler
        try:
            opcodes[opcode](sock, payload_size, payload, lock)
        except KeyError:
            print("Error while handling request. The user has been disconnected.")
            server_receive.logout_user(lock)
            sock.close()
            return


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("ERROR: Usage 'python3 client.py <port>'")
        sys.exit()

    # get the port
    server_port = sys.argv[1]

    # setup socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', int(server_port)))
    server_sock.listen(5)	 # param represents the number of queued connections

    # lock that threads will acquire to access user data base in server_data
    data_lock = threading.Lock()

    while True:
        # for every new connection create a new thread
        client_sock, address = server_sock.accept()

        print("Connected to a new user.")
        threading.Thread(target=message_handler, args=(client_sock, data_lock), daemon=True).start()
