# frontend.py

import socket
import struct
import sys
import threading
import os
import kafka

import frontend_receive
import frontend_send
import frontend_data

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

# define header length, version code and op codes
HEADER_LEN = 6
version = b'\x01'

# opcode associations; note that these opcodes will be sent by the client
opcodes = {
    b'\x10': frontend_receive.transfer,  # create_request
    b'\x20': frontend_receive.login,  # login_request
    b'\x30': frontend_receive.logout_request,
    b'\x40': frontend_receive.transfer,  # delete_request
    b'\x50': frontend_receive.transfer,  # list_request
    # b'\x60': server_receive.retrieve_request
    b'\x70': frontend_receive.transfer  # send_request
}


def kafka_message_handler(sock, lock, user_name):
    """

    :param sock: socket object
    :param lock: threading.lock
    :param user_name
    :return:
    """

    consumer = kafka.KafkaConsumer(user_name,
                                   bootstrap_servers=[frontend_data.broker_host + ":" + frontend_data.broker_port])
    # keep reading off messages from the consumer as they arrive
    for message in consumer:
        # we transfer messages directly to the client, they already have the appropriate header
        try:
            frontend_send.message_transfer(sock, message.value)
        except:
            print("A user has been disconnected.")
            frontend_receive.logout_user(lock)
            # TODO make sure the other thread also dies
            sock.close()
            return
        header = struct.unpack('!cIc', message.value[:handle_messages.HEADER_LEN])
        # if the message was a delete request, log the user out after sending the relative request to the user
        if header[2] == b'\x40':
            frontend_receive.logout_user(lock)
            sock.close()
            return

    return


def client_message_handler(sock, lock, producer, frontend_id):
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
            frontend_receive.logout_user(lock)
            # TODO make sure the other thread also dies
            sock.close()
            return

        payload_version = header[0]
        payload_size = header[1]
        opcode = header[2]

        # Version does not match: for now, log user out!
        if payload_version != version:
            print("Version number did not match. The user has been disconnected.")
            frontend_receive.logout_user(lock)
            # TODO make sure the other thread also dies
            sock.close()
            return

        # Try to send packet to correct handler
        try:
            opcodes[opcode](sock, header, payload_size, payload, producer, frontend_id, lock)
        except KeyError:
            print("Error while handling request. The user has been disconnected.")
            frontend_receive.logout_user(lock)
            # TODO make sure the other thread also dies
            sock.close()
            return


if __name__ == '__main__':
    if len(sys.argv) != 5:
        print("ERROR: Usage 'python3 frontend.py <port> <frontend_id> <broker_host> <broker_port>'")
        sys.exit()

    # parse arguments
    server_port = sys.argv[1]
    frontend_id = sys.argv[2]
    broker_host = sys.argv[3]
    broker_port = sys.argv[4]

    frontend_data.broker_host = broker_host
    frontend_data.broker_port = broker_port

    # setup socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', int(server_port)))
    server_sock.listen(5)	 # param represents the number of queued connections

    # Setup a kafka producer
    producer = kafka.KafkaProducer(bootstrap_servers=[broker_host + ':' + broker_port])
    # todo is there a way to check that the connection was actually successful?

    # lock that threads will acquire to access user data base in frontend_data
    data_lock = threading.Lock()

    while True:
        # for every new connection create a new threads
        client_sock, address = server_sock.accept()

        print("Connected to a new client.")
        # start thread handeling requests from the client, and producing to kafka
        threading.Thread(target=client_message_handler, args=(client_sock, data_lock, producer, frontend_id), daemon=True).start()
