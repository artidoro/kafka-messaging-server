# frontend.py

import socket
import struct
import sys
import threading
import os
import kafka

import frontend_receive
import frontend_data

print("before main")

# sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
# import handle_messages

# define header length, version code and op codes
HEADER_LEN = 6
version = b'\x01'

# opcode associations; note that these opcodes will be sent by the client
opcodes = {
    b'\x10': frontend_receive.transfer_to_kafka,  # create_request
    b'\x20': frontend_receive.login_request,  # login_request
    b'\x30': frontend_receive.logout_request,
    b'\x40': frontend_receive.transfer_to_kafka,  # delete_request
    b'\x50': frontend_receive.transfer_to_kafka,  # list_request
    # b'\x60': server_receive.retrieve_request
    b'\x70': frontend_receive.transfer_to_kafka  # send_request
}


if __name__ == '__main__':
    print("entering main")
    if len(sys.argv) != 5:
        print("ERROR: Usage 'python3 frontend.py <port> <frontend_id> <broker_host> <broker_port>'")
        sys.exit()

    # parse arguments
    frontend_port = sys.argv[1]
    frontend_id = sys.argv[2]
    broker_host = sys.argv[3]
    broker_port = sys.argv[4]

    frontend_data.broker_host = broker_host
    frontend_data.broker_port = broker_port

    print("setup socket")

    # setup socket
    frontend_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    frontend_sock.bind(('', int(frontend_port)))
    frontend_sock.listen(5)	 # param represents the number of queued connections

    # Setup a kafka producer
    print("Setup Kafka producer")
    # producer = kafka.KafkaProducer(bootstrap_servers=[broker_host + ':' + broker_port])
    # todo is there a way to check that the connection was actually successful?

    producer = 0
    # lock that threads will acquire to access user data base in frontend_data
    print("Setup threading lock")
    data_lock = threading.Lock()

    while True:
        print("start waiting for connection")
        # for every new connection create a new thread
        client_sock, address = frontend_sock.accept()

        print("Connected to a new client.")
        # start thread handeling requests from the client, and producing to kafka
        threading.Thread(target=frontend_receive.client_message_handler, args=(client_sock, data_lock, producer, frontend_id), daemon=True).start()
