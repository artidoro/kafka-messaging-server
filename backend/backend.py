import socket
import struct
import sys
import threading
import os
from kafka import KafkaProducer, KafkaConsumer
import backend_data
import backend_receive
import backend_send

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

# define header length, version code and op codes
HEADER_LEN = 6
version = b'\x01'

# opcode associations; note that these opcodes will be sent by the client
opcodes = {
    b'\x10': backend_receive.create_request,
    b'\x20': backend_receive.login_request,
    b'\x30': backend_receive.logout_request,
    b'\x40': backend_receive.delete_request,
    b'\x50': backend_receive.list_request,
    b'\x60': backend_receive.retrieve_request,
    b'\x70': backend_receive.send_request,
}

frontend_topics = {1: 'frontend1'}

class BackendServer():

    def __init__(self):
        self.consumers = {}
        self.producer = KafkaProducer(bootstrap_servers='kafka:9092', api_version=(0,10,1))
        for f_id, topic in frontend_topics.items():
            self.consumers[f_id] = KafkaConsumer(topic, bootstrap_servers='kafka:9092', api_version=(0,10,1))


    def message_handler(self, lock):
        """
        Function that listens on socket connection and handles messages.

        Upon reception of a message the message header is parsed and unpacked and the message is redirected to the
        appropriate function handler. The function checks that the message is using the correct version of the protocol.
        We only support version 1. If the connection is down the function will close the current socket and log the user
        out.

        :param sock: socket object
        :return:
        """
        while True:
            # Retrieve header data
            for f_id, consumer in self.consumers.items():
                try:
                    self.consume_msgs(consumer, lock)
                except:
                    continue


    def consume_msgs(self, consumer, lock):
        
        for msg in consumer:
            
            message = msg.value
            print(message)

            ## do processing here
            header_data = message[:HEADER_LEN]
            header = struct.unpack('!cIc', header_data)
            payload_version = header[0]
            payload_size = header[1]
            opcode = header[2]
            payload = message[HEADER_LEN:(HEADER_LEN + payload_size)]
            username_len = len(message) - HEADER_LEN - payload_size
            username, = struct.unpack('!{}s'.format(username_len), message[(HEADER_LEN + payload_size):])
            username = username.decode()

            print("For this request... username is ->>> {}".format(username))

            # Version does not match: for now, log user out!
            if payload_version != version:
                print("Version number did not match. The user will be disconnected.")
                backend_receive.logout_user(lock, username)
                return

            # Try to send packet to correct handler
            try:
                print("CALLING OPCODE: username -> {} | opcode -> {} |payload_size -> {} | payload -> {}".format(username, opcode, payload_size, payload))
                opcodes[opcode](self.producer, username, payload_size, payload, lock)
            except Exception as e:
                print(e)
                print("Error while handling request. The user has been disconnected.")
                backend_receive.logout_user(lock, username)
                return


if __name__ == '__main__':

    backend = BackendServer()

    # lock that threads will acquire to access user data base in backend_data
    data_lock = threading.Lock()

    handler_thread = threading.Thread(target=backend.message_handler, args=(data_lock,), daemon=True)

    handler_thread.start()
    handler_thread.join()