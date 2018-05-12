import struct
import os.path
from kafka import KafkaProducer, KafkaConsumer
import backend_receive

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


class BackendServer:
    """
    Backend Server object. Contains a dictionary of consumers and a producer object.
    """

    def __init__(self):
        self.consumers = {}
        self.producer = KafkaProducer(bootstrap_servers='kafka:9092', api_version=(0, 10, 1))
        for f_id, topic in frontend_topics.items():
            self.consumers[f_id] = KafkaConsumer(topic, bootstrap_servers='kafka:9092', api_version=(0, 10, 1))

    def message_handler(self):
        """
        Message handler that continuously polls whether messages were received from kafka in an infinite loop.

        :return:
        """
        while True:
            # Retrieve header data
            for f_id, consumer in self.consumers.items():
                try:
                    self.consume_msgs(consumer)
                except:
                    continue

    def consume_msgs(self, consumer):
        """
        Upon reception of a message the message header is parsed and unpacked and the message is redirected to the
        appropriate function handler. The function checks that the message is using the correct version of the protocol.
        We only support version 1. If the connection is down the function will close the current socket and log the user
        out.

        :param consumer: kafka consumer object
        :return:
        """

        for msg in consumer:

            # parse message header and body
            message = msg.value
            header_data = message[:HEADER_LEN]
            header = struct.unpack('!cIc', header_data)
            payload_version = header[0]
            payload_size = header[1]
            opcode = header[2]
            payload = message[HEADER_LEN:(HEADER_LEN + payload_size)]
            token_len = len(message) - HEADER_LEN - payload_size
            token, = struct.unpack('!{}s'.format(token_len), message[(HEADER_LEN + payload_size):])
            token = token.decode()

            # Version does not match: for now, log user out!
            if payload_version != version:
                print("Version number did not match. The user will be disconnected.")
                backend_receive.logout_user(token)
                return

            # Try to send packet to correct handler
            try:
                opcodes[opcode](self.producer, token, payload_size, payload)
            except Exception as e:
                print(e)
                print("Error while handling request. The user has been disconnected.")
                backend_receive.logout_user(token)
                return


if __name__ == '__main__':
    # set up database if empty
    if not os.path.isfile('./db/data.db') or os.stat('./db/data.db').st_size == 0:
        backend_receive.save_obj(dict(), './db/data.db')

    # starts the back end server message handler
    backend = BackendServer()
    backend.message_handler()
