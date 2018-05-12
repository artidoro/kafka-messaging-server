import socket
import sys
import os
import uuid
import threading
import struct
from kafka import KafkaProducer, KafkaConsumer

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

class FrontendServer():

    def __init__(self):
        self.sockets = {}
        self.producer = KafkaProducer(bootstrap_servers='kafka:9092', api_version=(0,10,1))
        self.consumers = {}
        self.counter = 0

    def message_handler(self, sock):
        """
        Function that listens on socket connection and handles messages.
        i) Upon reception of a message, it is posted to Kafka under the frontend's topic;
        ii) Upon consumption of message from kafka topic corresponding to user, send consumed msg to user
        :param sock: socket object
        :return:
        """
        username = None

        while True:
            try:
                header, payload = handle_messages.recv_message(sock)

                if header[2] == b'\x10' or header[2] == b'\x20':
                    username, = struct.unpack('!{}s'.format(header[1]), payload)
                    username = username.decode()
                    if username not in self.sockets.keys():
                        self.sockets[username] = sock
                    if username not in self.consumers.keys():
                        self.spawn_consumer(username)
                else:
                    for u in self.sockets.keys():
                        if self.sockets[u] == sock:
                            username = u
                
                core = header[0] + struct.pack('!I', header[1]) + header[2] + payload
                username_conv = str.encode(username, 'utf-8')
                msg = core + struct.pack('!{}s'.format(len(username_conv)), username_conv)
                future = self.producer.send(topic='frontend1', value=msg)
                result = future.get(timeout=30)
                payload_size = header[1]

                print("---> Hello {} !!!!!\n".format(username))


            except socket.error as e:
                pass
                #print("Nothing has been received from this user!")
            except Exception as e:
                # remove this user from self.consumers
                print(e)
                print("A user has been disconnected.")
                sock.close()
                del self.sockets[username]
                return

            try:
                consumer = self.consumers[username]
                self.consume_msgs(consumer, username)
            except Exception as e:
                pass

    def spawn_consumer(self, topic):
        consumer = KafkaConsumer(bootstrap_servers='kafka:9092', api_version=(0,10,1), auto_offset_reset='earliest', group_id=None)
        consumer.subscribe([topic])
        self.consumers[topic] = consumer


    def consume_msgs(self, consumer, username):
        #print("Trying to consume for topic: {}".format(username))

        msgs_dict = consumer.poll()
        #print(messages)
        if not msgs_dict:
            return

        for _, messages in msgs_dict.items():
            for message in messages:
                msg = message.value
                print("front consuming msg: {}".format(msg))
                sock = self.sockets[username]
                handle_messages.send_bytes(sock, msg, len(msg))


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("ERROR: Usage 'python3 ./frontend/frontend.py <port>'")
        sys.exit()
    
    # setup frontend
    frontend = FrontendServer()

    # get the port
    server_port = sys.argv[1]

    # setup socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(('', int(server_port)))
    server_sock.listen(5)	 # param represents the number of queued connections

    while True:
        # for every new connection create a new thread
        client_sock, address = server_sock.accept()
        client_sock.setblocking(0)

        print("Connected to a new user.")
        threading.Thread(target=frontend.message_handler, args=(client_sock,), daemon=True).start()
