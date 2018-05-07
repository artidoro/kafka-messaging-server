import kafka
import os
import struct
import sys
import threading
import uuid


import frontend
import frontend_send
import frontend_data

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

# TODO
# upon receiving a message, if log in request set up a consumer thread
# TODO create new thread listening on right Kafka topic once logged in

# For all other messages produce the request to kafka


def consume_login(user_name):
    consumer = kafka.KafkaConsumer(user_name,
                                   bootstrap_servers=[frontend_data.broker_host + ":" + frontend_data.broker_port])
    for message in consumer:
        # get the message.value and check if it is the login success (should be just the header here)
        header = struct.unpack('!cIc', message.value[:handle_messages.HEADER_LEN])
        return header[2] == b'\x21'

    return


def transfer(conn, header, payload_length, raw_payload, producer, frontend_id, lock):
    print("Producing request for Kafka broker")
    message = header + raw_payload
    # this is asynchronous
    # TODO: check that this is what we want and handle this not working
    producer.send(frontend_id, message)
    return


def login_request(conn, header, payload_length, raw_payload, producer, frontend_id, lock):
    """
    Function that handles the request to log in.

    The function checks if the username exists and the user is already
    logged in with another connection. If the provided user name is valid, updates the entry in the global user
    database user_db. Uses lock to access the user_db. Generates new unique token to send back to the user, and sets
    the thread.local() variable with the user name provided. This will allow the thread to identify the user currently
    connected. Sends a success message back to the client or a failure message.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Login request.")
    # get username
    user_name = struct.unpack('!{}s'.format(payload_length), raw_payload)[0]
    with lock:
        # only allow one connection at a time for each user
        if user_name in frontend_data.user_db and frontend_data.user_db[user_name]['token'] is not None:
            failure_msg = b'USER ALREADY CONNECTED! '
            frontend_send.general_failure(conn, failure_msg)
            return

    # transfer request through Kafka to the backend
    transfer(conn, header, payload_length, raw_payload, producer, frontend_id, lock)

    # if successful verification of the username by backend through kafka
    if consume_login(user_name):
        # start a thread doing consuming
        threading.Thread(target=frontend.kafka_message_handler, args=(conn, lock, user_name), daemon=True).start()

        with lock:
            # create user session token using uuid (which may be overkill)
            token = uuid.uuid4().bytes
            # store user info in global database
            frontend_data.user_db[user_name] = {
                'socket': conn,
                'message_queue': [],  # initialize empty message queue for user
                'token': token,
            }
            # store thread local user name
            frontend_data.thread_local.user_name = user_name

        # send token back to user
        frontend_send.general_message(conn, token)

    else:
        # send general failure otherwise
        failure_msg = b'USERNAME ALREADY EXISTS! '
        frontend_send.general_failure(conn, failure_msg)
        return



def logout_request(conn, header, payload_length, raw_payload, producer, frontend_id, lock):
    """
    Function that handles the request to logout an account.

    Retrieves the username of the user on this connection
    from the thread.local() variable in server_data and resets the entry corresponding to the user in the user
    database user_db. Uses lock to access the user database.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Logout request. A user has been disconnected.")
    logout_user(lock)
    conn.close()
    sys.exit()
    # TODO kill the other thread if any (consumer)
    return


def logout_user(lock):
    """
    Helper function that logs the user on the present connection out, if not already logged out. Uses lock to access
    user database user_db, and gets the user name from the thread.local() variable. Logging out means setting token to
    None and socket to None in the user_db.
    :param lock: threading.lock
    :return:
    """
    with lock:
        # get username on this connection from thread_local
        user_name = getattr(frontend_data.thread_local, 'user_name', None)
        if user_name is not None:
            # update the user info in the userdb
            frontend_data.user_db[user_name]['token'] = None
            frontend_data.user_db[user_name]['socket'] = None
    return
