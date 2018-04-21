# client_receive.py

import struct

import client_data

ENCODING = 'utf-8'  # encoding format for strings


def create_success(conn, payload_length, raw_payload, lock):
    """
    Function that handles successful account creation messages from server. Sets token identifier with value provided
    by server.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    # unpack the message
    token = struct.unpack('!{}s'.format(payload_length), raw_payload)[0]

    # update global client session token to indicate being logged in
    with lock:
        client_data.token = token

    # communicate success to user
    print('Account successfully created!\n')
    return


def login_success(conn, payload_length, raw_payload, lock):
    """
    Function that handles successful login messages from server. Sets token identifier with value provided
    by server.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """

    # unpack the message
    token = struct.unpack('!{}s'.format(payload_length), raw_payload)[0]

    # update global client session token to indicate being logged in
    with lock:
        client_data.token = token

    # communicate success to user
    print('Successfully logged in!\n')
    return


def retrieve_success(conn, payload_length, raw_payload, lock):
    """
    Function that handles response from server containing unread messages, following retrieve request.
    Displays the decoded messages to the screen.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    # unpack the message
    msg_list = struct.unpack('!{}s'.format(payload_length), raw_payload)[0]
    # decode message
    msg_list = msg_list.decode(ENCODING)
    print(msg_list + '\n')
    return


def list_success(conn, payload_length, raw_payload, lock):
    """
    Function that handles response from server containing list of users on the messaging app. Displays the decoded
    list of users to the screen.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    # unpack the message
    user_list = struct.unpack('!{}s'.format(payload_length), raw_payload)[0]
    # decode message
    user_list = user_list.decode(ENCODING)
    print(user_list + '\n')
    return


def send_success(conn, payload_length, raw_payload, lock):
    """
    Function that handles server response notifying successful delivery to connected user. Displays the decoded
    success message to the screen.


    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    success_msg, = struct.unpack('!{}s'.format(payload_length), raw_payload)
    success_msg = success_msg.decode(ENCODING)
    print(success_msg + '\n')
    return


def message_alert(conn, payload_length, raw_payload, lock):
    """
    Function that handles server message containing message from another user. This function is called when a message
    is delivered to the client because the client is online.

    The structure of the message received is: <sender_name_length><sender_name><message_body>

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    # unpack the sender name length
    sender_length, = struct.unpack('!I', raw_payload[:4])
    # length mismatch means some encoding error happened
    if sender_length >= payload_length - 4:
        print('Client: error decoding incoming message. ')
        return

    # parse message payload
    message_len = payload_length - 4 - sender_length

    # parse sender name and message body (both in byte format)
    sender, message_body = struct.unpack('!{}s{}s'.format(sender_length, message_len), raw_payload[4:])

    # decode messages to strings
    sender, message_body = sender.decode(ENCODING), message_body.decode(ENCODING)
    print('New message from {}: \n\t{}'.format(sender, message_body))

    return


def general_failure(conn, payload_length, raw_payload, lock):
    """
    Function that handles general failure messages from the server. The failure message is decoded and displayed.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    failure_msg, = struct.unpack('!{}s'.format(payload_length), raw_payload)
    failure_msg = failure_msg.decode(ENCODING)
    print('Request error: \t{}'.format(failure_msg) + '\n')
