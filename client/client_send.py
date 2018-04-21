# client_send.py

import socket
import struct
import sys
import os

import client_data

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

HEADERLEN = 6
ENCODING = 'utf-8'  # encoding format for strings
version = b'\x01'


def create_request(conn):
    """
    Function that sends request to create a new account. Prompts user for new username, converts to a string using the
    network endianness, and sends request to server

    :param conn: socket object
    :return:
    """
    print("CREATING AN ACCOUNT")
    # get username
    user_name, length = get_input(prompt='pick a username: \n')
    # constructs raw payload of chars
    raw_payload = struct.pack('!{}s'.format(length), bytes(user_name, ENCODING))
    # send message to server
    handle_messages.send_message(version, b'\x10', raw_payload, conn)

    return


def delete_request(conn, data_lock):
    """
    Function that generates request to delete account, and logs out by shutting the socket down and resetting client
    global token to None. Note that the message body is empty.

    :param conn: socket object
    :param data_lock: threading.lock
    :return:
    """
    # no payload for delete user account
    raw_payload = b''

    # send delete message to server
    handle_messages.send_message(version, b'\x40', raw_payload, conn)

    # close the socket
    conn.shutdown(socket.SHUT_RDWR)
    conn.close()

    # reset client global token to None
    with data_lock:
        client_data.token = None

    print("ACCOUNT SUCCESSFULLY DELETED\n")

    return


def login_request(conn):
    """
    Function that sends request to log in. Prompts user for username, converts to a string using the network
    endianness, and sends request to server

    :param conn: socket object
    :return:
    """
    print("LOGGING YOU IN")

    # get username
    user_name, length = get_input(prompt='enter your username:: \n')
    # constructs raw payload of chars
    raw_payload = struct.pack('!{}s'.format(length), bytes(user_name, ENCODING))
    # send message to server
    handle_messages.send_message(version, b'\x20', raw_payload, conn)

    return


def logout_request(conn, data_lock):
    """
    Function that logs out client. Shuts the socket down and resets client global token to None.

    :param conn: socket object
    :param data_lock: threading.lock
    :return:
    """
    # no payload for logout user account
    raw_payload = b''

    # send logout message to server
    handle_messages.send_message(version, b'\x30', raw_payload, conn)

    # close the socket
    conn.shutdown(socket.SHUT_RDWR)
    conn.close()

    # reset client global token to None
    with data_lock:
        client_data.token = None


    print("LOGOUT SUCCESSFUL\n")

    return


def send_request(conn):
    """
    Function that prompts user to get recipient and message body, converts message to network endianness and sends it to
    server.
    After the 6-byte header, the message payload format is <recipient_name_length><recipient_name><message_body>

    :param conn: client socket connection with server
    :return:
    """
    # input recipient username
    recipient, recipient_len = get_input(prompt='Enter recipient name: \n')

    # input message content
    message_body, message_len = get_input(prompt='Enter message: \n')

    # pack message payload
    # message format is: <recipient_name_length><recipient_name><message_body>
    recipient = bytes(recipient, ENCODING)
    message_body = bytes(message_body, ENCODING)
    raw_payload = struct.pack('!I{}s{}s'.format(recipient_len, message_len), recipient_len, recipient, message_body)
    handle_messages.send_message(version, b'\x70', raw_payload, conn)

    return


def retrieve_request(conn):
    """
    Function that sends request to retrieve queued messages from the server. Note that the message body is empty

    :param conn: socket object
    :return:
    """
    print("GETTING UNREAD MESSAGES\n")

    # no payload for getting unread messages
    raw_payload = struct.pack('!{}s'.format(0), b'')
    # send message to server
    handle_messages.send_message(version, b'\x60', raw_payload, conn)

    return


def list_request(conn):
    """
    Function that sends request to list users in the messaging app. Note that the message body is empty

    :param conn: socket object
    :return:
    """
    print("GETTING USER NAMES")

    # no payload for getting user names
    raw_payload = struct.pack('!{}s'.format(0), b'')
    # send message to server
    handle_messages.send_message(version, b'\x50', raw_payload, conn)

    return


def get_input(prompt=''):
    """
    Helper function for getting client text input

    :param prompt: string, what will be prompted to the user
    :return: (input string, input length)
    """
    while True:
        try:
            # get user input
            input_buffer = input(prompt + '>> ')
        except ValueError:
            continue
        # check size of user input
        length = len(input_buffer)

        return input_buffer, length


