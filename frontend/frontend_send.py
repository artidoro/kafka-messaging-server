# frontend_send.py
# server_send.py

import struct
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages


version = b'\x01'


def general_failure(conn, failure_msg):
    """
    Function that sends failure_msg to client. Packs the message according to network endianness.

    :param conn: socket object
    :param failure_msg: message content, in byte format
    :return:
    """
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(failure_msg)), failure_msg)
    # send message to client
    handle_messages.send_message(version, b'\x22', raw_payload, conn)
    return


def message_transfer(conn, msg):
    """
    Function that sends msg to client. Assumes msg is already packed with the network endianness and contains the header
    the version and the payload length.

    :param conn: socket object
    :param msg: message content, in byte format
    :return:
    """
    # send message to client
    handle_messages.send_bytes(conn, msg, len(msg))
    return


# def create_success(conn, token):
#     """
#     Function that sends account creation success message back to client. The message body contains the unique token.
#     The message body is packed following the network endianness.
#
#     :param conn: socket object
#     :param token: byte string, unique user token
#     :return:
#     """
#     # pack token to send back to client
#     raw_payload = struct.pack('!{}s'.format(len(token)), token)
#     # send message to client
#     handle_messages.send_message(version, b'\x11', raw_payload, conn)
#     return
#
#
# def login_success(conn, token):
#     """
#     Function that sends log in success message back to client. The message body contains the unique token.
#     The message body is packed following the network endianness.
#
#     :param conn: socket object
#     :param token: byte string, unique user token
#     :return:
#     """
#     # pack token to send back to client
#     raw_payload = struct.pack('!{}s'.format(len(token)), token)
#     # send message to client
#     handle_messages.send_message(version, b'\x21', raw_payload, conn)
#     return
#
#
# def send_success(conn):
#     """
#     Function that sends success message back to client. This function is called when the target of a message is logged
#     in and the message is delivered directly to the user. The message is packed with network endianness.
#
#     :param conn: socket object
#     :return:
#     """
#     success_msg = b'\tmessage delivered to logged in user.'
#     raw_payload = struct.pack('!{}s'.format(len(success_msg)), success_msg)
#     handle_messages.send_message(version, b'\x71', raw_payload, conn)
#     return


# def message_alert(conn, sender, message_body):
#     """
#     Function that sends message to target when the target user is online.
#     Message payload format is <sender_name_length><sender_name><message_body>
#
#     :param conn: socket connection to *recipient* (not sender)
#     :param sender: sender username, in byte format
#     :param message_body: message content, in byte format
#     :return:
#     """
#     sender_length = len(sender)
#     message_length = len(message_body)
#     # construct message payload
#     raw_payload = struct.pack('!I{}s{}s'.format(sender_length, message_length), sender_length, sender, message_body)
#     handle_messages.send_message(version, b'\x72', raw_payload, conn)
#     return

#
# def list_success(conn, user_names):
#     """
#     Function that sends the list of all users to the client. Converts the list to a bytestrings and packs it according
#     to the network endianness.
#
#     :param conn: socket object
#     :param user_names: user names list
#     :return:
#     """
#     # get the usernames int the user_names list in a single string
#     user_names = b'; '.join(user_names)
#     # pack token to send back to client
#     raw_payload = struct.pack('!{}s'.format(len(user_names)), user_names)
#     # send message to client
#     handle_messages.send_message(version, b'\x51', raw_payload, conn)
#     return
#
#
# def retrieve_success(conn, queue):
#     """
#     Function that sends the list of all users to the client. Converts the list to a bytestrings and packs it according
#     to the network endianness. Note that a message is a tuple of (user: message)
#
#     :param conn: socket object
#     :param queue: list of queued messages
#     :return:
#     """
#     # get the messages int the queue in a single string
#     messages = b'\n\n'.join(map(lambda l: b': '.join(list(l)), queue))
#     # pack token to send back to client
#     raw_payload = struct.pack('!{}s'.format(len(messages)), messages)
#     # send message to client
#     handle_messages.send_message(version, b'\x61', raw_payload, conn)
#     return
