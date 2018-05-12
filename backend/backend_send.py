# server_send.py

import struct
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

version = b'\x01'


def create_success(producer, topic, token):
    """
    Function that sends account creation success message back to client. The message body contains the unique token.
    The message body is packed following the network endianness.

    :param topic: topic object
    :param token: byte string, unique user token
    :return:
    """
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(token)), token)
    # send message to client
    handle_messages.produce_message(producer, topic, version, b'\x11', raw_payload)
    return


def login_success(producer, topic, token):
    """
    Function that sends log in success message back to client. The message body contains the unique token.
    The message body is packed following the network endianness.

    :param topic: topic object
    :param token: byte string, unique user token
    :return:
    """
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(token)), token)
    # send message to client
    handle_messages.produce_message(producer, topic, version, b'\x21', raw_payload)
    return


def send_success(producer, topic):
    """
    Function that sends success message back to client. This function is called when the target of a message is logged
    in and the message is delivered directly to the user. The message is packed with network endianness.

    :param topic: topic object
    :return:
    """
    success_msg = b'\tmessage delivered to logged in user.'
    raw_payload = struct.pack('!{}s'.format(len(success_msg)), success_msg)
    handle_messages.produce_message(producer, topic, version, b'\x71', raw_payload)
    return


def message_alert(producer, topic, sender, message_body):
    """
    Function that sends message to target when the target user is online.
    Message payload format is <sender_name_length><sender_name><message_body>

    :param topic: topic connection to *recipient* (not sender)
    :param sender: sender username, in byte format
    :param message_body: message content, in byte format
    :return:
    """
    sender_length = len(sender)
    message_length = len(message_body)
    # construct message payload
    raw_payload = struct.pack('!I{}s{}s'.format(sender_length, message_length), sender_length, sender, message_body)
    handle_messages.produce_message(producer, topic, version, b'\x72', raw_payload)
    return


def general_failure(producer, topic, failure_msg):
    """
    Function that sends failure_msg to client. Packs the message according to network endianness.

    :param topic: topic object
    :param failure_msg: message content, in byte format
    :return:
    """
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(failure_msg)), failure_msg)
    # send message to client
    handle_messages.produce_message(producer, topic, version, b'\x22', raw_payload)
    return


def list_success(producer, topic, user_names):
    """
    Function that sends the list of all users to the client. Converts the list to a bytestrings and packs it according
    to the network endianness.

    :param topic: topic object
    :param user_names: user names list
    :return:
    """
    # get the usernames int the user_names list in a single string
    user_names = b'; '.join(user_names)
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(user_names)), user_names)
    # send message to client
    handle_messages.produce_message(producer, topic, version, b'\x51', raw_payload)
    return


def retrieve_success(producer, topic, queue):
    """
    Function that sends the list of all users to the client. Converts the list to a bytestrings and packs it according
    to the network endianness. Note that a message is a tuple of (user: message)

    :param topic: topic object
    :param queue: list of queued messages
    :return:
    """
    # get the messages int the queue in a single string

    messages = b'\n\n'.join(map(lambda l: b': '.join(list(l)), queue))
    # pack token to send back to client
    raw_payload = struct.pack('!{}s'.format(len(messages)), messages)
    # send message to client
    handle_messages.produce_message(producer, topic, version, b'\x61', raw_payload)
    return
