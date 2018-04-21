# handle_messages.py

import os
import sys
import struct
import socket

sys.path.append(os.path.join(os.path.dirname(__file__), '../server/'))
import server_data

HEADER_LEN = 6
MAX_SOCKET_BYTES = 4096


def send_message(version, opcode, raw_payload, conn):
    """
    Helper function that sends a raw_payload through the connection. We handle and size of messages.
    Packs the message according to the protocol: <version><payload_size><opcode><raw_payload>
    raw_payload should be packed according to the network endianness.

    :param version: byte, indicating version
    :param opcode: byte, indicating opcode
    :param raw_payload: byte string
    :param conn: socket object
    :return:
    """
    payload_size = len(raw_payload)
    # pack the message
    message = version + struct.pack('!I', payload_size) + opcode + raw_payload
    # send it
    send_bytes(conn, message, len(message))


def recv_message(sock):
    """
    Helper function to receive a message. Will parse header first and then receive the rest of the message with another
    helper function. We handle any size of messages.

    :param sock: socket object
    :return: header (tuple), payload (byte string)
    """
    # receive header
    header_data = recv_bytes(sock, HEADER_LEN)
    header = struct.unpack('!cIc', header_data)

    # unpack header
    payload_size = header[1]
    # receive the rest of the message
    payload = recv_bytes(sock, payload_size)

    return header, payload


def recv_bytes(sock, num_bytes):
    """
    Helper function that receives variable size message. If the connection is down logs user out and kills thread.

    :param sock: socket object
    :param num_bytes: int
    :return: byte string
    """
    remaining = num_bytes
    data = b''

    # keep receiving until you are done with supposed message length
    while remaining > 0:
        to_receive = min(remaining, MAX_SOCKET_BYTES)
        buf = sock.recv(to_receive)

        if len(buf) == 0:
            raise Exception("Closed connection")

        remaining = remaining - len(buf)
        data = data + buf
    return data


def send_bytes(sock, buf, num_bytes):
    """
    Helper function that sends a variable size message through the socket.

    :param sock: socket object
    :param buf: byte string
    :param num_bytes: int
    :return:
    """
    assert(num_bytes <= len(buf))
    total_sent = 0

    # keep sending until all the bytes are sent
    while total_sent < num_bytes:
        bytes_sent = sock.send(buf[total_sent:])
        total_sent = total_sent + bytes_sent
