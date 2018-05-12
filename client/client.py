# client.py

import socket
import struct
import sys
import time
import threading
import os

import client_data
import client_receive
import client_send

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages

# define header length, version code
HEADER_LEN = 6
version = b'\x01'

# opcode associations; note that these opcodes will be returned by the server
opcodes = {
    b'\x11': client_receive.create_success,
    b'\x21': client_receive.login_success,
    b'\x22': client_receive.general_failure,
    b'\x51': client_receive.list_success,
    b'\x61': client_receive.retrieve_success,
    b'\x71': client_receive.send_success,
    b'\x72': client_receive.message_alert,
}


def init_input():
    """
    Helper function displaying the main menu and retrieving the option selected by user. The function will not return
    until the user enters the correct input.

    :return:
    """
    # loop until correct user input
    while True:
        print('CONNECTED TO MESSAGING SERVER - type the number of a function:\n'
              '    (1) Create Account\n'
              '    (2) Log In\n'
              '    ')
        # receive user input
        opt = input('>> ')

        # exit function if correct user input
        if opt in ['1', '2']:
            break
        else:
            print('INPUT ERROR: please input numbers 1 or 2')
    return opt


def loggedin_input():
    """
    Helper function displaying the main menu and retrieving the option selected by user. The function will not return
    until the user enters the correct input.

    :return:
    """
    # loop until correct user input
    while True:
        print('LOGGED IN TO MESSAGING SERVER - type the number of a function:\n'
              '    (1) Read Message Inbox\n'
              '    (2) Send Message\n'
              '    (3) List Users\n'
              '    (4) Log Out\n'
              '    (5) Delete Account\n'
              '    ')
        # receive user input
        opt = input('>> ')

        # exit function if correct user input
        if opt in [str(i + 1) for i in range(5)]:
            break
        else:
            print('INPUT ERROR: please input numbers 1, 2, 3, 4 or 5')
    return opt


def process_init_input(input_buffer, client_sock):
    """
    Helper function that executes appropriate request based on the option selected by the user during init_input().

    :param input_buffer: string, option selected by user
    :param client_sock: socket object
    :return:
    """
    # create new account
    if input_buffer == str(1):
        client_send.create_request(client_sock)

    # log in
    elif input_buffer == str(2):
        client_send.login_request(client_sock)

    else:
        return


def process_loggedin_input(input_buffer, client_sock, data_lock):
    """
    Helper function that executes appropriate request based on the option selected by the user during loggedin_input().

    :param input_buffer: string, option selected by user
    :param client_sock: socket object
    :param data_lock: threading.lock
    :return:
    """
    # read message inbox
    if input_buffer == str(1):
        client_send.retrieve_request(client_sock)

    # send a new message
    elif input_buffer == str(2):
        client_send.send_request(client_sock)

    # list all users
    elif input_buffer == str(3):
        client_send.list_request(client_sock)

    # log out
    elif input_buffer == str(4):
        client_send.logout_request(client_sock, data_lock)

    # delete account
    elif input_buffer == str(5):
        client_send.delete_request(client_sock, data_lock)
    
    else:
        return


def message_handler(conn, lock, return_on_message=False):
    """
    Function that listens on socket connection and handles messages.

    Upon reception of a message the message header is parsed and unpacked and the message is redirected to the
    appropriate function handler. The function checks that the message is using the correct version of the protocol.
    We only support version 1. If the connection is down the function will log the user out by resetting the token to
    None.

    :param sock: socket object
    :param lock: threading.lock
    :return:
    """
    while True:
        # receive message
        try:
            header, payload = handle_messages.recv_message(conn)
            print('MESSAGE HERE: {} {}'.format(header, payload))
        except:
            with lock:
                client_data.token = None
            sys.exit()

        # Unpack the header
        payload_version = header[0]
        payload_size = header[1]
        opcode = header[2]

        # Version did not match
        if version != payload_version:
            client_data.token = None
            break

        # Try to send packet to correct handler
        opcodes[opcode](conn, payload_size, payload, lock)

        # If required, thread exits upon completion
        if return_on_message:
            return


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("ERROR: Usage 'python3 client.py <host> <port>'")
        sys.exit()

    # get the address of the server
    server_addr = sys.argv[1]
    server_port = sys.argv[2]

    # initialize data lock, threads will acquire this lock to access the connection token
    data_lock = threading.Lock()

    while True:
        # create socket and connect to it
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            client_sock.connect((server_addr, int(server_port)))
        except Exception as e:
            print("ERROR: could not connect to {}:{}\nError: {}".format(server_addr, server_port, e))
            sys.exit()

        # if not logged in present main menu
        while client_data.token is None:
            # print menu and get selection
            input_buffer = init_input()
            # broadcast request to server
            process_init_input(input_buffer, client_sock)
            # handle response
            message_handler(client_sock, data_lock, return_on_message=True)

        # spin off thread that keeps listening for server messages and handles them
        threading.Thread(target=message_handler, args=(client_sock, data_lock), daemon=True).start()

        # once logged in present secondary menu
        while client_data.token is not None:
            # print menu and get selection
            input_buffer = loggedin_input()
            # broadcase request to server
            process_loggedin_input(input_buffer, client_sock, data_lock)
            time.sleep(1)

        # if logged out, go back to login menu (init_input)
