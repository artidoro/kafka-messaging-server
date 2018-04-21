# server_receive.py

import os
import struct
import sys
import uuid

import server_data
import server_send

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages


def create_request(conn, payload_length, raw_payload, lock):
    """
    Function that handles the request to create a new account.

    The function checks if the username already exists.
    If the provided user name is valid, creates a new entry in the global user database user_db. Uses lock to access
    the user_db. Generates new unique token to send back to the user, and sets the thread.local() variable with the
    user name provided. This will allow the thread to identify the user currently connected. Sends a success message
    back to the client or a failure message.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Create username request.")

    # get username
    user_name, = struct.unpack('!{}s'.format(payload_length), raw_payload)
    with lock:
        # check that username is valid
        if user_name in server_data.user_db.keys():
            failure_msg = b'USERNAME ALREADY EXISTS. TRY ANOTHER! '
            server_send.general_failure(conn, failure_msg)

        else:
            # create user session token using uuid (which may be overkill)
            token = uuid.uuid4().bytes
            # store user info in global database
            server_data.user_db[user_name] = {
                'socket': conn,
                'message_queue': [],  # initialize empty message queue for user
                'token': token,
            }
            # store thread local user name
            server_data.thread_local.user_name = user_name
            # send token back to user
            server_send.create_success(conn, token)
    return


def delete_request(conn, payload_length, raw_payload, lock):
    """
    Function that handles the request to delete an account.

    Retrieves the username of the user on this connection
    from the thread.local() variable in server_data and deletes the entry corresponding to the user in the user
    database user_db. Uses lock to access the user database. Resets the thread.local() variable to None.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Delete request.")
    with lock:
        # get username for this connection
        user_name = server_data.thread_local.user_name
        # delete user entry from db
        del server_data.user_db[user_name]
        # delete username from local thread var
        server_data.thread_local.user_name = None
    return


def logout_request(conn, payload_length, raw_payload, lock):
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
    return


def send_request(conn, payload_length, raw_payload, lock):
    """
    Function that handles send requests from the client.

    The function parses the message from the client to retrieve
    the username of the target user and the message body. The function then checks if the target user is connected
    by checking whether the user database contains a token and a socket object for the given user. Then there are two
    cases. If the target user is currently logged in, the function tries to send the message directly to the user. If
    this fails due to a connection error or if the user is not logged in the function adds the message to a queue
    stored in the user database. In the first case sends a success message back to the sender. Uses lock to access
    user database.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Send message request.")

    # parse payload
    # first, get recipient name length; error if length too long
    recipient_length, = struct.unpack('!I', raw_payload[:4])
    if recipient_length >= payload_length - 4:
        # send error message back to client
        server_send.general_failure(conn, b'MESSAGE PAYLOAD ENCODING ERROR. ')
        return

    # parse recipient name and message body (both in byte format)
    message_len = payload_length - 4 - recipient_length
    recipient, message_body = struct.unpack('!{}s{}s'.format(recipient_length, message_len), raw_payload[4:])

    with lock:
        # check if recipient exists
        if recipient not in server_data.user_db.keys():
            server_send.general_failure(conn, b'RECIPIENT DOES NOT EXIST, PLEASE SELECT A VALID USERNAME. ')
            return

        # check if recipient is online, send if yes
        recipient_conn = server_data.user_db[recipient]['socket']

        if recipient_conn is not None:
            try:
                server_send.message_alert(recipient_conn, server_data.thread_local.user_name, message_body)
                # notify sender of delivery success
                server_send.send_success(conn)

            except:  # delivery attempt failed even though there is a connection to recipient
                # mark recipient as offline
                server_data.user_db[recipient]['socket'] = None
                server_data.user_db[recipient]['token'] = None

                server_data.user_db[recipient]['message_queue'].append(
                    (server_data.thread_local.user_name, message_body)
                )
                # notify sender of delivery failure
                server_send.general_failure(conn, b'FAILED MESSAGE DELIVERY ATTEMPT. ')

        else:
            # otherwise, store message in queue for recipient for future delivery
            server_data.user_db[recipient]['message_queue'].append(
                (server_data.thread_local.user_name, message_body)
            )

    return


def login_request(conn, payload_length, raw_payload, lock):
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
        # check that username is valid
        if user_name not in server_data.user_db.keys():
            failure_msg = b'USERNAME DOES NOT EXIST! '
            server_send.general_failure(conn, failure_msg)

        # only allow one connection at a time for each user
        elif server_data.user_db[user_name]['token'] is not None:
            failure_msg = b'USER ALREADY CONNECTED! '
            server_send.general_failure(conn, failure_msg)

        else:
            # create user session token using uuid (which may be overkill)
            token = uuid.uuid4().bytes
            # store user info in global database
            server_data.user_db[user_name]['socket'] = conn
            server_data.user_db[user_name]['token'] = token

            # store thread local user name
            server_data.thread_local.user_name = user_name
            # send token back to user
            server_send.login_success(conn, token)
        return


def retrieve_request(conn, payload_length, raw_payload, lock):
    """
    Function that handles the request to retrieve unread messages.

    Retrieves the username of the user on this connection from the thread.local() variable in server_data and
    retrieves the queued messages from the user database user_db. Uses lock to access the user_db. Sends the queued
    messages to user and deletes the queue.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Retrieve request.")
    with lock:
        # get username for this connection
        user_name = server_data.thread_local.user_name

        # get the queued messages
        queue = server_data.user_db[user_name]['message_queue']

        # send usernames back to user
        server_send.retrieve_success(conn, queue)

        # flush out message queue
        server_data.user_db[user_name]['message_queue'] = []
    return


def list_request(conn, payload_length, raw_payload, lock):
    """
    Function that handles the request to list the users on the messaging app.

    Retrieves the user names from the user database. Uses lock to access the database. Sends the user names back to the
    client.

    :param conn: socket object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("List users request.")
    with lock:
        # get user names
        user_names = list(server_data.user_db.keys())

        # send user names back to user
        server_send.list_success(conn, user_names)
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
        user_name = getattr(server_data.thread_local, 'user_name', None)
        if user_name is not None:
            # update the user info in the userdb
            server_data.user_db[user_name]['token'] = None
            server_data.user_db[user_name]['socket'] = None
    return
