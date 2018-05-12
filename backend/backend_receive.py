# backend_receive.py

import os
import struct
import sys
import uuid

import backend_data
import backend_send

sys.path.append(os.path.join(os.path.dirname(__file__), '../networking_utils/'))
import handle_messages


def create_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to create a new account.

    The function checks if the username already exists.
    If the provided user name is valid, creates a new entry in the global user database user_db. Uses lock to access
    the user_db. Generates new unique token to send back to the user, and sets the thread.local() variable with the
    user name provided. This will allow the thread to identify the user currently topicected. Sends a success message
    back to the client or a failure message.

    :param topic: topic object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Create username request.")

    # get username
    user_name= topic 
    with lock:
        # check that username is valid
        if user_name in backend_data.user_db.keys():
            failure_msg = b'USERNAME ALREADY EXISTS. TRY ANOTHER! '
            backend_send.general_failure(producer, topic, failure_msg)

        else:
            # create user session token using uuid (which may be overkill)
            token = uuid.uuid4().bytes
            # store user info in global database
            backend_data.user_db[user_name] = {
                'topic': topic,
                'message_queue': [],  # initialize empty message queue for user
                'token': token,
            }
            # store thread local user name
            topic = user_name
            # send token back to user
            backend_send.create_success(producer, topic, token)
    return


def delete_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to delete an account.

    Retrieves the username of the user on this topicection
    from the thread.local() variable in backend_data and deletes the entry corresponding to the user in the user
    database user_db. Uses lock to access the user database. Resets the thread.local() variable to None.

    :param topic: topico to write to
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Delete request.")
    with lock:
        # get username for this topicection
        user_name = topic 
        # delete user entry from db
        del backend_data.user_db[user_name]
        # delete username from local thread var
        topic = None
    return


def logout_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to logout an account.

    Retrieves the username of the user on this topicection
    from the thread.local() variable in backend_data and resets the entry corresponding to the user in the user
    database user_db. Uses lock to access the user database.

    :param topic: topic object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Logout request. A user has been disconnected.")
    logout_user(lock, topic)
    return


def send_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles send requests from the client.

    The function parses the message from the client to retrieve
    the username of the target user and the message body. The function then checks if the target user is topicected
    by checking whether the user database contains a token and a topic object for the given user. Then there are two
    cases. If the target user is currently logged in, the function tries to send the message directly to the user. If
    this fails due to a topicection error or if the user is not logged in the function adds the message to a queue
    stored in the user database. In the first case sends a success message back to the sender. Uses lock to access
    user database.

    :param topic: topic object
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
        backend_send.general_failure(producer, topic, b'MESSAGE PAYLOAD ENCODING ERROR. ')
        return

    # parse recipient name and message body (both in byte format)
    message_len = payload_length - 4 - recipient_length
    recipient, message_body = struct.unpack('!{}s{}s'.format(recipient_length, message_len), raw_payload[4:])
    recipient = recipient.decode('utf-8')
    print('TARGET RECIPIENT IS {}'.format(recipient))

    with lock:
        # check if recipient exists
        if recipient not in backend_data.user_db.keys():
            backend_send.general_failure(producer, topic, b'RECIPIENT DOES NOT EXIST, PLEASE SELECT A VALID USERNAME. ')
            return

        # check if recipient is online, send if yes
        recipient_topic = backend_data.user_db[recipient]['topic']
        print("RECIPIENT TOPIC:::: {}".format(recipient_topic))

        if recipient_topic is not None:
            try:
                backend_send.message_alert(producer, recipient_topic, topic, message_body)
                # notify sender of delivery success
                backend_send.send_success(producer, topic)

            except:  # delivery attempt failed even though there is a topicection to recipient
                # mark recipient as offline
                backend_data.user_db[recipient]['token'] = None
                
                username_conv = topic.encode('utf-8')
                backend_data.user_db[recipient]['message_queue'].append(
                    (username_conv, message_body)
                )
                # notify sender of delivery failure
                backend_send.general_failure(producer, topic, b'FAILED MESSAGE DELIVERY ATTEMPT. ')

        else:
            # otherwise, store message in queue for recipient for futur'e delivery
            username_conv = topic.encode('utf-8')
            backend_data.user_db[recipient]['message_queue'].append(
                (username_conv, message_body)
            )

    return


def login_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to log in.

    The function checks if the username exists and the user is already
    logged in with another topicection. If the provided user name is valid, updates the entry in the global user
    database user_db. Uses lock to access the user_db. Generates new unique token to send back to the user, and sets
    the thread.local() variable with the user name provided. This will allow the thread to identify the user currently
    topicected. Sends a success message back to the client or a failure message.

    :param topic: topic object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    # get username
    user_name = topic
    with lock:
        # check that username is valid
        if user_name not in backend_data.user_db.keys():
            failure_msg = b'USERNAME DOES NOT EXIST! '
            backend_send.general_failure(producer, topic, failure_msg)

        # only allow one token at a time for each user
        elif backend_data.user_db[user_name]['token'] is not None:
            failure_msg = b'USER ALREADY CONNECTED! '
            backend_send.general_failure(producer, topic, failure_msg)

        else:
            # create user session token using uuid (which may be overkill)
            token = uuid.uuid4().bytes
            # store user info in global database
            backend_data.user_db[user_name]['topic'] = topic
            backend_data.user_db[user_name]['token'] = token

            # store thread local user name
            topic = user_name
            # send token back to user
            backend_send.login_success(producer, topic, token)
        return


def retrieve_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to retrieve unread messages.

    Retrieves the username of the user on this topicection from the thread.local() variable in backend_data and
    retrieves the queued messages from the user database user_db. Uses lock to access the user_db. Sends the queued
    messages to user and deletes the queue.

    :param topic: topic object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("Retrieve request.")
    with lock:
        # get username for this topice
        user_name = topic

        # get the queued messages
        queue = backend_data.user_db[user_name]['message_queue']

        # send usernames back to user
        backend_send.retrieve_success(producer, topic, queue)

        # flush out message queue
        backend_data.user_db[user_name]['message_queue'] = []
    return


def list_request(producer, topic, payload_length, raw_payload, lock):
    """
    Function that handles the request to list the users on the messaging app.

    Retrieves the user names from the user database. Uses lock to access the database. Sends the user names back to the
    client.

    :param topic: topic object
    :param payload_length: int, len of raw payload
    :param raw_payload: byte string, packed with network endianness
    :param lock: threading.lock
    :return:
    """
    print("List users request.")
    with lock:
        # get user names
        user_names = list(backend_data.user_db.keys())
        # send user names back to user
        backend_send.list_success(producer, topic, user_names)
    return


def logout_user(lock, topic):
    """
    Helper function that logs the user on the present topicection out, if not already logged out. Uses lock to access
    user database user_db, and gets the user name from the thread.local() variable. Logging out means setting token to
    None and topic to None in the user_db.
    :param lock: threading.lock
    :return:
    """
    with lock:
        # get username on this topicection from thread_local
        user_name = topic
        print(backend_data.user_db)
        if user_name is not None:
            # update the user info in the userdb
            backend_data.user_db[user_name]['token'] = None
            backend_data.user_db[user_name]['topic'] = None

        print(backend_data.user_db)
    return
