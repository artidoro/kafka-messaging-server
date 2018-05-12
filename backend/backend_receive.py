# backend_receive.py

import pickle
import struct

import backend_send


def create_request(producer, topic, payload_length, raw_payload):
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
    user_name, = struct.unpack('!{}s'.format(payload_length), raw_payload)
    # check that username is valid
    dic = load_obj('backend/data.db')

    if user_name in dic.keys():
        failure_msg = b'USERNAME ALREADY EXISTS. TRY ANOTHER! '
        backend_send.general_failure(producer, topic, failure_msg)

    else:
        # store user info in global database
        dic[user_name] = {
            'topic': topic,
            'message_queue': [],  # initialize empty message queue for user
        }
        # send token back to user
        token = topic.encode('utf-8')
        backend_send.create_success(producer, topic, token)
        save_obj(dic, 'backend/data.db')
    return


def delete_request(producer, topic, payload_length, raw_payload):
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
    # get username for this topic
    dic = load_obj('backend/data.db')

    for u in dic:
        if dic[u]['topic'] == topic:
            user_name = u

    # delete user entry from db
    del dic[user_name]
    logout_user(topic)

    save_obj(dic, 'backend/data.db')

    return


def logout_request(producer, topic, payload_length, raw_payload):
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
    logout_user(topic)
    return


def send_request(producer, topic, payload_length, raw_payload):
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
    dic = load_obj('backend/data.db')

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
    print('TARGET RECIPIENT IS {}'.format(recipient))

    # check if recipient exists
    if recipient not in dic.keys():
        backend_send.general_failure(producer, topic, b'RECIPIENT DOES NOT EXIST, PLEASE SELECT A VALID USERNAME. ')
        return

    # check if recipient is online, send if yes
    recipient_topic = dic[recipient]['topic']
    print("RECIPIENT TOPIC:::: {}".format(recipient_topic))

    if recipient_topic is not None:
        try:
            print("try to send")
            for u in dic:
                if dic[u]['topic'] == topic:
                    user_name = u
            backend_send.message_alert(producer, recipient_topic, user_name, message_body)
            # notify sender of delivery success
            print("try to send success")
            backend_send.send_success(producer, topic)

        except:  # delivery attempt failed even though there is a connection to recipient
            # mark recipient as offline
            dic[recipient]['topic'] = None

            for u in dic:
                if dic[u]['topic'] == topic:
                    user_name = u
            dic[recipient]['message_queue'].append(
                (user_name, message_body)
            )
            # notify sender of delivery failure
            backend_send.general_failure(producer, topic, b'FAILED MESSAGE DELIVERY ATTEMPT. ')

    else:
        # otherwise, store message in queue for recipient for futur'e delivery
        for u in dic:
            if dic[u]['topic'] == topic:
                user_name = u
        dic[recipient]['message_queue'].append(
            (user_name, message_body)
        )
    save_obj(dic, 'backend/data.db')
    return


def login_request(producer, topic, payload_length, raw_payload):
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
    dic = load_obj('backend/data.db')

    # get username
    user_name, = struct.unpack('!{}s'.format(payload_length), raw_payload)

    # check that username is valid
    if user_name not in dic.keys():
        failure_msg = b'USERNAME DOES NOT EXIST! '
        backend_send.general_failure(producer, topic, failure_msg)

    # only allow one connection at a time for each user
    elif dic[user_name]['topic'] is not None:
        failure_msg = b'USER ALREADY CONNECTED! '
        backend_send.general_failure(producer, topic, failure_msg)

    else:
        # store user info in global database
        dic[user_name]['topic'] = topic

        # send token back to user
        token = topic.encode('utf-8')
        backend_send.login_success(producer, topic, token)
    save_obj(dic, 'backend/data.db')
    return


def retrieve_request(producer, topic, payload_length, raw_payload):
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
    dic = load_obj('backend/data.db')

    # get username for this topice
    for u in dic:
        if dic[u]['topic'] == topic:
            user_name = u

    # get the queued messages
    queue = dic[user_name]['message_queue']

    # send usernames back to user
    backend_send.retrieve_success(producer, topic, queue)

    # flush out message queue
    dic[user_name]['message_queue'] = []

    save_obj(dic, 'backend/data.db')
    return


def list_request(producer, topic, payload_length, raw_payload):
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
    dic = load_obj('backend/data.db')

    # get user names
    user_names = list(dic.keys())
    # send user names back to user
    backend_send.list_success(producer, topic, user_names)
    return


def logout_user(topic):
    """
    Helper function that logs the user on the present topicection out, if not already logged out. Uses lock to access
    user database user_db, and gets the user name from the thread.local() variable. Logging out means setting token to
    None and topic to None in the user_db.
    :param lock: threading.lock
    :return:
    """
    dic = load_obj('backend/data.db')

    # get username on this topicection from thread_local
    for u in dic:
        if dic[u]['topic'] == topic:
            user_name = u
    print(dic)
    if user_name is not None:
        # update the user info in the userdb
        dic[user_name]['topic'] = None

    print(dic)
    save_obj(dic, 'backend/data.db')
    return


def save_obj(obj, name):
    with open(name, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def load_obj(name):
    with open(name, 'rb') as f:
        return pickle.load(f)
