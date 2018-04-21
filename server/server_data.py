# server_data.py

import threading

# local database for storing user information, threads should acquire lock before accessing user_db
user_db = dict()

# thread-specific storage for user_name retrieval
thread_local = threading.local()

