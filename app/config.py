"""Configuration for the app."""

import os

chunk_size = int(os.environ.get('CHUNK_SIZE_BYTES', 500000))
memcached_host = os.environ.get('MEMCACHED_HOST', 'localhost')
memcached_port = int(os.environ.get('MEMCACHED_PORT', 11211))
max_file_size = int(os.environ.get('MAX_FILE_SIZE_MB', 50))
db_str = os.environ.get('DB_STR', "mysql+mysqlconnector://root:password@localhost/file-store-engine")

logger_name = "file_store_engine"
