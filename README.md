# File Store Engine

## About
File Store Engine is a service that stores large files(upto 50mb) in a remote cache datastore. File Storage engine benefits include easy storage and access of files using REST APIs.

## How does file-store-engine work?
File Store Engine stores data by chunking complete file data into smaller chunks, The information to join these chunks back like sequence number is stored in database whereas the chunk data is stored in memory-caching-system.

File-Store-Engine has 3 major components/microservices which are:
1. **API Engine**: This component written in python is responsible to expose REST APIs for various operations. This is the component which has business logic to chunk data and store.
2. **Memcached**: Memcached is a high-performance distributed memory cache service which act as in-memory data storage system.
3. **MySQL**: MySQL is relational database management system which in our case is used to store file and chunk information like sequence number, checksum etc.

#### Prerequisites
To run app prerequisites are:
* python 3.7
* pip 21.1

#### Run API Engine
1. Install dependencies for the project
```console
$ pip install -r app/requirements.txt
```

2. Run API Engine using command
```console
$ cd app/
$ python app.py
```