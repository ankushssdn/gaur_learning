"""File chunk storage."""

from pymemcache.client.base import Client
from pymemcache.exceptions import MemcacheError, MemcacheIllegalInputError
from uuid import uuid4
import config
import logging

from exception.memcache import MemcacheKeyNotFound, MemcacheConnectionError
from config import memcached_host, memcached_port

logger = logging.getLogger(config.logger_name)


def store(content: str) -> str:
    """Store a chunk in the backend datastore.
    This method will generate a UUID and write the provided content to storage.
    Args:
        content: data to store in memcached
    Returns: UUID generated for this chunk, needed to retrieve it again.
    """
    try:
        client = memcached_client()
        key = str(uuid4())
        logger.debug(f"Attempting to store data at key {key} in memcached")
        client.set(key, content)
        return key
    # TODO: Handle specific exceptions
    except MemcacheError as me:
        logger.error(f"Got error in storing chunk data on id {key} in memcached: {me}")
        raise me


def remove(ids: list) -> None:
    """
    Remove keys from memcached.
    Args:
        ids: list of ids

    Returns: None
    """
    try:
        logger.debug(f"Attempting to delete keys from memcached, keys are: {ids}")
        client = memcached_client()
        client.delete_many(ids)
    except MemcacheIllegalInputError as me:
        logger.error(f"Got Key error while deleting ids {ids} in memcached: {me}")
        raise MemcacheKeyNotFound(description=f"Got Key error while deleting ids {ids} in memcached: {me}")
    except MemcacheError as me:
        logger.error(f"Got error while deleting ids {ids} in memcached: {me}")
        raise me


def fetch(key: str) -> str:
    """
    Retrieve stored data from the backend datastore.
    Args:
        key: id of key in memcached
    Returns:
        str: data stored for the provided ID.
    """
    try:
        logger.debug(f"Fetching data from memcached for id {key}")
        client = memcached_client()
        # get value from memcached
        value = client.get(key)
        if not value:
            logger.error(f"Could not find key {key} in memcached")
            raise MemcacheKeyNotFound(description=f"Key with id {key} could not be found in memcached.")
        return client.get(key)
    except MemcacheError as ex:
        logger.error(f"Got error in fetching id {key} from memcached: {ex}")
        raise ex


def check_all_keys(memcached_keys: list) -> bool:
    """
    Check if all keys are present in memcached
    If any of key is not present, delete all keys as corrupted data is consuming memory.
    Args:
        memcached_keys: list of keys to check

    Returns:
        boolean: True if all keys are present, False if one or more keys are not present

    """
    client = memcached_client()
    found = True
    logger.debug(f"Checking if all supplied keys are present in memcached: {memcached_keys}")
    for memcached_key in memcached_keys:
        found = client.get(memcached_key)
        if not found:
            # if key is not found then delete corrupted record from memcached
            logger.debug(f"Could not find {memcached_key} key in memcached")
            _clean_keys(memcached_keys)
            found = False
            # break the loop as all keys are now cleaned
            break;
    return found


def _clean_keys(keys_to_clean: list) -> None:
    """
    Delete supplied keys from memcached
    Args:
        keys_to_clean:

    Returns: None

    """
    try:
        logger.debug(f"Attempting to delete keys from memcached: {keys_to_clean}")
        remove(keys_to_clean)
    except MemcacheKeyNotFound as ex:
        logger.error(f"Got Key error while deleting ids {keys_to_clean} in memcached: {ex}")
        pass


def memcached_client():
    """
    Get memcached client
    Returns: memcached client

    """
    try:
        return Client((memcached_host, memcached_port))
    except Exception as ex:
        logger.error(f"Got error while connecting to memcached: {ex}")
        raise MemcacheConnectionError(description=ex)
