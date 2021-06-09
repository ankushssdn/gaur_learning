"""Application models."""

import logging
from hashlib import sha256

from exception.memcache import MemcacheKeyNotFound, MemcacheKeyDataCorrupt
from database import db
from file_store import store, fetch, remove
from config import chunk_size, logger_name

logger = logging.getLogger(logger_name)


class File(db.Model):
    """User-submitted file."""

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    file_name = db.Column(db.String(496), nullable=False)
    checksum = db.Column(db.String(496))
    parts = db.relationship('FilePart', backref='file', lazy=True)

    def save(self, stream, checksum: str) -> list:
        """Write contents for this file.
        stream is expected to be a file-like object.
        This method reads a file in parts and stores the contents and
        metadata.

        Args:
            stream: byte stream
            checksum: checksum of data in stream

        Returns:
             parts: list of FilePart objects for each portion of the file written.

        """

        stream_checksum = sha256()

        parts = []
        mem_cache_ids = []
        try:
            logger.debug(f"Chunking data for file id {self.id}")
            for chunk in self.__class__._read_stream(stream, chunk_size):
                # we have already calculated file checksum earlier so we are
                # going to use it instead of calculating a new one.
                chunk_hash = sha256(chunk).hexdigest()
                mem_id = store(chunk)
                # store mem_cache_ids , this will be used to free memcached in case of exception
                mem_cache_ids.append(mem_id)
                file_part = FilePart(checksum=chunk_hash,
                                     memcached_key=mem_id,
                                     sequence=len(parts) + 1,
                                     file_id=self.id)
                parts.append(file_part)
                stream_checksum.update(chunk)
            # After reading the entire stream, store the checksum of the data
            self.checksum = checksum
        except Exception as ex:
            logger.error(ex)
            # if any exception occurs free up memcached
            if len(mem_cache_ids) > 0:
                remove(mem_cache_ids)
            raise ex
        return parts

    @property
    def contents(self) -> bytes:
        """Retrieves the stored contents of this file.
         This method reads the file contents into memory and returns it as
         a single blob.

        Returns:
            bytes: content in byte
        """
        parts = []
        logger.debug(f"Attempting to fetch content for id {self.id}")
        for part in self.parts:
            try:
                memchached_part = fetch(part.memcached_key)
                parts.append(memchached_part)
                # match checksum of chunk with checksum in db
                if not part.checksum == sha256(memchached_part).hexdigest():
                    raise MemcacheKeyDataCorrupt(description=f"File Part corrupt for file id {part.file_id}")
            except (MemcacheKeyNotFound, MemcacheKeyDataCorrupt) as ex:
                # if any key is not found in memcached then remove whole file from memcached and from database
                self._clean_keys()
                self._delete_from_database()
                raise ex
        return b''.join(parts)

    def _delete_from_database(self) -> None:
        """
        Delete file record from database.
        Returns: None

        """
        file_id = self.parts[0].file_id
        logger.debug(f"Attempting to delete file record from database for id {file_id}")
        File.query.filter_by(id=file_id).delete()
        db.session.commit()


    def _clean_keys(self) -> None:
        """
        Delete all keys from memcached for file
        Returns: None

        """
        keys_to_clean = []
        logger.debug(f"Fetching all memcached keys to clean for id {self.id}")
        for local_part in self.parts:
            keys_to_clean.append(local_part.memcached_key)
        try:
            remove(keys_to_clean)
        except MemcacheKeyNotFound as ex:
            logger.error(f"Got Key error while deleting ids {keys_to_clean} in memcached: {ex}")
            pass

    @staticmethod
    def _read_stream(stream: bytes, segment_size: int) -> bytes:
        """Generator to read a file-like object in parts.

        stream is assumed to be an object with a read method that behaves like
        a buffer or file.

        segment_size is the amount of data that should be read each time this
        method is called.

        Args:
            stream: byte stream
            segment_size: size of segment in bytes to read from stream
        Returns:
            streamed_data: stream data of segment size
        """
        streamed_data = stream.read(segment_size)

        while streamed_data:
            yield streamed_data
            streamed_data = stream.read(segment_size)


class FilePart(db.Model):
    """Portion or chunk of a file."""

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    checksum = db.Column(db.String(496), nullable=False)
    memcached_key = db.Column(db.String(496), nullable=False)
    sequence = db.Column(db.String(496), nullable=False)
    file_id = db.Column(db.Integer, db.ForeignKey('file.id', ondelete='CASCADE'), nullable=False)
