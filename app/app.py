#!flask/bin/python

import logging
import werkzeug.datastructures
from flask import Flask, request, abort

from log_util import LogUtil
from werkzeug.exceptions import BadRequest
from hashlib import sha256

import config
from database import db, migrate
from models import File
from file_store import memcached_client, check_all_keys
from exception.memcache import MemcacheKeyNotFound

logger_name = config.logger_name
# initialize logger
LogUtil(logger_name)
logger = logging.getLogger(logger_name)

# Main flask application
app = Flask(__name__)

# Max file size to accept in bytes with buffer of 250 bytes for extra data like key for file.
app.config['MAX_CONTENT_LENGTH'] = (config.max_file_size * 1024 * 1024) + 250
# DB Connection String
app.config['SQLALCHEMY_DATABASE_URI'] = config.db_str

""" Flask-SQLAlchemy has its own event notification system that gets layered on top of SQLAlchemy.
 To do this, it tracks modifications to the SQLAlchemy session. This takes extra resources,
 so the option SQLALCHEMY_TRACK_MODIFICATIONS allows us to disable the modification tracking system."""
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


@app.route('/api/healthcheck', methods=['GET'])
def health_check() -> str:
    """
    Checks health check of application.

    Returns:
        str: "OK"
    """
    try:
        logger.debug("Performing HealthCheck")
        # check database connection status
        db.session.execute('SELECT 1')
        # check memcached connection status
        memcached_client().version()
    except Exception as ex:
        logger.error(f"Health Check Failed with error: {ex}")
        abort(500, "Health Check Failed")
    return "OK"


@app.route('/api/liveness', methods=['GET'])
def liveness_check() -> str:
    """
    Checks liveness of application

    Returns:
        str: "OK"

    """
    logger.debug("Performing Liveness Check")
    return "OK"


@app.route('/api/files/<string:file_id>', methods=['GET'])
def get_files(file_id: str) -> str:
    """
    Get file data for supplied file id
    Args:
        file_id: unique id of file

    Returns: file data

    """
    try:
        logger.info(f"Fetching data for file id {file_id}")
        file = File.query.get_or_404(file_id,
                                     f'Sorry, could\'t find any file with the id {file_id}')
        content = file.contents
        return content
    # TODO: Handle specific types of exception as per db and memcached
    except MemcacheKeyNotFound as mem_ex:
        abort(404, f"Data has been evicted or is corrupted.")
    except Exception as ex:
        logger.error(f"Got exception in retrieving data for id {file_id}: {ex}")
        # send error whatever is thrown from application
        if hasattr(ex, 'description'):
            if ex.description:
                abort(ex.code, ex.description)
        # for other type of errors
        abort(500, f"Error in retrieving data for id {file_id}")


@app.route('/api/files', methods=['POST'])
def post_files() -> int:
    """
    Store one or more files.

    This method expects the POST request to include one or more files as part
    of the request.
    Returns: id of file

    """
    file_ids = []
    files = request.files
    # Check if file is supplied
    if not len(files):
        logger.error("No file is supplied in request")
        raise BadRequest("No file is supplied.")

    # Additional Validation on files
    is_validated, resp_msg = _validate_files(files)
    if not is_validated:
        raise BadRequest(resp_msg)
    try:
        for file_id, file_data in files.items():
            file_record, checksum = _file_record(file_data)

            if file_record:
                file_ids.append(str(file_record.id))
            else:
                # Create the file in the database
                user_file = File(file_name=file_id)

                db.session.add(user_file)
                db.session.flush()
                file_data.stream.seek(0)
                # Store each portion of the file.  What happens if this fails though :
                # we will free up memcached in case of failure
                for part in user_file.save(file_data.stream, checksum):
                    db.session.add(part)

                db.session.commit()
                file_ids.append(str(user_file.id))
                logger.info(f"Stored files for request with ids: {file_ids}")
        return ','.join(file_ids)
    except Exception as exep:
        logger.error(f"Got exception in processing request: {exep}")
        # in case of error rollback session
        db.session.remove()
        abort(500, "Could not process your request due to some technical error")


def _validate_files(files: dict) -> tuple:
    """
    Add some validation on received file object
    Args:
        files: files received in request

    Returns:
        flag: boolean if file is valid
        msg: error message, None if there is no error

    """
    msg = None
    flag = True
    logger.debug(f"Validating {len(files)} files supplied in request")
    for file_id, file_stream in files.items():
        if not (file_id and file_stream.filename):
            msg = "One or more File Id(s) or File(s) are not supplied."
            logger.error(msg)
            flag = False
            break
        elif not isinstance(file_stream, werkzeug.datastructures.FileStorage):
            msg = f"File Type Not correct for file id {file_id}"
            logger.error(msg)
            flag = False
            break
        # Uncomment and indent below lines if empty file is not to be allowed
        # elif not file_stream.stream.read(1):
        #     msg = "File is empty"
        #    logger.error(msg)
        #    flag = False
        #    break
    return flag, msg


def _file_record(stream) -> tuple:
    """
    Checks if file record is present with us in db.
    Also checks if its data is present in memcached.
    Args:
        stream: stream having data

    Returns:
        file_row: details of file stored in db
        checksum: checksum of data supplied in stream

    """
    checksum = sha256(stream.read()).hexdigest()
    logger.debug(f"Checking if entry for file with checksum {checksum} is present in db")
    file_row = File.query.filter_by(checksum=checksum).first()
    if file_row is not None:
        # check if no chunk is evicted from memcached
        memcached_keys = [part.memcached_key for part in file_row.parts]
        check = check_all_keys(memcached_keys)
        if not check:
            logger.debug(f"Attempting to delete entry from db for file id {file_row.id}")
            File.query.filter_by(id=file_row.id).delete()
            db.session.commit()
            # As we deleted corrupted data and record, this entry shouldn't be returned
            file_row = None
    return file_row, checksum


if __name__ == '__main__':
    # Create an instance of db to store information about each file.

    db.init_app(app)
    migrate.init_app(app, db)
    # Initialize the database.
    db.create_all(app=app)

    app.run(debug=False, threaded=True, host='0.0.0.0')
