import math
import logging
import time
import tempfile
import json
import urlparse
import datetime
import traceback
import sys

import requests
from rq import get_current_job
import sqlalchemy as sa

from ckan.plugins.toolkit import get_action
try:
    from ckan.plugins.toolkit import config
except ImportError:
    from pylons import config

import loader
import ckanext.xloader.db as db
from ckanext.xloader.job_exceptions import JobError, HTTPError, DataTooBigError, FileCouldNotBeLoadedError

if config.get('ckanext.xloader.ssl_verify') in ['False', 'FALSE', '0', False, 0]:
    SSL_VERIFY = False
else:
    SSL_VERIFY = True
if not SSL_VERIFY:
    requests.packages.urllib3.disable_warnings()

MAX_CONTENT_LENGTH = int(config.get('ckanext.xloader.max_content_length') or 1e9)
MAX_EXCERPT_LINES = int(config.get('ckanext.xloader.max_excerpt_lines') or 0)
CHUNK_SIZE = 16 * 1024  # 16kb
DOWNLOAD_TIMEOUT = 30

def xspatialloader_data_into_datastore(input):
    '''This is the func that is queued. It is a wrapper for
    xloader_data_into_datastore, and makes sure it finishes by calling
    xloader_hook to update the task_status with the result.

    Errors are stored in task_status and job log and this method returns
    'error' to let RQ know too. Should task_status fails, then we also return
    'error'.
    '''
    # First flag that this task is running, to indicate the job is not
    # stillborn, for when xloader_submit is deciding whether another job would
    # be a duplicate or not
    job_dict = dict(metadata=input['metadata'],
                    status='running')
    callback_xloader_hook(result_url=input['result_url'],
                          api_key=input['api_key'],
                          job_dict=job_dict)

    job_id = get_current_job().id
    errored = False
    try:
        xspatialloader_data_into_datastore_(input, job_dict)
        job_dict['status'] = 'complete'
        db.mark_job_as_completed(job_id, job_dict)
    except JobError as e:
        db.mark_job_as_errored(job_id, str(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('xspatialloader error: {0}, {1}'.format(e, traceback.format_exc()))
        errored = True
    except Exception as e:
        db.mark_job_as_errored(
            job_id, traceback.format_tb(sys.exc_traceback)[-1] + repr(e))
        job_dict['status'] = 'error'
        job_dict['error'] = str(e)
        log = logging.getLogger(__name__)
        log.error('xspatialloader error: {0}, {1}'.format(e, traceback.format_exc()))
        errored = True
    finally:
        # job_dict is defined in xloader_hook's docstring
        is_saved_ok = callback_xloader_hook(result_url=input['result_url'],
                                            api_key=input['api_key'],
                                            job_dict=job_dict)
        errored = errored or not is_saved_ok
    return 'error' if errored else None


def xspatialloader_data_into_datastore_(input, job_dict):
    '''This function:
    * downloads the resource (metadata) from CKAN
    * downloads the data
    * calls the loader to load the data into DataStore
    * calls back to CKAN with the new status

    (datapusher called this function 'push_to_datastore')
    '''
    job_id = get_current_job().id
    db.init(config)

    # Store details of the job in the db
    try:
        db.add_pending_job(job_id, **input)
    except sa.exc.IntegrityError:
        raise JobError('job_id {} already exists'.format(job_id))

    # Set-up logging to the db
    handler = StoringHandler(job_id, input)
    level = logging.DEBUG
    handler.setLevel(level)
    logger = logging.getLogger(job_id)
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    # also show logs on stderr
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    validate_input(input)

    data = input['metadata']

    resource_id = data['resource_id']

    try:
        resource, dataset = get_resource_and_dataset(resource_id)
    except JobError, e:
        # try again in 5 seconds just in case CKAN is slow at adding resource
        time.sleep(5)
        resource, dataset = get_resource_and_dataset(resource_id)
    resource_ckan_url = '/dataset/{}/resource/{}' \
        .format(dataset['name'], resource['id'])
    logger.info('Express Load starting: {}'.format(resource_ckan_url))

    # check if the resource url_type is a datastore
    if resource.get('url_type') == 'datastore':
        logger.info('Ignoring resource - url_type=datastore - dump files are '
                    'managed with the Datastore API')
        return

    # check scheme
    url = resource.get('url')
    scheme = urlparse.urlsplit(url).scheme
    if scheme not in ('http', 'https', 'ftp'):
        raise JobError(
            'Only http, https, and ftp resources may be fetched.'
        )

    # Load it
    logger.info('Loading spatial file')
    try:
        loader.do_ingesting(dataset_id=dataset['id'], force=True, ext_logger=logger)
        job_dict['status'] = 'running_but_viewable'
        callback_xloader_hook(result_url=input['result_url'],
                              api_key=input['api_key'],
                              job_dict=job_dict)
        logger.info('Data now available to users: {}'.format(resource_ckan_url))

    except JobError as e:
        logger.error('Error during load: {}'.format(e))
        raise
    except FileCouldNotBeLoadedError as e:
        logger.warning('Loading excerpt for this format not supported.')
        logger.error('Loading file raised an error: {}'.format(e))
        raise JobError('Loading file raised an error: {}'.format(e))

    logger.info('Express spatial Load completed')


def callback_xloader_hook(result_url, api_key, job_dict):
    '''Tells CKAN about the result of the xloader (i.e. calls the callback
    function 'xloader_hook'). Usually called by the xloader queue job.
    Returns whether it managed to call the sh
    '''
    api_key_from_job = job_dict.pop('api_key', None)
    if not api_key:
        api_key = api_key_from_job
    headers = {'Content-Type': 'application/json'}
    if api_key:
        if ':' in api_key:
            header, key = api_key.split(':')
        else:
            header, key = 'Authorization', api_key
        headers[header] = key

    try:
        result = requests.post(
            result_url,
            data=json.dumps(job_dict, cls=DatetimeJsonEncoder),
            headers=headers)
    except requests.ConnectionError:
        return False

    return result.status_code == requests.codes.ok

def validate_input(input):
    # Especially validate metadata which is provided by the user
    if 'metadata' not in input:
        raise JobError('Metadata missing')

    data = input['metadata']

    if 'resource_id' not in data:
        raise JobError('No id provided.')
    if 'ckan_url' not in data:
        raise JobError('No ckan_url provided.')
    if not input.get('api_key'):
        raise JobError('No CKAN API key provided')


def get_resource_and_dataset(resource_id):
    """
    Gets available information about the resource and its dataset from CKAN
    """
    res_dict = get_action('resource_show')(None, {'id': resource_id})
    pkg_dict = get_action('package_show')(None, {'id': res_dict['package_id']})
    return res_dict, pkg_dict

class StoringHandler(logging.Handler):
    '''A handler that stores the logging records in a database.'''
    def __init__(self, task_id, input):
        logging.Handler.__init__(self)
        self.task_id = task_id
        self.input = input

    def emit(self, record):
        conn = db.ENGINE.connect()
        try:
            # Turn strings into unicode to stop SQLAlchemy
            # "Unicode type received non-unicode bind param value" warnings.
            message = unicode(record.getMessage())
            level = unicode(record.levelname)
            module = unicode(record.module)
            funcName = unicode(record.funcName)

            conn.execute(db.LOGS_TABLE.insert().values(
                job_id=self.task_id,
                timestamp=datetime.datetime.now(),
                message=message,
                level=level,
                module=module,
                funcName=funcName,
                lineno=record.lineno))
        finally:
            conn.close()


class DatetimeJsonEncoder(json.JSONEncoder):
    # Custom JSON encoder
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return json.JSONEncoder.default(self, obj)

