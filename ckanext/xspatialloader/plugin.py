import json
import datetime

import ckan.plugins as plugins
import ckan.logic as logic
import ckanext.xloader.interfaces as xloader_interfaces

try:
    config = plugins.toolkit.config
except AttributeError:
    from pylons import config
import jobs

try:
    enqueue_job = plugins.toolkit.enqueue_job
except AttributeError:
    from ckanext.rq.jobs import enqueue as enqueue_job
try:
    import ckan.lib.jobs as rq_jobs
except ImportError:
    import ckanext.rq.jobs as rq_jobs
get_queue = rq_jobs.get_queue
log = __import__('logging').getLogger(__name__)


class XSpatialLoaderPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable)
    plugins.implements(xloader_interfaces.IXloader)

    # IConfigurable

    def configure(self, config_):
        for config_option in ('ckan.site_url',):
            if not config_.get(config_option):
                raise Exception(
                    'Config option `{0}` must be set to use ckanext-xspatialloader.'
                        .format(config_option))

    # IXloader
    def can_upload(self, res_id):
        context = {}
        data_dict = {}
        log.debug(" xspatial sees %s", res_id)
        try:
            resource_dict = plugins.toolkit.get_action('resource_show')(context, {
                'id': res_id,
            })
        except logic.NotFound:
            return False
        ###
        site_url = config['ckan.site_url']
        callback_url = site_url + '/api/3/action/xloader_hook'

        site_user = plugins.toolkit.get_action('get_site_user')({'ignore_auth': True}, {})

        # Check if this resource is already in the process of being xloadered
        task = {
            'entity_id': res_id,
            'entity_type': 'resource',
            'task_type': 'xloader',
            'last_updated': str(datetime.datetime.utcnow()),
            'state': 'submitting',
            'key': 'xloader',
            'value': '{}',
            'error': '{}',
        }
        try:
            existing_task = plugins.toolkit.get_action('task_status_show')(context, {
                'entity_id': res_id,
                'task_type': 'xloader',
                'key': 'xloader'
            })
            assume_task_stale_after = datetime.timedelta(seconds=int(
                config.get('ckanext.xloader.assume_task_stale_after', 3600)))
            assume_task_stillborn_after = \
                datetime.timedelta(seconds=int(
                    config.get('ckanext.xloader.assume_task_stillborn_after', 5)))
            if existing_task.get('state') == 'pending':
                import re  # here because it takes a moment to load
                queued_res_ids = [
                    re.search(r"'resource_id': u'([^']+)'",
                              job.description).groups()[0]
                    for job in get_queue().get_jobs()
                    if 'xspatialloader_to_datastore' in str(job)  # filter out test_job etc
                ]
                updated = datetime.datetime.strptime(
                    existing_task['last_updated'], '%Y-%m-%dT%H:%M:%S.%f')
                time_since_last_updated = datetime.datetime.utcnow() - updated
                if (res_id not in queued_res_ids and
                        time_since_last_updated > assume_task_stillborn_after):
                    # it's not on the queue (and if it had just been started then
                    # its taken too long to update the task_status from pending -
                    # the first thing it should do in the xloader job).
                    # Let it be restarted.
                    log.info('A pending task was found %r, but its not found in '
                             'the queue %r and is %s hours old',
                             existing_task['id'], queued_res_ids,
                             time_since_last_updated)
                elif time_since_last_updated > assume_task_stale_after:
                    # it's been a while since the job was last updated - it's more
                    # likely something went wrong with it and the state wasn't
                    # updated than its still in progress. Let it be restarted.
                    log.info('A pending task was found %r, but it is only %s hours'
                             ' old', existing_task['id'], time_since_last_updated)
                else:
                    log.info('A pending task was found %s for this resource, so '
                             'skipping this duplicate task', existing_task['id'])
                    return False

            task['id'] = existing_task['id']
        except logic.NotFound:
            pass

        context['ignore_auth'] = True
        context['user'] = ''  # benign - needed for ckan 2.5
        plugins.toolkit.get_action('task_status_update')(context, task)

        data = {
            'api_key': site_user['apikey'],
            'job_type': 'xspatialloader_to_datastore',
            'result_url': callback_url,
            'metadata': {
                'ignore_hash': data_dict.get('ignore_hash', False),
                'ckan_url': site_url,
                'resource_id': res_id,
                'set_url_type': data_dict.get('set_url_type', False),
                'task_created': task['last_updated'],
                'original_url': resource_dict.get('url'),
            }
        }
        timeout = config.get('ckanext.xloader.job_timeout', '3600')
        try:
            try:
                job = enqueue_job(jobs.xspatialloader_data_into_datastore, [data],
                                  timeout=timeout)
            except TypeError:
                # older ckans didn't allow the timeout keyword
                job = _enqueue(jobs.xspatialloader_data_into_datastore, [data], timeout=timeout)
        except Exception:
            log.exception('Unable to enqueued xspatialloader res_id=%s', res_id)
            return False
        log.debug('Enqueued xspatialloader job=%s res_id=%s', job.id, res_id)

        value = json.dumps({'job_id': job.id})

        task['value'] = value
        task['state'] = 'pending'
        task['last_updated'] = str(datetime.datetime.utcnow()),
        plugins.toolkit.get_action('task_status_update')(context, task)

        ###

        return False

    def get_Xloader(self):
        return {
            'can_upload': self.can_upload
        }


def _enqueue(fn, args=None, kwargs=None, title=None, queue='default',
             timeout=180):
    '''Same as latest ckan.lib.jobs.enqueue - earlier CKAN versions dont have
    the timeout param'''
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    job = get_queue(queue).enqueue_call(func=fn, args=args, kwargs=kwargs,
                                        timeout=timeout)
    job.meta[u'title'] = title
    job.save()
    msg = u'Added background job {}'.format(job.id)
    if title:
        msg = u'{} ("{}")'.format(msg, title)
    msg = u'{} to queue "{}"'.format(msg, queue)
    log.info(msg)
    return job
