import logging
import sys
import requests
from requests.packages.urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests import Session, exceptions
from magnumbi_depot.Job import Job
from string import Template

# URL Templates
status_template = Template('$hostname:$port/job/')
request_template = Template('$hostname:$port/job/request')
submit_template = Template('$hostname:$port/job/submit')
complete_template = Template('$hostname:$port/job/complete')
is_empty_template = Template('$hostname:$port/job/isempty')
logger = logging.getLogger(__name__)

_CONNECTION_TIMEOUT = 60

if sys.version_info[0] < 3:
    raise Exception("Python 3 or newer required.")


class DispatchClient:
    def check_status(self):
        """
        Check the status of the dispatch server.

        :rtype: bool
        :return: True iff running correctly.
        """
        logger.debug("Checking status.")
        r = self.session.get(status_template.substitute(hostname=self.hostname, port=self.port), verify=self.ssl_verify,
                             timeout=3)
        logger.debug("Status check: %s", str(r.status_code))
        if r.status_code is 200:
            return True
        else:
            return False

    def request_job(self, app_id, request_timeout=-1, job_timeout=120):
        """
        Gets a new waiting job.

        :return:
        :rtype: Job
        """
        logger.debug("Requesting job for %s", app_id)

        if request_timeout is -1:
            http_timeout = _CONNECTION_TIMEOUT
        elif request_timeout > 0:
            http_timeout = request_timeout + _CONNECTION_TIMEOUT
        else:
            raise ValueError("Request timeout not valid.")

        r = self.session.post(request_template.substitute(hostname=self.hostname, port=self.port), json={
            "jobHandleTimeoutSeconds": job_timeout,
            "appId": app_id,
            "timeout": request_timeout
        }, verify=self.ssl_verify, timeout=http_timeout)
        if r.status_code is not 200:
            raise ValueError("Request against server unsuccessful " + r.reason)
        else:
            logger.debug('Request return %s', str(r.json()))

        # Create job object.
        job_obj = r.json()
        if 'jobId' not in job_obj:
            return None
        else:
            job = Job(job_id=job_obj['jobId'],
                      data=job_obj['data'],
                      start_datetime=job_obj['startDateTime'] if 'startDateTime' in job_obj else None)
            return job

    def submit_job(self, app_id, data, previous_jobs=None):
        logger.debug('Submitting new job for %s', app_id)
        r = self.session.post(submit_template.substitute(hostname=self.hostname, port=self.port), json={
            "appId": app_id,
            "data": data,
            "previousJobs": previous_jobs
        }, verify=self.ssl_verify, timeout=_CONNECTION_TIMEOUT)
        r.raise_for_status()

    def close(self):
        self.session.close()

    def complete_job(self, app_id, job_id):
        logger.debug('Completing job for %s', app_id)
        r = self.session.post(complete_template.substitute(hostname=self.hostname, port=self.port), json={
            "appId": app_id,
            "jobId": job_id
        }, verify=self.ssl_verify, timeout=_CONNECTION_TIMEOUT)
        r.raise_for_status()

    def is_empty(self, app_id):
        """
        Checks if a given applications job queue is empty.
        :param app_id:
        :return:
        """
        logger.debug('Check if %s is empty', app_id)
        r = self.session.post(is_empty_template.substitute(hostname=self.hostname, port=self.port), json={
            "appId": app_id
        }, verify=self.ssl_verify, timeout=_CONNECTION_TIMEOUT)
        if r.status_code is not 200:
            raise ValueError("Request against server unsuccessful " + r.reason)
        else:
            logger.debug('Is empty return %s', str(r.json()))

        isempty_obj = r.json()
        if 'empty' not in isempty_obj:
            raise Exception('Unknown return object %s', str(isempty_obj))
        return isempty_obj['empty']

    def __init__(self, uri, access_key=None, secret_key=None, port=6883, ssl_verify=True, conn_keep_alive=False):
        """
        Creates a new MagnumBI Dispatch Client

        :type ssl_verify: bool
        :type port: int
        :param uri: The URI of the MagnumBI Dispatch Server, i.e. https://10.0.0.1 or http://10.0.0.1.
        :param access_key: The access key used to authenticate the requests.
        :param secret_key: The secret key used to authenticate the requests.
        :param port: The port to connect with.
        :param ssl_verify: If false we will not attempt to verify the ssl certificate of the requests. This not recommended.
        :param conn_keep_alive: The underling http session will try and stay alive between requests.
        
        """

        global logger

        # Config stuff
        self.hostname = uri
        self.port = port
        self.session = s = Session()
        self.ssl_verify = ssl_verify
        if access_key:
            s.auth = (access_key, secret_key)
        s.keep_alive = conn_keep_alive

        retries = Retry(total=3,
                        backoff_factor=3,
                        status_forcelist=[500, 400])

        a = HTTPAdapter(max_retries=retries)
        b = HTTPAdapter(max_retries=retries)
        s.mount('http://', a)
        s.mount('https://', b)

        # Logger stuff.
        logger = logging.getLogger('magnumbi_dispatch')
        logging.captureWarnings(True)
        logging.getLogger('requests').setLevel(logging.ERROR)
        logging.getLogger('py.warnings').setLevel(logging.ERROR)
        logging.getLogger('urllib3.connectionpool').setLevel(logging.WARN)
