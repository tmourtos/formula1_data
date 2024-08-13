import requests
import time
import simplejson as json

from http.client import responses
from pprint import pformat, pprint
from typing import Generator

from utils.utils import exponential_backoff_retries


class Response(object):
    def __init__(self):
        self.ok = False
        self.status = None
        self.description = None
        self.data = None

    def reset(self):
        self.ok = False
        self.status = None
        self.description = None
        self.data = None

    def __repr__(self):
        return pformat(vars(self))


class RequestsWrapper:
    batch_size = 500

    def __init__(self):

        self.prefix = 'https://ergast.com/api/f1'
        self.response = Response()
        self.session = None

    def __repr__(self):
        s = '%s' % self.__class__.__name__
        return s

    def _execute(self, method, url, parameters=None):
        for sleep_time in exponential_backoff_retries():
            try:
                payload = {
                    'url': url,
                    'timeout': 300,
                    'params': {
                        'limit': 1000,
                    }
                }

                if parameters:
                    for parameter, value in parameters.items():
                        payload['params'][parameter] = value

                request_response = getattr(self.session, method)(**payload)
                self.response.ok = request_response.ok
                self.response.status = request_response.status_code
                self.response.description = responses.get(request_response.status_code, None)
                if request_response.ok:
                    try:
                        self.response.data = request_response.json()

                    except json.errors.JSONDecodeError:
                        self.response.data = dict()

                    break

            except requests.exceptions.HTTPError as http_error:
                self.response.status = 'HTTP Error'
                self.response.description = http_error
                print(f'HTTP Error ({method}): {url}')
                time.sleep(sleep_time)
            except requests.exceptions.ConnectionError as connection_error:
                self.response.status = 'Connection Error'
                self.response.description = connection_error
                print(f'Connection Error ({method}): {url}')
                time.sleep(sleep_time)
            except requests.exceptions.Timeout as timeout_error:
                self.response.status = 'Timeout Error'
                self.response.description = timeout_error
                print(f'Timeout Error ({method}): {url}')
                time.sleep(sleep_time)
            except requests.exceptions.RequestException as request_exception:
                self.response.status = 'Requests Exception'
                self.response.description = request_exception
                print(f'Requests Exception ({method}): {url}')
                time.sleep(sleep_time)
            except Exception as exception:
                self.response.status = 'Exception'
                self.response.description = exception
                print(f'Exception ({method}): {url}')
                time.sleep(sleep_time)

    def get(self, target, parameters=None):
        """
            Get data from target
        :param target: The request target
        :param parameters: The request parameters
        """
        self.response.reset()
        self.session = requests.Session()

        url = f'{self.prefix}/{target}'
        self._execute(method='get', url=url, parameters=parameters)

        return self.response
