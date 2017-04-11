import json
import decimal
import logging
from tornado.web import RequestHandler

from .exceptions import APIError

logger = logging.getLogger('tornado.application')


def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError


class BaseHandler(RequestHandler):
    def data_received(self, chunk):
        return super().data_received(chunk)

    def initialize(self):
        self.session = self.application.session
        self.db = self.application.db

    def on_finish(self):
        self.session.remove()


class JSendMixin(object):
    """http://labs.omniti.com/labs/jsend

    JSend is a specification that lays down some rules for how JSON
    responses from web servers should be formatted.

    JSend focuses on application-level (as opposed to protocol- or
    transport-level) messaging which makes it ideal for use in
    REST-style applications and APIs.
    """

    def success(self, data=None):
        """When an API call is successful, the JSend object is used as a simple
        envelope for the results, using the data key.

        :type  data: A JSON-serializable object
        :param data: Acts as the wrapper for any data returned by the API
            call. If the call returns no data, data should be set to null.
        """
        self.write(json.dumps({'status': 'success', 'data': data}, default=decimal_default))
        self.finish()

    def fail(self, data):
        """There was a problem with the data submitted, or some pre-condition
        of the API call wasn't satisfied.

        :type  data: A JSON-serializable object
        :param data: Provides the wrapper for the details of why the request
            failed. If the reasons for failure correspond to POST values,
            the response object's keys SHOULD correspond to those POST values.
        """
        self.logger.error(data)
        self.set_status(400)
        self.write(json.dumps({'status': 'fail', 'data': data}, default=decimal_default))
        self.finish()

    def error(self, message, data=None, code=400):
        """An error occurred in processing the request, i.e. an exception was
        thrown.

        :type  data: A JSON-serializable object
        :param data: A generic container for any other information about the
            error, i.e. the conditions that caused the error,
            stack traces, etc.
        :type  message: A JSON-serializable object
        :param message: A meaningful, end-user-readable (or at the least
            log-worthy) message, explaining what went wrong
        :type  code: int
        :param code: A numeric code corresponding to the error, if applicable
        """
        self.logger.error(message)
        self.set_status(code)
        result = {'status': 'error', 'message': message}
        if data:
            result['data'] = data
        if code:
            result['code'] = code
        self.write(json.dumps(result, default=decimal_default))
        self.finish()


class RESTHandler(BaseHandler, JSendMixin):
    def initialize(self, *args, **kwargs):
        """
        - Set Content-type for JSON
        """
        super().initialize()
        self.set_header("Content-Type", "application/json")
        self.logger = logger

    def prepare(self):
        pass

    def data_received(self, chunk):
        return super().data_received(chunk)

    @property
    def data(self):
        if self.request.headers.get('Content-Type') in ['json', 'application/json'] and self.request.body:
            try:
                json_data = json.loads(self.request.body.decode('utf-8'), 'utf-8')
                return json_data
            except ValueError:
                message = 'Unable to parse JSON.'
                self.error(code=400, message=message)
        return {}

    @property
    def arguments(self):
        return {k: self.get_argument(k) for k in self.request.arguments}

    def write_error(self, status_code=400, **kwargs):
        """Override of RequestHandler.write_error

        Calls ``error()`` or ``fail()`` from JSendMixin depending on which
        exception was raised with provided reason and status code.

        :type  status_code: int
        :param status_code: HTTP status code
        """
        def get_exc_message(exception):
            return exception.log_message if \
                hasattr(exception, "log_message") else str(exception)

        self.clear()
        self.set_status(status_code)

        # Any APIError exceptions raised will result in a JSend fail written
        # back with the log_message as data. Hence, log_message should NEVER
        # expose internals. Since log_message is proprietary to HTTPError
        # class exceptions, all exceptions without it will return their
        # __str__ representation.
        # All other exceptions result in a JSend error being written back,
        # with log_message only written if debug mode is enabled
        exception = kwargs["exc_info"][1]
        if any(isinstance(exception, c) for c in [APIError]):
            # ValidationError is always due to a malformed request
            self.logger.error(get_exc_message(exception))
            self.fail(get_exc_message(exception))
        else:
            self.logger.error(get_exc_message(exception))
            if not get_exc_message(exception):
                message = 'Reason: {}, Uri: {}, Method: {}'.format(self._reason, self.request.uri, self.request.method)
                if self.request.headers.get('X-Forwarded-For'):
                    message += ', IP: {}'.format(self.request.headers['X-Forwarded-For'])
            else:
                message = self._reason
            self.error(
                message=message,
                data=get_exc_message(exception) if self.settings.get("debug")
                else None,
                code=status_code
            )


class Handler404(RESTHandler):
    def prepare(self):
        self.set_status(404)
        self.write('Not found')
        self.finish()
