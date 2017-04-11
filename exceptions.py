from tornado.web import HTTPError


class APIError(HTTPError):
    pass


class BadRequest(HTTPError):
    pass


class ArgsParsingError(Exception):
    pass


class ApiException(HTTPError):
    pass


class ObjectNotFound(ApiException):
    def __init__(self, status_code=404, message='', *args, **kwargs):
        super(ObjectNotFound, self).__init__(status_code=404, log_message=message)


class NoResultsFound(ApiException):
    def __init__(self, status_code=400, message='', *args, **kwargs):
        super(NoResultsFound, self).__init__(status_code=404, log_message=message)


class MethodNotAllowed(ApiException):
    def __init__(self, status_code=405, message='The method is not allowed for the requested URL.', *args, **kwargs):
        super(MethodNotAllowed, self).__init__(status_code=status_code, log_message=message)
