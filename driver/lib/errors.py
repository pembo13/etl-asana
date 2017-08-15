class AuthRevokedError(RuntimeError):
    """
    Raised when ETL task discovers access to datasource has been revoked.
    """
    def __init__(self, error=None):
        super(AuthRevokedError, self).__init__(u"Authorization revoked error",
                                               error)
        self.error = error


class RateLimitError(RuntimeError):
    """
    Raised when a data source indicates a rate limit is being violated.
    """
    def __init__(self, error=None, duration_seconds=1800):
        super(RateLimitError, self).__init__("Rate limit error", error)
        self.error = error
        self.duration_seconds = duration_seconds


class ServiceUnavailableError(RuntimeError):
    """
    Raised when a data source throws an exception indicating diffultities
    on their end (e.g. HTTP 500 result)
    """
    def __init__(self, error=None):
        super(ServiceUnavailableError, self)\
            .__init__("Service unavailable error", error)
        self.error = error
