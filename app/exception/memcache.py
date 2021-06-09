""" Class for memcaches specific exceptions """


class MemcacheException(Exception):

    def __init__(self, status_code=500, description=""):
        self.status_code = status_code
        self.description = description
        super().__init__(self.description)


class MemcacheKeyNotFound(MemcacheException):

    def __init__(self, status_code=404, description=""):
        self.status_code = status_code
        self.description = description
        super().__init__(self.description)


class MemcacheKeyDataCorrupt(MemcacheException):

    def __init__(self, status_code=500, description=""):
        self.status_code = status_code
        self.description = description
        super().__init__(self.description)


class MemcacheConnectionError(MemcacheException):

    def __init__(self, status_code=500, description=""):
        self.status_code = status_code
        self.description = description
        super().__init__(self.description)
