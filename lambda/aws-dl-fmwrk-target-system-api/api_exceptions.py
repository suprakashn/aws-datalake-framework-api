class ApiException(Exception):
    def __init__(self):
        pass


class EntryDoesntExist(ApiException):
    """
    When the information is not present in the backend DB
    """
    def __init__(self, message):
        super.__init__()


class NonEditableParams(Exception):
    """

    """
    pass