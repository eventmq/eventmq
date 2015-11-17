class EventMQError(Exception):
    """
    All exceptions raised by EventMQ inherit from this base exception
    """


class MessageError(EventMQError):
    """
    Raised when there is a problem with the structure of the message
    """
