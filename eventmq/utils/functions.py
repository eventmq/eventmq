import hashlib
import importlib
import inspect
import json
import sys

import six

from .. import log
from ..exceptions import CallableFromPathError

logger = log.setup_logger(__name__)


class IgnoreJSONEncoder(json.JSONEncoder):
    """JSON Encoder that ignores unknown keys."""
    def default(self, obj):
        try:
            return super(IgnoreJSONEncoder, self).default(obj)
        except TypeError:
            return None


def run_function(callable_name,
                 class_args=(), class_kwargs=None, args=(), kwargs=None):
    """Constructs a callable from `path` and `callable_name` and calls it.

    Args:
        callable_name (str): The name and path of the function you wish to run,
            eg ``eventmq.utils.functions.run_function``.
        class_args (list): If the callable is a class method, these args will
            be used to initialize the class.
        class_kwargs (dict): If the callable is a class method, these kwargs
            will be used to initialize the class.
        args (list): These arguments will be passed as parameters to the
            callable
        kwargs (dict): These keyword arguments will be passed as parameters to
            the callable.

    Return:
        object: Whatever is returned by calling the callable will be returned
            from this function.
    """
    if not class_args:
        class_args = ()

    if not class_kwargs:
        class_kwargs = {}

    if not args:
        args = ()

    if not kwargs:
        kwargs = {}

    try:
        callable_ = callable_from_name(
            callable_name, *class_args, **class_kwargs)
    except CallableFromPathError as e:
        logger.exception('Error importing callable {}: {}'.format(
            callable_name, str(e)))
        return

    return callable_(*args, **kwargs)


def arguments_hash(*args, **kwargs):
    """Takes `args` and `kwargs` and creates a unique identifier."""
    args = {
        'args': args,
        'kwargs': kwargs,
    }

    data = json.dumps(args, cls=IgnoreJSONEncoder)
    return hashlib.sha1(six.ensure_binary(data)).hexdigest()


def name_from_callable(func):
    """
    Builds the module path in string format for a callable.

    .. note:
       To use a callable Object, pass Class.__call__ as func and provide any
       class_args/class_kwargs. This is so side effects from pickling won't
       occur.

    Args:
        func (callable): The function or method to build the path for

    Returns:
        str: The callable path and name, eg
            ``"eventmq.utils.functions.name_from_callable"`` would be returned
            if you called this function on itself.
    """
    callable_name = None

    path = None
    # Methods also have the func_name property
    if inspect.ismethod(func):
        path = ("{}:{}".format(func.__module__,
                               func.__self__.__class__.__name__))
        try:
            callable_name = func.func_name
        except AttributeError:
            callable_name = func.__name__
    elif inspect.isfunction(func):
        path = func.__module__
        try:
            callable_name = func.func_name
        except AttributeError:
            callable_name = func.__name__
    else:
        # We should account for another callable type so log information
        # about it
        if hasattr(func, '__class__') and isinstance(func, func.__class__):
            func_type = 'instanceobject'
        else:
            func_type = type(func)

        logger.error('Encountered unknown callable ({}) type {}'.format(
            func,
            func_type
        ))
        return None

    if not callable_name:
        logger.error(
            'Encountered callable with no name in {}'.format(func.__module__))
        return None

    if not path:
        try:
            func_name = func.func_name
        except AttributeError:
            func_name = func.__name__
        logger.error(
            'Encountered callable with no __module__ path {}'.format(
                func_name))
        return None

    return '{}.{}'.format(path, callable_name)


def split_callable_name(callable_name):
    """Split a callable name into it's path and name components.

    Args:
        callable_name (str): The callable name, with path, eg,
            ``eventmq.utils.functions.callable_from_name``.

    Returns:
        (str, str): The path and callable name, or None, None if a path cannot
            be found.
    """
    if not callable_name or '.' not in callable_name:
        return None, None

    elements = callable_name.split('.')
    path = '.'.join(elements[:-1])
    return path, elements[-1]


def callable_from_name(callable_name, *args, **kwargs):
    """Build a callable from a path and callable_name.

    This function is the opposite of `name_from_callable`.  It takes what is
    returned from `build_module_name` and converts it back to the original
    caller.

    Args:
        callable_name (str): The name (and path) of the function, eg,
            ``eventmq.utils.functions.callable_from_name``.
        *args (list): if `callable_name` is a method on a class, these
            arguments will be passed to the constructor when instantiating the
            class.
        *kwargs (dict): if `callable_name` is a method on a class, these
            arguments will be passed to the constructor when instantiating the
            class.

    Returns:
        function: The callable
    """
    path, callable_name = split_callable_name(callable_name)
    if ':' in path:
        _pksplit = path.split(':')
        s_package = _pksplit[0]
        s_cls = _pksplit[1]
    else:
        s_package = path
        s_cls = None

    try:
        package = importlib.import_module(s_package)
        if sys.version[0] == '2':
            reload(package)  # noqa - flake8 fails here on py3
        else:
            importlib.reload(package)
    except Exception as e:
        raise CallableFromPathError(str(e))

    if s_cls:
        cls = getattr(package, s_cls)
        obj = cls(*args, **kwargs)
    else:
        obj = package

    try:
        callable_ = getattr(obj, callable_name)
    except AttributeError as e:
        raise CallableFromPathError(str(e))

    return callable_


def get_timeout_from_headers(headers):
    """Return the timeout value if it exists in the given headers

    Retruns:
        timeout(int): The timeout if found, else None
    """
    timeout = None
    for header in headers.split(','):
        if 'timeout:' in header:
            timeout = int(header.split(':')[1])
    return timeout
