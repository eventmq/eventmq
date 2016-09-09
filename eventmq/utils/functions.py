import json
import hashlib
import importlib
import inspect

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


def run_function(path, callable_name,
                 class_args=(), class_kwargs=None, args=(), kwargs=None):
    """Constructs a callable from `path` and `callable_name` and calls it.

    Args:
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
        callable_ = callable_from_path(
            path, callable_name, *class_args, **class_kwargs)
    except CallableFromPathError as e:
        logger.exception('Error importing callable {}.{}: {}'.format(
            path, callable_name, str(e)))
        return

    return callable_(*args, **kwargs)


def arguments_hash(*args, **kwargs):
    """Takes `args` and `kwargs` and creates a unique identifier."""
    args = {
        'args': args,
        'kwargs': kwargs,
    }

    data = json.dumps(args, cls=IgnoreJSONEncoder)
    return hashlib.sha1(data).hexdigest()


def path_from_callable(func):
    """
    Builds the module path in string format for a callable.

    .. note:
       To use a callable Object, pass Class.__call__ as func and provide any
       class_args/class_kwargs. This is so side effects from pickling won't
       occur.

    Args:
        func (callable): The function or method to build the path for

    Returns:
        list: (import path (w/ class seperated by a ':'), callable name) or
            (None, None) on error.
    """
    callable_name = None

    path = None
    # Methods also have the func_name property
    if inspect.ismethod(func):
        path = ("{}:{}".format(func.__module__, func.im_class.__name__))
        callable_name = func.func_name
    elif inspect.isfunction(func):
        path = func.__module__
        callable_name = func.func_name
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
        return None, None

    return path, callable_name


def callable_from_path(path, callable_name, *args, **kwargs):
    """Build a callable from a path and callable_name.

    This function is the opposite of `path_from_callable`.  It takes what is
    returned from `build_module_name` and converts it back to the original
    caller.

    Args:
        path (str): The module path of the callable.  This is the first
            position in the tuple returned from `path_from_callable`.
        callable_name (str): The name of the function.  This is the second
            position of the tuple returned from `path_from_callable`.
        *args (list): if `callable_name` is a method on a class, these
            arguments will be passed to the constructor when instantiating the
            class.
        *kwargs (dict): if `callable_name` is a method on a class, these
            arguments will be passed to the constructor when instantiating the
            class.

    Returns:
        function: The callable
    """
    if ':' in path:
        _pksplit = path.split(':')
        s_package = _pksplit[0]
        s_cls = _pksplit[1]
    else:
        s_package = path
        s_cls = None

    try:
        package = importlib.import_module(s_package)
        reload(package)
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
