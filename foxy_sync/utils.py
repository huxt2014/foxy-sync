
import hashlib
import logging
import functools


logger = logging.getLogger(__name__)


def get_md5(path=None, block_size=64*1024):
    md5 = hashlib.md5()
    if path:
        with open(path, "rb") as f:
            while True:
                block = f.read(block_size)
                if not block:
                    break
                md5.update(block)
        return md5.hexdigest()
    else:
        raise FoxyException("calculate md5 failed: path missing.")


class SingletonMeta(type):

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = type.__call__(cls, *args, **kwargs)
        return cls.instance


DEFAULT_LOG = {
    'version': 1,
    'formatters': {
        'basic': {
            'format': '%(asctime)s-%(levelname)s:%(message)s'
            }
        },
    'handlers': {
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'basic',
            'filename': '/tmp/foxy-sync.log',
            'when': 'D',
            },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
            }
        },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
        },
    'loggers': {
        # suppress the log info from requests to warn level
        'requests': {
            'level': 'WARN',
            'handlers': ['file'],
            'propagate': False
            },
        },
    # if True, the logger initialized before configuration
    # happening will be disabled.
    'disable_existing_loggers': False
    }


class Config(metaclass=SingletonMeta):
    """Singleton class that stores all the configuration. Though it can be
    initialized in any where of the program, you'd better to initialize it at
    the beginning of the program.
    """

    # for AliOSS
    access_key_id = None
    access_key_secret = None
    end_point = None
    bucket = None
    test_bucket = None

    # for local file system
    test_local = None

    # for transaction
    max_workers = 5
    dump_dir = "/tmp"

    # log configuration
    log_config = DEFAULT_LOG
    log_file = None

    def __init__(self):
        import foxy_sync_settings
        for key in ("access_key_id", "access_key_secret", "end_point", "bucket",
                    "test_bucket", "test_local", "max_workers", "dump_dir",
                    "log_config", "log_file"):
            value = getattr(foxy_sync_settings, key, None)
            if value is not None:
                setattr(self, key, value)


class lazy_property(object):
    """Use descriptor.
    """

    def __init__(self, function):
        self.function = function
        functools.update_wrapper(self, function)

    def __get__(self, instance, owner):
        result = self.function(instance)
        instance.__dict__[self.function.__name__] = result
        return result


class FoxyException(Exception):
    pass


class SnapshotError(FoxyException):
    pass


class TransactionError(FoxyException):
    pass


class JobError(TransactionError):
    pass
