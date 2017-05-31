
import hashlib
import logging
import functools

import oss2

logger = logging.getLogger(__name__)


class Auth(oss2.Auth):
    def __get_resource_string(self, req, bucket_name, key):
        return '/{0}/{1}{2}'.format(bucket_name, key, '?restore')


class Bucket(oss2.Bucket):

    auth_for_restore = None

    def restore(self, key):
        """Restore an archive object. The oss2 does not offer this interface. I
        guess the main reason is that oss2 use requests to make http request,
        but requests.Request can not generate the url /obejct?restore using the
        params, which cause ass2.Auth._sign_request failed.
        """
        if self.auth_for_restore is None:
            self.auth_for_restore = Auth(self.auth.id, self.auth.secret)

        key = oss2.compat.to_string(key)
        url = '%s?restore' % self._make_url(self.bucket_name, key)
        req = oss2.http.Request('POST', url)
        self.auth_for_restore._sign_request(req, self.bucket_name, key)

        resp = self.session.do_request(req, timeout=self.timeout)
        if resp.status // 100 != 2:
            raise oss2.exceptions.make_exception(resp)

        return resp


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
    multipart_threshold = 30*1024*1024

    # for transaction
    num_threads = 2
    cache_dir = "/tmp"

    # log configuration
    log_config = None
    log_file = None

    # skip directory
    skip_dir = []

    def __init__(self):
        import foxy_sync_settings
        for key in ("access_key_id", "access_key_secret", "end_point", "bucket",
                    "multipart_threshold", "num_threads", "cache_dir",
                    "log_config", "log_file", "skip_dir"):
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
