

import hashlib


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
        raise Exception("path missing.")


class SingletonMeta(type):

    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = type.__call__(cls, *args, **kwargs)
        return cls.instance


class Config(metaclass=SingletonMeta):

    access_key_id = None
    access_key_secret = None
    end_point = None
    test_bucket = None
    test_local = None
    max_workers = None

    def __init__(self):
        import foxy_sync_settings
        for key in ("access_key_id", "access_key_secret", "end_point",
                    "test_bucket", "test_local", "max_workers"):
            setattr(self, key, getattr(foxy_sync_settings, key, None))
