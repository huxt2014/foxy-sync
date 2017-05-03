

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

