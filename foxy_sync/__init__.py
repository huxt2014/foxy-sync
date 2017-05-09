
import sys
import copy
import argparse
import logging
import logging.config

from .snapshot import *
from .transaction import *
from . import utils

version = "0.1"

logger = logging.getLogger(__name__)


class Run:

    parser = argparse.ArgumentParser()
    parser.add_argument("src")
    parser.add_argument("dest", nargs="?")
    parser.add_argument("-i", action="store_true",
                        help="start transaction immediately.")
    parser.add_argument("--version", action="version", version=version)

    def start(self):
        try:
            self._start()
        except utils.TransactionError as e:
            print(e)
            sys.exit(1)
        except Exception as e:
            logger.exception(e)
            print("some error happened.")
            sys.exit(1)

    def _start(self):
        args = self.parser.parse_args()

        # load configuration
        config = utils.Config()

        # configure logging
        if config.log_config is not None:
            logging.config.dictConfig(config.log_config)
        else:
            c = copy.deepcopy(utils.DEFAULT_LOG)
            if config.log_file is not None:
                c["handlers"]["file"]["filename"] = config.log_file
            logging.config.dictConfig(c)

        if args.dest is None:
            # load a transaction dump
            ts = Transaction.load(args.src)
            if args.i:
                ts.start()
            else:
                print(ts)
        else:
            src = Snapshot.get_instance(args.src)
            dest = Snapshot.get_instance(args.dest)
            ts = src.push_to(dest)
            ts.get_jobs()

            if args.i:
                ts.start()
            else:
                ts.dump()
                print(ts.dump_path)
