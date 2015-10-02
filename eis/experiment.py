import numpy as np
import pandas as pd
import yaml
import sqlalchemy
import logging
import sys

from eis import setup_environment


def main():
    logging.basicConfig(format="%(asctime)s %(message)s",
                        filename="default.log", level=logging.INFO)
    log = logging.getLogger("ex")

    screenlog = logging.StreamHandler(sys.stdout)
    screenlog.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
    screenlog.setFormatter(formatter)
    log.addHandler(screenlog)

    try:
        engine = setup_environment.get_connection_from_profile()
        log.info("Loaded database connection")
    except:
        log.exception("Failed to get database connection")

    print("test")

    return None


def get_officers():
    pass

if __name__ == "__main__":
    main()
