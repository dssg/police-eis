import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime

log = logging.getLogger(__name__)


def setup(config):
    fake_today = datetime.datetime.strptime(config["fake_today"], "%d%b%Y")
    train_start_date = datetime.datetime.strptime(config["fake_today"],
                                                  "%d%b%Y") - \
        datetime.timedelta(days=config["training_interval_days"])
    test_end_date = datetime.datetime.strptime(config["fake_today"],
                                               "%d%b%Y") + \
        datetime.timedelta(days=config["testing_interval_days"])

    log.info("Train window start: {}".format(train_start_date))
    log.info("Train window stop: {}".format(fake_today))
    log.info("Test window start: {}".format(fake_today))
    log.info("Test window stop: {}".format(test_end_date))

    log.info("Loading dispatch events and features to use as training...")
    pass

    log.info("Loading dispatch events and features to use as testing...")
    pass

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_y": test_y,
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date}
