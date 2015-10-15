import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime

from eis import dataset

log = logging.getLogger(__name__)


def setup(config):
    fake_today = datetime.datetime.strptime(config["fake_today"], "%d%b%Y")
    train_start_date = datetime.datetime.strptime(config["fake_today"],
                                                  "%d%b%Y") - \
        datetime.timedelta(days=config["prediction_interval"])
    test_end_date = datetime.datetime.strptime(config["fake_today"],
                                               "%d%b%Y") + \
        datetime.timedelta(days=config["prediction_interval"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(fake_today))
    log.info("Test label window start: {}".format(fake_today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    train_x, train_y, train_id, names = dataset.grab_officer_data(
        config["features"], train_start_date, fake_today, train_start_date)

    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id, names = dataset.grab_officer_data(
        config["features"], fake_today, test_end_date, fake_today)

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_y": test_y,
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date}
