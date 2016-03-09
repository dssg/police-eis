import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime
from sklearn import preprocessing

from eis import dataset, compare_eis

log = logging.getLogger(__name__)


def pilot_setup(config):
    pilot_today = datetime.datetime.strptime(config["pilot_today"], "%d%b%Y")
    train_start_date = datetime.datetime.strptime(config["pilot_today"],
                                                  "%d%b%Y") - \
        datetime.timedelta(days=config["prediction_interval"])
    test_end_date = datetime.datetime.strptime(config["pilot_today"],
                                               "%d%b%Y") + \
        datetime.timedelta(days=config["prediction_interval"])

    log.info("Train label window start for pilot: {}".format(train_start_date))
    log.info("Train label window stop for pilot: {}".format(pilot_today))
    log.info("Test label window start for pilot: {}".format(pilot_today))
    log.info("Test label window stop for pilot: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    train_x, train_y, train_id, names = dataset.grab_officer_data(
        config["features"], train_start_date, pilot_today, train_start_date,
        config["accidents"], config["noinvest"])

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    log.info("Loading officers and features to use as testing...")
    test_x, __, test_id, names = dataset.grab_officer_data(
        config["features"], pilot_today, test_end_date, pilot_today,
        config["accidents"], True)

    train_x_index = train_x.index
    test_x_index = test_x.index
    features = train_x.columns.values

    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}


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
        config["features"], train_start_date, fake_today, train_start_date,
        config["accidents"], config["noinvest"])

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id, names = dataset.grab_officer_data(
        config["features"], fake_today, test_end_date, fake_today,
        config["accidents"], True)

    train_x_index = train_x.index
    test_x_index = test_x.index
    features = train_x.columns.values

    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)
 
    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_y": test_y,
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}