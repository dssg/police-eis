import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime
from sklearn import preprocessing

from eis import dataset

log = logging.getLogger(__name__)


def run_traintest(config):
    result = setup(config, config["fake_today"])
    return result


def run_pilot(config):
    result = setup(config, config["pilot_today"])
    return result


def setup(config, today):
    """
    Sets up officer-level experiment

    Args:
    config: dict with config file
    today: string containing the date to split on for temporal cross-validation
    """

    today = datetime.datetime.strptime(today, "%d%b%Y")
    train_start_date = today - datetime.timedelta(days=config["prediction_window"])
    test_end_date = today + datetime.timedelta(days=config["prediction_window"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(today))
    log.info("Test label window start: {}".format(today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    train_x, train_y, train_id, names = dataset.grab_officer_data(
        config["features"], train_start_date, today, train_start_date,
        config["accidents"], config["noinvest"])

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id, names = dataset.grab_officer_data(
        config["features"], today, test_end_date, today,
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
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}
