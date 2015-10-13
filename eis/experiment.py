import numpy as np
import pandas as pd
import yaml
import sqlalchemy
import logging
import sys
import pickle
import pdb
import datetime
from itertools import product

from eis import setup_environment, dataset, models


def main(config_file_name="default.yaml"):
    logging.basicConfig(format="%(asctime)s %(message)s",
                        filename="default.log", level=logging.INFO)
    log = logging.getLogger("Police EIS")

    screenlog = logging.StreamHandler(sys.stdout)
    screenlog.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
    screenlog.setFormatter(formatter)
    log.addHandler(screenlog)

    timestamp = datetime.datetime.now().isoformat()

    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
        log.info("Loaded experiment file")
    except:
        log.exception("Failed to get experiment configuration file!")

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

    if config["unit"] == "officer":
        log.info("Loading officers and features to use as training...")
        train_x, train_y, train_id, names = dataset.grab_officer_data(
            config["features"], train_start_date, fake_today, fake_today)

        log.info("Loading officers and features to use as testing...")
        test_x, test_y, test_id, names = dataset.grab_officer_data(
            config["features"], fake_today, test_end_date, fake_today)

    elif config["unit"] == "dispatch":
        log.info("Loading dispatch events and features to use as training...")
        pass

        log.info("Loading dispatch events and features to use as testing...")
        pass

    log.info("Running models on dataset...")

    parameter_names = sorted(config["parameters"])
    parameter_values = [config["parameters"][p] for p in parameter_names]
    all_params = product(*parameter_values)

    for each_param in all_params:
        timestamp = datetime.datetime.now().isoformat()

        parameters = {name: value for name, value
                          in zip(parameter_names, each_param)}
        log.info("Training model: {} with {}".format(config["model"],
            parameters))
        result_y, importances = models.run(train_x, train_y,
                                           test_x, config["model"],
                                           parameters)

        config["parameters"] = parameters
        log.info("Saving pickled results...")
        to_save = {"test_labels": test_y,
                   "test_predictions": result_y,
                   "config": config,
                   "features": names,
                   "timestamp": timestamp,
                   "parameters": parameters,
                   "train_start_date": train_start_date,
                   "test_end_date": test_end_date,
                   "feature_importances": importances}

        pkl_file = "{}{}_{}.pkl".format(
            config['directory'], config['pkl_prefix'], timestamp)
        pickle_results(pkl_file, to_save)

    log.info("Done!")
    return None


def pickle_results(pkl_file, to_save):
    """
    Save contents of experiment to pickle file for later use
    """

    with open(pkl_file, 'wb') as f:
        pickle.dump(to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

    return None

if __name__ == "__main__":
    main()
