import numpy as np
import pandas as pd
import yaml
import sqlalchemy
import logging
import sys
import pickle
import pdb
import datetime

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
        datetime.timedelta(days=config["testing_interval_days"])
    test_end_date = datetime.datetime.strptime(config["fake_today"],
                                               "%d%b%Y") + \
        datetime.timedelta(days=config["training_interval_days"])

    log.info("Train window start: {}".format(train_start_date))
    log.info("Train window stop: {}".format(fake_today))
    log.info("Test window start: {}".format(fake_today))
    log.info("Test window stop: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    train_x, train_y, train_id = dataset.grab_data(config["features"],
                                                   train_start_date,
                                                   fake_today)

    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id = dataset.grab_data(config["features"],
                                                train_start_date,
                                                fake_today)

    log.info("Running models on dataset...")
    result_y, importances = models.run(train_x, train_y, test_x, config)

    log.info("Saving pickled results...")
    to_save = {"test_labels": test_y,
               "test_predictions": result_y,
               "config": config,
               "features": config["features"],
               "timestamp": timestamp, 
               "parameters": config["parameters"],
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
