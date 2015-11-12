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

from eis import setup_environment, models, officer, dispatch, explore


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

    if config["unit"] == "officer":
        exp_data = officer.setup(config)
    elif config["unit"] == "dispatch":
        exp_data = dispatch.setup(config)

    if config["make_feat_dists"] == True:
        explore.make_all_dists(exp_data)

    log.info("Training data: {} rows. Testing data: {} rows.".format(
        len(exp_data["train_y"]), len(exp_data["test_x"])))

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
        result_y, importances = models.run(exp_data["train_x"],
                                           exp_data["train_y"],
                                           exp_data["test_x"],
                                           config["model"],
                                           parameters)

        config["parameters"] = parameters
        log.info("Saving pickled results...")
        to_save = {"test_labels": exp_data["test_y"],
                   "test_predictions": result_y,
                   "config": config,
                   "features": exp_data["names"],
                   "timestamp": timestamp,
                   "parameters": parameters,
                   "train_start_date": exp_data["train_start_date"],
                   "test_end_date": exp_data["test_end_date"],
                   "feature_importances": importances,
                   "feature_importances_names": exp_data["train_x"].columns.values}

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
