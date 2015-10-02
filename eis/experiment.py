import numpy as np
import pandas as pd
import yaml
import sqlalchemy
import logging
import sys
import pickle
import datetime

from eis import setup_environment, dataset, evaluate


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
        engine = setup_environment.get_connection_from_profile()
        log.info("Connected to PostgreSQL database")
    except IOError:
        log.exception("Failed to get database connection")
        sys.exit(1)

    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
        log.info("Loaded configuration file")
    except:
        log.exception("Failed to get experiment configuration file!")

    log.info("Loading officers to use as training...")
    log.info("Loading features for training on officers...")

    log.info("Loading officers to be tested...")
    log.info("Loading features for officers to be tested...")

    log.info("Running models on dataset...")

    log.info("Evaluating model...")

    log.info("Saving pickled results...")
    pkl_file = "{}{}_{}.pkl".format(
        config['directory'], config['pkl_prefix'])
    pickle_results(pkl_file, config)

    log.info("Done!")
    return None


def get_officers():
    pass


def pickle_results(pkl_file, config):
    """
    Save contents of experiment to pickle file for later use
    """

    to_save = {"config": config}

    with open(pkl_file, 'wb') as f:
        pickle.dump(to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

if __name__ == "__main__":
    main()
