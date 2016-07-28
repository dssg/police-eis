import numpy as np
import pandas as pd
import yaml
import logging
import sys
import argparse
import pickle
import psycopg2
import datetime
import time
import pdb

from . import setup_environment, models, scoring
from . import dataset, experiment, groups
from . import populate_features

def main(config_file_name, args):
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        level=logging.DEBUG)
    log = logging.getLogger('eis')

    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
        log.info("Loaded experiment file: {}".format(config_file_name))
    except:
        log.exception("Failed to get experiment configuration file!")

    # If asked to generate features, then do that and stop.
    if args.buildfeatures:

        log.info("Re-building features...")

        # set the features table name based on type of prediction (officer / dispatch)
        table_name = '{}_features'.format(config['unit'])

        # Create the features table.
        populate_features.create_features_table(config, table_name)

        # Populate the featuress table
        populate_features.populate_features_table(config, table_name)

        log.info('Done creating features table')
        sys.exit()

    all_experiments = experiment.generate_models_to_run(config)

    log.info("Running models on dataset...")
    batch_timestamp = datetime.datetime.now().isoformat()
    for my_exp in all_experiments:
        start = time.time()
        timestamp = datetime.datetime.now().isoformat()

        result_y, result_y_binary, importances, modelobj, individual_imps = models.run(
            my_exp.exp_data["train_x"],
            my_exp.exp_data["train_y"],
            my_exp.exp_data["test_x"],
            my_exp.config["model"],
            my_exp.config["parameters"],
            my_exp.config["n_cpus"])

        if my_exp.config["aggregation"]:
            groupscores = groups.aggregate(my_exp.exp_data["test_x_index"],
                                           result_y, my_exp.config["fake_today"])
        else:
            groupscores = []

        if my_exp.config["pilot"]:
            log.info("Generating pilot")
            pilot_y, pilot_importances, __, pilot_individual_imps = models.run(
                my_exp.pilot_data["train_x"], my_exp.pilot_data["train_y"],
                my_exp.pilot_data["test_x"], my_exp.config["model"],
                my_exp.config["parameters"], my_exp.config["n_cpus"])
            pilot_save = {"test_predictions": pilot_y,
                          "feature_importances": pilot_importances,
                          "individual_importances": pilot_individual_imps,
                          "features": my_exp.pilot_data["names"],
                          "officer_id_train": my_exp.pilot_data["train_x_index"],
                          "officer_id_test": my_exp.pilot_data["test_x_index"],
                          "train_x": my_exp.pilot_data["train_x"],
                          "train_y": my_exp.pilot_data["train_y"],
                          "test_x": my_exp.pilot_data["test_x"]}
            pilot_file = "{}pilot_experiment_{}.pkl".format(my_exp.config["pilot_dir"], timestamp)
            pickle_results(pilot_file, pilot_save)
        #commented out for now, but we need to create a flag for whether an eis system already exists in the department
        #if config['eis_table']:
        #    confusion_matrices = scoring.test_thresholds(
        #	    my_exp.exp_data["test_x_index"], result_y,
        #        my_exp.config['fake_today'], my_exp.exp_data["test_end_date"])
        #else:
        confusion_matrices = []

        # TODO: make this more robust for officer vs dispatch level predictions
        end = time.time()
        #pdb.set_trace()
        model_time_in_seconds = "%.3f" % (end-start)
        print(model_time_in_seconds)

        if config['unit'] == 'officer':
            to_save = {"test_labels": my_exp.exp_data["test_y"],
                       "test_predictions": result_y,
                       "test_predictions_binary" : result_y_binary,
                       "config": my_exp.config,
                       "officer_id_train": my_exp.exp_data["train_x_index"],
                       "officer_id_test": my_exp.exp_data["test_x_index"],
                       "features": my_exp.exp_data["names"],
                       "timestamp": timestamp,
                       "parameters": my_exp.config["parameters"],
                       "train_start_date": my_exp.exp_data["train_start_date"],
                       "test_end_date": my_exp.exp_data["test_end_date"],
                       "feature_importances": importances,
                       "feature_importances_names": my_exp.exp_data["features"],
                       "aggregation": groupscores,
                       "eis_baseline": confusion_matrices,
                       "modelobj": modelobj,
                       "individual_importances": individual_imps,
                       "time_for_model_in_seconds": model_time_in_seconds }

        elif config['unit'] == 'dispatch':
            to_save = {"test_labels": my_exp.exp_data["test_y"],
                       "test_predictions": result_y,
                       "test_predictions_binary": result_y_binary,
                       "config": my_exp.config,
                       "timestamp": timestamp,
                       "parameters": my_exp.config["parameters"],
                       "feature_importances_names": my_exp.exp_data["features"],
                       "modelobj": modelobj,
                       "time_for_model_in_seconds": model_time_in_seconds }

        # get all model metrics.
        all_metrics = scoring.calculate_all_evaluation_metrics( list( my_exp.exp_data["test_y"]), list(result_y), list(result_y_binary), model_time_in_seconds )

        # create a pickle object to store into the database.
        model_data_pickle_object = pickle.dumps( to_save )

        # package data for storing into results schema.
        unit_id_train    = list( my_exp.exp_data["train_x_index"] )
        unit_id_test     = list( my_exp.exp_data["test_x_index"] )
        unit_predictions = list( result_y )
        unit_labels      = list( my_exp.exp_data["test_y"] )

        # Store information about this experiment into the results schema.
        dataset.store_model_info( timestamp, batch_timestamp, my_exp.config, model_data_pickle_object )
        dataset.store_prediction_info( timestamp, unit_id_train, unit_id_test, unit_predictions, unit_labels )
        dataset.store_evaluation_metrics( timestamp, all_metrics )

        if my_exp.config["auditing"]:
            audit_outputs = {"train_x": my_exp.exp_data["train_x"],
                             "train_y": my_exp.exp_data["train_y"],
                             "officer_id_train": my_exp.exp_data["train_x_index"],
                             "officer_id_test": my_exp.exp_data["test_x_index"],
                             "test_predictions": result_y,
                             "test_y": my_exp.exp_data["test_y"],
                             "test_x": my_exp.exp_data["test_x"]}
            audit_file = "{}audit_{}.pkl".format(my_exp.config['audits'], timestamp)
            pickle_results(audit_file, audit_outputs)

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
    parser = argparse.ArgumentParser()
    parser.add_argument("config", type=str, help="pass your config", default="default.yaml")
    parser.add_argument( "-f", "--buildfeatures", help="build the features and stop", action='store_true' )
    args = parser.parse_args()
    main(args.config, args)
