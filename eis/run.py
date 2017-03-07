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
import os
import pdb
from joblib import Parallel, delayed
from multiprocessing.util import register_after_fork

from . import setup_environment 
from . import populate_features, populate_labels
from . import utils
from . import officer
from .run_models import RunModels

log = logging.getLogger(__name__)

def main(config_file_name, labels_config_file, args):

    now = datetime.datetime.now().strftime('%d-%m-%y_%H:%M:S')
    log_filename = 'logs/{}.log'.format(now)
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        level=logging.DEBUG,
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    log = logging.getLogger('eis')

    # read config files
    config = utils.read_yaml(config_file_name)
    labels_config = utils.read_yaml(labels_config_file)

    log.debug("feature table name: {}".format(config['officer_feature_table_name']))

    # If asked to generate features, then do that and stop.
    if args.buildfeatures:

        log.info("Re-building features...")

        # Create the features and labels table.
        populate_labels.create_labels_table(config, config['officer_label_table_name'])

        # Populate the featuress  and labels table
        populate_features.populate_features_table(config, config['officer_feature_table_name'], config["schema_feature_blocks"])
        populate_labels.populate_labels_table(config, labels_config, config['officer_label_table_name'])

        log.info('Done creating features table')
        sys.exit()

    ## Get feature names
    features = officer.get_officer_features_table_columns(config)
    # modify models_config
    grid_config = utils.generate_model_config(config)

    # Generate temporal_sets
    temporal_sets = utils.generate_temporal_info(config['temporal_info'])

    # Add more arguments
    misc_db_parameters = {'config': config,
            'test': config['test_flag'],
            'model_comment': config['model_comment'],
            'batch_comment': config['batch_comment'],
            'labels_config': labels_config,
            }

    log.info("Running models on dataset...")
    models_args = {'labels': config['labels'],
                   'features': features,
                   'features_table_name':config['officer_feature_table_name'],
                   'labels_config': labels_config,
                   'labels_table_name': config['officer_label_table_name'],
                   'grid_config':grid_config,
                   'project_path':config['project_path'],
                   'misc_db_parameters': misc_db_parameters}

    n_cups = config['n_cpus']

    if args.generatematrices:
        # Parallelization
        for temporal_set in temporal_sets:
            generate_all_matrices(temporal_set, **models_args)

        log.info('Done creating all matrices')
        sys.exit() 

    # Run models
    Parallel(n_jobs=n_cups, verbose=5)(delayed(apply_train_test)(temporal_set, **models_args)
                                                 for temporal_set in temporal_sets)

    log.info("Done!")
    return None


def generate_all_matrices(temporal_set, **kwargs):
    # Connect to db
    try:
        db_engine = setup_environment.get_database()
    except:
        log.warning('Could not connect to the database')
        raise

    run_model = RunModels(labels=kwargs['labels'],
                      features=kwargs['features'],
                      features_table_name=kwargs['features_table_name'],
                      labels_config=kwargs['labels_config'],
                      labels_table_name=kwargs['labels_table_name'],
                      temporal_split=temporal_set,
                      grid_config=kwargs['grid_config'],
                      project_path=kwargs['project_path'],
                      misc_db_parameters=kwargs['misc_db_parameters'],
                      db_engine=db_engine)
    pdb.set_trace()
    run_model.generate_matrices()
    db_engine.dispose()
    return None

def apply_train_test(temporal_set, **kwargs):
    # Connect to db
    try:
        db_engine = setup_environment.get_database()
    except:
        log.warning('Could not connect to the database')
        raise
    
    run_model = RunModels(labels=kwargs['labels'],
                      features=kwargs['features'],
                      features_table_name=kwargs['features_table_name'],
                      labels_config=kwargs['labels_config'],
                      labels_table_name=kwargs['labels_table_name'],
                      temporal_split=temporal_set,
                      grid_config=kwargs['grid_config'],
                      project_path=kwargs['project_path'],
                      misc_db_parameters=kwargs['misc_db_parameters'],
                      db_engine=db_engine)

    log.info('Run models for temporal set: {}'.format(temporal_set))
    train_uuid, model_ids = run_model.train_models()
    log.info('Run tests')
    run_model.test_models(train_uuid, model_ids)
    db_engine.dispose() 
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="pass your config", default="default.yaml")
    parser.add_argument("--labels", type=str, help="pass your labels config", default="labels.yaml")
    parser.add_argument("-b", "--buildfeatures", help="build the features and stop", action='store_true')
    parser.add_argument("-m", "--generatematrices", help="build all matrices used for running models", action='store_true')
    args = parser.parse_args()
    main(args.config, args.labels, args)
