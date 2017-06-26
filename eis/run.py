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
from itertools import product
from joblib import Parallel, delayed
import json

from triage.storage import InMemoryModelStorageEngine
from . import setup_environment
from . import populate_features, populate_labels
from . import utils
from .run_models import RunModels
from triage.utils import save_experiment_and_get_hash
from dateutil.relativedelta import relativedelta

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

    # If you specify production in the args
    if args.production:
        # print(args.modelgroup)
        # print(args.date)

        db_engine = setup_environment.get_database()

        # Write query to pull all information, write to a new config to pass to populate_features
        config_query = '''
          SELECT config->'feature_blocks',
          config->'unit',
          config->'temporal_info'->'timegated_feature_lookback_duration', --needs to be updated
          model_groups.model_type,
          model_groups.model_parameters,
          model_groups.model_config
          from results.models
          JOIN results.model_groups USING (model_group_id)
          where model_group_id = {} LIMIT 1;'''.format(args.modelgroup)

        config_df = pd.read_sql(config_query, db_engine)

        prod_config = {'feature_blocks': config_df.iloc[0, 0],
                       'unit': config_df.iloc[0, 1],
                       'temporal_info': {'timegated_feature_lookback_duration':config_df.iloc[0, 2]},
                       'model_type': config_df.iloc[0, 3],
                       'model_parameters': config_df.iloc[0, 4]}

        # individual model parameters
        model_config = config_df.iloc[0, 5]

        # set time parameters:
        prod_config['temporal_info']['prediction_window'] = [model_config['prediction_window']]
        prod_config['temporal_info']['train_size'] = [model_config['train_size']]
        prod_config['temporal_info']['features_frequency'] = [model_config['features_frequency']]

        #set the config parameter
        prod_config['officer_features']=model_config['blocks']

        # Load in arguments, convert to appropriate time deltas
        pred_wind = prod_config['temporal_info']['prediction_window']
        pred_wind_dict = utils.relative_deltas_conditions(pred_wind)
        pred_wind_delta = relativedelta(**pred_wind_dict[pred_wind[0]])

        train_size = prod_config['temporal_info']['train_size']
        train_size_dict = utils.relative_deltas_conditions(train_size)
        train_size_delta = relativedelta(**train_size_dict[train_size[0]])

        start_date = pd.to_datetime(args.date) - pred_wind_delta - train_size_delta

        prod_config['temporal_info']['start_date'] = start_date.strftime("%Y-%m-%d")
        prod_config['temporal_info']['end_date'] = args.date
        prod_config['temporal_info']['update_window'] = ['99y']
        prod_config['temporal_info']['test_time_ahead'] = ['0d']
        prod_config['temporal_info']['test_frequency'] = ['1d']
        prod_config['temporal_info']['officer_past_activity_window'] = ['1y']

        # Specify number of cpus for feature building
        cpu = {'n_cpus': config['n_cpus']}
        prod_config.update(cpu)

        # To generate matrices need this info
        temporal_sets = utils.generate_temporal_info(prod_config['temporal_info'])
        block_sets = utils.feature_blocks_sets(prod_config['officer_features'], 0)

        # needs to be a list
        params = dict((k[0], [k[1]]) for k in prod_config['model_parameters'].items())
        grid_config = {prod_config['model_type']: params}

        # More info
        misc_db_parameters = {'config': prod_config,
                              'test': config['test_flag'],
                              'model_comment': config['model_comment'],
                              'batch_comment': config['batch_comment']
                              }
        # TODO: lables needs to be read from the database too
        models_args = {'labels': config['labels'],
                       'features': prod_config['feature_blocks'],
                       'schema_name': config["production_schema_feature_blocks"],
                       'feature_lookback_duration': prod_config['temporal_info']['timegated_feature_lookback_duration'],
                       'labels_config': labels_config,
                       'labels_table_name': config['production_officer_label_table_name'],
                       # config['officer_label_table_name'],
                       'grid_config': grid_config,
                       'project_path': config['project_path'],
                       'misc_db_parameters': misc_db_parameters}

        populate_features.populate_features_table(prod_config, config['production_schema_feature_blocks'])

        # Create labels table-> Right now the labels configuration is read from the command line and not from the database
        populate_labels.create_labels_table(config,  config['production_officer_label_table_name'])
        populate_labels.populate_labels_table(config, labels_config, config['production_officer_label_table_name'])

        log.info("Done building the features required for production use")

        # build the matrices
        Parallel(n_jobs=1, verbose=51)(delayed(generate_all_matrices)(temporal_set, blocks, **models_args)
                                       for temporal_set, blocks in product(temporal_sets, block_sets))

        log.info("Done building the matrices required for production use")

        # run the model
        # Run models
        db_engine = setup_environment.get_database()

        Parallel(n_jobs=1, verbose=51)(
            delayed(apply_score_day)(temporal_set, blocks, args.modelgroup, args.date, **models_args)
            for temporal_set, blocks in product(temporal_sets, block_sets))

        log.info('Done running the model and storing the output')
        sys.exit()

    # If asked to generate features, then do that and stop.
    if args.buildfeatures:
        log.info("Re-building features...")

        # Create the features and labels table.
        populate_labels.create_labels_table(config, config['officer_label_table_name'])

        # Populate the featuress  and labels table
        populate_features.populate_features_table(config, config["schema_feature_blocks"])
        populate_labels.populate_labels_table(config, labels_config, config['officer_label_table_name'])

        log.info('Done creating features table')
        sys.exit()

    # modify models_config
    grid_config = utils.generate_model_config(config)

    # Generate temporal_sets
    temporal_sets = utils.generate_temporal_info(config['temporal_info'])
    # Combination of blocks for iteration
    block_sets = utils.feature_blocks_sets(config['officer_features'], config['leave_out'])

    # Add more arguments
    misc_db_parameters = {'config': config,
                          'test': config['test_flag'],
                          'model_comment': config['model_comment'],
                          'batch_comment': config['batch_comment']
                          }

    models_args = {'labels': config['labels'],
                   'features': config['feature_blocks'],
                   'schema_name': config["schema_feature_blocks"],
                   'feature_lookback_duration': config['temporal_info']['timegated_feature_lookback_duration'],
                   'labels_config': labels_config,
                   'labels_table_name': config['officer_label_table_name'],
                   'grid_config': grid_config,
                   'project_path': config['project_path'],
                   'misc_db_parameters': misc_db_parameters}

    n_cups = config['n_cpus']

    if args.generatematrices:
        # Parallelization
        Parallel(n_jobs=n_cups, verbose=51)(delayed(generate_all_matrices)(temporal_set, blocks, **models_args)
                                            for temporal_set, blocks in product(temporal_sets, block_sets))

        log.info('Done creating all matrices')
        sys.exit()


        # Run models
    db_engine = setup_environment.get_database()
    experiment_hash = save_experiment_and_get_hash(config, db_engine)
    models_args['experiment_hash'] = experiment_hash

    Parallel(n_jobs=n_cups, verbose=5)(delayed(apply_train_test)(temporal_set, blocks, **models_args)
                                       for temporal_set, blocks in product(temporal_sets, block_sets))

    log.info("Done!")
    return None


def generate_all_matrices(temporal_set, blocks, **kwargs):
    # Connect to db
    try:
        db_engine = setup_environment.get_database()
    except:
        log.warning('Could not connect to the database')
        raise

    run_model = RunModels(labels=kwargs['labels'],
                          features=kwargs['features'],
                          schema_name=kwargs['schema_name'],
                          blocks=blocks,
                          feature_lookback_duration=kwargs['feature_lookback_duration'],
                          labels_config=kwargs['labels_config'],
                          labels_table_name=kwargs['labels_table_name'],
                          temporal_split=temporal_set,
                          grid_config=kwargs['grid_config'],
                          project_path=kwargs['project_path'],
                          misc_db_parameters=kwargs['misc_db_parameters'],
                          db_engine=db_engine)

    log.info('Run models for temporal set: {}'.format(temporal_set))
    log.info('Run models for feature blocks: {}'.format(blocks))
    run_model.generate_matrices()
    db_engine.dispose()
    return None


def apply_train_test(temporal_set, blocks, **kwargs):
    # Connect to db
    try:
        db_engine = setup_environment.get_database()
    except:
        log.warning('Could not connect to the database')
        raise

    run_model = RunModels(labels=kwargs['labels'],
                          features=kwargs['features'],
                          schema_name=kwargs['schema_name'],
                          blocks=blocks,
                          feature_lookback_duration=kwargs['feature_lookback_duration'],
                          labels_config=kwargs['labels_config'],
                          labels_table_name=kwargs['labels_table_name'],
                          temporal_split=temporal_set,
                          grid_config=kwargs['grid_config'],
                          project_path=kwargs['project_path'],
                          misc_db_parameters=kwargs['misc_db_parameters'],
                          experiment_hash=kwargs['experiment_hash'],
                          db_engine=db_engine)

    log.info('Run models for temporal set: {}'.format(temporal_set))
    log.info('Run models for feature blocks: {}'.format(blocks))

    model_storage = InMemoryModelStorageEngine('empty')
    train_matrix_uuid, model_ids_generator = run_model.setup_train_models(model_storage)
    if train_matrix_uuid is None:
        return None

    log.info('Run tests')
    run_model.train_test_models(train_matrix_uuid, model_ids_generator, model_storage)
    db_engine.dispose()
    return None


def apply_score_day(temporal_set, blocks, chosen_model_group_id, date, **kwargs):
    # Connect to db
    try:
        db_engine = setup_environment.get_database(production=True)
    except:
        log.warning('Could not connect to the database')
        raise

    run_model = RunModels(labels=kwargs['labels'],
                          features=kwargs['features'],
                          schema_name=kwargs['schema_name'],
                          blocks=blocks,
                          feature_lookback_duration=kwargs['feature_lookback_duration'],
                          labels_config=kwargs['labels_config'],
                          labels_table_name=kwargs['labels_table_name'],
                          temporal_split=temporal_set,
                          grid_config=kwargs['grid_config'],
                          project_path=kwargs['project_path'],
                          misc_db_parameters=kwargs['misc_db_parameters'],
                          db_engine=db_engine)

    log.info('Run models for temporal set: {}'.format(temporal_set))
    log.info('Run models for feature blocks: {}'.format(blocks))

    model_storage = InMemoryModelStorageEngine('empty')
    train_matrix_uuid, model_ids_generator = run_model.setup_train_models(model_storage)
    if train_matrix_uuid is None:
        return None

    log.info('Score model')
    run_model.train_score_models(model_ids_generator, model_storage)

    ### update statistic tables
    # time delta
    query = "select production.populate_time_delta();"
    conn = db_engine.raw_connection()
    conn.cursor().execute(query)
    conn.commit()

    db_engine.dispose()
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="pass your config", default="default.yaml")
    parser.add_argument("--labels", type=str, help="pass your labels config", default="labels.yaml")
    parser.add_argument("--date", type=str, help="pass date")
    parser.add_argument("--modelgroup", type=str, help="specify model group id")
    parser.add_argument("-p", "--production", help="populate the production schemas", action='store_true')
    parser.add_argument("-b", "--buildfeatures", help="build the features and stop", action='store_true')
    parser.add_argument("-m", "--generatematrices", help="build all matrices used for running models",
                        action='store_true')
    args = parser.parse_args()
    main(args.config, args.labels, args)
