import numpy as np
import pandas as pd
import yaml
import json
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

from triage.storage import InMemoryModelStorageEngine
from . import setup_environment
from . import populate_features, populate_labels
from . import utils
from .run_models import RunModels
from triage.utils import save_experiment_and_get_hash 

from sklearn import linear_model, preprocessing

log = logging.getLogger(__name__)


def main(chosen_model_group_id, matrix_location):

    now = datetime.datetime.now().strftime('%d-%m-%y_%H:%M:S')
    log_filename = 'logs/{}.log'.format(now)
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        level=logging.DEBUG,
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    log = logging.getLogger('eis')

    try:
        db_engine = setup_environment.get_database()
    except:
        log.warning('Could not connect to the database')

    test_object = populate_production_schema(db_engine, chosen_model_group_id,
        matrix_location) 
    test_object.populate_production_predictions()
    test_object.populate_production_time_delta()
    test_object.populate_production_individual_importances()

    log.info("Done!")
    return None



def get_query(feature_name):
    ## Load the features_config
    with open('eis/features/features_descriptions.yaml') as f:
        features_config = yaml.load(f)
    query = """SELECT * from public.get_feature_complete_description('{feature}',
        '{feature_names}'::JSON, '{time_aggregations}'::JSON, '{metrics}'::JSON)""".format(feature=feature_name,feature_names=json.dumps(features_config['feature_names']), time_aggregations = json.dumps(features_config['time_aggregations']), metrics = json.dumps(features_config['metrics_name']))
    return query


class populate_production_schema(object):

    def __init__(self, db_engine, chosen_model_group_id, matrix_location):
        self.db_engine = db_engine
        self.chosen_model_group_id = chosen_model_group_id
        self.matrix_location = matrix_location


    def populate_production_predictions(self):
        """
        Assumes production.predictions table exists
        INPUT:      the model group id you want to use and database info
        OUTPUT:     returns 'true' on completion
        """

        db_conn = self.db_engine.raw_connection()
        query = "select production.populate_predictions({:d});".format(self.chosen_model_group_id)
        db_conn.cursor().execute(query)
        db_conn.commit()
        db_conn.close()


    def populate_production_time_delta(self):
        """
        Assumes production.time_delta table exists
        INPUT:  database info
        OUTPUT: returns 'true' on completion
        """

        db_conn = self.db_engine.raw_connection()
        query = "select production.populate_time_delta();"
        db_conn.cursor().execute(query)
        db_conn.commit()
        db_conn.close()

    def populate_production_individual_importances(self):
        """
        Assumes production.individual_importances table exists
        INPUT:      the model group id you want to use and database info
        OUTPUT:     returns 'true' on completion
        """

        db_conn = self.db_engine.raw_connection()

        query = "select production.populate_individual_importances({:d});".format(self.chosen_model_group_id)
        db_conn.cursor().execute(query)
        db_conn.commit()

        ## Get the feature names you are trying to map
        feature_query = "SELECT distinct risk_1 as feature from production.individual_importances order by risk_1;"
        feature_names = pd.read_sql(feature_query, db_conn)

        list_of_dfs = []
        for i in range(len(feature_names)):
            list_of_dfs.append(pd.read_sql(get_query(feature_names.feature[i]), db_conn))

        ## Concat the dfs into one df
        feature_mapping = pd.concat(list_of_dfs, axis=0, ignore_index=True)
        feature_mapping['column_new_name'] = feature_mapping['metric_name'] + ' ' + feature_mapping['feature_long_name'] + ' ' + feature_mapping['of_type'] + ' ' + feature_mapping['time_aggregation']
        feature_mapping = feature_mapping.drop(['metric_name', 'feature_long_name', 'of_type', 'time_aggregation'], axis=1)

        ## replace old values with new values
        individual_importances = pd.read_sql('select * from production.individual_importances;', db_conn)
        individual_importances1 = pd.merge(individual_importances[['model_id', 'as_of_date','risk_1']], feature_mapping, left_on='risk_1', right_on='column_original_name', how='left')
        individual_importances2 = pd.merge(individual_importances[['model_id', 'as_of_date','risk_2']], feature_mapping, left_on='risk_2', right_on='column_original_name', how='left')
        individual_importances3 = pd.merge(individual_importances[['model_id', 'as_of_date','risk_3']], feature_mapping, left_on='risk_3', right_on='column_original_name', how='left')
        individual_importances4 = pd.merge(individual_importances[['model_id', 'as_of_date','risk_4']], feature_mapping, left_on='risk_4', right_on='column_original_name', how='left')
        individual_importances5 = pd.merge(individual_importances[['model_id', 'as_of_date','risk_5']], feature_mapping, left_on='risk_5', right_on='column_original_name', how='left')
        
        individual_importances = pd.merge(individual_importances1, individual_importances2, on=['model_id', 'as_of_date'])
        individual_importances = pd.merge(individual_importances, individual_importances3, on=['model_id', 'as_of_date'])
        individual_importances = pd.merge(individual_importances, individual_importances4, on=['model_id', 'as_of_date'])
        individual_importances = pd.merge(individual_importances, individual_importances5, on=['model_id', 'as_of_date'])


        ## Write to csv
        individual_importances.to_sql('production.individual_importances', db_conn, if_exists='replace')
        

        db_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--modelgroupid", type=int, help="pass your chosen model group id", default=1)
    parser.add_argument("--matrixlocation", type=str, help="pass the path to your stored hdf files")
    args = parser.parse_args()
    main(args.modelgroupid, args.matrixlocation)

