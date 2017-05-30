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

from triage.storage import InMemoryModelStorageEngine
from . import setup_environment
from . import populate_features, populate_labels
from . import utils
from .run_models import RunModels
from triage.utils import save_experiment_and_get_hash 

from sklearn import linear_model, preprocessing

log = logging.getLogger(__name__)


def main(#config_file_name, labels_config_file, args
        ):

    now = datetime.datetime.now().strftime('%d-%m-%y_%H:%M:S')
    log_filename = 'logs/{}.log'.format(now)
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        level=logging.DEBUG,
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    log = logging.getLogger('eis')

    db_engine = setup_environment.get_database()

    test_object = populate_production_schema(db_engine) 
    test_object.populate_production_predictions(632)
    test_object.populate_production_time_delta()
    test_object.populate_production_individual_importances('/mnt/data/public_safety/charlotte_eis/triage/matrix_feature_mix_1m_6y/')

    log.info("Done!")
    return None



class populate_production_schema(object):

    def __init__(self, db_engine):
        self.db_engine = db_engine


    def populate_production_predictions(self, chosen_model_group_id):
        """
        Assumes production.predictions table exists
        INPUT:      the model group id you want to use and database info
        OUTPUT:     returns 'true' on completion
        """

        try:
            db_engine = setup_environment.get_database()
        except:
            log.warning('Could not connect to the database')
            raise
        
        db_conn = self.db_engine.raw_connection()
        query = "select production.populate_predictions({:d});".format(chosen_model_group_id)
        db_conn.cursor().execute(query)
        db_conn.commit()
        db_conn.close()


    def populate_production_time_delta(self):
        """
        Assumes production.time_delta table exists
        INPUT:  database info
        OUTPUT: returns 'true' on completion
        """

        try:
            db_engine = setup_environment.get_database()
        except:
            log.warning('Could not connect to the database')
            raise

        db_conn = self.db_engine.raw_connection()
        query = "select production.populate_time_delta();"
        db_conn.cursor().execute(query)
        db_conn.commit()
        db_conn.close()


    def populate_production_individual_importances(self, matrix_location):
        """
        Assumes production.individual_importances table exists
        """

        try:
            db_engine = setup_environment.get_database()
        except:
            log.warning('Could not connect to the database')
            raise

        db_conn = db_engine.raw_connection()
        query = """ select model_id, as_of_date, entity_id, a.score, b.matrix_uuid
                    from production.predictions as a
                    inner join results.predictions as b using (model_id, as_of_date, entity_id);"""
        #result = db_conn.cursor().execute(query)
        #print(type(result))
        #db_conn.close()
        #query = "select model_id, entity_id, as_of_date, score from production.predictions where as_of_date = '2015-03-30';"



        individual_explanations_object = pd.DataFrame(columns=['model_id', 'as_of_date', 'entity_id',
            'risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5'])
        y_df = pd.read_sql(query, db_conn)
        
        for matrix in np.unique(y_df['matrix_uuid']):
            file_location = matrix_location + '/' + matrix + '.h5'
            try:
                X_df = pd.read_hdf(file_location, 'df')
            except:
                log.warning('Failed to read hdf file')

            del X_df['outcome']

            data = pd.merge(X_df, y_df, left_on=['officer_id', 'as_of_date'], right_on=['entity_id', 'as_of_date'])
            del data['as_of_date']
            del data['model_id']
            y = data['score']
            X = preprocessing.normalize(data.drop('score', axis=1), copy=False)
            X_names = data.columns.drop('score')

            reg = linear_model.Ridge()
            reg.fit(X, y)

"""
            #daterange = pd.date_range('2015-01-01', '2016-03-31')


i = 0
for single_date in daterange:

    for row in range(data.shape[0]):
        score_contribution = X[row,:] * reg.coef_
        score_contribution_abs = np.absolute(score_contribution)

        individual_features = np.argpartition(score_contribution_abs, -5)[-5:]
        individual_features_names = X_names[individual_features]

        row_insert = [y_df['model_id'][row], single_date, y_df['entity_id'][row]] + list(individual_features_names)
        individual_explanations_object.loc[i,:] = row_insert

        i += 1
"""


if __name__ == '__main__':
    main()


