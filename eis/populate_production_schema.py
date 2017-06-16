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
        db_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--modelgroupid", type=int, help="pass your chosen model group id", default=1)
    parser.add_argument("--matrixlocation", type=str, help="pass the path to your stored hdf files")
    args = parser.parse_args()
    main(args.modelgroupid, args.matrixlocation)

