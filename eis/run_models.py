import os
import pandas as pd
import pickle
import pdb
import datetime
import logging
import json

from flufl.lock import Lock

from triage.model_trainers import ModelTrainer
from triage.predictors import Predictor
from triage.storage import FSModelStorageEngine, InMemoryMatrixStore

import metta.metta_io

from .feature_loader import FeatureLoader
from . import dataset
from . import utils
from . import setup_environment
from . import officer
from . import scoring

log = logging.getLogger(__name__)


class RunModels():
    def __init__(
            self,
            labels,
            features,
            features_table_name,
            labels_config,
            labels_table_name,
            temporal_split,
            grid_config,
            project_path,
            misc_db_parameters,
            db_engine=None
    ):

        self.labels = labels
        self.features = features
        self.features_table_name = features_table_name
        self.labels_config = labels_config
        self.labels_table_name = labels_table_name
        self.temporal_split = temporal_split
        self.grid_config = grid_config
        self.project_path = project_path
        self.misc_db_parameters = misc_db_parameters
        self.db_engine = db_engine
        self.matrices_path = self.project_path + '/matrices'

        # feature loader
        self.feature_loader = FeatureLoader(self.features,
                                            self.features_table_name,
                                            self.labels_config,
                                            self.labels,
                                            self.labels_table_name,
                                            self.temporal_split['prediction_window'],
                                            self.temporal_split['officer_past_activity_window']
                                            )

    def load_store_matrix(self, metadata, as_of_dates, return_matrix=True):
        """
        Calls get_dataset to return a pandas DataFrame for the as_of_dates selected
        Args:
           list as_of_dates: as_of_dates to use
        Returns:
           matrix: dataframe with the features and the last column as the label (called: outcome)
        """
        uuid = metta.metta_io.generate_uuid(metadata)
        matrix_filename = self.matrices_path + '/' + uuid

        with Lock(matrix_filename + '.lock',lifetime=datetime.timedelta(minutes=20)):
            if os.path.isfile(matrix_filename + '.h5'):
                log.debug(' Matrix {} already stored'.format(uuid))
                if return_matrix:
                    df = metta.metta_io.recover_matrix(metadata)
                    return df, uuid

            else:
                db_conn = self.db_engine.raw_connection()
                df = self.feature_loader.get_dataset(as_of_dates, db_conn)
                db_conn.close()
                log.debug('storing matrix {}'.format(uuid))
                metta.metta_io.archive_matrix(matrix_config=metadata,
                                              df_matrix=df,
                                              directory=self.matrices_path,
                                              format='hd5')
                if return_matrix:
                    return df, uuid

    def _make_metadata(self, start_time, end_time, matrix_id, as_of_dates):

        matrix_metadata = {
            # temporal information
            'start_time': start_time,
            'end_time': end_time,
            'prediction_window': self.temporal_split['prediction_window'],
            'feature_as_of_dates': as_of_dates,

            # Other infomation
            'label_name': 'outcome',
            'feature_names': self.features,
            'matrix_id': matrix_id,
            'labels': self.labels,
            'labels_config': self.labels_config,
            'indices': ['officer_id', 'as_of_date']}

        return self._make_hashable(matrix_metadata)

    def __sorting_multiple_types(self, list_to_sort):
        for i in range(0, len(list_to_sort)):
            min = i
            for j in range(i + 1, len(list_to_sort)):
                if isinstance(list_to_sort[j], (tuple,dict)):
                    if sorted(list_to_sort[j])[0] < list_to_sort[min]:
                        min = j
                elif isinstance(list_to_sort[min], (tuple,dict)):
                    if list_to_sort[j] < sorted(list_to_sort[min])[0]:
                        min = j
                else:
                    if list_to_sort[j] < list_to_sort[min]:
                        min = j
            list_to_sort[i], list_to_sort[min] = list_to_sort[min], list_to_sort[i] 
            
        return list_to_sort

    def _make_hashable(self, o):
            if isinstance(o, (tuple,list)):
                l = []
                for e in o:

                    if isinstance(e, (str, int)):
                        l.append(str(e))

                    elif isinstance(e, (dict)):
                        l.append(self._make_hashable(e))

                    else:
                        l.append(e)

                return self.__sorting_multiple_types(l)
                
            if isinstance(o, dict):
                return {k:self._make_hashable(o[k]) for k in sorted(o)}
    
            if isinstance(o, (set, frozenset)):
                return list(sorted(self._make_hashable(e) for e in o))
            
            return o

    def generate_matrices(self):

        train_matrix_id = str([sorted(self.temporal_split['train_as_of_dates']),
                               self.labels,
                               self.temporal_split['prediction_window']])

        # Train matrix
        train_metadata = self._make_metadata(
            datetime.datetime.strptime(self.temporal_split['train_start_date'], "%Y-%m-%d"),
            datetime.datetime.strptime(self.temporal_split['train_end_date'], "%Y-%m-%d"),
            train_matrix_id,
            self.temporal_split['train_as_of_dates']
        )

        self.load_store_matrix(train_metadata, self.temporal_split['train_as_of_dates'], return_matrix=False)

        # Loop over testing as of dates
        for test_date in self.temporal_split['test_as_of_dates']:
            # Load and store matrixes
            log.info('Load test matrix for as of date: {}'.format(test_date))
            test_matrix_id = str([test_date,
                                self.labels,
                               self.temporal_split['prediction_window']])

            test_metadata = self._make_metadata(
                datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                test_matrix_id,
                [test_date]
            )

            self.load_store_matrix(test_metadata, [test_date], return_matrix=False)

    def train_models(self):
        train_matrix_id = str([sorted(self.temporal_split['train_as_of_dates']),
                                    self.labels_config,
                                    self.temporal_split['prediction_window']])

        # Train matrix
        train_metadata = self._make_metadata(
            datetime.datetime.strptime(self.temporal_split['train_start_date'], "%Y-%m-%d"),
            datetime.datetime.strptime(self.temporal_split['train_end_date'], "%Y-%m-%d"),
            train_matrix_id,
            self.temporal_split['train_as_of_dates']
        )
        # Inlcude metadata in config for db
        self.misc_db_parameters['config']['train_metadata'] = train_metadata

        # Load train matrix
        log.info('Load train matrix using as of dates: {}'.format(self.temporal_split['train_as_of_dates']))
        train_df, train_uuid = self.load_store_matrix(train_metadata, self.temporal_split['train_as_of_dates'])
        # Store in metta
        log.info('Store in metta')
        # add to parameters to store in db
        self.misc_db_parameters['train_matrix_uuid'] = train_uuid
        train_matrix_store = InMemoryMatrixStore(train_df.iloc[:, :-1], train_metadata, train_df.iloc[:, -1])

        trainer = ModelTrainer(project_path=self.project_path,
                               model_storage_engine=FSModelStorageEngine(self.project_path),
                               matrix_store=train_matrix_store,
                               db_engine=self.db_engine
                               )
        log.info('Train Models')
        model_ids = trainer.train_models(grid_config=self.grid_config, misc_db_parameters=self.misc_db_parameters)

        return train_uuid, model_ids

    def test_models(self, train_uuid, model_ids):

        predictor = Predictor(project_path=self.project_path,
                              model_storage_engine=FSModelStorageEngine(self.project_path),
                              db_engine=self.db_engine)

        # Loop over testing as of dates
        for test_date in self.temporal_split['test_as_of_dates']:
            # Load matrixes
            log.info('Load test matrix for as of date: {}'.format(test_date))
            test_matrix_id = '_'.join([test_date,
                                       self.labels_config,
                                       self.temporal_split['prediction_window']])

            test_metadata = self._make_metadata(
                datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                test_matrix_id,
                [test_date]
            )

            test_df, test_uuid = self.load_store_matrix(test_metadata, [test_date])
            misc_db_parameters = {'matrix_uuid': test_uuid}
            # Store matrix
            test_matrix_store = InMemoryMatrixStore(test_df.iloc[:, :-1], test_metadata_dict, test_df.iloc[:, -1])

            for model_id in model_ids:
                ## Prediction
                log.info('Predict for model_id: {}'.format(model_id))
                predictions_binary, predictions_proba = predictor.predict(model_id, test_matrix_store,
                                                                          misc_db_parameters)
                ## Evaluation
                log.info('Generate Evaluations for model_id: {}'.format(model_id))
                self.evaluations(predictions_proba, predictions_binary, test_df.iloc[:, -1], model_id, test_date)

        return None

    def evaluations(self, predictions_proba, predictions_binary, test_y, model_id, test_date):
        all_metrics = scoring.calculate_all_evaluation_metrics(test_y.tolist(),
                                                               predictions_proba.tolist(),
                                                               predictions_binary.tolist())
        db_conn = self.db_engine.raw_connection()
        for key in all_metrics:
            evaluation = all_metrics[key]
            metric = key.split('|')[0]
            try:
                metric_parameter = key.split('|')[1]
                if metric_parameter == '':
                    metric_parameter.replace('', None)
                else:
                    pass
            except:
                metric_parameter = None

            try:
                comment = str(key.split('|')[2])
            except:
                comment = None

            dataset.store_evaluation_metrics(model_id, evaluation, metric, test_date, db_conn, metric_parameter,
                                             comment)
        db_conn.close()
        return None


def main_test(config_file_name, db_engine):
    now = datetime.datetime.now().strftime('%d-%m-%y_%H:%M:S')
    log_filename = 'logs/{}.log'.format(now)
    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        level=logging.DEBUG,
                        handlers=[logging.FileHandler(log_filename), logging.StreamHandler()])
    log = logging.getLogger('eis')

    # Read config file
    config = utils.read_config(config_file_name)

    # features
    features = officer.get_officer_features_table_columns(config)
    log.info('features: {}'.format(features))
    # labels
    labels = [key for key in config["officer_labels"] if config["officer_labels"][key] == True]

    # modify models_config
    grid_config = utils.generate_model_config(config)
    # Generate temporal_sets
    log.info('Generate temporal splits')
    temporal_sets = utils.generate_temporal_info(config['temporal_info'])

    # Add more arguments
    misc_db_parameters = {'config': config,
                          'test': config['test_flag'],
                          'model_comment': config['model_comment'],
                          'batch_comment': config['batch_comment']
                          }
    models_args = {'labels': labels,
                   'features': features,
                   'features_table_name': config['officer_feature_table_name'],
                   'labels_table_name': config['officer_label_table_name'],
                   'grid_config': grid_config,
                   'project_path': config['project_path'],
                   'misc_db_parameters': misc_db_parameters}

    for temporal_split in temporal_sets:
        log.info('Run models for temporal split: {}'.format(temporal_split))
    # Parallel(n_jobs=-1, verbose=5)(delayed(apply_train_test)(temporal_split, db_engine, **models_args)
    #                                             for temporal_split in temporal_sets)

    log.info('Done!')


if __name__ == '__main__':
    #        project_path,
    config_file_name = 'config_test_difftest.yaml'

    try:
        engine = setup_environment.get_database()
        # db_conn = engine.raw_connection()
    except:
        log.warning('Could not connect to the database')
        raise

    main_test(config_file_name, engine)
