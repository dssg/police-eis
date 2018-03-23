import datetime
import json
import logging
import os

import numpy as np
import pandas as pd
from flufl.lock import Lock
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.ensemble import RandomForestClassifier

import metta.metta_io
from triage.model_trainers import ModelTrainer
from triage.predictors import Predictor
from triage.storage import InMemoryMatrixStore
from . import dataset
from . import scoring
from . import setup_environment
from . import utils
from .feature_loader import FeatureLoader

log = logging.getLogger(__name__)


class RunModels():
    def __init__(
            self,
            labels,
            features,
            schema_name,
            blocks,
            feature_lookback_duration,
            labels_config,
            labels_table_name,
            temporal_split,
            grid_config,
            project_path,
            misc_db_parameters,
            experiment_hash=None,
            db_engine=None
    ):

        self.labels = labels
        self.features = features
        self.schema_name = schema_name
        self.blocks = blocks
        self.feature_lookback_duration = feature_lookback_duration
        self.labels_config = labels_config
        self.labels_table_name = labels_table_name
        self.temporal_split = temporal_split
        self.grid_config = grid_config
        self.project_path = project_path
        self.misc_db_parameters = misc_db_parameters
        self.experiment_hash = experiment_hash
        self.db_engine = db_engine
        self.matrices_path = self.project_path + '/matrices'

        # Save only used labels in labels_config
        self.labels_config = {}
        for l1 in self.labels:
            for l2 in l1:
                self.labels_config[l2] = labels_config[l2]

        # feature loader
        self.feature_loader = FeatureLoader(self.features,
                                            self.schema_name,
                                            self.blocks,
                                            self.labels_config,
                                            self.labels,
                                            self.labels_table_name,
                                            self.temporal_split['prediction_window'],
                                            self.temporal_split['officer_past_activity_window'],
                                            self.feature_lookback_duration,
                                            self.db_engine
                                            )
        self.features_list = self.feature_loader.features_list()

    def dt_handler(self, x):
        if isinstance(x, datetime.datetime) or isinstance(x, datetime.date):
            return x.isoformat()
        raise TypeError("Unknown type")

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

        with Lock(matrix_filename + '.lock', lifetime=datetime.timedelta(minutes=20)):
            if os.path.isfile(matrix_filename + '.h5'):
                log.debug(' Matrix {} already stored'.format(uuid))
                if return_matrix:
                    df = metta.metta_io.recover_matrix(metadata, self.matrices_path)
                    return df, uuid

            else:

                df = self.feature_loader.get_dataset(as_of_dates)
                log.debug(
                    'Start storing matrix {}, memory consumption: {}'.format(uuid, df.memory_usage(index=True).sum()))
                metta.metta_io.archive_matrix(matrix_config=metadata,
                                              df_matrix=df,
                                              directory=self.matrices_path,
                                              format='hd5')
                log.debug('Done storing matrix {}'.format(uuid))

                if return_matrix:
                    return df, uuid

    def _make_metadata(self, start_time, end_time, matrix_id, as_of_dates):

        model_config = {
            'labels_config': self.labels_config,
            'prediction_window': self.temporal_split['prediction_window'],
            'train_size': self.temporal_split['train_size'],
            'features_frequency': self.temporal_split['features_frequency'],
            'blocks': sorted(self.blocks)
        }

        matrix_metadata = {
            # temporal information
            'start_time': start_time,
            'end_time': end_time,

            # windows
            'prediction_window': self.temporal_split['prediction_window'],
            'train_size': self.temporal_split['train_size'],
            'features_frequency': self.temporal_split['features_frequency'],
            'feature_as_of_dates': as_of_dates,
            'model_config': self._make_hashable(model_config),

            # Other infomation
            'label_name': 'outcome',
            'feature_names': self.features_list,
            'matrix_id': matrix_id,
            'labels': self.labels,
            'labels_config': self.labels_config,
            'indices': ['officer_id', 'as_of_date']}

        return self._make_hashable(matrix_metadata)

    def __sorting_multiple_types(self, list_to_sort):
        for i in range(0, len(list_to_sort)):
            min = i
            for j in range(i + 1, len(list_to_sort)):
                if isinstance(list_to_sort[j], (tuple, dict)):
                    if sorted(list_to_sort[j])[0] < list_to_sort[min]:
                        min = j
                elif isinstance(list_to_sort[min], (tuple, dict)):
                    if list_to_sort[j] < sorted(list_to_sort[min])[0]:
                        min = j
                else:
                    if list_to_sort[j] < list_to_sort[min]:
                        min = j
            list_to_sort[i], list_to_sort[min] = list_to_sort[min], list_to_sort[i]

        return list_to_sort

    def _make_hashable(self, o):
        if isinstance(o, (tuple, list)):
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
            return {k: self._make_hashable(o[k]) for k in sorted(o)}

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

    def setup_train_models(self, model_storage):
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

        # Inlcude metadata in config for db
        self.misc_db_parameters['config']['train_metadata'] = json.dumps(train_metadata, default=self.dt_handler,
                                                                         sort_keys=True)
        self.misc_db_parameters['config']['labels_config'] = self.labels_config
        # self.misc_db_parameters['config'] = json.dump(self.misc_db_parameters['config'],default=self.dt_handler, sort_keys=True)

        # Load train matrix
        log.info('Load train matrix using as of dates: {}'.format(self.temporal_split['train_as_of_dates']))
        train_df, train_matrix_uuid = self.load_store_matrix(train_metadata, self.temporal_split['train_as_of_dates'])

        if len(train_df.iloc[:, -1].unique()) == 1:
            log.warning('''Train Matrix %s had only one
                        unique value, no point in training this model. Skipping
                        ''', train_matrix_uuid)
            return None, None

        # remove the index from the data-frame
        for column in train_metadata['indices']:
            if column in train_df.columns:
                del train_df[column]

        # Store in metta
        log.info('Store in metta')
        # add to parameters to store in db
        self.misc_db_parameters['train_matrix_uuid'] = train_matrix_uuid
        train_matrix_store = InMemoryMatrixStore(train_df.iloc[:, :-1], train_metadata, train_df.iloc[:, -1])

        trainer = ModelTrainer(project_path=self.project_path,
                               experiment_hash=self.experiment_hash,
                               model_storage_engine=model_storage,
                               matrix_store=train_matrix_store,
                               db_engine=self.db_engine
                               )
        log.info('Train Models')
        model_ids_generator = trainer.generate_trained_models(grid_config=self.grid_config,
                                                              misc_db_parameters=self.misc_db_parameters,
                                                              replace=True)

        return train_matrix_uuid, model_ids_generator

    def train_test_models(self, train_matrix_uuid, model_ids_generator, model_storage):

        predictor = Predictor(project_path=self.project_path,
                              model_storage_engine=model_storage,
                              db_engine=self.db_engine)

        for trained_model_id in model_ids_generator:
            ## Prediction
            log.info('Predict for model_id: {}'.format(trained_model_id))

            # Loop over testing as of dates
            for test_date in self.temporal_split['test_as_of_dates']:
                # Load matrixes
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

                test_df, test_uuid = self.load_store_matrix(test_metadata, [test_date])
                misc_db_parameters = {'matrix_uuid': test_uuid}

                # remove the index from the data-frame
                for column in test_metadata['indices']:
                    if column in test_df.columns:
                        del test_df[column]

                # Store matrix
                test_matrix_store = InMemoryMatrixStore(test_df.iloc[:, :-1], test_metadata, test_df.iloc[:, -1])

                predictions_binary, predictions_proba = predictor.predict(trained_model_id, test_matrix_store,
                                                                          misc_db_parameters)
                ## Evaluation

                if len(test_df.iloc[:, -1].unique()) == 1:
                    log.warning('''Test Matrix %s had only one
                                unique value, no point in testing this matrix. Skipping
                                ''', test_uuid)
                    self.individual_feature_ranking(
                        fitted_model=predictor.load_model(trained_model_id),
                        test_matrix=test_df.iloc[:, :-1],
                        model_id=trained_model_id,
                        test_date=test_date,
                        n_ranks=200)
                else:
                    log.info('Generate Evaluations for model_id: {}'.format(trained_model_id))
                    self.evaluations(predictions_proba, predictions_binary, test_df.iloc[:, -1], trained_model_id,
                                     test_date)
                    self.individual_feature_ranking(
                        fitted_model=predictor.load_model(trained_model_id),
                        test_matrix=test_df.iloc[:, :-1],
                        model_id=trained_model_id,
                        test_date=test_date,
                        n_ranks=200)

            # remove trained model from memory
            predictor.delete_model(trained_model_id)

        return None

    # this function is used for training and scoring a day
    def train_score_models(self, model_ids_generator, model_storage):

        predictor = Predictor(project_path=self.project_path,
                              model_storage_engine=model_storage,
                              db_engine=self.db_engine)

        for trained_model_id in model_ids_generator:
            ## Prediction
            log.info('Predict for model_id: {}'.format(trained_model_id))

            # Loop over testing as of dates
            for test_date in self.temporal_split['test_as_of_dates']:
                # Load matrixes
                log.info('Load production matrix for as of date: {}'.format(test_date))
                test_matrix_id = str([test_date,
                                      self.labels,
                                      self.temporal_split['prediction_window']])

                test_metadata = self._make_metadata(
                    datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                    datetime.datetime.strptime(test_date, "%Y-%m-%d"),
                    test_matrix_id,
                    [test_date]
                )

                test_df, test_uuid = self.load_store_matrix(test_metadata, [test_date])
                misc_db_parameters = {'matrix_uuid': test_uuid}

                # remove the index from the data-frame
                for column in test_metadata['indices']:
                    if column in test_df.columns:
                        del test_df[column]

                # Store matrix
                test_matrix_store = InMemoryMatrixStore(test_df.iloc[:, :-1], test_metadata, test_df.iloc[:, -1])

                predictions_binary, predictions_proba = predictor.predict(trained_model_id, test_matrix_store,
                                                                          misc_db_parameters)

                self.individual_feature_ranking(
                    fitted_model=predictor.load_model(trained_model_id),
                    test_matrix=test_df.iloc[:, :-1],
                    model_id=trained_model_id,
                    test_date=test_date,
                    n_ranks=200)

            # remove trained model from memory
            predictor.delete_model(trained_model_id)

        return None

    def evaluations(self, predictions_proba, predictions_binary, test_y, model_id, test_date):
        all_metrics = scoring.calculate_all_evaluation_metrics(test_y.tolist(),
                                                               predictions_proba.tolist(),
                                                               predictions_binary.tolist())
        db_conn = self.db_engine.raw_connection()

        # remove all existing evaluations before re-writing
        query = "DELETE FROM results.evaluations where model_id = {} and evaluation_start_time = '{}'::TIMESTAMP ".format(
            model_id, test_date)
        db_conn.cursor().execute(query)
        db_conn.commit()

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

    def individual_feature_ranking(self, fitted_model, test_matrix, model_id, test_date, n_ranks):
        ###################
        # This method is a beta version tested for top k optimized random forests
        ###################

        if not (isinstance(fitted_model, RandomForestClassifier) or isinstance(fitted_model, ExtraTreesClassifier)):
            log.info(
                'Individual Feature Ranking is currently only implemented for tree based methods, skipping: {}'.format(
                    model_id))
            return None

        # get the list of all features in the test matrix
        feature_list = test_matrix.columns

        # extract all global feature importance values from the tree
        importance_dict = {}
        for j, value in enumerate(fitted_model.feature_importances_):
            if value > 0:
                importance_dict[feature_list[j]] = value

        # filter out dummies
        importance_dict_filtered = {k: v for k, v in importance_dict.items() if not 'dummy' in k}

        rftree_feature_list = sorted(importance_dict_filtered, key=importance_dict_filtered.get, reverse=True)

        # create a new text matrix with float conversion (panda stores some columns from postgres as decimal object)
        tmp_test = test_matrix.applymap(lambda x: float(x))
        test_matrix_reduced = pd.DataFrame()

        # add the top n_ranks features from the RandomForest used without dummies
        for feature in rftree_feature_list[:n_ranks]:
            test_matrix_reduced[feature] = tmp_test[feature]

        test_matrix_rank_distance = pd.DataFrame()

        for feature in test_matrix_reduced.columns:
            feature_median = test_matrix_reduced[feature].median()
            test_matrix_reduced[feature + '_rank'] = test_matrix_reduced[feature].rank()
            # the index of the median value
            idx = np.nanargmin(np.abs(test_matrix_reduced[feature] - feature_median))
            median_rank = test_matrix_reduced[feature + '_rank'].iloc[idx]
            test_matrix_rank_distance[feature] = np.abs(test_matrix_reduced[feature + '_rank'] - median_rank)

        test_matrix_rank_distance = test_matrix_rank_distance.reset_index(drop=True)

        # find the top 5 features where the distance to the median rank value (for standardisation) is the highest
        # across the top 30 features
        matrix_transposed = test_matrix_rank_distance.T
        result = pd.DataFrame(np.zeros((0, 5)), columns=['risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5'])
        for i in matrix_transposed.columns:
            df1row = pd.DataFrame(matrix_transposed.nlargest(5, i).index.tolist(),
                                  index=['risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5']).T
            result = pd.concat([result, df1row], axis=0)
        result = result.reset_index(drop=True)

        # prepare table insert
        result['entity_id'] = test_matrix_reduced.index.values
        result['model_id'] = model_id
        result['as_of_date'] = test_date

        db_conn = self.db_engine.raw_connection()

        # remove all existing evaluations before re-writing
        query = "DELETE FROM results.individual_importances where model_id = {} and as_of_date = '{}'::TIMESTAMP ".format(
            model_id, test_date)
        db_conn.cursor().execute(query)
        db_conn.commit()

        # write new entries
        result.to_sql("individual_importances", self.db_engine, if_exists="append", schema="results", index=False)

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
                   'feature_blocks': feature_blocks,
                   'schema_name': schema_name,
                   'blocks': blocks,
                   'labels_config': labels_config,
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
