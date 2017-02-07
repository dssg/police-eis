import pandas as pd
import pickle
import pdb
import datetime
import logging
from joblib import Parallel, delayed

from triage.model_trainers import ModelTrainer
from triage.predictors import Predictor
from triage.storage import FSModelStorageEngine, InMemoryMatrixStore

import metta.metta_io

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
        self.labels_table_name = labels_table_name
        self.temporal_split = temporal_split
        self.grid_config = grid_config
        self.project_path = project_path
        self.misc_db_parameters = misc_db_parameters
        self.db_engine = db_engine
        self.matrices_path = self.project_path + '/matrices/'

    def load_matrix(self, as_of_dates):
        """
        Calls get_dataset to return a pandas DataFrame for the as_of_dates selected
        Args:
           list as_of_dates: as_of_dates to use
        Returns:
           matrix: dataframe with the features and the last column as the label (called: outcome)
        """
        db_conn = self.db_engine.raw_connection()
        df = dataset.get_dataset(self.temporal_split['prediction_window'],
                                         self.temporal_split['officer_past_activity_window'],
                                         self.features,
                                         self.labels,
                                         self.features_table_name,
                                         self.labels_table_name,
                                         as_of_dates,
                                         db_conn)
        db_conn.close()
        return df

    def train_models(self):
        # Metadata
        train_metadata = {'start_time': self.temporal_split['train_start_date'],
                          'end_time': self.temporal_split['train_end_date'],
                          'prediction_window': self.temporal_split['prediction_window'], 
                          'label_name': 'outcome', #"_".join(sorted(self.labels)),
                          'feature_names': self.features,
                          'matrix_id': 'TRAIN_{}_{}'.format(self.temporal_split['train_start_date'][:4], 
                                                            self.temporal_split['train_end_date'][:4]),
                          'as_of_dates': self.temporal_split['train_as_of_dates']}
        
        # Inlcude metadata in config for db
        self.misc_db_parameters['config']['train_metadata'] = train_metadata

        # Load train matrix
        log.info('Load train matrix using as of dates: {}'.format(self.temporal_split['train_as_of_dates']))
        train_df = self.load_matrix(self.temporal_split['train_as_of_dates'])
        # Store in metta
        log.info('Store in metta')
        train_uuid = metta.metta_io.archive_matrix(train_metadata,
                                                   train_df, 
                                                   self.matrices_path, 
                                                   format='hd5')
        # add to parameters to store in db
        self.misc_db_parameters['train_matrix_uuid'] = train_uuid
        train_matrix_store = InMemoryMatrixStore(train_df.iloc[:,:-1], train_metadata, train_df.iloc[:,-1])
        
        trainer = ModelTrainer( project_path=self.project_path,
                               model_storage_engine=FSModelStorageEngine(self.project_path),
                               matrix_store=train_matrix_store,
                               db_engine=self.db_engine
                              )
        log.info('Train Models')
        model_ids = trainer.train_models(grid_config=self.grid_config, misc_db_parameters=self.misc_db_parameters)

        return train_uuid, model_ids

    def test_models(self, train_uuid, model_ids):

        predictor = Predictor(project_path = self.project_path,
                              model_storage_engine = FSModelStorageEngine(self.project_path),
                              db_engine=self.db_engine)

        # Loop over testing as of dates
        for test_date in self.temporal_split['test_as_of_dates']:
            # Load matrixes
            log.info('Load test matrix for as of date: {}'.format(test_date))
            test_df = self.load_matrix([test_date])
            test_metadata_dict = {'start_time': test_date,
                                  'end_time': test_date,
                                  'prediction_window': self.temporal_split['prediction_window'],
                                  'label_name': 'outcome', #"_".join(sorted(self.labels)),
                                  'feature_names': self.features,
                                  'matrix_id': 'TEST_{}'.format(test_date)}
            # Store in metta
            test_uuid = metta.metta_io.archive_matrix(test_metadata_dict,
                                                      test_df,
                                                      self.matrices_path,
                                                      format='hd5',
                                                      train_uuid=train_uuid)
            misc_db_parameters = {'matrix_uuid': test_uuid}
            # Store matrix
            test_matrix_store = InMemoryMatrixStore(test_df.iloc[:,:-1], test_metadata_dict, test_df.iloc[:,-1])
        
            for model_id in model_ids:
                ## Prediction
                log.info('Predict for model_id: {}'.format(model_id))
                predictions_binary, predictions_proba = predictor.predict(model_id, test_matrix_store, misc_db_parameters)
                ## Evaluation
                log.info('Generate Evaluations for model_id: {}'.format(model_id))
                self.evaluations( predictions_proba, predictions_binary, test_df.iloc[:,-1],  model_id, test_date )

        return None

    def evaluations(self, predictions_proba, predictions_binary, test_y, model_id, test_date):
        all_metrics = scoring.calculate_all_evaluation_metrics( test_y.tolist(),
                                                                predictions_proba.tolist(),
                                                                predictions_binary.tolist() )
        db_conn = self.db_engine.raw_connection()
        for key in all_metrics:
            evaluation = all_metrics[key]
            metric = key.split('|')[0]
            try:
                metric_parameter = key.split('|')[1]
                if metric_parameter=='':
                    metric_parameter.replace('', None)
                else:
                    pass
            except:
                metric_parameter = None
    
            try:
                comment = str(key.split('|')[2])
            except:
                comment = None
             
            dataset.store_evaluation_metrics(model_id, evaluation, metric, test_date, db_conn, metric_parameter, comment)
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
    features = officer.get_officer_features_table_columns( config )
    log.info('features: {}'.format(features))
    #labels
    labels =  [ key for key in config["officer_labels"] if config["officer_labels"][key] == True ]

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
                   'features_table_name':config['officer_feature_table_name'],
                   'labels_table_name': config['officer_label_table_name'],
                   'grid_config':grid_config,
                   'project_path':config['project_path'],
                   'misc_db_parameters': misc_db_parameters}
 
    for temporal_split in temporal_sets:
        log.info('Run models for temporal split: {}'.format(temporal_split))
    #Parallel(n_jobs=-1, verbose=5)(delayed(apply_train_test)(temporal_split, db_engine, **models_args) 
    #                                             for temporal_split in temporal_sets)

    log.info('Done!')

if __name__ == '__main__':
#        project_path,
    config_file_name = 'config_test_difftest.yaml'

    try:
        engine = setup_environment.get_database()
        #db_conn = engine.raw_connection()
    except:
        log.warning('Could not connect to the database')
        raise

    main_test(config_file_name, engine)
     
