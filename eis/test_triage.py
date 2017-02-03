import pandas as pd
import pickle
import pdb

from .triage.triage.model_trainers import ModelTrainer
from .triage.triage.predictors import Predictor
from .triage.triage.storage import FSModelStorageEngine, InMemoryMatrixStore

from . import dataset
from . import utils
from . import setup_environment
from . import officer
from . import scoring

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

    def load_matrix(self, as_of_dates):
        """
        Calls get_dataset to return a pandas DataFrame for the as_of_dates selected
        Args:
           list as_of_dates: as_of_dates to use
        Returns:
           matrix: dataframe with the features and the last column as the label (called: outcome)
        """
        df_X, df_y = dataset.get_dataset(   self.temporal_split['prediction_window'],
                                        self.temporal_split['officer_past_activity_window'],
                                        self.features,
                                        self.labels,
                                        self.features_table_name,
                                        self.labels_table_name,
                                        as_of_dates)
        return df_X, df_y

    def train_models(self):
        # Metadata
        train_metadata = {'start_time': self.temporal_split['train_start_date'],
                          'end_time': self.temporal_split['train_end_date'],
                          'prediction_window': self.temporal_split['prediction_window'], 
                          'label_name': "_".join(sorted(self.labels)),
                          'feature_names': self.features,
                          'matrix_id': 'TRAIN_{}_{}'.format(self.temporal_split['train_start_date'][:4], 
                                                            self.temporal_split['train_end_date'][:4]),
                          'as_of_dates': self.temporal_split['training_as_of_dates']}
        
        # Inlcude metadata in config for db
        self.misc_db_parameters['config']['train_metadata'] = train_metadata

        # Load train matrix
        train_X, train_y = self.load_matrix(self.temporal_split['training_as_of_dates'])
        train_matrix_store = InMemoryMatrixStore(train_X, train_metadata, train_y)

        trainer = ModelTrainer( project_path=self.project_path,
                               model_storage_engine=FSModelStorageEngine(self.project_path),
                               matrix_store=train_matrix_store,
                               db_engine=self.db_engine
                              )

        model_ids = trainer.train_models(grid_config=self.grid_config, misc_db_parameters=self.misc_db_parameters)

        return model_ids

    def test_models(self, model_ids):

        predictor = Predictor(project_path = self.project_path,
                              model_storage_engine = FSModelStorageEngine(self.project_path),
                              db_engine=self.db_engine)

        # Loop over testing as of dates
        for test_date in self.temporal_split['testing_as_of_dates']:
            # Load matrixes
            test_X, test_y = self.load_matrix([test_date])
            test_metadata_dict = {'start_time':test_date,
                                  'end_time': test_date,
                                  'prediction_window': self.temporal_split['prediction_window'],
                                  'label_name': "_".join(sorted(self.labels)),
                                  'feature_names': self.features,
                                  'matrix_id': 'TEST_{}'.format(test_date)}
            # Store matrix
            test_matrix_store = InMemoryMatrixStore(test_X, test_metadata_dict, test_y)
        
            for model_id in model_ids:
                ## Prediction
                predictions_binary, predictions_proba = predictor.predict(model_id, test_matrix_store)
                ## Evaluation
                self.evaluations( predictions_proba, predictions_binary, test_y,  model_id, test_date )

        return None

    def evaluations(self, predictions_proba, predictions_binary, test_y, model_id, test_date):
        all_metrics = scoring.calculate_all_evaluation_metrics( test_y.tolist(),
                                                                predictions_proba.tolist(),
                                                                predictions_binary.tolist() )
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

            dataset.store_evaluation_metrics( model_id, evaluation, metric, test_date, metric_parameter, comment )
        return None



def main_test(config_file_name, db_engine):
    # Read config file
    config = utils.read_config(config_file_name)
    to_save = config.copy()

    # features
    features = officer.get_officer_features_table_columns( config )
    #labels
    labels =  [ key for key in config["officer_labels"] if config["officer_labels"][key] == True ]

    # modify models_config
    grid_config = utils.generate_model_config(config)
    # Generate temporal_sets
    temporal_sets = utils.generate_temporal_sets(config)

    # Add more arguments
    misc_db_parameters = {'config': config,
            'test': config['test_flag'],
            'model_comment': config['model_comment'],
            'batch_comment': config['batch_comment']
            }
    # model_group_id
    ## feature important ranks
 
    for temporal_split in temporal_sets:
        run_model = RunModels(labels=labels,
                              features=features,
                              features_table_name=config['officer_feature_table_name'],
                              labels_table_name=config['officer_label_table_name'],
                              temporal_split=temporal_split,
                              grid_config=grid_config,
                              project_path=config['project_path'],
                              misc_db_parameters=misc_db_parameters,
                              db_engine=db_engine)
        train_X, train_y = run_model.load_matrix(run_model.temporal_split['training_as_of_dates'])
        model_ids = run_model.train_models()
        run_model.test_models(model_ids)        

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
     
