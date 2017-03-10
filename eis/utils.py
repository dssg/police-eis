import pandas as pd
import numpy as np
import copy
import pdb
import re
import yaml
import datetime
import logging
from itertools import product
from dateutil.relativedelta import relativedelta

from . import  officer

log = logging.getLogger(__name__)

class EISExperiment(object):
   """The EISExperiment class defines each individual experiment
   Attributes:
       config: dict containing configuration
       exp_data: dict containing data
       pilot_data: dict containing data for pilot if defined
   """

   def __init__(self, config):
       self.config = config.copy()
       self.exp_data = None
       self.pilot_data = None

# Read config
def read_yaml(config_file_name):
    """
    This function reads the config file
    """
    with open(config_file_name, 'r') as f:
        config = yaml.load(f)
    return config


def generate_temporal_info(config):
    """
    """
    time_format = "%Y-%m-%d"
    end_date = datetime.datetime.strptime(config['end_date'], "%Y-%m-%d")
    start_date = datetime.datetime.strptime(config['start_date'], "%Y-%m-%d")

    # convert windows to deltas
    prediction_window_deltas = relative_deltas_conditions(config['prediction_window'])
    update_window_deltas = relative_deltas_conditions(config['update_window'])
    train_size_deltas = relative_deltas_conditions(config['train_size'])
    features_frequency_deltas = relative_deltas_conditions(config['features_frequency'])
    test_frecuency_deltas = relative_deltas_conditions(config['test_frecuency'])
    test_time_ahead_deltas = relative_deltas_conditions(config['test_time_ahead'])
   
    temporal_info = [] 
    for prediction_window, update_window, officer_past_activity, \
        train_size, features_frequency, test_frequency, test_time_ahead \
             in product(    
               config['prediction_window'], config['update_window'],
               config['officer_past_activity_window'], config['train_size'],
               config['features_frequency'], config['test_frecuency'],
               config['test_time_ahead']):

        test_end_date = end_date
        # loop moving giving an update_window
        while start_date <= test_end_date - 2*relativedelta(**prediction_window_deltas[prediction_window]):

            test_start_date = test_end_date - relativedelta(**test_time_ahead_deltas[test_time_ahead])
            test_as_of_dates = as_of_dates_in_window(test_start_date,
                                                     test_end_date,
                                                     test_frequency)

            train_end_date = test_start_date  - relativedelta(**prediction_window_deltas[prediction_window])
            train_start_date = train_end_date - relativedelta(**train_size_deltas[train_size])
            train_as_of_dates = as_of_dates_in_window(train_start_date,
                                                      train_end_date,
                                                      features_frequency)

            tmp_info = {'test_end_date': test_end_date.strftime(time_format),
                        'test_start_date': test_start_date.strftime(time_format),
                        'test_as_of_dates': test_as_of_dates,
                        'train_end_date': train_end_date.strftime(time_format),
                        'train_start_date': train_start_date.strftime(time_format),
                        'train_as_of_dates': train_as_of_dates,
                        'train_size': train_size,
                        'features_frequency': features_frequency,
                        'prediction_window':prediction_window,
                        'officer_past_activity_window': officer_past_activity}
            log.info(tmp_info)
            temporal_info.append(tmp_info)
            test_end_date -= relativedelta(**update_window_deltas[update_window])

    return temporal_info

def relative_deltas_conditions(times):

    dict_abbreviations = {'d':'days', 'm':'months', 'y':'years', 'w':'weeks'}
    time_agrguments = {}
    time_arguments = {x :{ dict_abbreviations[re.findall(r'\d+(\w)', x )[0]]: int(re.findall(r'\d+', x)[0])}
                        for x in times}
    return time_arguments


def as_of_dates_in_window(start_date, end_date, window):
    """
    Generate a list of as_of_dates between start_date and end_date 
    moving through window
    """
    # Generate condition for relative delta
    window_delta = relative_deltas_conditions([window])

    as_of_dates = []
    while end_date >= start_date:
        as_of_date = end_date
        end_date -= relativedelta(**window_delta[window])
        as_of_dates.append(as_of_date)
   
    time_format = "%Y-%m-%d"
    as_of_dates_uniques = set(as_of_dates)
    as_of_dates_uniques = [ as_of_date.strftime(time_format) for as_of_date in as_of_dates_uniques]
    return sorted(as_of_dates_uniques)


def generate_model_config( config ):
    models_sklearn = { 'RandomForest': 'sklearn.ensemble.RandomForestClassifier',
                       'ExtraTrees': 'sklearn.ensemble.ExtraTreesClassifier',
                       'AdaBoost': 'sklearn.ensemble.AdaBoostClassifier',
                       'LogisticRegression': 'sklearn.linear_model.LogisticRegression',
                       'SVM': 'sklearn.svm.SVC',
                       'GradientBoostingClassifier': 'sklearn.ensemble.GradientBoostingClassifier',
                       'DecisionTreeClassifier': 'sklearn.tree.DecisionTreeClassifier',
                       'SGDClassifier': 'sklearn.linear_model.SGDClassifier',
                       'KNeighborsClassifier': 'sklearn.neighbors.KNeighborsClassifier'
                      }

    model_config = {}
    models = config['model']
    for model in models:
        model_config[models_sklearn[model]] = config['parameters'][model]
   
    return model_config


if __name__ == '__main__':
    config_file_name = 'officer_config_collate_daily.yaml'
    config_file = read_config(config_file_name)
    # exp = generate_experiments(config_file)
    temporal_sets = generate_temporal_info(config_file['temporal_info'])
    pdb.set_trace()
    models = generate_model_config(config_file) 
