import pandas as pd
import numpy as np
import copy
import pdb
import re
import yaml
import datetime
from itertools import product
from dateutil.relativedelta import relativedelta

from . import  officer

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
def read_config(config_file_name):
    """
    This function reads the config file
    """
    with open(config_file_name, 'r') as f:
        config = yaml.load(f)
    return config


def generate_temporal_sets(config ):
    """
    This function generates a sets of temporal infos 
    """
    temporal_sets = []

    train_start_date = '2013-01-01'
    train_end_date = '2014-01-01'
    features_window = '1m'
    test_start_date = '2014-01-01'
    test_end_date = '2015-01-01'
    test_window = '1m'
    prediction_window = '1y'
    officer_past_activity_window = '1y'
    # Get as_of_dates for training and testing
    training_as_of_dates = as_of_dates_in_window( train_start_date,
                                                  train_end_date,
                                                  features_window )
    testing_as_of_dates = as_of_dates_in_window( test_start_date,
                                                 test_end_date,
                                                 test_window )

    temporal_sets.append({ 'train_start_date': train_start_date,
                      'train_end_date': train_end_date,
                      'features_window': features_window,
                      'test_start_date': test_start_date,
                      'test_end_date': test_end_date,
                      'test_window': test_window,
                      'prediction_window': prediction_window,
                      'officer_past_activity_window': officer_past_activity_window,
                      'training_as_of_dates': training_as_of_dates,
                      'testing_as_of_dates': ['2015-01-02', '2015-02-01']})

    return temporal_sets


def relative_deltas_conditions( times ):

    dict_abbreviations = {'d':'days', 'm':'months', 'y':'years', 'w':'weeks'}
    time_agrguments = {}
    time_arguments = {x :{ dict_abbreviations[re.findall(r'\d+(\w)', x )[0]]: int(re.findall(r'\d+', x)[0])}
                        for x in times}
    return time_arguments


def as_of_dates_in_window( start_date, end_date,  window ):
    """
    Generate a list of as_of_dates between start_date and end_date 
    moving through window
    """
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    # Generate condition for relative delta
    window_delta = relative_deltas_conditions([ window] )
    as_of_dates = []
    while end_date > start_date:
        as_of_date = end_date
        end_date -= relativedelta(**window_delta[window])
        as_of_dates.append(as_of_date)
   
    time_format = "%Y-%m-%d %X"
    as_of_dates_uniques = set(as_of_dates)
    as_of_dates_uniques = [ as_of_date.strftime(time_format) for as_of_date in as_of_dates_uniques]
    return as_of_dates_uniques


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
    config_file_name = 'config_test_difftest.yaml'
    config_file = read_config(config_file_name)
    # exp = generate_experiments(config_file)
    temporal_sets = generate_temporal_sets(config_file)
    models = generate_model_config(config_file) 
    pdb.set_trace()
