import logging
import datetime

import pandas as pd

from sklearn import cross_validation
from sklearn import preprocessing

from . import dataset

log = logging.getLogger(__name__)

def run_traintest(config):
    """Get training and testing datasets from the database"""
    result = setup(config)
    return result

def setup(config):
    """
    Sets up dispatch-level experiment

    Args:
        config(dict): dict with information from the config file
        today(datetime.datetime): python datetime object containing the date to 
                                  split on for temporal cross-validation
    """

    fake_today = datetime.datetime.strptime(config['fake_today'], "%d%b%Y")
    train_start_date = fake_today - datetime.timedelta(days=config["training_window"])
    test_end_date = fake_today + datetime.timedelta(days=config["prediction_window"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(fake_today))
    log.info("Test label window start: {}".format(fake_today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("feature table name: {}".format(config["feature_table_name"]))

    log.info("Loading dispatch feature TRAINING data ...")
    # load the features and labels for the TRAINING set
    train_X, train_y, train_id, train_names = dataset.grab_dispatch_data(
        features = config["dispatch_features"], 
        start_date = train_start_date,
        end_date = fake_today,
        def_adverse = config["def_adverse"],
        table_name = config["dispatch_feature_table_name"])

    log.info("Loading dispatch feature TESTING data ...")
    # load the features and labels for the TESTING set
    test_X, test_y, test_id, test_names = dataset.grab_dispatch_data(
        features = config["dispatch_features"], 
        start_date = fake_today,
        end_date = test_end_date,
        def_adverse = config["def_adverse"],
        table_name = config["dispatch_feature_table_name"])

    # in case train_X and test_X have different categorical values, and thus different
    # dummy columns added
    train_X, test_X = add_empty_categorical_columns(train_X, test_X)

    # create some things that the EISExperiment class is supposed to return
    train_x_index = train_X.index.values
    test_x_index = test_X.index.values
    features = train_X.columns

    print(train_X.shape)
    print(train_X.head(5))
    print(train_y.shape)
    print(pd.DataFrame(train_y).head(5))
    
    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_X)
    scaled_train_X = scaler.transform(train_X)
    scaled_test_X = scaler.transform(test_X)

    return {"train_x": scaled_train_X,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": scaled_test_X,
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_id,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}

def add_empty_categorical_columns(train_X, test_X):
    """Return versions of train_X and test_X that have the same number of columns
    (in case they have different categorical categories, and different dummy variable columns
    
    Args:
        train_X(pd.DataFrame): training X data, potentially with categorical dummy columns
        test_X(pd.DataFrame): testing X data, potentially with categorical dummy columns
    """

    # get the columns in train_X and test_X separately
    train_cols = set(train_X.columns)
    test_cols = set(test_X.columns)

    # figure out which columns are missing from train_X and test_X
    all_cols = train_cols | test_cols
    train_missing_cols = all_cols - train_cols
    test_missing_cols = all_cols - test_cols

    # add columns full of 0s for the missing categorical columns
    for missing_col in test_missing_cols:
        test_X[missing_col] = 0

    for missing_col in train_missing_cols:
        train_X[missing_col] = 0

    return train_X, test_X
