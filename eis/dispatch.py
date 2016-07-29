import logging
import datetime

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

    today = datetime.datetime.strptime(config['fake_today', "%d%b%Y")
    train_start_date = today - datetime.timedelta(days=config["training_window"])
    test_end_date = today + datetime.timedelta(days=config["prediction_window"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(today))
    log.info("Test label window start: {}".format(today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("Loading dispatch feature and label data...")

    # load the features and labels for all dispatches
    features, labels, ids, feature_names = dataset.grab_dispatch_data(
                                            features = config['dispatch_features'],
                                            def_adverse = config['def_adverse'],
                                            table_name = 'dispatch_features')
                                                
    log.info("Splitting the data into training and testing subsets...")

    # NOTE: !!IMPORTANT!! the random_state must be the same for the following two train_test_splits
    #                     so that the ids are correctly matched to their rows
    
    # split the features and labels
    train_X, test_X, train_y, test_y = cross_validation.train_test_split(features, labels, test_size=0.4, random_state=0)

    # split the row indexes (dispatch_ids) using same split as the feature / labels
    train_id, test_id, _, _ = cross_validation.train_test_split(ids, labels, test_size=0.4, random_state=0)
    
    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_X)
    train_X = scaler.transform(train_X)
    test_X = scaler.transform(test_X)

    return {"train_x": train_X,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_X,
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_id,
            "features": features}
