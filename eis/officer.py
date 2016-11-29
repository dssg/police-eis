import logging
import datetime
from sklearn import preprocessing
from dateutil.relativedelta import relativedelta
import pdb
from . import dataset
from .features import class_map 

log = logging.getLogger(__name__)

def run_traintest(config):
    result = setup(config)
    return result


def run_pilot(config):
    result = setup(config)
    return result


def setup(config):
    """
    Sets up officer-level experiment

    Args:
    config: dict with config file
    today: string containing the date to split on for temporal cross-validation
    """
    
    train_start_date = datetime.datetime.strptime(config['train_start_date'], "%Y-%m-%d")
    train_end_date = datetime.datetime.strptime(config['train_end_date'], "%Y-%m-%d")
    test_end_date =  datetime.datetime.strptime(config['test_end_date'], "%Y-%m-%d")
    test_start_date =  datetime.datetime.strptime(config['test_start_date'], "%Y-%m-%d")

    log.info("Train start: {}".format(train_start_date))
    log.info("Train end date: {}".format(train_end_date))
    log.info("Test start date: {}".format(test_start_date))
    log.info("Test end datet: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    features_table  = config["officer_feature_table_name"]
    train_x, train_y = dataset.get_dataset(
          train_start_date,
          train_end_date,
          config['prediction_window'],
          config['officer_past_activity_window'],
          config["officer_features"],
          config['officer_labels'],
          config['officer_feature_table_name'],
          config['officer_label_table_name'])
     
    log.info("Loading officers and features to use as testing...")
    test_x, test_y = dataset.get_dataset(
          test_start_date,
          test_end_date,
          config['prediction_window'],
          config['officer_past_activity_window'],
          config["officer_features"],
          config['officer_labels'],
          config['officer_feature_table_name'],
          config['officer_label_table_name'])
    
    # Testing data should include ALL officers, ignoring "noinvest" keyword
    testing_labelling_config = config["officer_labels"].copy()
    #testing_labelling_config["noinvest"] = True

    train_x_index = train_x.index
    test_x_index = test_x.index
    features = train_x.columns.values

    # Feature scaling
    #scaler = preprocessing.StandardScaler().fit(train_x)
    #train_x = scaler.transform(train_x)
    #test_x = scaler.transform(test_x)
    train_x = train_x.as_matrix()
    test_x = test_x.as_matrix()

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_x_index,
            "test_x": test_x,
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_x_index,
            "names": features,
            "train_start_date": train_start_date,
            "train_end_date": train_end_date,
            "officer_past_activity_window": config["officer_past_activity_window"],
            "test_start_date": test_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}

def get_officer_features_table_columns( config ):
    """ Creates temporary instances of feature classes to get a list of all feature table column names """
        
    # get a list of all features that are set to true.
    feature_names = config["officer_features"]
    active_features = [ key for key in feature_names if feature_names[key] == True ] 

    # loop over active features, populating list of column names.
    feature_table_columns = []
    for active_feature in active_features:
        feature_class = class_map.lookup(   active_feature,
                                            unit = 'officer',
                                            as_of_date=datetime.datetime.now() ,
                                            table_name="junk",
                                            lookback_durations=config["timegated_feature_lookback_duration"] )

        # if this object is time gated, get a list of column names.
        if hasattr( feature_class, "feature_column_names" ):
            feature_table_columns.extend( feature_class.feature_column_names )
        else:
            feature_table_columns.extend( [feature_class.feature_name] )

    return feature_table_columns 
