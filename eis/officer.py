import logging
import datetime
from sklearn import preprocessing
import pdb
from . import dataset
from .features import class_map 

log = logging.getLogger(__name__)

def run_traintest(config):
    result = setup(config, config["fake_today"])
    return result


def run_pilot(config):
    result = setup(config, config["pilot_today"])
    return result


def setup(config, today):
    """
    Sets up officer-level experiment

    Args:
    config: dict with config file
    today: string containing the date to split on for temporal cross-validation
    """

    today = datetime.datetime.strptime(today, "%d%b%Y")
    train_start_date = today - datetime.timedelta(days=config["training_window"])
    test_end_date = today + datetime.timedelta(days=config["prediction_window"])

    log.info("Train label window start: {}".format(train_start_date))
    log.info("Train label window stop: {}".format(today))
    log.info("Test label window start: {}".format(today))
    log.info("Test label window stop: {}".format(test_end_date))

    log.info("Loading officers and features to use as training...")
    table_name = config["feature_table_name"]
    train_x, train_y, train_id, names = dataset.grab_officer_data(
        config["officer_features"], 
        train_start_date,
        today,
        train_start_date,
        config["def_adverse"],
        config["labelling"],
        table_name)

    # Testing data should include ALL officers, ignoring "noinvest" keyword
    testing_labelling_config = config["labelling"].copy()
    testing_labelling_config["noinvest"] = True

    log.info("Loading officers and features to use as testing...")
    test_x, test_y, test_id, names = dataset.grab_officer_data(
        config["officer_features"],
        today,
        test_end_date,
        today,
        config["def_adverse"],
        testing_labelling_config,
        table_name)

    train_x_index = train_x.index
    test_x_index = test_x.index
    features = train_x.columns.values

    # Feature scaling
    scaler = preprocessing.StandardScaler().fit(train_x)
    train_x = scaler.transform(train_x)
    test_x = scaler.transform(test_x)

    return {"train_x": train_x,
            "train_y": train_y,
            "train_id": train_id,
            "test_x": test_x,
            "test_y": test_y,  # For pilot test_y will not be used
            "test_id": test_id,
            "names": names,
            "train_start_date": train_start_date,
            "test_end_date": test_end_date,
            "train_x_index": train_x_index,
            "test_x_index": test_x_index,
            "features": features}

def get_officer_features_table_columns( config ):
    """ Creates temporary instances of feature classes to get a list of all feature table column names """
        
    # get a list of all features that are set to true.
    feature_names = config["officer_features"]
    print( type( feature_names ) )
    active_features = [ key for key in feature_names if feature_names[key] == True ] 

    # loop over active features, populating list of column names.
    feature_table_columns = []
    for active_feature in active_features:
        feature_class = class_map.lookup(   active_feature,
                                            fake_today=datetime.datetime.now() ,
                                            table_name="junk",
                                            lookback_durations=config["timegated_feature_lookback_duration"] )

        # if this object is time gated, get a list of column names.
        if hasattr( feature_class, "feature_column_names" ):
            feature_table_columns.extend( feature_class.feature_column_names )
        else:
            feature_table_columns.extend( [feature_class.feature_name] )

    return feature_table_columns 
