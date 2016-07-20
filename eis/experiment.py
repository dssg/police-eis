import pdb
import copy
from itertools import product
import datetime
import logging
from IPython.core.debugger import Tracer

from . import officer, dispatch, explore

# Potential data sources in the police dept to draw from
MASTER_FEATURE_GROUPS = ["basic", "ia", "unit_div", "arrests",
                         "citations", "incidents", "field_interviews",
                         "cad", "training", "traffic_stops", "eis",
                         "extraduty", "neighborhood"]


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


def get_fake_todays(prediction_window, begin_date="01Jan2007", end_date="01Jan2016"):
    """Generate a list of start dates for temporal cross validation. Start with
    begin_date and increment by prediction_window until you get to end_date."""

    first_today = datetime.datetime.strptime(begin_date, "%d%b%Y")
    last_today = datetime.datetime.strptime(end_date, "%d%b%Y")
    generated_todays = [first_today]
    test_today = first_today

    # think generated_todays = all_days[begin_date:end_date:prediction_window]
    while test_today + datetime.timedelta(days=prediction_window) < last_today:
        test_today += datetime.timedelta(days=prediction_window)
        generated_todays.append(test_today)

    return generated_todays 


def generate_time_info(config):
    """Takes a config file and generates a list of dicts, each of which
    will be an experiment.

    If autogen_fake_today is set to False, then for every element of the
    fake_today array, we try every combination of training_window and
    prediction_window. 

    If the autogeneration is set to True, then first we use
    prediction_window to generate the fake_today array (by stepping forward
    by one prediction_window for every time step). Then for every fake_today
    we try each training_window. Note that if the prediction_window is 
    quite small, then this option can result in a large number of experiments
    to run.

    Args: 
        config: Python dict read in from YAML config file containing
                user-supplied details of the experiments to be run

    Returns:
        temporal_info: dict containing time related fields for experiments 
    """

    temporal_info = []

    # automatically generate 'fake_todays' for temporal cross validation
    if config["autogen_fake_todays"]:
        fake_todays = []
        for prediction_window in config["prediction_window"]:
            fake_todays.extend(get_fake_todays(prediction_window))

    # use 'fake_todays' specified in the experiment configuration file
    else:
        fake_todays = config["fake_today"] 

    
    # populate temporal_info with all the parameter dictionaries
    for fake_today, training_window, prediction_window in product(
        fake_todays, config['training_window'], config['prediction_window']):

            temporal_info.append({"fake_today": fake_today,
                                  "training_window": training_window,
                                  "prediction_window": prediction_window})

    return temporal_info 


def generate_models_to_run(config, query_db=True):
    """Generates a list of experiments with the various options
    that we want to test, e.g. different temporal cross-validation
    train/test splits, model types, hyperparameters, features, etc.

    Args:
        config: Python dict read in from YAML config file containing
                user-supplied details of the experiments to be run
        query_db [optional (bool)]: keyword arg describing if we should
                                    get the data from the db or just generate
                                    the configs

    Returns: 
        experiment_list: list of EISExperiment objects to be run
    """

    experiment_list = []

    if config["try_feature_sets_by_group"] == True:
        feature_groups = MASTER_FEATURE_GROUPS 
    else:
        feature_groups = ["all"]

    # generate a list of {fake_today, training_window, prediction_window} dictionaries
    all_temporal_info = generate_time_info(config)

    for temporal_info in all_temporal_info:

        # create a copy of the dictionary from the experiment configuration yaml and add
        # the temporal cross validation information
        this_config = copy.copy(config)
        this_config["prediction_window"] = temporal_info["prediction_window"]
        this_config["training_window"] = temporal_info["training_window"]
        this_config["fake_today"] = temporal_info["fake_today"]
        
        for group in feature_groups:

            # leave out features related to the selected group
            features_to_use = {}
            if config["try_feature_sets_by_group"] == True:
                feature_groups_to_use = copy.copy(feature_groups)
                feature_groups_to_use.remove(group)
                log.info("Running models without feature set {}!".format(
                group))
            else:
                feature_groups_to_use = copy.copy(MASTER_FEATURE_GROUPS)

            for features in feature_groups_to_use:
                print( config["features"] )
                features_to_use.update(config["features"][features])

            this_config["features"] = features_to_use

            for model in config["model"]:
                if query_db:
                    if config["unit"] == "officer":
                        exp_data = officer.run_traintest(this_config)
                    elif config["unit"] == "dispatch":
                        exp_data = dispatch.setup(this_config)
                else:
                    exp_data = {"test_x": None, "train_y": None}

                this_config["parameters"] = config["parameters"][model]
                this_config["model"] = model

                if config["pilot"]:
                    pilot_data = officer.run_pilot(this_config)

                if config["make_feat_dists"]:
                    explore.make_all_dists(exp_data)

                parameter_names = sorted(this_config["parameters"])
                parameter_values = [this_config["parameters"][p] for p in parameter_names]
                all_params = product(*parameter_values)

                for each_param in all_params:
                    timestamp = datetime.datetime.now().isoformat()

                    parameters = {name: value for name, value
                                  in zip(parameter_names, each_param)}
                    log.info("Training model: {} with {}".format(this_config["model"],
                         parameters))

                    this_config["parameters"] = parameters
                    new_experiment = EISExperiment(this_config)
                    new_experiment.exp_data = exp_data
                    if config["pilot"]:
                        new_experiment.pilot_data = pilot_data
                    experiment_list.append(new_experiment)

    return experiment_list
