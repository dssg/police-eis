import pdb
import copy
from itertools import product
import datetime
import logging
from dateutil.relativedelta import relativedelta
from . import officer

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

def generate_as_of_dates(config):
    """
    Given the start_date, end_date and update_window from the config
    it generates a list of as_of_dates that will be used for feature generation

    Args:
       config: Python dict read in from YAML config file containing
                user-supplied details of the experiments to be run

    Returns:
       as_of_dates: 
    """
    end_date = datetime.datetime.strptime(config['end_date'], "%Y-%m-%d")
    start_date = datetime.datetime.strptime(config['start_date'], "%Y-%m-%d")
    as_of_dates = set()
    

    as_of_dates = []
    for update_window in config['update_window']:
        while end_date > start_date:
            log.debug('end_date: {}'.format(end_date))
            for prediction_window in config['prediction_window']:
                as_of_date = end_date
                while as_of_date > start_date:
                    as_of_dates.append(as_of_date)
                    as_of_date -= relativedelta(months=prediction_window)
                    log.debug(as_of_date)
            end_date -= relativedelta(days=update_window) 

    return as_of_dates

def generate_time_sets(config):
    """Takes a config file and generates a list of dicts, each of which
    will be an experiment.

    Args:
         config: Python dict read in from YAML config file containing
                user-supplied details of the experiments to be run

    Returns:
        temporal_info: dict containing time related fields for experiments
    """
    temporal_info = []

    end_date = datetime.datetime.strptime(config['end_date'], "%Y-%m-%d")
    start_date = datetime.datetime.strptime(config['start_date'], "%Y-%m-%d")

    for prediction_window, update_window, officer_past_activity, training_window in product(
                           config['prediction_window'], config['update_window'], 
                           config['officer_past_activity_window'], config['training_window']):

        test_end_date = end_date
        # loop moving giving an update_window
        while start_date < test_end_date - relativedelta(months=prediction_window*2):
            test_start_date = test_end_date - relativedelta(months=prediction_window)
            train_end_date = test_start_date
            train_start_date = train_end_date - relativedelta(months=training_window)
            temporal_info.append({ 'test_end_date': test_end_date,
                                   'test_start_date': test_start_date,
                                   'train_end_date': train_end_date,
                                   'train_start_date': train_start_date,
                                    'prediction_window':prediction_window,
                                    'officer_past_activity_window': officer_past_activity})
            log.debug("test_end_date:'{}', test_start_date:'{}', train_end_date:'{}', train_start_date:'{}',"
                       "update_window: '{}', 'prediction_window: '{}' '".format(test_end_date, test_start_date,
                       train_end_date, train_start_date, update_window, prediction_window))
            test_end_date -= relativedelta(days=update_window)
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

    # generate a list of {fake_today, training_window, prediction_window} dictionaries
    all_temporal_info = generate_time_sets(config)
    all_as_of_dates = generate_as_of_dates(config)

    for temporal_info in all_temporal_info:

        # create a copy of the dictionary from the experiment configuration yaml and add
        # the temporal cross validation information
        this_config = copy.copy(config)
        this_config["train_start_date"] = temporal_info["train_start_date"].strftime("%Y-%m-%d")
        this_config["train_end_date"] = temporal_info["train_end_date"].strftime("%Y-%m-%d")
        this_config["test_start_date"] = temporal_info["test_start_date"].strftime("%Y-%m-%d")
        this_config["test_end_date"] = temporal_info["test_end_date"].strftime("%Y-%m-%d")
        this_config["prediction_window"] = temporal_info["prediction_window"]
        this_config["officer_past_activity_window"] = temporal_info["officer_past_activity_window"]

        # pass only the labels names selected in the config as True
        this_config["officer_labels"] = [ key for key in config["officer_labels"] if config["officer_labels"][key] == True ]
        
        # get the appropriate feature data from the database
        if config["unit"] == "officer":
            # get officer-level features to use
            this_config["officer_features"] = officer.get_officer_features_table_columns( config )
            exp_data = officer.run_traintest(this_config)

        elif config["unit"] == "dispatch":
            exp_data = dispatch.run_traintest(this_config)
        else:
            log.error("Invalid 'unit' specified in config file: {}".format(config['unit']))

        for model in config["model"]:

            this_config["parameters"] = config["parameters"][model]
            this_config["model"] = model

            parameter_names = sorted(this_config["parameters"])
            parameter_values = [this_config["parameters"][p] for p in parameter_names]
            all_params = product(*parameter_values)

            for each_param in all_params:
                timestamp = datetime.datetime.now().isoformat()

                parameters = {name: value for name, value
                              in zip(parameter_names, each_param)}
                log.info("Will train model: {} with {}".format(this_config["model"],
                     parameters))

                this_config["parameters"] = parameters
                new_experiment = EISExperiment(this_config)
                new_experiment.exp_data = exp_data
                experiment_list.append(new_experiment)

    return experiment_list
