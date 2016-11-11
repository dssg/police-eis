import pdb
import copy
from itertools import product
import datetime
import logging
from IPython.core.debugger import Tracer
from dateutil.relativedelta import relativedelta
from . import officer, dispatch, explore

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


    for prediction_window, update_window, officer_past_activity in product(
         config['prediction_window'], config['update_window'], config['officer_past_activity_window']):
        test_end_date = end_date
        while start_date < test_end_date - relativedelta(months=prediction_window*2):
            train_end_date = test_end_date - relativedelta(months=prediction_window)
            lookup_back_activity = train_end_date - relativedelta( )
            if lookup_back_activity >= start_date:
                temporal_info.append({'prediction_window':prediction_window,
                                      'officer_past_activiy_window': officer_past_activity,
                                      'test_end_date': test_end_date,
                                      'train_end_date': train_end_date})
                log.debug("test_end_date:'{}', train_end_date:'{}', update_window: '{}', 'prediction_window: '{}' '".format(test_end_date,
                       train_end_date, update_window, prediction_window)) 
            test_end_date -= relativedelta(months=update_window)

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

    for temporal_info in all_temporal_info:

        # create a copy of the dictionary from the experiment configuration yaml and add
        # the temporal cross validation information
        this_config = copy.copy(config)
        this_config["train_end_date"] = temporal_info["train_end_date"].strftime("%d%b%Y")
        this_config["test_end_date"] = temporal_info["test_end_date"].strftime("%d%b%Y")
        this_config["prediction_window"] = temporal_info["prediction_window"]
        this_config["officer_past_activiy_window"] = temporal_info["officer_past_activiy_window"]
        
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
