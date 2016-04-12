import pdb
import copy
from itertools import product
import datetime
import logging

import officer, dispatch, explore

# Potential data sources in the police dept to draw from
MASTER_FEATURE_GROUPS = ["basic", "ia", "unit_div", "arrests",
                         "citations", "incidents", "field_interviews",
                         "cad", "training", "traffic_stops", "eis",
                         "extraduty", "neighborhood"]


log = logging.getLogger("Police EIS")

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
            features_to_use.update(config["features"][features])

        this_config = copy.copy(config)
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
