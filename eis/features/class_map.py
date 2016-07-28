import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime

from . import officers
from . import dispatches

log = logging.getLogger(__name__)


class UnknownFeatureError(Exception):
    def __init__(self, feature):
        self.feature = feature

    def __str__(self):
        return "Unknown feature: {}".format(self.feature)

def find_categorical_features(feature_list):
    """Given a list of feature names return the names of the
    features which are categorical

    Args:
        feature_list(list): list of feature names to check

    Returns:
        categorical_features(list): the features which are categorical
    """

    # TODO: make it so that we don't need to supply a bogus fake today to instantiate an OfficerFeature
    dummy_kwargs = {'fake_today':datetime.datetime.today(), 'table_name':'dummy_table'}
    feature_classes = [lookup(feature, **dummy_kwargs) for feature in feature_list]

    categorical_features = [feature.feature_name for feature in feature_classes if feature.is_categorical]

    return categorical_features


def lookup(feature, **kwargs):

    if feature[1:3] == "yr":
        kwargs["feat_time_window"] = int(feature[0])
    else:
        kwargs["feat_time_window"] = 15

    dict_lookup = { 'AcademyScore': officers.AcademyScore(**kwargs),
                    'ArrestCount1Yr': officers.ArrestCount1Yr(**kwargs),
                    'ArrestCountCareer': officers.ArrestCountCareer(**kwargs),
                    'DivorceCount': officers.DivorceCount(**kwargs),
                    'SustainedRuleViolations': officers.SustainedRuleViolations(**kwargs),
                    'IncidentCount': officers.IncidentCount(**kwargs),
		    'MeanHoursPerShift': officers.MeanHoursPerShift(**kwargs),
                    'MilesFromPost': officers.MilesFromPost(**kwargs),
                    'OfficerGender': officers.OfficerGender(**kwargs),
	            'RandomFeature': dispatches.RandomFeature(**kwargs),
                    'DummyFeature': dispatches.DummyFeature(**kwargs),
                    'TimeGatedDummyFeature': officers.TimeGatedDummyFeature(**kwargs),
		    'OfficerRace': officers.OfficerRace(**kwargs),
                    'AllAllegations': officers.AllAllegations(**kwargs),
		    'DummyFeature': dispatches.DummyFeature(**kwargs)
                  }

    if feature not in dict_lookup.keys():
        raise UnknownFeatureError(feature)

    return dict_lookup[feature]
