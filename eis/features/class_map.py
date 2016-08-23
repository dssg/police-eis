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
    # TODO: make the passing of 'unit' to lookup nicer
    dummy_kwargs = {'unit':'dispatch', 'to_date': '', 'from_date': '', 'fake_today':datetime.datetime.today(), 'table_name':'dummy_table'}
    feature_classes = [lookup(feature, **dummy_kwargs) for feature in feature_list]

    categorical_features = [feature.feature_name for feature in feature_classes if feature.is_categorical]

    return categorical_features


def lookup(feature_name, unit, **kwargs):
    
    '''
    
    Instantiates an object of class feature_name.
    
    :str feature_name: The name of the feature to instantiate
    :str unit: The name of the type of feature being built; either 'officer' or 'dispatch'
    :returns: Object of feature class
    :rtype: unit.feature_name object
 
    '''
    
    # Assign the module to find the feature class in   
    if unit == 'officer':
        unit = officers
    elif unit == 'dispatch':
        unit = dispatches
    
    # Read in the feature class
    try:
        feature_class = getattr(unit, feature_name)
    except NameError:
        raise UnknownFeatureError(feature)
    
    # Instantiate the feature class
    feature = feature_class(**kwargs)    
    
    return feature


def find_label_features(feature_list):
    """Given a list of feature names return the names of the
    features which are labels

    Args:
        feature_list(list): list of feature names to check

    Returns:
        label_features(list): the features which are labels
    """

    # TODO: make it so that we don't need to supply a bogus fake today to instantiate an OfficerFeature
    # TODO: make passing 'unit' to lookup nicer
    dummy_kwargs = {'unit':'dispatch', 'to_date': '', 'from_date': '', 'fake_today':datetime.datetime.today(), 'table_name':'dummy_table'}
    feature_classes = [lookup(feature, **dummy_kwargs) for feature in feature_list]

    label_features = [feature.feature_name for feature in feature_classes if feature.is_label]

    return label_features
