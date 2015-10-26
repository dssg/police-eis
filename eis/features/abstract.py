#!/usr/bin/env python
import pdb
import logging
import yaml

from eis import setup_environment

log = logging.getLogger(__name__)


class OfficerFeature():
    def __init__(self, **kwargs):
        self.description = ""
        self.time_bound = None
        self.num_features = 1
        self.type_of_features = "float"
        self.start_date = None
        self.end_date = kwargs["time_bound"]
        self.query = None
        self.name_of_features = ""
        self.type_of_imputation = ""
        self.feat_time_window = None