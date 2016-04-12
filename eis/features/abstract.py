#!/usr/bin/env python
import pdb
import logging
import yaml
import datetime

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
        self.type_of_imputation = "zero"
        self.feat_time_window = None


class OfficerTimeBoundedFeature(OfficerFeature):
    def __init__(self, **kwargs):
        OfficerFeature.__init__(self, **kwargs)
        self.feat_time_window = kwargs["feat_time_window"] * 365
        self.start_date = self.end_date - datetime.timedelta(
            days=self.feat_time_window)
        self.type_of_imputation = "zero"


class DispatchFeature():
    pass


class DispatchTimeBoundedFeature(DispatchFeature):
    pass
