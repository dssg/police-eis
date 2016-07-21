#!/usr/bin/env python
import pdb
import logging
import yaml
import datetime

log = logging.getLogger(__name__)


class OfficerFeature():
    def __init__(self, **kwargs):
        self.description = ""
        self.description_long =""
        self.fake_today = kwargs["fake_today"]
        self.query = None
        self.feature_name = self.__class__.__name__
        self.table_name = kwargs["table_name"]

    def build_and_insert( self, engine ):
        engine.execute( self.query )

#        self.time_bound = None
#        self.num_features = 1
#        self.type_of_features = "float"
#        self.start_date = None
#        self.end_date = kwargs["time_bound"]
#        self.name_of_features = ""  # DEPRECATED
#        self.type_of_imputation = "zero"
#        self.feat_time_window = None


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
