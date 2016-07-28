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

class TimeGatedOfficerFeature(OfficerFeature):
    def __init__(self, **kwargs):
        OfficerFeature.__init__(self, **kwargs)
        self.lookback_durations = kwargs[ "lookback_durations" ]
        self.type_of_imputation = "zero"
        self.feature_column_names = [ feature_name + "_" + duration.replace(" ","_") for duration in self.lookback_duration ]

    def build_and_insert( self, engine ):
        for duration in self.lookback_durations:
            query_str = self.query
            query_str.replace( "INTERVAL", 
            engine.execute( self.query )
    
class DispatchFeature():
    def __init__(self, **kwargs):
        self.description = ""
        self.description_long =""
        self.table_name = kwargs["table_name"]
        self.feature_name = self.__class__.__name__
        # self.query should return two columns, named 'dispatch_id' and '<feature_name>'
        self.query = None
        # self.update_query take the result of the feature query and inserts it into the feature table
        self.update_query = ("UPDATE features.{} AS feature_table "
                            "SET {} = staging_table.feature_column "
                            "FROM ({}) AS staging_table "
                            "WHERE feature_table.dispatch_id = staging_table.dispatch_id ")

    def build_and_insert(self, engine):
        build_query = self.update_query.format(
                                self.table_name,
                                self.feature_name,
                                self.query)
        engine.execute(build_query)


class DispatchTimeBoundedFeature(DispatchFeature):
    pass
