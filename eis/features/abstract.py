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
        self.is_categorical = False
        self.type_of_imputation = None
        self.set_null_counts_to_zero = False

    def build_and_insert( self, engine ):
        engine.execute( self.query )

        if self.set_null_counts_to_zero:
            # option to set all nulls to zeros (for use with counts)
            update_query = ("UPDATE features.{0} SET {1} = 0 "
                            "WHERE {1} IS null; ".format(self.table_name, self.feature_name))
            engine.execute( update_query )


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
    def __init__(self, **kwargs):
        self.description = ""
        self.description_long =""
        self.table_name = kwargs["table_name"]
        self.feature_name = self.__class__.__name__
        self.is_categorical = False
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
