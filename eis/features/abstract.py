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
        self.query = None
        self.feature_name = self.__class__.__name__
        self.is_categorical = False
        self.type_of_imputation = None
        self.set_null_counts_to_zero = False
        
        # allow instantiation without kwargs
        try:
            self.fake_today = kwargs["fake_today"]
            self.table_name = kwargs["table_name"]
        except (KeyError, AttributeError):
            pass

    def build_and_insert( self, engine ):
        engine.execute( self.query )
        if self.set_null_counts_to_zero:
            # option to set all nulls to zeros (for use with counts)
            update_query = ("UPDATE features.{0} SET {1} = 0 "
                            "WHERE {1} IS null; ".format(self.table_name, self.feature_name))
            engine.execute( update_query )

class TimeGatedOfficerFeature(OfficerFeature):
    
    # class-defined wildcards for writing template queries. 
    DURATION = "durationwildcardstring"
    COLUMN   = "columnwildcardstring"

    def __init__(self, **kwargs):
        OfficerFeature.__init__(self, **kwargs)

        # allow instantiation without kwargs
        try:
            self.lookback_durations = kwargs[ "lookback_durations" ]
            self.type_of_imputation = "zero"
            self.feature_column_names = [ self.feature_name + "_" + duration.replace(" ","_") for duration in self.lookback_durations ]
        except KeyError:
            pass

    def build_and_insert( self, engine ):
        for duration, column in zip( self.lookback_durations, self.feature_column_names):
            this_query = self.query
            this_query = this_query.replace( self.DURATION, duration )
            this_query = this_query.replace( self.COLUMN, column )
            engine.execute( this_query )
    
class DispatchFeature():
    def __init__(self, **kwargs):
        try:
            self.from_date = kwargs["from_date"]
            self.to_date = kwargs["to_date"]
            self.table_name = kwargs["table_name"]
        except KeyError:
            pass
        self.description = ""
        self.description_long =""
        self.feature_name = self.__class__.__name__
        self.is_categorical = False
        # self.query should return two columns, named 'dispatch_id' and '<feature_name>'
        self.query = None
        # self.update_query take the result of the feature query and inserts it into the feature table
        self.update_query = ("UPDATE features.{} AS feature_table "
                            "SET {} = staging_table.feature_column "
                            "FROM ({}) AS staging_table "
                            "WHERE feature_table.dispatch_id = staging_table.dispatch_id ")
        
        # allow instantiation without kwargs
        try:
            self.table_name = kwargs["table_name"]
        except KeyError:
            pass

    def build_and_insert(self, engine):
        build_query = self.update_query.format(
                                self.table_name,
                                self.feature_name,
                                self.query)
        engine.execute(build_query)


class DispatchTimeBoundedFeature(DispatchFeature):
    pass
