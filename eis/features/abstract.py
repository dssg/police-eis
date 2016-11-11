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
        self.is_label = False
        self.query = None
        self.feature_name = self.__class__.__name__
        self.is_categorical = False
        self.type_of_imputation = None
        self.set_null_counts_to_zero = False
        # allow instantiation without kwargs
        try:
            # self.fake_today = kwargs["fake_today"]
            self.as_of_date = kwargs["as_of_date"]
            self.table_name = kwargs["table_name"]
        except (KeyError, AttributeError):
            log.info("WARNING: no fake today set for officer feature: {}".format(self.feature_name))
            pass

    def build_and_insert( self, engine ):
        engine.execute( self.query )
        if self.set_null_counts_to_zero:
            # option to set all nulls to zeros (for use with counts)
            update_query = ("UPDATE features.{0} SET {1} = 0 "
                            "WHERE {1} IS null; ".format(self.table_name, self.feature_name))
            engine.execute( update_query )

class CategoricalOfficerFeature(OfficerFeature):

    # class-defined wildcards for writing template queries.
    COLUMN     = "columnwildcardstring"
    LOOKUPCODE = "lookupcodewildcardstring"

    def __init__(self, **kwargs):
        OfficerFeature.__init__(self, **kwargs)
        self.type_of_imputation = "zero"

        # generate a list of column names to insert into.
        self.feature_column_names = []
        for key in self.categories:
            self.feature_column_names.append( self.feature_name + "_" + self.categories[key].replace( " ", "_" ) )

    def build_and_insert( self, engine ):
        for key in self.categories:

            # get the column name.
            column = self.feature_name + "_" + self.categories[key].replace(" ", "_")

            # insert.
            this_query = self.query
            this_query = this_query.replace( self.COLUMN, column )
            this_query = this_query.replace( self.LOOKUPCODE, str(key) )
            engine.execute( this_query )

            # if set_null_counts_to_zero update the column. This is similar to
            # the build and execute above, and might could need to be refactored
            # to deduplicate
            if self.set_null_counts_to_zero:
                # option to set all nulls to zeros (for use with counts)
                update_query = ("UPDATE features.{0} SET {1} = 0 "
                                "WHERE {1} IS null; ".format(self.table_name, column))
                engine.execute( update_query )

class TimeGatedCategoricalOfficerFeature(OfficerFeature):

    # class-defined wildcards for writing template queries.
    DURATION   = "durationwildcardstring"
    COLUMN     = "columnwildcardstring"
    LOOKUPCODE = "lookupcodewildcardstring"

    def __init__(self, **kwargs):
        OfficerFeature.__init__(self, **kwargs)
        self.lookback_durations = kwargs[ "lookback_durations" ]
        self.type_of_imputation = "zero"
        self.feature_column_names = [ self.feature_name + "_" + duration.replace(" ","_") for duration in self.lookback_durations ]

        # generate a list of column names to insert into.
        self.feature_column_names = []
        for duration in self.lookback_durations:
            for key in self.categories:
                self.feature_column_names.append( self.feature_name + "_" + self.categories[key].replace( " ", "_" ) + "_" + duration.replace(" ","_") )

    def build_and_insert( self, engine ):
        for duration in self.lookback_durations:
            for key in self.categories:

                # get the column name.
                column = self.feature_name + "_" + self.categories[key].replace(" ", "_") + "_" + duration.replace(" ","_")

                # insert.
                this_query = self.query
                this_query = this_query.replace( self.DURATION, duration )
                this_query = this_query.replace( self.COLUMN, column )
                this_query = this_query.replace( self.LOOKUPCODE, str(key) )
                engine.execute( this_query )

                # if set_null_counts_to_zero update the column. This is similar to
                # the build and execute above, and might could need to be refactored
                # to deduplicate
                if self.set_null_counts_to_zero:
                    # option to set all nulls to zeros (for use with counts)
                    update_query = ("UPDATE features.{0} SET {1} = 0 "
                                    "WHERE {1} IS null; ".format(self.table_name, column))
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

            # if set_null_counts_to_zero update the column. This is similar to
            # the build and execute above, and might could need to be refactored
            # to deduplicate
            if self.set_null_counts_to_zero:
                # option to set all nulls to zeros (for use with counts)
                update_query = ("UPDATE features.{0} SET {1} = 0 "
                                "WHERE {1} IS null; ".format(self.table_name, column))
                engine.execute( update_query )

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
        self.is_label = False
        # self.query should return two columns, named 'dispatch_id' and '<feature_name>'
        self.query = None
        # self.update_query take the result of the feature query and inserts it into the feature table
        self.update_query = ("CREATE UNLOGGED TABLE features_prejoin.{feature_name} "
                            "   AS ({query}); "
                            "CREATE UNIQUE INDEX {feature_name}_index "
                            "   ON features_prejoin.{feature_name} (dispatch_id);"
                            "ALTER TABLE features_prejoin.{feature_name} "
                            "   ADD PRIMARY KEY USING INDEX {feature_name}_index;")
        
        # allow instantiation without kwargs
        try:
            self.table_name = kwargs["table_name"]
        except KeyError:
            pass

    def build_and_insert(self, engine):
        build_query = self.update_query.format(
                                feature_name = self.feature_name,
                                query = self.query)

        engine.execute(build_query)


class DispatchTimeBoundedFeature(DispatchFeature):
    pass
