#!/usr/bin/env python
import pdb
import logging
import yaml
import datetime

from .. import setup_environment
from . import abstract

log = logging.getLogger(__name__)
try:
    _, tables = setup_environment.get_database()
except:
    pass

time_format = "%Y-%m-%d %X"

class dummyfeature(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Dummy feature for testing 2016 schema")
        self.num_features = 1
        self.name_of_features = ["dummy"]
        self.query = ("SELECT officer_id, COUNT(event_type_code) "
                      "FROM events_hub "
                      "WHERE event_type_code = 4 "
                      "GROUP BY officer_id")
        self.type_of_imputation = "mean"

class academy_score(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer's score at the police academy")
        self.num_features = 1
        self.name_of_features = ["academy_score"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.score "
                      "FROM (   SELECT officer_id, score "
                      "         FROM staging.officer_trainings "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name ) )
        self.type_of_imputation = "mean"

class divorce_count(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of divorces for the officer")
        self.num_features = 1
        self.name_of_features = ["divorce_count"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_marital " 
                      "         WHERE marital_status_code = 4 "
                      "         AND last_modified <= '{}'::date "
                      "         GROUP BY officer_id ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )
        self.type_of_imputation = "mean"

class miles_from_post(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of miles that the officer lives from the post")
        self.num_features = 1
        self.name_of_features = ["miles_from_post"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.miles_to_assignment "
                      "FROM (   SELECT officer_id, miles_to_assignment "
                      "         FROM staging.officer_addresses " 
                      "         WHERE last_modified <= '{}'::date ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )
        self.type_of_imputation = "mean"

class arrest_count_career(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of career arrests" )
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=3 "
                      "         AND event_datetime <= '{}'::date "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{}'::date" 
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format),
                                self.fake_today.strftime(time_format)))
        self.type_of_features = "categorical"
        self.name_of_features = ["arrest_count_career"]
        self.type_of_imputation = "mean"

class arrest_count_1yr(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of arrests by officer in past 1 yr"
        self.name_of_features = ["arrest_count_1yr"]
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=3 "
                      "         AND event_datetime <= '{}'::date "
                      "         AND event_datetime >= '{}'::date - interval '1 year' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{}'::date" 
                      .format(  self.table_name,
                                self.feature_name, 
                                self.fake_today.strftime(time_format),
                                self.fake_today.strftime(time_format),
                                self.fake_today.strftime(time_format)))
        self.type_of_imputation = "mean"
        self.name_of_features = ["arrest_count_1yr"]
        self.type_of_features = "categorical"


class mean_hours_per_shift(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of hours worked on a shift on average")
        self.num_features = 1
        self.name_of_features = ["mean_hours_per_shift"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.avg "
                      "FROM (   SELECT officer_id, AVG( EXTRACT( EPOCH from shift_length)/3600 )"
                      "         FROM staging.officer_shifts "
                      "         WHERE EXTRACT( EPOCH from shift_length)/3600 < 48 " # Discarding tremendous outliers (bad data).
                      "         AND stop_datetime < '{}'::date "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name, 
                                self.fake_today.strftime(time_format)))
        self.type_of_imputation = "mean"

