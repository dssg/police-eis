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


### Basic Officer Features

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

class HeightWeight(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer height and weight, calculated as "
                            "an average across all SI cases involving "
                            "that officer.")
        self.num_features = 2
        self.name_of_features = ["weight", "height"]
        self.query = ("select officer_id, avg(weight_int) as avg_weight, "
                      "avg(height_inches_int) as avg_height_inches "
                      "from {} group by officer_id".format(tables['si_table']))
        self.type_of_imputation = "mean"


class Education(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer education level"
        self.num_features = 12
        self.type_of_features = "categorical"
        self.name_of_features = [""]
        self.query = ("select officer_id, education_level_cleaned "
                      "from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"


class MaritalStatus(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.type_of_features = "categorical"
        self.description = "Marital status of officer"
        self.query = ("select officer_id, marital_status as "
                      "married from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"


class YrsExperience(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of years of experience for police officer"
        self.fake_today = kwargs["fake_today"]
        self.name_of_features = ["years_experience"]
        self.query = ("select officer_id, {} - EXTRACT(YEAR FROM "
                      "hire_date_employed) as "
                      "yrs_experience from {}".format(self.fake_today.year,
                                                      tables['officer_table']))
        self.type_of_imputation = "mean"


class DaysExperience(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of days of experience for police officer"
        self.fake_today = kwargs["fake_today"]
        self.name_of_features = ["days_experience"]
        self.query = ("select officer_id, EXTRACT('days' FROM '{}'::date - "
                      "hire_date_employed) as "
                      "days_experience from {}".format(
                          self.fake_today.strftime(time_format),
                          tables['officer_table']))
        self.type_of_imputation = "mean"


class MaleFemale(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Is officer male or female"
        self.query = ("select officer_id, empl_sex_clean as "
                      "male_female from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"
        self.type_of_imputation = "mean"


class Race(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer race"
        self.query = ("select officer_id, race_code as "
                      "race from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"
        self.type_of_imputation = "mean"


class Age(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer age"
        self.fake_today = kwargs["fake_today"]
        self.name_of_features = ["age"]
        self.query = ("select officer_id, {} - birthdate_year "
                      "as age from {}".format(self.fake_today.year,
                                              tables['officer_table']))
        self.type_of_imputation = "mean"


class AgeAtHire(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer age at hire"
        self.name_of_features = ["age_at_hire"]
        self.query = ("select officer_id, extract(year from "
                      "hire_date_employed)-birthdate_year as "
                      "age_at_hire from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"

### Arrest History Features

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


class FractionMaleFemale(abstract.OfficerFeature):
    pass


class ArrestTimeSeries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Timeseries of arrest counts (1 yr agg) for officer"
        self.name_of_features = ["timeseries_arrests"]
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=2920)
        self.query = ("select officer_id, array_agg(intervals_count) as "
                      "arrest_timeseries from (select "
                      "a.officer_id as officer_id, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.arrcount) "
                      "as intervals_count from (select a.officer_id, "
                      "b.intervals, intervalid, 0 as default_count "
                      "from (select distinct officer_id "
                      "from {}) as a left join (select intervals, "
                      "row_number() over () as intervalid from ( "
                      "select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, "
                      "'1 day') as d order by intervals) as foo) as b "
                      "on True) as a "
                      "left join (select officer_id, date_trunc('year',arrest_date) "
                      "as intervals, count(distinct aa_id)::int as arrcount "
                      "from {} group by officer_id, date_trunc('year',arrest_date)) "
                      "as b on a.officer_id = b.officer_id and a.intervals = b.intervals) "
                      "as koo group by officer_id").format(
                          tables["arrest_charges_table"], self.start_date,
                          self.end_date, tables["arrest_charges_table"])
        self.type_of_features = "series"
        self.type_of_imputation = "mean"


class ArrestRateDelta(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Delta recent (<1yr) arrest rate to career rate"
        self.name_of_features = ["delta_arrest_rate"]
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.query = ("select a.officer_id, a.career_rate, b.recent_rate, "
                      "b.recent_rate / a.career_rate as "
                      "recent_career_arrest_rate_delta from "
                      "(select a.officer_id, a.career_arrest_count / (extract(year "
                      "from coalesce(terminationdate, "
                      "'{}'::date)) - extract(year from b.hire_date) + 2) "
                      "as career_rate from "
                      "(select count(distinct aa_id) as career_arrest_count, "
                      "officer_id from {} where arrest_date <= '{}'::date "
                      "group by officer_id) as a "
                      "left join {} as b "
                      "on a.officer_id = b.officer_id) as a "
                      "left join (select count(distinct aa_id) "
                      "as recent_rate, officer_id from {} where "
                      "arrest_date <= '{}'::date and arrest_date >= "
                      "'{}'::date group by officer_id) as b "
                      "on a.officer_id = b.officer_id ").format(
                          self.end_date, tables["arrest_charges_table"],
                          self.end_date, tables["officer_table"],
                          tables["arrest_charges_table"], self.end_date,
                          self.start_date)
        self.type_of_imputation = "mean"


class DiscOnlyArrestsCount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career disc ONLY arrests for officer"
        self.name_of_features = ["disc_only_count"]
        self.query = ("select officer_id, count(distinct aa_id) as disc_only_count "
                      "from ( select a.*, b.aa_id as aa_id2 from "
                      "( select aa_id, officer_id, arrest_date from {} "
                      "where charge_desc like '%DISORDERLY%' "
                      "or charge_desc like '%OBSTRUCT%' "
                      "or charge_desc like '%RESIST%' "
                      "or charge_desc like '%%DELAY' "
                      "and arrest_date <= '{}'::date) as a "
                      "left join (select aa_id from {} "
                      "where charge_desc not like '%DISORDERLY%' "
                      "and charge_desc not like '%OBSTRUCT%' "
                      "and charge_desc not like '%RESIST%' "
                      "and charge_desc not like '%DELAY%' "
                      "and arrest_date <= '{}'::date) as b "
                      "on a.aa_id = b.aa_id) as foo where aa_id2 "
                      "is null group by officer_id").format(
                          tables["arrest_charges_table"],
                          self.end_date,
                          tables["arrest_charges_table"],
                          self.end_date)


class AvgAgeArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average age of arrestees for officer"
        self.name_of_features = ["avg_age_arrestees"]
        self.query = ("select avg(age) as avg_age_arrestees, officer_id "
                      "from {} "
                      "where arrest_date <= '{}'::date "
                      "group by officer_id").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "mean"


class AvgTimeOfDayArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average arrest time of day for officer"
        self.name_of_features = ["tod_arrest"]
        self.query = ("select avg(extract(hour from arrest_date)) "
                      "as arrest_avg_hour, officer_id from {} "
                      "where arrest_date <= '{}'::date "
                      "group by officer_id").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "mean"


class DiscArrests(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of discretionary arrests for officer"
        self.name_of_features = ["disc_arrest_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, officer_id "
                      "from ( select count(*) as c, officer_id, string_agg("
                      "charge_desc::text, '    ') as charges from "
                      "{} where charge_desc is not null "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by officer_id, aa_id) a "
                      "where c=1 and charges similar to "
                      "'%(DISORDERLY|RESIST|OBSTRUCT|DELAY)%' "
                      "group by officer_id").format(
                          self.name_of_features[0],
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class NPCArrests(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of no probable cause arrests for officer"
        self.name_of_features = ["npc_arrest_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(distinct aa_id) as {}, "
                      "officer_id from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by officer_id").format(
                          self.name_of_features[0],
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class ArrestCentroids(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Arrest Centroids"
        self.name_of_features = ["arrest_centroids"]
        self.type_of_features = "categorical"
        self.query = ("select a.officer_id, b.subbeat "
                      "from( select *, st_setsrid(st_makepoint( "
                      "cent_long,cent_lat), 4326) as point "
                      "from( "
                      "select avg(long) as cent_long, avg(lat) as "
                      "cent_lat, officer_id "
                      "from( select distinct on (aa_id) "
                      "aa_id, arrest_date, officer_id, lat, long "
                      "from {} ) as foo "
                      "where arrest_date <= '{}'::date "
                      "group by officer_id ) as doo ) as a "
                      "left join {} as b "
                      "on st_contains(b.geom2::geometry, a.point::"
                      "geometry)").format(tables["arrest_charges_table"],
                       self.end_date, tables["sub_beats"])
        self.type_of_imputation = "mean"


### Citations

class NPCCitations(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of no probable cause citations"
        self.name_of_features = ["npc_citations_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id,count(*) as {} "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["citations_table"],
                      self.end_date, self.start_date)


class Citations(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of citations"
        self.name_of_features = ["citations_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id,count(*) as {} "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["citations_table"],
                      self.end_date, self.start_date)

## CAD

class CADStatistics(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "CAD Statistics"
        feat_prefixes = ['avg_seq_assgn_',
                         'avg_diff_arrv_assgn_',
                         'avg_travel_time_',
                         'std_travel_time_',
                         'avg_response_time_',
                         'avg_scene_time_',
                         'avg_prior_orig_',
                         'std_prior_orig_',
                         'avg_prior_fin_',
                         'std_prior_fin_',
                         'priority_diff_']
        all_featnames = []
        for prefix in feat_prefixes:
            all_featnames.append('{}_{}yr'.format(prefix, int(self.feat_time_window/365)))
        self.name_of_features = all_featnames
        self.query = ("select avg(seq_assigned) as {x[0]}, "
                      "avg(seq_arrived-seq_assigned) as {x[1]}, "
                      "avg(travel_time) as {x[2]}, "
                      "stddev(travel_time) as {x[3]}, "
                      "avg(response_time) as {x[4]}, "
                      "log(avg(at_scene_time)+1) as {x[5]}, "
                      "avg(priority_org::int) as {x[6]}, "
                      "stddev(priority_org::int) as {x[7]}, "
                      "avg(priority_fin::int) as {x[8]}, "
                      "stddev(priority_fin::int) as {x[9]}, "
                      "avg(priority_org::int - priority_fin::int) as "
                      "{x[10]}, officer_id from {table} "
                      "where date_add <= '{date1}'::date "
                      "and date_add >= '{date2}'::date "
                      "group by officer_id").format(x=self.name_of_features,
                      table=tables["dispatch_table"],
                      date1=self.end_date, date2=self.start_date)
        self.type_of_imputation = "mean"

termination_types = ["CANCOMM", "FTC", "UNKNOWN", "CANCCOMP", "CANCOFC", "DUPNCAN",
                     "CANCALRM", "ASSTUNIT", "UL", "CITE_NMV", "CITE_MV", "ACCIDENT",
                     "WARNING", "OO/CASE", "MI"]


class CountCADTerminationTypes(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Normalized number of shifts spent in certain divisions"
        all_featnames, all_queries = [], []
        for term in termination_types:
            this_feature = "in_division_{}_{}yr".format(
            term, int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when div='{}' then 1 else null end)::float "
            "as {} from {} "
            "WHERE date_add >= '{}'::date "
            "AND date_add <= '{}'::date "
            "group by officer_id").format(term, this_feature,
            tables["dispatch_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries

## Former EIS

eis_warning_types_name_map = {'Use Of Force':'uof',
                    'Complaint':'cml',
                    'Pursuit':'pur',
                    'Sick Leave Frequency':'slf',
                    'Injury':'inj',
                    'Sick Leave/Days Off':'sld',
                    'Combination':'comb',
                    'Accident':'acc',
                    'Supv Initiated':'sup'}

eis_intervention_types_name_map = {'No Intervention Required':'noint',
                        'Training':'trn',
                        'Other':'oth',
                        'EAP':'eap',
                        'Counseling':'coun',
                        'Combined':'comb'}


class EISWarningsCount(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Count of EIS warnings"
        self.name_of_features = ["eis_warning_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(distinct eisno) as {}, officer_id "
                      "from {} where "
                      "datecreated <= '{}'::date "
                      "and datecreated >= '{}'::date "
                      "group by officer_id").format(self.name_of_features[0],
                      tables["eis_table"],
                      self.end_date, self.start_date)


class EISWarningByTypeFrac(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Frac of each EIS warning by type"
        all_featnames, all_queries = [], []
        for warning_type in list(eis_warning_types_name_map.keys()):
            this_feature = "eis_frac_warnings_{}_{}yr".format(
            eis_warning_types_name_map[warning_type],
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when eventtype LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when eventtype is not null then 1 else null end)+1) as {} "
            "from (select distinct eisno, eventtype, officer_id, datecreated from {}) as a "
            "where datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by officer_id").format(warning_type, this_feature,
            tables["eis_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class EISWarningByTypeCount(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Count of each EIS warning by type"
        all_featnames, all_queries = [], []
        for warning_type in list(eis_warning_types_name_map.keys()):
            this_feature = "eis_count_warnings_{}_{}yr".format(
            eis_warning_types_name_map[warning_type],
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(distinct eisno) as {} "
            "from {} "
            "where eventtype = '{}' "
            "and datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by officer_id").format(this_feature, tables["eis_table"],
            warning_type, self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class EISWarningInterventionFrac(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Frac of each EIS warning by type"
        all_featnames, all_queries = [], []
        for intervention_type in list(eis_intervention_types_name_map.keys()):
            this_feature = "eis_frac_intervention_{}_{}yr".format(
            eis_intervention_types_name_map[intervention_type],
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when intervention LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when intervention is not null then 1 else null end)+1) as {} "
            "from (select distinct eisno, intervention, officer_id, datecreated from {}) as a "
            "where datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by officer_id").format(intervention_type, this_feature,
            tables["eis_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class EISInterventionByTypeCount(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Count of each EIS intervention by type"
        all_featnames, all_queries = [], []
        for intervention_type in list(eis_intervention_types_name_map.keys()):
            this_feature = "eis_count_interventions_{}_{}yr".format(
            eis_warning_types_name_map[intervention_type],
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(distinct eisno) as {} "
            "from {} "
            "where intervention = '{}' "
            "and datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by officer_id").format(this_feature, tables["eis_table"],
            intervention_type, self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries

## Field interviews


class FICount(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews"
        self.name_of_features = ["fi_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, officer_id "
                      "from {} "
                      "where corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["field_int_table"],
                      self.end_date, self.start_date)


class NonTrafficFICount(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of non-traffic field interviews"
        self.name_of_features = ["fi_nontraffic_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, officer_id "
                      "from {} where traffic_stop_yn = 'N' "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["field_int_table"],
                      self.end_date, self.start_date)


class HighCrimeAreaFI(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews in high crime area"
        self.name_of_features = ["fi_highcrime_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, officer_id "
                      "from {} where narrative like '%high crime area%' "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["field_int_table"],
                      self.end_date, self.start_date)


loiter_sleep_sit = """ (narrative like '%loiter%' or narrative like '%sleep%'
                      or narrative like '%sitting%' or narrative like '%walk%'
                      and narrative not like '%call for service%') """


class LoiterFI(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews of loiterers"
        self.name_of_features = ["fi_loiter_count_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, officer_id "
                      "from {} where {} "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["field_int_table"], loiter_sleep_sit,
                      self.end_date, self.start_date)


class CareerBlackFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Fraction of field interviews where the suspect is black"
        self.name_of_features = ["career_frac_black_suspects"]
        self.query = ("select officer_id, "
                      "(SUM(CASE WHEN rac_code = 'B' then 1 else null end))::float/"
                      "(count(*) + 1) as fi_prop_black from {} "
                      "where corrected_interview_date <= '{}'::date "
                      " group by officer_id").format(
                      tables["field_int_table"],
                      self.end_date)


class CareerWhiteFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Fraction of field interviews where the suspect is white"
        self.name_of_features = ["career_frac_white_suspects"]
        self.query = ("select officer_id, "
                      "(SUM(CASE WHEN rac_code = 'W' then 1 else null end))::float/"
                      "(count(*) + 1) as fi_prop_white from {} "
                      "where corrected_interview_date <= '{}'::date "
                      " group by officer_id").format(
                      tables["field_int_table"],
                      self.end_date)


class FIAvgSuspectAge(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average age of suspects in field interviews"
        self.name_of_features = ["avg_age_suspects_fi"]
        self.query = ("select avg(age) as fi_avg_age, officer_id from {} "
                      "group by officer_id").format(
                      tables["field_int_table"],
                      self.end_date)


class FIAvgTimeOfDay(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average time of day for field interviews"
        self.name_of_features = ["avg_tod_fi"]
        self.query = ("select avg(extract(hour from corrected_interview_date)) "
                      "as fi_avg_hour, officer_id from {} "
                      "group by officer_id").format(
                      tables["field_int_table"],
                      self.end_date)


class FITimeseries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=2920)
        self.type_of_features = "series"
        self.description = "Timeseries for interviews"
        self.name_of_features = ["fi_timeseries"]
        self.query = ("select officer_id, array_agg(intervals_count) as fi_timeseries "
                      "from ( select a.officer_id as officer_id, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.ficount) as intervals_count "
                      "from (select a.officer_id, b.intervals, intervalid, "
                      "0 as default_count from "
                      "(select distinct officer_id from {}) as a "
                      "left join (select intervals, row_number() over () as intervalid "
                      "from ( select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, '1 day') "
                      "as d order by intervals) as foo) as b on True) as a "
                      "left join (select officer_id, date_trunc('year',corrected_interview_date) "
                      "as intervals, count(*)::int as ficount from {} "
                      "group by officer_id, date_trunc('year',corrected_interview_date)) as b "
                      "on a.officer_id = b.officer_id and a.intervals = b.intervals) as koo "
                      "group by officer_id").format(
                      tables["field_int_table"], self.start_date,
                      self.end_date, tables["field_int_table"])
        self.type_of_imputation = "mean"


## Incidents

class YearNumSuicides(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of suicides in last year"
        self.name_of_features = ["suicides_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_suicide "
                      "from {} where ucr_desc = 'Suicide' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumJuvenileVictim(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of juvenile victims in last year"
        self.name_of_features = ["juvenile_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_juv_victim "
                      "from {} where victim1_age_int < 16 "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumDomesticViolence(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of domestic violence incidents in last year"
        self.name_of_features = ["domestic_violence_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_domestic_violence "
                      "from {} where domestic_violence_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumHate(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of hate incidents in last year"
        self.name_of_features = ["hate_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_hate "
                      "from {} where bias_hate_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumNarcotics(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of narcotics incidents in last year"
        self.name_of_features = ["narcotics_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_narcotics "
                      "from {} where narcotics_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumGang(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of gang incidents in last year"
        self.name_of_features = ["gang_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_gang "
                      "from {} where gang_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumGunKnife(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of gun/knife incidents in last year"
        self.name_of_features = ["gun_knife_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_guns "
                      "from {} where gang_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumPersWeaps(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Number of personal weapons incidents in last year"
        self.name_of_features = ["personal_weapon_count"]
        self.query = ("select officer_id,count(distinct inc_id) as num_weapons "
                      "from {} where weapon_type_code = 'G' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class AvgAgeVictims(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Average age of victims in last year"
        self.name_of_features = ["avg_victim_age"]
        self.query = ("select avg(cast(victim1_age as float)) as victim_age, "
                      "officer_id from {} "
                      "where date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class MinAgeVictims(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.description = "Min age of victims in last year"
        self.name_of_features = ["min_victim_age"]
        self.query = ("select avg(cast(victim1_age as float)) as min_victim_age,"
                      "officer_id from {} "
                      "where date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by officer_id").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


## Traffic stops

runtag_reasons = ["tag", "seatbelt", "random",
                            "expired", "safety", "crime", "light",
                            "speeding", "speed", "insurance",
                            "verify", "29"]

search_reasons = ["crime", "suspicious", "marijuana", "consent", "all my stops", "area", "drug"]

#stop_reasons = ['DWI', 'INV', 'SAFE', 'SPD', 'STBLT', 'STPLT', 'VEHQP', 'VEHRG']
stop_results = ['A', 'C', 'VW', 'WW']

races = ['B', 'W', 'A', 'U', 'I']

genders = ['M', 'F']

class TrafficStopsSearch(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = ("Number of times officer asks for "
                            "consent to search in traffic stop")
        self.name_of_features = ["traffic_stops_search_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id,count(*) as {} "
                      "from {} where consent_search='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class TrafficStopSearchReason(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Frac of each traffic stop search reasons"
        all_featnames, all_queries = [], []
        for reason in search_reasons:
            this_feature = "traffic_stops_search_reason_{}_{}yr".format(
            reason.replace(" ", ""), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when search_reason LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when search_reason is not null then 1 else null end)+1) as {} "
            "from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by officer_id").format(reason, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopRunTagReason(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Frac of each traffic stop run tag reasons"
        all_featnames, all_queries = [], []
        for reason in runtag_reasons:
            this_feature = "traffic_stops_runtag_reason_{}_{}yr".format(
            reason.replace(" ", ""), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when runtag_reason LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when runtag_reason is not null then 1 else null end)+1) as {} "
            "from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by officer_id").format(reason, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopResult(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Frac of each traffic stop results"
        all_featnames, all_queries = [], []
        for reason in stop_results:
            this_feature = "traffic_stops_result_{}_{}yr".format(
            reason.replace(" ", ""), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when resultofstop='{}' then 1 else null end)::float/"
            "(count(*)+1) as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by officer_id").format(reason, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopFracRace(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Fraction of traffic stops by suspect race"
        all_featnames, all_queries = [], []
        for race in races:
            this_feature = "traffic_stops_byrace_{}_{}yr".format(
            race.replace(" ", ""), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when race = '{}' then 1 else null end)"
            "::float/(count(case when race is not null then 1 else null end)+1) "
            "as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by officer_id").format(race, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopFracGender(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Fraction of traffic stops by suspect gender"
        all_featnames, all_queries = [], []
        for gender in genders:
            this_feature = "traffic_stops_bygender_{}_{}yr".format(
            gender.replace(" ", ""), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when race = '{}' then 1 else null end)"
            "::float/(count(case when gender is not null then 1 else null end)+1) "
            "as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by officer_id").format(gender, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopTimeSeries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Timeseries of traffic stop counts (1 yr agg) for officer"
        self.name_of_features = ["timeseries_trafficstops"]
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=2920)
        self.query = ("select officer_id, array_agg(intervals_count) as "
                      "trf_stop_timeseries from (select "
                      "a.officer_id as officer_id, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.trfcount) "
                      "as intervals_count from (select a.officer_id, "
                      "b.intervals, intervalid, 0 as default_count "
                      "from (select distinct officer_id "
                      "from {}) as a left join (select intervals, "
                      "row_number() over () as intervalid from ( "
                      "select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, "
                      "'1 day') as d order by intervals) as foo) as b "
                      "on True) as a "
                      "left join (select officer_id, date_trunc('year',date_time_action) "
                      "as intervals, count(distinct inc_key) as trfcount "
                      "from {} group by officer_id, date_trunc('year',date_time_action)) "
                      "as b on a.officer_id = b.officer_id and a.intervals = b.intervals) "
                      "as koo group by officer_id").format(
                          tables["officer_table"], self.start_date,
                          self.end_date, tables["stops_table"])
        self.type_of_features = "series"
        self.type_of_imputation = "mean"


class NumTrafficStops(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description =  "Number of traffic stops"
        self.name_of_features = ["num_traffic_stops_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id, count(distinct inc_key) as "
                      "{} from {} "
                      "where date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class NumTStopRunTagUOFOrArrest(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description =  ("Number of traffic stops where the tag was "
                            "run and then force was used or an arrest was made")
        self.name_of_features = ["num_traffic_stops_taguofarrest_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id, count(*) as {} "
                      "from {} where run_tag='Y' and (uof='Y' or "
                      "arrest_driver='Y' or arrest_pass='Y') "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class NumTrafficStopsForce(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description =  "Number of traffic stops where force is used"
        self.name_of_features = ["num_traffic_stops_uof_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id, count(distinct inc_key) as "
                      "{} from {} where uof='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class TSPercBlackDayNight(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Ratio of times officer stops a black person at night vs during the day"
        self.name_of_features = ["ratio_bl_night_day_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select officer_id, "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 1 else 0 end )+1)::float/ "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 0 else 1 end )+1) as {} "
                      "from {} where race='B' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class CareerNumTrafficStopsResist(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of traffic stops in career where resistance is encountered"
        self.name_of_features = ["career_ts_physresist"]
        self.query = ("select officer_id, count(distinct inc_key) as "
                      "career_ts_physresist from {} where physical_resist='Y' "
                      "and date_time_action <= '{}'::date "
                      " group by officer_id").format(
                      tables["stops_table"],
                      self.end_date)


class TrafficStopsSearch(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of times officer asks for "
                            "consent to search in traffic stop")
        self.feat_time_window = kwargs["feat_time_window"] * 365
        self.name_of_features = ["traffic_stops_search_{}days".format(
            self.feat_time_window)]
        self.start_date = self.end_date - datetime.timedelta(
            days=self.feat_time_window)
        self.query = ("select officer_id,count(*) as {} "
                      "from {} where consent_search='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class NumTrafficStopsResist(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.name_of_features = ["trafficstop_physresist_{}yr".format(
            int(self.feat_time_window/365))]
        self.description = "Number of traffic stops where resistance is encountered"
        self.query = ("select officer_id, count(distinct inc_key) as "
                      "{} from {} where physical_resist='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)

## Training

class ElectHoursTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of elective hours of training"
        self.name_of_features = ["elect_hrs_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} where rtyp_id = 'ELECTIVE' "
                      "and compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours of training"
        self.name_of_features = ["hrs_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursPhysFit(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours working out"
        self.name_of_features = ["hrs_physfit_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_typ_id = 'PHYS TEST' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursROCTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in Rules of Conduct training"
        self.name_of_features = ["hrs_roc_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Rules of Conduct%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursProfTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in profiling training"
        self.name_of_features = ["hrs_proftrain_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Profil%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursDomViolTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in domestic violence training"
        self.name_of_features = ["hrs_domestic_violence_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%DOMESTIC_VIOLENCE%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursMilitaryReturn(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in military training"
        self.name_of_features = ["hrs_military_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%ilitary%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursTaserTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in taser training"
        self.name_of_features = ["hrs_taser_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Taser%' "
                      "or cpnt_id like '%TASER%'"
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursBiasTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in bias training"
        self.name_of_features = ["hrs_bias_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Bias%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class HoursForceTrain(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of hours in use of force training"
        self.name_of_features = ["hrs_force_train_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(credit_hrs) as {},"
                      "officer_id from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Force%' "
                      " group by officer_id").format(
                      self.name_of_features[0],
                      tables["plateau_table"],
                      self.end_date, self.start_date)


## Districts and units

class CountDivision(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Normalized number of shifts spent in certain divisions"
        all_featnames, all_queries = [], []
        for division in tables["divisions"]:
            this_feature = "in_division_{}_{}yr".format(
            division, int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when div='{}' then 1 else null end)::float/"
            "(count(case when div is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE date_ln >= '{}'::date "
            "AND date_ln <= '{}'::date "
            "group by officer_id").format(division, this_feature,
            tables["logonoff"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class CountUnit(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Normalized number of shifts spent in certain unit types"
        all_featnames, all_queries = [], []
        for division in tables["units"]:
            this_feature = "in_unittype_{}_{}yr".format(
            division, int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when unityp='{}' then 1 else null end)::float/"
            "(count(case when unityp is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE date_ln >= '{}'::date "
            "AND date_ln <= '{}'::date "
            "group by officer_id").format(division, this_feature,
            tables["logonoff"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


## Internal Affairs allegations and investigations

weapons_use = ['Duty Weapon', 'Tackling', 'Elbow Strike',
               'Pressure Points', 'Baton', 'Pepper spray', 'Taser',
               'Knee Strike',  'Firearm', 'Hands/Fists']

dof_types = ['Property DamageObject', 'Non-Fatal InjuryAnimal',
             'No DamageObject', 'Non-Fatal InjuryPerson',
             'No DamageAnimal', 'MissPerson',
             'Property DamageGround', 'MissObject',
             'MissAnimal', 'No DamageGround',
             'Fatal InjuryAnimal']

directives = ['insub', 'driving', 'law_break', 'assoc', 'alcohol',
              'superv', 'neg_duty', 'bad_conduct', 'harass',
              'know_reg', 'courtesy', 'dept_eqp', 'uow', 'absence',
              'evidence', 'abuse_position', 'telephone', 'lying',
              'emp_outside', 'drugs',
              'id', 'uof', 'unsat_perf', 'intervention', 'profiling',
              'arrest_seiz', 'part_adm_inv',
              'viol_rules', 'dept_rep', 'radio']

event_types = ['Use Of Force', 'TDD', 'Complaint', 'Pursuit',
               'DOF', 'Raid And Search', 'Injury', 'NFSI', 'Accident']


class NormalizedCountsWeaponsUse(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Normalized counts of Weapons Use"
        all_featnames, all_queries = [], []
        for weapon in weapons_use:
            this_feature = "avg_{}_{}yr".format(
            weapon.replace(" ", "").lower().replace('/',''),
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, "
            "count(case when empweapons='{}' then 1 else null end)::float/"
            "(count(case when empweapons is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by officer_id").format(weapon, this_feature,
            tables["si_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class DOFTypeCounts(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Discharge of firearm allegations"
        all_featnames, all_queries = [], []
        for dof in dof_types:
            this_feature = "dofcount_{}_{}yr".format(
            dof.lower().replace(" ", "").replace("-", ""),
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, count(alleg_doflevel || "
            "alleg_doftype = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by officer_id").format(dof, this_feature,
            tables["si_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class DirectiveViolCounts(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Directive violations allegations"
        all_featnames, all_queries = [], []
        for directive in directives:
            this_feature = "directive_viol_count_{}_{}yr".format(
            directive.lower().replace(" ", ""),
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, count( "
            "rocdesc_cleaned = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by officer_id").format(directive, this_feature,
            tables["si_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class IAEventTypeCounts(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "IA event types"
        all_featnames, all_queries = [], []
        for event in event_types:
            this_feature = "ia_eventtype_{}_{}yr".format(
            event.lower().replace(" ", ""),
            int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, count( "
            "eventtype = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by officer_id").format(event, this_feature,
            tables["si_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class IARate(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Career IA rate of allegations"
        self.name_of_features = ["ia_rate_{}yr".format(int(self.feat_time_window/365))]
        self.query = ("select "
                      "a.officer_id, a.iacount/extract(day from '{date}' - b.startdate)*365 as {name} "
                      "from "
                      "(select officer_id, count(*) as iacount from {iatable} "
                      "where dateoccured < '{date}' and {si_bad} group by officer_id) as a "
                      "left join (select officer_id, greatest('2005-01-01'::timestamp,date_employed) "
                      "as startdate from {officers}) as b "
                      "on a.officer_id = b.officer_id").format(date=self.end_date,
                                                     name=self.name_of_features[0],
                                                     iatable=tables["si_table"],
                                                     si_bad=tables["si_bad_definition"],
                                                     officers=tables["officer_table"])


class CountPriorAdverse(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior adverse incidents"
        self.name_of_features = ["num_prior_adverse_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "officer_id from {} "
                      "where {} "
                      "AND eventtype != 'Accident' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition_all"], self.start_date,
                      self.end_date)


class CountPriorAccident(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior preventable accidents"
        self.name_of_features = ["num_prior_accidents_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "officer_id from {} "
                      "where {} "
                      "AND eventtype = 'Accident' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition_all"], self.start_date,
                      self.end_date)


class CountPriorFilteredAdverse(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior filtered adverse incidents"
        self.name_of_features = ["num_prior_filtered_adverse_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "officer_id from {} "
                      "where {} "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition"], self.start_date,
                      self.end_date)


class CountRocCOC(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of rules of conduct COC violations"
        self.name_of_features = ["num_roc_coc_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "officer_id from {} "
                      "where roc is not null and rlevel = 'COC' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountRocIA(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of rules of conduct IA violations"
        self.name_of_features = ["num_roc_ia_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "officer_id from {} "
                      "where roc is not null and rlevel = 'IA' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountPreventable(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of preventable allegations"
        self.name_of_features = ["num_preventable_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "officer_id from {} "
                      "where finalsidisposition = 'Preventable' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountUnjustified(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of unjustified allegations"
        self.name_of_features = ["num_unjustified_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "officer_id from {} "
                      "where finalsidisposition = 'Not Justified' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountSustainedComplaints(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of sustained complaints"
        self.name_of_features = ["num_sustained_complaints_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "officer_id from {} "
                      "where internaldisposition = 'Sustained' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by officer_id").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class IAConcerns(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Concerns from IA about officers"
        feat_prefixes = ['si_safety_concerns', 'si_comm_concerns', 'si_tactics_concerns']
        all_featnames = []
        for prefix in feat_prefixes:
            all_featnames.append('{}_{}yr'.format(prefix, int(self.feat_time_window/365)))
        self.name_of_features = all_featnames
        self.query = ("select sum(has_safety_concerns) as {x[0]}, "
                      "       sum(has_comm_concerns) as {x[1]}, "
                      "       sum(has_tactics_concerns) as {x[2]}, "
                      "officer_id from {table} "
                      "where internaldisposition = 'Sustained' "
                      "AND dateoccured >= '{date1}'::date "
                      "AND dateoccured <= '{date2}'::date "
                      "group by officer_id").format(
                      x=self.name_of_features, table=tables["si_table"],
                      date1=self.start_date, date2=self.end_date)


class SuspensionCounselingTime(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of suspended days, counseling, correctives written"
        feat_prefixes = ["active_susp_days", "inactive_susp_days",
                         "si_counseling", "si_corrective", "ever_susp",
                         "injury_count"]
        all_featnames = []
        for prefix in feat_prefixes:
            all_featnames.append('{}_{}yr'.format(prefix, int(self.feat_time_window/365)))
        self.name_of_features = all_featnames
        self.query = ("select sum(suspensionactive) as {x[0]}, "
                      "       sum(suspensioninactive) as {x[1]}, "
                      "       sum(counselling) as {x[2]}, "
                      "       sum(correctivewritten) as {x[3]}, "
                      "       (case when sum(suspensionactive)>0 or "
                      "sum(suspensioninactive)>0 then 1 else 0 end) as {x[4]}, "
                      "sum(case when injurydesc is not null then 1 else 0 end) as {x[5]}, "
                      "officer_id from {table} "
                      "WHERE dateoccured >= '{date1}'::date "
                      "AND dateoccured <= '{date2}'::date "
                      "group by officer_id").format(
                      x=self.name_of_features, table=tables["si_table"],
                      date1=self.start_date, date2=self.end_date)


## Neighborhood features

neighb_feats1 = ["Population_Density_2013", "Age_of_Residents_2013", "Black_Population_2010", "311_Calls_2013",
                 "Household_Income_2013", "Employment_Rate_2013", "Vacant_Land_Area_2013", "Voter_Participation_2012",
                 "Household_Income_Log_2013", "Vacant_Land_Area_Log_2013"]
neighb_feats2 = ["Age_of_Death_2012", "Housing_Density_2013", "Nuisance_Violations_Total_2013",
                 "Violent_Crime_Rate_2013", "Property_Crime_Rate_2013",
                 "Sidewalk_Availability_2013", "Foreclosures_2013", "Disorder_Call_Rate_Log_2013"]

class AvgNeighborhoodFeatures1(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Average neighborhood features where an officer makes their arrests"
        all_featnames, all_queries = [], []
        for each_feat in neighb_feats1:
            this_feature = "avg_{}_{}yr".format(
            each_feat.lower(), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, avg(\"{}\") as {} "
            "from (select officer_id, npa, arrest_date from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where arrest_date <= '{}'::date "
            "and arrest_date >= '{}'::date "
            "group by officer_id").format(each_feat, this_feature,
            tables["arrest_charges_table"], tables["city_neigh_1"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class AvgNeighborhoodFeatures2(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Average neighborhood features where an officer makes their arrests"
        all_featnames, all_queries = [], []
        for each_feat in neighb_feats2:
            this_feature = "avg_{}_{}yr".format(
            each_feat.lower(), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, avg(\"{}\") as {} "
            "from (select officer_id, npa, arrest_date from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where arrest_date <= '{}'::date "
            "and arrest_date >= '{}'::date "
            "group by officer_id").format(each_feat, this_feature,
            tables["arrest_charges_table"], tables["city_neigh_2"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries

### xtraduty

class ExtraDutyNeighborhoodFeatures1(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Average neighborhood features in extra duty areas"
        all_featnames, all_queries = [], []
        for each_feat in neighb_feats1:
            this_feature = "extraduty_avg_{}_{}yr".format(
            each_feat.lower(), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, avg(\"{}\") as {} "
            "from (select officer_id, npa, jobdate from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where jobdate <= '{}'::date "
            "and jobdate >= '{}'::date "
            "group by officer_id").format(each_feat, this_feature,
            tables["extra_duty_geocoded"], tables["city_neigh_1"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class ExtraDutyNeighborhoodFeatures2(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Average neighborhood features in extra duty areas"
        all_featnames, all_queries = [], []
        for each_feat in neighb_feats2:
            this_feature = "extraduty_avg_{}_{}yr".format(
            each_feat.lower(), int(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select officer_id, avg(\"{}\") as {} "
            "from (select officer_id, npa, jobdate from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where jobdate <= '{}'::date "
            "and jobdate >= '{}'::date "
            "group by officer_id").format(each_feat, this_feature,
            tables["extra_duty_geocoded"], tables["city_neigh_2"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class ExtraDutyHours(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Count of extra duty hours worked"
        self.name_of_features = ["extra_duty_hours_{}yr".format(
            int(self.feat_time_window/365))]
        self.query = ("select sum(extract(hour from corrected_actualendtime - corrected_actualstarttime)) "
                      "as {}, officer_id from {} "
                      "where (corrected_actualendtime - corrected_actualstarttime) > '0' "
                      "and jobdate <= '{}'::date "
                      "and jobdate >= '{}'::date "
                      "group by officer_id").format(self.name_of_features[0],
                      tables["extra_duty_table"],
                      self.end_date, self.start_date)
