#!/usr/bin/env python
import pdb
import logging
import yaml
import datetime
from math import ceil

from eis import setup_environment
from eis.features import abstract

log = logging.getLogger(__name__)
_, tables = setup_environment.get_database()
time_format = "%Y-%m-%d %X"


### Basic Officer Features

class OfficerHeightWeight(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer height and weight, calculated as "
                            "an average across all SI cases involving "
                            "that officer.")
        self.num_features = 2
        self.name_of_features = ["weight", "height"]
        self.query = ("select newid, avg(weight_int) as avg_weight, "
                      "avg(height_inches_int) as avg_height_inches "
                      "from {} group by newid".format(tables['si_table']))
        self.type_of_imputation = "mean"


class OfficerEducation(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer education level"
        self.num_features = 12
        self.type_of_features = "categorical"
        self.name_of_features = [""]
        self.query = ("select newid, education_level_cleaned "
                      "from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"


class OfficerMaritalStatus(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.type_of_features = "categorical"
        self.description = "Marital status of officer"
        self.query = ("select newid, marital_status as "
                      "married from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"


class OfficerYrsExperience(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of years of experience for police officer"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["years_experience"]
        self.query = ("select newid, {} - EXTRACT(YEAR FROM "
                      "hire_date_employed) as "
                      "yrs_experience from {}".format(self.time_bound.year,
                                                      tables['officer_table']))
        self.type_of_imputation = "mean"


class OfficerDaysExperience(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of days of experience for police officer"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["days_experience"]
        self.query = ("select newid, EXTRACT('days' FROM '{}'::date - "
                      "hire_date_employed) as "
                      "days_experience from {}".format(
                          self.time_bound.strftime(time_format),
                          tables['officer_table']))
        self.type_of_imputation = "mean"


class OfficerMaleFemale(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Is officer male or female"
        self.query = ("select newid, empl_sex_clean as "
                      "male_female from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"
        self.type_of_imputation = "mean"


class OfficerRace(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer race"
        self.query = ("select newid, empl_race_cleaned as "
                      "race from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"
        self.type_of_imputation = "mean"


class OfficerAge(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer age"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["age"]
        self.query = ("select newid, {} - birthdate_year "
                      "as age from {}".format(self.time_bound.year,
                                              tables['officer_table']))
        self.type_of_imputation = "mean"


class OfficerAgeAtHire(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Officer age at hire"
        self.name_of_features = ["age_at_hire"]
        self.query = ("select newid, extract(year from "
                      "hire_date_employed)-birthdate_year as "
                      "age_at_hire from {}".format(tables['officer_table']))
        self.type_of_imputation = "mean"

### Arrest History Features

class NumRecentArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of recent (<1yr) arrests for officer"
        self.name_of_features = ["1yr_arrest_count"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select count(distinct aa_id) as year_arrest_count, "
                      "newid from {} "
                      "where arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class OfficerFractionMaleFemale(abstract.OfficerFeature):
    pass


class OfficerArrestTimeSeries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Timeseries of arrest counts (1 yr agg) for officer"
        self.name_of_features = ["timeseries_arrests"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=2920)
        self.query = ("select newid, array_agg(intervals_count) as "
                      "arrest_timeseries from (select "
                      "a.newid as newid, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.arrcount) "
                      "as intervals_count from (select a.newid, "
                      "b.intervals, intervalid, 0 as default_count "
                      "from (select distinct newid "
                      "from {}) as a left join (select intervals, "
                      "row_number() over () as intervalid from ( "              
                      "select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, "
                      "'1 day') as d order by intervals) as foo) as b "
                      "on True) as a "
                      "left join (select newid, date_trunc('year',arrest_date) "
                      "as intervals, count(distinct aa_id)::int as arrcount "
                      "from {} group by newid, date_trunc('year',arrest_date)) "
                      "as b on a.newid = b.newid and a.intervals = b.intervals) "
                      "as koo group by newid").format(
                          tables["arrest_charges_table"], self.start_date, 
                          self.end_date, tables["arrest_charges_table"])
        self.type_of_features = "series"
        self.type_of_imputation = "mean"


class ArrestRateDelta(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Delta recent (<1yr) arrest rate to career rate"
        self.name_of_features = ["delta_arrest_rate"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select a.newid, a.career_rate, b.recent_rate, "
                      "b.recent_rate / a.career_rate as "
                      "recent_career_arrest_rate_delta from "
                      "(select a.newid, a.career_arrest_count / (extract(year "
                      "from coalesce(terminationdate, "
                      "'{}'::date)) - extract(year from b.hire_date) + 2) "
                      "as career_rate from "
                      "(select count(distinct aa_id) as career_arrest_count, "
                      "newid from {} where arrest_date <= '{}'::date "
                      "group by newid) as a "
                      "left join {} as b "
                      "on a.newid = b.newid) as a "
                      "left join (select count(distinct aa_id) "
                      "as recent_rate, newid from {} where "
                      "arrest_date <= '{}'::date and arrest_date >= "
                      "'{}'::date group by newid) as b "
                      "on a.newid = b.newid ").format(
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
        self.query = ("select newid, count(distinct aa_id) as disc_only_count "
                      "from ( select a.*, b.aa_id as aa_id2 from "
                      "( select aa_id, newid, arrest_date from {} "
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
                      "is null group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date,
                          tables["arrest_charges_table"],
                          self.end_date)


class OfficerAvgAgeArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average age of arrestees for officer"
        self.name_of_features = ["avg_age_arrestees"]
        self.query = ("select avg(age) as avg_age_arrestees, newid "
                      "from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "mean"


class OfficerAvgTimeOfDayArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average arrest time of day for officer"
        self.name_of_features = ["tod_arrest"]
        self.query = ("select avg(extract(hour from arrest_date)) "
                      "as arrest_avg_hour, newid from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "mean"


class DiscArrests(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of discretionary arrests for officer"
        self.name_of_features = ["disc_arrest_count_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, newid "
                      "from ( select count(*) as c, newid, string_agg("
                      "charge_desc::text, '    ') as charges from "
                      "{} where charge_desc is not null "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid, aa_id) a "
                      "where c=1 and charges similar to "
                      "'%(DISORDERLY|RESIST|OBSTRUCT|DELAY)%' "
                      "group by newid").format(
                          self.name_of_features[0],
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class OfficerCareerArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career arrests for officer"
        self.name_of_features = ["career_arrest_count"]
        self.query = ("select count(distinct aa_id) as career_arrest_count, "
                      "newid from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)


class NPCArrests(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of no probable cause arrests for officer"
        self.name_of_features = ["npc_arrest_count_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(distinct aa_id) as {}, "
                      "newid from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          self.name_of_features[0],
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class ArrestCentroids(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Arrest Centroids"
        self.name_of_features = ["arrest_centroids"]
        self.type_of_features = "categorical"
        self.query = ("select a.newid, b.subbeat "
                      "from( select *, st_setsrid(st_makepoint( "
                      "cent_long,cent_lat), 4326) as point "
                      "from( "
                      "select avg(long) as cent_long, avg(lat) as "
                      "cent_lat, newid "
                      "from( select distinct on (aa_id) "
                      "aa_id, arrest_date, newid, lat, long "
                      "from {} ) as foo "
                      "where arrest_date <= '{}'::date "
                      "group by newid ) as doo ) as a "
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
            ceil(self.feat_time_window/365))]
        self.query = ("select newid,count(*) as {} "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by newid").format(
                      self.name_of_features[0],
                      tables["citations_table"],
                      self.end_date, self.start_date)


class Citations(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of citations"
        self.name_of_features = ["citations_count_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select newid,count(*) as {} "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by newid").format(
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
            all_featnames.append('{}_{}yr'.format(prefix, ceil(self.feat_time_window/365)))
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
                      "{x[10]}, newid from {table} "
                      "where date_add <= '{date1}'::date "
                      "and date_add >= '{date2}'::date "
                      "group by newid").format(x=self.name_of_features,
                      table=tables["dispatch_table"],
                      date1=self.end_date, date2=self.start_date)
        self.type_of_imputation = "mean"


termination_types = ['CANCOMM', 'FTC', 'UNKNOWN', 'CANCCOMP', 'CANCOFC', 'DUPNCAN',
              'CANCALRM', 'ASSTUNIT', 'UL', 'CITE_NMV', 'CITE_MV', 'ACCIDENT', 'WARNING', 'OO/CASE',
              "MI"]


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
            ceil(self.feat_time_window/365))]
        self.query = ("select count(distinct eisno) as {}, newid "
                      "from {} where "
                      "datecreated <= '{}'::date " 
                      "and datecreated >= '{}'::date "
                      "group by newid").format(self.name_of_features[0],
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when eventtype LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when eventtype is not null then 1 else null end)+1) as {} "
            "from (select distinct eisno, eventtype, newid, datecreated from {}) as a "
            "where datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by newid").format(warning_type, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(distinct eisno) as {} "
            "from {} "
            "where eventtype = '{}' "
            "and datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by newid").format(this_feature, tables["eis_table"],
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when intervention LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when intervention is not null then 1 else null end)+1) as {} "
            "from (select distinct eisno, intervention, newid, datecreated from {}) as a "
            "where datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by newid").format(intervention_type, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(distinct eisno) as {} "
            "from {} "
            "where intervention = '{}' "
            "and datecreated <= '{}'::date "
            "and datecreated >= '{}'::date "
            "group by newid").format(this_feature, tables["eis_table"],
            intervention_type, self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries

## Field interviews

class CareerFICount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews in career"
        self.name_of_features = ["career_fi_count"]
        self.query = ("select count(*) as all_fi_count, newid "
                      "from {} "
                      "where corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class RecentFICount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of field interviews in last year"
        self.name_of_features = ["recent_fi_count"]
        self.query = ("select count(*) as recent_fi_count, newid "
                      "from {} "
                      "where corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date, self.start_date)


class CareerNonTrafficFICount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of non-traffic field interviews in career"
        self.name_of_features = ["career_fi_nontraffic_count"]
        self.query = ("select count(*) as career_fi_count_nontraffic, newid "
                      "from {} where traffic_stop_yn = 'N' "
                      "and corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class RecentNonTrafficFICount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of non-traffic field interviews in last year"
        self.name_of_features = ["recent_fi_nontraffic_count"]
        self.query = ("select count(*) as recent_fi_count_nontraffic, newid "
                      "from {} where traffic_stop_yn = 'N' "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date, self.start_date)


class RecentHighCrimeAreaFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of field interviews in last year in high crime area"
        self.name_of_features = ["recent_fi_highcrime_count"]
        self.query = ("select count(*) as recent_fi_count_highcrime, newid "
                      "from {} where narrative like '%high crime area%' "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date, self.start_date)


class CareerHighCrimeAreaFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews in career in high crime area"
        self.name_of_features = ["career_fi_highcrime_count"]
        self.query = ("select count(*) as career_fi_count_highcrime, newid "
                      "from {} where narrative like '%high crime area%' "
                      "and corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


loiter_sleep_sit = """ (narrative like '%loiter%' or narrative like '%sleep%'
                      or narrative like '%sitting%' or narrative like '%walk%'
                      and narrative not like '%call for service%') """


class LoiterFI(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews of loiterers"
        self.name_of_features = ["fi_loiter_count_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, newid "
                      "from {} where {} "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by newid").format(
                      self.name_of_features[0],
                      tables["field_int_table"], loiter_sleep_sit,
                      self.end_date, self.start_date)


class CareerBlackFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Fraction of field interviews where the suspect is black"
        self.name_of_features = ["career_frac_black_suspects"]
        self.query = ("select newid, "
                      "(SUM(CASE WHEN rac_code = 'B' then 1 else null end))::float/"
                      "(count(*) + 1) as fi_prop_black from {} "
                      "where corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class CareerWhiteFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Fraction of field interviews where the suspect is white"
        self.name_of_features = ["career_frac_white_suspects"]
        self.query = ("select newid, "
                      "(SUM(CASE WHEN rac_code = 'W' then 1 else null end))::float/"
                      "(count(*) + 1) as fi_prop_white from {} "
                      "where corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class FIAvgSuspectAge(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average age of suspects in field interviews"
        self.name_of_features = ["avg_age_suspects_fi"]
        self.query = ("select avg(age) as fi_avg_age, newid from {} "
                      "group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class FIAvgTimeOfDay(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average time of day for field interviews"
        self.name_of_features = ["avg_tod_fi"]
        self.query = ("select avg(extract(hour from corrected_interview_date)) "
                      "as fi_avg_hour, newid from {} "
                      "group by newid").format(
                      tables["field_int_table"],
                      self.end_date)


class FITimeseries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=2920)
        self.type_of_features = "series"
        self.description = "Timeseries for interviews"
        self.name_of_features = ["fi_timeseries"]
        self.query = ("select newid, array_agg(intervals_count) as fi_timeseries "
                      "from ( select a.newid as newid, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.ficount) as intervals_count "
                      "from (select a.newid, b.intervals, intervalid, "
                      "0 as default_count from "
                      "(select distinct newid from {}) as a "
                      "left join (select intervals, row_number() over () as intervalid "
                      "from ( select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, '1 day') "
                      "as d order by intervals) as foo) as b on True) as a "
                      "left join (select newid, date_trunc('year',corrected_interview_date) "
                      "as intervals, count(*)::int as ficount from {} "
                      "group by newid, date_trunc('year',corrected_interview_date)) as b "
                      "on a.newid = b.newid and a.intervals = b.intervals) as koo "
                      "group by newid").format(
                      tables["field_int_table"], self.start_date,
                      self.end_date, tables["field_int_table"])
        self.type_of_imputation = "mean"


## Incidents

class YearNumSuicides(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of suicides in last year"
        self.name_of_features = ["suicides_count"]
        self.query = ("select newid,count(distinct inc_id) as num_suicide "
                      "from {} where ucr_desc = 'Suicide' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumJuvenileVictim(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of juvenile victims in last year"
        self.name_of_features = ["juvenile_count"]
        self.query = ("select newid,count(distinct inc_id) as num_juv_victim "
                      "from {} where victim1_age_int < 16 "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumDomesticViolence(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of domestic violence incidents in last year"
        self.name_of_features = ["domestic_violence_count"]
        self.query = ("select newid,count(distinct inc_id) as num_domestic_violence "
                      "from {} where domestic_violence_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumHate(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hate incidents in last year"
        self.name_of_features = ["hate_count"]
        self.query = ("select newid,count(distinct inc_id) as num_hate "
                      "from {} where bias_hate_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumNarcotics(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of narcotics incidents in last year"
        self.name_of_features = ["narcotics_count"]
        self.query = ("select newid,count(distinct inc_id) as num_narcotics "
                      "from {} where narcotics_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumGang(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of gang incidents in last year"
        self.name_of_features = ["gang_count"]
        self.query = ("select newid,count(distinct inc_id) as num_gang "
                      "from {} where gang_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumGunKnife(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of gun/knife incidents in last year"
        self.name_of_features = ["gun_knife_count"]
        self.query = ("select newid,count(distinct inc_id) as num_guns "
                      "from {} where gang_flag = 'Y' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class YearNumPersWeaps(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of personal weapons incidents in last year"
        self.name_of_features = ["personal_weapon_count"]
        self.query = ("select newid,count(distinct inc_id) as num_weapons "
                      "from {} where weapon_type_code = 'G' "
                      "and date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class AvgAgeVictims(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Average age of victims in last year"
        self.name_of_features = ["avg_victim_age"]
        self.query = ("select avg(cast(victim1_age as float)), newid as victim_age,"
                      "newid from {} "
                      "where date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
                      tables["incidents_table"],
                      self.end_date, self.start_date)


class MinAgeVictims(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Min age of victims in last year"
        self.name_of_features = ["min_victim_age"]
        self.query = ("select avg(cast(victim1_age as float)) as min_victim_age,"
                      "newid from {} "
                      "where date_incident_began <= '{}'::date "
                      "and date_incident_began >= '{}'::date "
                      " group by newid").format(
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
            ceil(self.feat_time_window/365))]
        self.query = ("select newid,count(*) as {} "
                      "from {} where consent_search='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by newid").format(
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
            reason.replace(" ", ""), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when search_reason LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when search_reason is not null then 1 else null end)+1) as {} "
            "from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by newid").format(reason, this_feature,
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
            reason.replace(" ", ""), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when runtag_reason LIKE '%{}%' then 1 else null end)"
            "::float/(count(case when runtag_reason is not null then 1 else null end)+1) as {} "
            "from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by newid").format(reason, this_feature,
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
            reason.replace(" ", ""), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when resultofstop='{}' then 1 else null end)::float/"
            "(count(*)+1) as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by newid").format(reason, this_feature,
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
            race.replace(" ", ""), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when race = '{}' then 1 else null end)"
            "::float/(count(case when race is not null then 1 else null end)+1) "
            "as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by newid").format(race, this_feature,
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
            gender.replace(" ", ""), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when race = '{}' then 1 else null end)"
            "::float/(count(case when gender is not null then 1 else null end)+1) "
            "as {} from {} "
            "where date_time_action <= '{}'::date "
            "and date_time_action >= '{}'::date "
            "group by newid").format(gender, this_feature,
            tables["stops_table"], self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries


class TrafficStopTimeSeries(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Timeseries of traffic stop counts (1 yr agg) for officer"
        self.name_of_features = ["timeseries_trafficstops"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=2920)
        self.query = ("select newid, array_agg(intervals_count) as "
                      "trf_stop_timeseries from (select "
                      "a.newid as newid, a.intervals as intervals, "
                      "intervalid, greatest(a.default_count,b.trfcount) "
                      "as intervals_count from (select a.newid, "
                      "b.intervals, intervalid, 0 as default_count "
                      "from (select distinct newid "
                      "from {}) as a left join (select intervals, "
                      "row_number() over () as intervalid from ( "              
                      "select distinct date_trunc('year',d) as intervals "
                      "from generate_series('{}'::timestamp, '{}'::timestamp, "
                      "'1 day') as d order by intervals) as foo) as b "
                      "on True) as a "
                      "left join (select newid, date_trunc('year',date_time_action) "
                      "as intervals, count(distinct inc_key) as trfcount "
                      "from {} group by newid, date_trunc('year',date_time_action)) "
                      "as b on a.newid = b.newid and a.intervals = b.intervals) "
                      "as koo group by newid").format(
                          tables["officer_table"], self.start_date, 
                          self.end_date, tables["stops_table"])
        self.type_of_features = "series"
        self.type_of_imputation = "mean"


class CareerNumTrafficStops(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of traffic stops in career"
        self.name_of_features = ["career_num_traffic_stops"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "career_trf_count from {} "
                      "and date_time_action <= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date)


class RecentNumTrafficStops(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description =  "Number of traffic stops in last year"
        self.name_of_features = ["recent_num_traffic_stops"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "recent_trf_count from {} "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date, self.start_date)


class CareerNumTStopRunTagUOFOrArrest(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description =  ("Number of traffic stops in career where the tag was "
                            "run and then force was used or an arrest was made")
        self.name_of_features = ["career_num_traffic_stops"]
        self.query = ("select newid, count(*) as career_ts_uof_or_arrest "
                      "from {} where run_tag='Y' and (uof='Y' or "
                      "arrest_driver='Y' or arrest_pass='Y') "
                      "and date_time_action <= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date)


class RecentNumTStopRunTagUOFOrArrest(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description =  ("Number of traffic stops in last year where the tag was "
                            "run and then force was used or an arrest was made")
        self.name_of_features = ["recent_num_traffic_stops"]
        self.query = ("select newid, count(*) as recent_ts_uof_or_arrest "
                      "from {} where run_tag='Y' and (uof='Y' or "
                      "arrest_driver='Y' or arrest_pass='Y') "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date, self.start_date)


class CareerNumTrafficStopsForce(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of traffic stops in career where force is used"
        self.name_of_features = ["career_num_traffic_stops_uof"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "career_trf_uof_count from {} where uof='Y' "
                      "and date_time_action <= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date)


class RecentNumTrafficStopsForce(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description =  "Number of traffic stops in last year where force is used"
        self.name_of_features = ["recent_num_traffic_stops_uof"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "recent_trf_uof_count from {} where uof='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date, self.start_date)


class CareerTSPercBlackDayNight(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Ratio of times officer stops a black person at night vs during the day in career"
        self.name_of_features = ["career_ratio_bl_night_day"]
        self.query = ("select newid, "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 1 else 0 end )+1)::float/ "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 0 else 1 end )+1) as career_ratio_bl_night_day "
                      "from {} where race='B' "
                      "and date_time_action <= '{}'::date "
                      "group by newid").format(
                      tables["stops_table"],
                      self.end_date)


class RecentTSPercBlackDayNight(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Ratio of times officer stops a black person at night vs during the day in last year"
        self.name_of_features = ["recent_ratio_bl_night_day"]
        self.query = ("select newid, "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 1 else 0 end )+1)::float/ "
                      "(SUM(CASE WHEN (extract(hour from date_time_action) "
                      "> 21 or extract(hour from date_time_action) < 5 ) "
                      "then 0 else 1 end )+1) as recent_ratio_bl_night_day "
                      "from {} where race='B' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by newid").format(
                      tables["stops_table"],
                      self.end_date, self.start_date)


class CareerNumTrafficStopsResist(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of traffic stops in career where resistance is encountered"
        self.name_of_features = ["career_ts_physresist"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "career_ts_physresist from {} where physical_resist='Y' "
                      "and date_time_action <= '{}'::date "
                      " group by newid").format(
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
        self.query = ("select newid,count(*) as {} "
                      "from {} where consent_search='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)


class NumTrafficStopsResist(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.name_of_features = ["trafficstop_physresist_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.description = "Number of traffic stops where resistance is encountered"
        self.query = ("select newid, count(distinct inc_key) as "
                      "{} from {} where physical_resist='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by newid").format(
                      self.name_of_features[0],
                      tables["stops_table"],
                      self.end_date, self.start_date)

## Training

class CareerElectHoursTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of elective hours of training"
        self.name_of_features = ["career_elect_hrs_train"]
        self.query = ("select sum(credit_hrs) as career_elec_training_hrs,"
                      "newid from {} where rtyp_id = 'ELECTIVE' "
                      "and compl_dte <= '{}'::date "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentElectHoursTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of elective hours of training in last year"
        self.name_of_features = ["recent_elect_hrs_train"]
        self.query = ("select sum(credit_hrs) as recent_elec_training_hrs,"
                      "newid from {} where rtyp_id = 'ELECTIVE' "
                      "and compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours of training in career"
        self.name_of_features = ["career_hrs_train"]
        self.query = ("select sum(credit_hrs) as career_training_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours of training in last year"
        self.name_of_features = ["recent_hrs_train"]
        self.query = ("select sum(credit_hrs) as recent_training_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursPhysFit(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours working out in career"
        self.name_of_features = ["career_hrs_physfit"]
        self.query = ("select sum(credit_hrs) as career_workout_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_typ_id = 'PHYS TEST' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursPhysFit(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours working out in last year"
        self.name_of_features = ["recent_hrs_physfit"]
        self.query = ("select sum(credit_hrs) as recent_workout_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_typ_id = 'PHYS TEST' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursROCTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in Rules of Conduct training in career"
        self.name_of_features = ["career_hrs_roc"]
        self.query = ("select sum(credit_hrs) as career_roc_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%Rules of Conduct%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursROCTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in Rules of Conduct training in last year"
        self.name_of_features = ["recent_hrs_roc"]
        self.query = ("select sum(credit_hrs) as recent_roc_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Rules of Conduct%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursProfTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in profiling training in career"
        self.name_of_features = ["career_hrs_proftrain"]
        self.query = ("select sum(credit_hrs) as career_proftrain_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%Profil%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursProfTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in profiling training in last year"
        self.name_of_features = ["recent_hrs_proftrain"]
        self.query = ("select sum(credit_hrs) as recent_proftrain_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Profil%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursDomViolTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in domestic violence training in career"
        self.name_of_features = ["career_hrs_proftrain"]
        self.query = ("select sum(credit_hrs) as career_domvioltrain_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%DOMESTIC_VIOLENCE%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursDomViolTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in domestic violence training in last year"
        self.name_of_features = ["recent_hrs_domestic_violence_train"]
        self.query = ("select sum(credit_hrs) as recent_domvioltrain_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%DOMESTIC_VIOLENCE%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursMilitaryReturn(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in military training in career"
        self.name_of_features = ["career_hrs_military_train"]
        self.query = ("select sum(credit_hrs) as career_military_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%ilitary%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursMilitaryReturn(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in military training in last year"
        self.name_of_features = ["recent_hrs_military_train"]
        self.query = ("select sum(credit_hrs) as recent_military_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%ilitary%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursTaserTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in taser training in career"
        self.name_of_features = ["career_hrs_taser_train"]
        self.query = ("select sum(credit_hrs) as career_taser_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%Taser%' "
                      "or cpnt_id like '%TASER%'"
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursTaserTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in taser training in last year"
        self.name_of_features = ["recent_hrs_taser_train"]
        self.query = ("select sum(credit_hrs) as recent_taser_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Taser%' "
                      "or cpnt_id like '%TASER%'"
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursBiasTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in bias training in career"
        self.name_of_features = ["career_hrs_bias_train"]
        self.query = ("select sum(credit_hrs) as career_bias_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%Bias%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursBiasTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in bias training in last year"
        self.name_of_features = ["recent_hrs_bias_train"]
        self.query = ("select sum(credit_hrs) as recent_bias_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Bias%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date, self.start_date)


class CareerHoursForceTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of hours in use of force training in career"
        self.name_of_features = ["career_hrs_force_train"]
        self.query = ("select sum(credit_hrs) as career_force_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and cpnt_id like '%Force%' "
                      " group by newid").format(
                      tables["plateau_table"],
                      self.end_date)


class RecentHoursForceTrain(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of hours in use of force training in last year"
        self.name_of_features = ["recent_hrs_force_train"]
        self.query = ("select sum(credit_hrs) as recent_force_hrs,"
                      "newid from {} "
                      "where compl_dte <= '{}'::date "
                      "and compl_dte >= '{}'::date "
                      "and cpnt_id like '%Force%' "
                      " group by newid").format(
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
            division, ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when div='{}' then 1 else null end)::float/"
            "(count(case when div is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE date_ln >= '{}'::date "
            "AND date_ln <= '{}'::date "
            "group by newid").format(division, this_feature,
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
            division, ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when unityp='{}' then 1 else null end)::float/"
            "(count(case when unityp is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE date_ln >= '{}'::date "
            "AND date_ln <= '{}'::date "
            "group by newid").format(division, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, "
            "count(case when empweapons='{}' then 1 else null end)::float/"
            "(count(case when empweapons is not null then 1 else null end)+1) as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by newid").format(weapon, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, count(alleg_doflevel || "
            "alleg_doftype = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by newid").format(dof, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, count( "
            "rocdesc_cleaned = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by newid").format(directive, this_feature,
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
            ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, count( "
            "eventtype = '{}')::float as {} "
            "from {} "
            "WHERE dateoccured >= '{}'::date "
            "AND dateoccured <= '{}'::date "
            "group by newid").format(event, this_feature,
            tables["si_table"],
            self.end_date, self.start_date)
            all_queries.append(this_query)
        self.name_of_features = all_featnames
        self.query = all_queries




class IARate(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Career IA rate of allegations"
        self.name_of_features = ["ia_rate_{}yr".format(ceil(self.feat_time_window/365))]
        self.query = ("select "
                      "a.newid, a.iacount/extract(day from '{date}' - b.startdate)*365 as {name} "
                      "from "
                      "(select newid, count(*) as iacount from {iatable} "
                      "where dateoccured < '{date}' and {si_bad} group by newid) as a "
                      "left join (select newid, greatest('2005-01-01'::timestamp,date_employed) "
                      "as startdate from {officers}) as b "
                      "on a.newid = b.newid").format(date=self.end_date, 
                                                     name=self.name_of_features[0],
                                                     iatable=tables["si_table"],
                                                     si_bad=tables["si_bad_definition"],
                                                     officers=tables["officer_table"])


class CountPriorAdverse(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior adverse incidents"
        self.name_of_features = ["num_prior_adverse_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "newid from {} "
                      "where {} "
                      "AND eventtype != 'Accident' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition_all"], self.start_date,
                      self.end_date)


class CountPriorAccident(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior preventable accidents"
        self.name_of_features = ["num_prior_accidents_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "newid from {} "
                      "where {} "
                      "AND eventtype = 'Accident' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition_all"], self.start_date,
                      self.end_date)


class CountPriorFilteredAdverse(abstract.OfficerTimeBoundedFeature):
    def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of prior filtered adverse incidents"
        self.name_of_features = ["num_prior_filtered_adverse_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(distinct silogno) as {}, "
                      "newid from {} "
                      "where {} "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      tables["si_bad_definition"], self.start_date,
                      self.end_date)


class CountRocCOC(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of rules of conduct COC violations"
        self.name_of_features = ["num_roc_coc_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "newid from {} "
                      "where roc is not null and rlevel = 'COC' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountRocIA(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of rules of conduct IA violations"
        self.name_of_features = ["num_roc_ia_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "newid from {} "
                      "where roc is not null and rlevel = 'IA' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountPreventable(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of preventable allegations"
        self.name_of_features = ["num_preventable_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "newid from {} "
                      "where finalsidisposition = 'Preventable' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountUnjustified(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of unjustified allegations"
        self.name_of_features = ["num_unjustified_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "newid from {} "
                      "where finalsidisposition = 'Not Justified' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class CountSustainedComplaints(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Number of sustained complaints"
        self.name_of_features = ["num_sustained_complaints_{}yr".format(
            ceil(self.feat_time_window/365))]
        self.query = ("select count(*) as {}, "
                      "newid from {} "
                      "where internaldisposition = 'Sustained' "
                      "AND dateoccured >= '{}'::date "
                      "AND dateoccured <= '{}'::date "
                      "group by newid").format(
                      self.name_of_features[0], tables["si_table"],
                      self.start_date, self.end_date)


class IAConcerns(abstract.OfficerTimeBoundedFeature):
   def __init__(self, **kwargs):
        abstract.OfficerTimeBoundedFeature.__init__(self, **kwargs)
        self.description = "Concerns from IA about officers"
        feat_prefixes = ['si_safety_concerns', 'si_comm_concerns', 'si_tactics_concerns']
        all_featnames = []
        for prefix in feat_prefixes:
            all_featnames.append('{}_{}yr'.format(prefix, ceil(self.feat_time_window/365)))
        self.name_of_features = all_featnames
        self.query = ("select sum(has_safety_concerns) as {x[0]}, "
                      "       sum(has_comm_concerns) as {x[1]}, "
                      "       sum(has_tactics_concerns) as {x[2]}, "
                      "newid from {table} "
                      "where internaldisposition = 'Sustained' "
                      "AND dateoccured >= '{date1}'::date "
                      "AND dateoccured <= '{date2}'::date "
                      "group by newid").format(
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
            all_featnames.append('{}_{}yr'.format(prefix, ceil(self.feat_time_window/365)))
        self.name_of_features = all_featnames
        self.query = ("select sum(suspensionactive) as {x[0]}, "
                      "       sum(suspensioninactive) as {x[1]}, "
                      "       sum(counselling) as {x[2]}, "
                      "       sum(correctivewritten) as {x[3]}, "
                      "       (case when sum(suspensionactive)>0 or "
                      "sum(suspensioninactive)>0 then 1 else 0 end) as {x[4]}, "
                      "sum(case when injurydesc is not null then 1 else 0 end) as {x[5]}, "
                      "newid from {table} "
                      "WHERE dateoccured >= '{date1}'::date "
                      "AND dateoccured <= '{date2}'::date "
                      "group by newid").format(
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
            each_feat.lower(), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, avg(\"{}\") as {} "
            "from (select newid, npa, arrest_date from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where arrest_date <= '{}'::date "
            "and arrest_date >= '{}'::date "
            "group by newid").format(each_feat, this_feature,
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
            each_feat.lower(), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, avg(\"{}\") as {} "
            "from (select newid, npa, arrest_date from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where arrest_date <= '{}'::date "
            "and arrest_date >= '{}'::date "
            "group by newid").format(each_feat, this_feature,
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
            each_feat.lower(), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, avg(\"{}\") as {} "
            "from (select newid, npa, jobdate from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where jobdate <= '{}'::date "
            "and jobdate >= '{}'::date "
            "group by newid").format(each_feat, this_feature,
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
            each_feat.lower(), ceil(self.feat_time_window/365))
            all_featnames.append(this_feature)
            this_query = ("select newid, avg(\"{}\") as {} "
            "from (select newid, npa, jobdate from {}) a "
            "left join "
            "(select * from {}) b "
            "on a.npa = b.\"NPA\" "
            "where jobdate <= '{}'::date "
            "and jobdate >= '{}'::date "
            "group by newid").format(each_feat, this_feature,
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
            ceil(self.feat_time_window/365))]
        self.query = ("select sum(extract(hour from corrected_actualendtime - corrected_actualstarttime)) "
                      "as {}, newid from {} "
                      "where (corrected_actualendtime - corrected_actualstarttime) > '0' "
                      "and jobdate <= '{}'::date " 
                      "and jobdate >= '{}'::date "
                      "group by newid").format(self.name_of_features[0],
                      tables["extra_duty_table"],
                      self.end_date, self.start_date)

