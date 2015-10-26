#!/usr/bin/env python
import pdb
import logging
import yaml
import datetime

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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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


class CareerDiscArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career discretionary arrests for officer"
        self.name_of_features = ["career_disc_arrest_count"]
        self.query = ("select count(*) as career_disc_arrest_count, newid "
                      "from ( select count(*) as c, newid, string_agg("
                      "charge_desc::text, '    ') as charges from "
                      "{} where charge_desc is not null "
                      "and arrest_date <= '{}'::date "
                      "group by newid, aa_id) a "
                      "where c=1 and charges similar to "
                      "'%(DISORDERLY|RESIST|OBSTRUCT|DELAY)%' "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "zero"


class RecentDiscArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of recent discretionary arrests for officer"
        self.name_of_features = ["recent_disc_arrest_count"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select count(*) as recent_disc_arrest_count, newid "
                      "from ( select count(*) as c, newid, string_agg("
                      "charge_desc::text, '    ') as charges from "
                      "{} where charge_desc is not null "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid, aa_id) a "
                      "where c=1 and charges similar to "
                      "'%(DISORDERLY|RESIST|OBSTRUCT|DELAY)%' "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


class CareerNPCArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career NPC arrests for officer"
        self.name_of_features = ["career_npc_arrest_count"]
        self.query = ("select count(distinct aa_id) as career_npc_count, "
                      "newid from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)
        self.type_of_imputation = "zero"


class RecentNPCArrests(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of recent NPC arrests for officer"
        self.name_of_features = ["recent_npc_arrest_count"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select count(distinct aa_id) as recent_npc_count, "
                      "newid from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)
        self.type_of_imputation = "zero"


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

class CareerNPCCitations(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career NPC citations"
        self.name_of_features = ["career_npc_citations_count"]
        self.query = ("select newid,count(*) as career_cit_npc "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date)
        self.type_of_imputation = "zero"


class RecentNPCCitations(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of recent NPC citations"
        self.name_of_features = ["recent_npc_citations_count"]
        self.query = ("select newid,count(*) as recent_cit_npc "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date, self.start_date)
        self.type_of_imputation = "zero"


class CareerCitations(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of career citations"
        self.name_of_features = ["career_citations_count"]
        self.query = ("select newid,count(*) as career_cit "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date)
        self.type_of_imputation = "zero"


class RecentCitations(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of recent citations"
        self.name_of_features = ["recent_citations_count"]
        self.query = ("select newid,count(*) as recent_cit "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      "and datet >= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date, self.start_date)
        self.type_of_imputation = "zero"


## CAD

class CareerCADStatistics(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Career CAD Statistics"
        self.name_of_features = ['career_avg_seq_assgn',
                                 'career_avg_diff_arrv_assgn',
                                 'career_avg_travel_time',
                                 'career_std_travel_time',
                                 'career_avg_response_time',
                                 'career_avg_scene_time',
                                 'career_avg_prior_orig',
                                 'career_std_prior_orig',
                                 'career_avg_prior_fin',
                                 'career_std_prior_fin',
                                 'career_priority_diff']
        self.query = ("select avg(seq_assigned) as career_avg_seq_assgn, "
                      "avg(seq_arrived-seq_assigned) as career_avg_diff_arrv_assgn, "
                      "avg(travel_time) as career_avg_travel_time, "
                      "stddev(travel_time) as career_std_travel_time, "
                      "avg(response_time) as career_avg_response_time, "
                      "log(avg(at_scene_time)+1) as career_avg_scene_time, "
                      "avg(priority_org::int) as career_avg_prior_orig, "
                      "stddev(priority_org::int) as career_std_prior_orig, "
                      "avg(priority_fin::int) as career_avg_prior_fin, "
                      "stddev(priority_fin::int) as career_std_prior_fin, "
                      "avg(priority_org::int - priority_fin::int) as "
                      "career_priority_diff, newid from {} "
                      "where date_add <= '{}'::date "
                      "group by newid").format(tables["dispatch_table"],
                      self.end_date)
        self.type_of_imputation = "mean"


class RecentCADStatistics(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Recent CAD Statistics"
        self.name_of_features = ['recent_avg_seq_assgn',
                                 'recent_avg_diff_arrv_assgn',
                                 'recent_avg_travel_time',
                                 'recent_std_travel_time',
                                 'recent_avg_response_time',
                                 'recent_avg_scene_time',
                                 'recent_avg_prior_orig',
                                 'recent_std_prior_orig',
                                 'recent_avg_prior_fin',
                                 'recent_std_prior_fin',
                                 'recent_priority_diff']
        self.query = ("select avg(seq_assigned) as recent_avg_seq_assgn, "
                      "avg(seq_arrived-seq_assigned) as recent_avg_diff_arrv_assgn, "
                      "avg(travel_time) as recent_avg_travel_time, "
                      "stddev(travel_time) as recent_std_travel_time, "
                      "avg(response_time) as recent_avg_response_time, "
                      "log(avg(at_scene_time)+1) as recent_avg_scene_time, "
                      "avg(priority_org::int) as recent_avg_prior_orig, "
                      "stddev(priority_org::int) as recent_std_prior_orig, "
                      "avg(priority_fin::int) as recent_avg_prior_fin, "
                      "stddev(priority_fin::int) as recent_std_prior_fin, "
                      "avg(priority_org::int - priority_fin::int) as "
                      "recent_priority_diff, newid from {} "
                      "where date_add <= '{}'::date "
                      "and date_add >= '{}'::date "
                      "group by newid").format(tables["dispatch_table"],
                      self.end_date, self.start_date)
        self.type_of_imputation = "mean"


termination_types = ['CANCOMM', 'FTC', 'UNKNOWN', 'CANCCOMP', 'CANCOFC', 'DUPNCAN',
              'CANCALRM', 'ASSTUNIT', 'UL', 'CITE_NMV', 'CITE_MV', 'ACCIDENT', 'WARNING', 'OO/CASE',
              "MI"]


## EIS


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero" 


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
        self.type_of_imputation = "zero"


loiter_sleep_sit = """ (narrative like '%loiter%' or narrative like '%sleep%'
                      or narrative like '%sitting%' or narrative like '%walk%'
                      and narrative not like '%call for service%') """


class RecentLoiterFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description = "Number of field interviews of loiterers in last year"
        self.name_of_features = ["recent_fi_loiter_count"]
        self.query = ("select count(*) as recent_fi_loiter, newid "
                      "from {} where {} "
                      "and corrected_interview_date <= '{}'::date "
                      "and corrected_interview_date >= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"], loiter_sleep_sit,
                      self.end_date, self.start_date)
        self.type_of_imputation = "zero" 


class CareerLoiterFI(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of field interviews of loiterers in career"
        self.name_of_features = ["career_fi_loiter_count"]
        self.query = ("select count(*) as career_fi_loiter, newid "
                      "from {} where {} "
                      "and corrected_interview_date <= '{}'::date "
                      " group by newid").format(
                      tables["field_int_table"], loiter_sleep_sit,
                      self.end_date)
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


class FIAvgSuspectAge(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Average age of suspects in field interviews"
        self.name_of_features = ["avg_age_suspects_fi"]
        self.query = ("select avg(age) as fi_avg_age, newid from {} "
                      "group by newid").format(
                      tables["field_int_table"],
                      self.end_date)
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


## Traffic stops

class TrafficStopsSearch(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of times officer asks for "
                            "consent to search in traffic stop")
        self.type_of_imputation = "zero"
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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"            


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


class RecentNumTrafficStopsResist(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.description =  "Number of traffic stops in last year where resistance is encountered"
        self.name_of_features = ["recent_ts_physresist"]
        self.query = ("select newid, count(distinct inc_key) as "
                      "recent_ts_physresist from {} where physical_resist='Y' "
                      "and date_time_action <= '{}'::date "
                      "and date_time_action >= '{}'::date "
                      " group by newid").format(
                      tables["stops_table"],
                      self.end_date, self.start_date)
        self.type_of_imputation = "zero"

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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"


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
        self.type_of_imputation = "zero"

## Investigations


# Adverse investigations in last year
#         qinvest = ("SELECT newid, count(adverse_by_ourdef) from {} "
#                  "WHERE dateoccured >= '{}'::date "
#                  "AND dateoccured <= '{}'::date "
#                  "group by newid "
#                  ).format(self.tables["si_table"],
#                           self.start_date, self.end_date)
#       qadverse = ("SELECT newid, count(adverse_by_ourdef) from {} "
#                    "WHERE adverse_by_ourdef = 1 "
#                    "AND dateoccured >= '{}'::date "
#                    "AND dateoccured <= '{}'::date "
#                    "group by newid "
#                    ).format(self.tables["si_table"],
#                             self.start_date, self.end_date)

### Internal Affairs allegations

class IAHistory(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.time_bound = kwargs["time_bound"]
        self.num_features = 2
        self.type_of_features = "float"
        self.name_of_features = ["weight", "height"]
        self.query = ()
