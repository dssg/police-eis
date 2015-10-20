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

class OfficerHeightWeight(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = ("Officer height and weight, calculated as "
                            "an average across all SI cases involving "
                            "that officer.")
        self.num_features = 2
        self.name_of_features = ["weight", "height"]
        self.query = ("select newid, avg(weight_int) as avg_weight, "
                      "avg(height_inches_int) as avg_height_inches "
                      "from {} group by newid".format(tables['si_table']))


class OfficerEducation(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Officer education level"
        self.num_features = 12
        self.type_of_features = "categorical"
        self.name_of_features = [""]
        self.query = ("select newid, education_level_cleaned "
                      "from {}".format(tables['officer_table']))


class OfficerMaritalStatus(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.type_of_features = "categorical"
        self.description = "Marital status of officer"
        self.query = ("select newid, marital_status as "
                      "married from {}".format(tables['officer_table']))


class OfficerYrsExperience(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of years of experience for police officer"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["years_experience"]
        self.query = ("select newid, {} - EXTRACT(YEAR FROM "
                      "hire_date_employed) as "
                      "yrs_experience from {}".format(self.time_bound.year,
                                                      tables['officer_table']))


class OfficerDaysExperience(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of days of experience for police officer"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["days_experience"]
        self.query = ("select newid, EXTRACT('days' FROM '{}'::date - "
                      "hire_date_employed) as "
                      "days_experience from {}".format(
                          self.time_bound.strftime(time_format),
                          tables['officer_table']))


class OfficerMaleFemale(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Is officer male or female"
        self.query = ("select newid, empl_sex_clean as "
                      "male_female from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"


class OfficerRace(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Officer race"
        self.query = ("select newid, empl_race_cleaned as "
                      "race from {}".format(tables['officer_table']))
        self.type_of_features = "categorical"


class OfficerAge(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Officer age"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["age"]
        self.query = ("select newid, {} - birthdate_year "
                      "as age from {}".format(self.time_bound.year,
                                              tables['officer_table']))


class OfficerAgeAtHire(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Officer age at hire"
        self.name_of_features = ["age_at_hire"]
        self.query = ("select newid, extract(year from "
                      "hire_date_employed)-birthdate_year as "
                      "age_at_hire from {}".format(tables['officer_table']))


### Arrest History Features

class NumRecentArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of recent (<1yr) arrests for officer"
        self.name_of_features = ["1yr_arrest_count"]
        self.end_date = kwargs["time_bound"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select count(distinct aa_id) as year_arrest_count, "
                      "newid from {} "
                      "where arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class OfficerFractionMaleFemale(abstract.Feature):
    pass


class OfficerArrestTimeSeries(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Timeseries of arrest counts (1 yr agg) for officer"
        self.name_of_features = ["timeseries_arrests"]
        self.end_date = kwargs["time_bound"]
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



class ArrestRateDelta(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Delta recent (<1yr) arrest rate to career rate"
        self.name_of_features = ["delta_arrest_rate"]
        self.end_date = kwargs["time_bound"]
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


class DiscOnlyArrestsCount(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of career disc ONLY arrests for officer"
        self.name_of_features = ["disc_only_count"]
        self.end_date = kwargs["time_bound"]
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


class OfficerAvgAgeArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Average age of arrestees for officer"
        self.name_of_features = ["avg_age_arrestees"]
        self.end_date = kwargs["time_bound"]
        self.query = ("select avg(age) as avg_age_arrestees, newid "
                      "from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)


class OfficerAvgTimeOfDayArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Average arrest time of day for officer"
        self.name_of_features = ["tod_arrest"]
        self.end_date = kwargs["time_bound"]
        self.query = ("select avg(extract(hour from arrest_date)) "
                      "as arrest_avg_hour, newid from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)


class CareerDiscArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of career discretionary arrests for officer"
        self.name_of_features = ["career_disc_arrest_count"]
        self.end_date = kwargs["time_bound"]
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


class RecentDiscArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of recent discretionary arrests for officer"
        self.name_of_features = ["recent_disc_arrest_count"]
        self.end_date = kwargs["time_bound"]
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


class OfficerCareerArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of career arrests for officer"
        self.name_of_features = ["career_arrest_count"]
        self.end_date = kwargs["time_bound"]
        self.query = ("select count(distinct aa_id) as career_arrest_count, "
                      "newid from {} "
                      "where arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)


class CareerNPCArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of career NPC arrests for officer"
        self.name_of_features = ["career_npc_arrest_count"]
        self.end_date = kwargs["time_bound"]
        self.query = ("select count(distinct aa_id) as career_npc_count, "
                      "newid from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date)


class RecentNPCArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of recent NPC arrests for officer"
        self.name_of_features = ["recent_npc_arrest_count"]
        self.end_date = kwargs["time_bound"]
        self.start_date = kwargs["time_bound"] - datetime.timedelta(days=365)
        self.query = ("select count(distinct aa_id) as recent_npc_count, "
                      "newid from {} where magistrate_action_mlov = 'MA03' "
                      "and arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class ArrestCentroids(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Arrest Centroids"
        self.name_of_features = ["arrest_centroids"]
        self.end_date = kwargs["time_bound"]
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


### Citations

class CareerNPCCitations(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.end_date = kwargs["time_bound"]
        self.description = "Number of career NPC citations"
        self.name_of_features = ["career_npc_citations_count"]
        self.query = ("select newid,count(*) as career_cit_npc "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date)


class RecentNPCCitations(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.end_date = kwargs["time_bound"]
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


class CareerCitations(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.end_date = kwargs["time_bound"]
        self.description = "Number of career citations"
        self.name_of_features = ["career_citations_count"]
        self.query = ("select newid,count(*) as career_cit "
                      "from {} where type = 'NPC' "
                      "and datet <= '{}'::date "
                      " group by newid").format(
                      tables["citations_table"],
                      self.end_date)


class RecentCitations(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.end_date = kwargs["time_bound"]
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


### Internal Affairs

class IAHistory(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.time_bound = kwargs["time_bound"]
        self.num_features = 2
        self.type_of_features = "float"
        self.name_of_features = ["weight", "height"]
        self.query = ()
