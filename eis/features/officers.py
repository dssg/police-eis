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


class OfficerArrestFracMale(abstract.Feature):
    pass


class OfficerArrestsTimeSeries(abstract.Feature):
    pass


class OfficerCareerArrests(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.description = "Number of career arrests for officer"
        self.name_of_features = ["career_arrest_count"]
        self.start_date = "1970-01-01"
        self.end_date = kwargs["time_bound"]
        self.query = ("select count(distinct aa_id) as career_arrest_count, "
                      "newid from {} "
                      "where arrest_date <= '{}'::date "
                      "and arrest_date >= '{}'::date "
                      "group by newid").format(
                          tables["arrest_charges_table"],
                          self.end_date, self.start_date)


class IAHistory(abstract.Feature):
    def __init__(self, **kwargs):
        abstract.Feature.__init__(self, **kwargs)
        self.time_bound = kwargs["time_bound"]
        self.num_features = 2
        self.type_of_features = "float"
        self.name_of_features = ["weight", "height"]
        self.query = ()
