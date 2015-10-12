#!/usr/bin/env python
import pdb
import logging
import yaml

from eis import setup_environment

log = logging.getLogger(__name__)
_, tables = setup_environment.get_database()


class Feature():
    def __init__(self, **kwargs):
        self.description = ""
        self.time_bound = None
        self.num_features = 1
        self.type_of_features = "float"
        self.start_date = None
        self.end_date = None
        self.query = None
        self.name_of_features = ""


class OfficerHeightWeight(Feature):
    def __init__(self, **kwargs):
        Feature.__init__(self, **kwargs)
        self.description = ("Officer height and weight, calculated as "
                            "an average across all SI cases involving "
                            "that officer.")
        self.num_features = 2
        self.name_of_features = ["weight", "height"]
        self.query = ("select newid, avg(weight_int) as avg_weight, "
                      "avg(height_inches_int) as avg_height_inches "
                      "from {} group by newid".format(tables['si_table']))


class OfficerEducation(Feature):
    def __init__(self, **kwargs):
        Feature.__init__(self, **kwargs)
        self.description = "Officer education level"
        self.num_features = 12
        self.type_of_features = "categorical"
        self.name_of_features = [""]
        self.query = ("select newid, education_level_cleaned "
                      "from {}".format(tables['officer_table']))


class IAHistory(Feature):
    def __init__(self, **kwargs):
        Feature.__init__(self, **kwargs)
        self.time_bound = kwargs["time_bound"]
        self.num_features = 2
        self.type_of_features = "float"
        self.name_of_features = ["weight", "height"]
        self.query = ()


class OfficerYearsExperience(Feature):
    def __init__(self, **kwargs):
        Feature.__init__(self, **kwargs)
        self.description = "Number of years of experience for police officer"
        self.time_bound = kwargs["time_bound"]
        self.name_of_features = ["years_experience"]
        self.query = ("select newid, {} - EXTRACT(YEAR FROM "
                      "hire_date_employed) as "
                      "yrs_experience from {}".format(self.time_bound.year,
                                                      tables['officer_table']))

