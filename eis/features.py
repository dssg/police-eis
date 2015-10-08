#!/usr/bin/env python
import pdb
import logging 

log = logging.getLogger(__name__)


class Feature():

    def __init__(self):
        self.time_bound = False
        self.num_features = 1
        self.type_of_features = "float"
        self.start_date = None
        self.end_date = None
        self.query = None
        self.name_of_features = ""


class Personal(Feature):

    def __init__(self, time_bound):
        Feature.__init__(self)
        self.time_bound = time_bound
        self.num_features = 2
        self.type_of_features = "float"
        self.name_of_features = ["weight", "height"]
        self.query = ("select newid, avg(weight_int) as avg_weight, "
                      "avg(height_inches_int) as avg_height_inches "
                      "from dssg.allegations_master group by newid")

pdb.set_trace()
