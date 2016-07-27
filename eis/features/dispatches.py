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


### Basic Dispatch Features

class dummy_feature(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = ("Dummy feature for testing 2016 schema")
        self.query = ("SELECT "
                      "     dispatch_id, "
                      "     COUNT(event_type_code) AS feature_column "
                      "FROM staging.events_hub "
                      "WHERE event_type_code = 4 "
                      "GROUP BY dispatch_id")
        self.type_of_imputation = "mean"


class random_feature(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = ("Random feature for testing")
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   random() AS feature_column "
                        "FROM staging.events_hub "
                        "GROUP BY dispatch_id")
        self.type_of_imputation = "mean"


class division_assigned(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Division in which the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   unit_div as feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")
