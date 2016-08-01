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

### LABEL


class Label(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Binary label, 1 if incident considered adverse occured during this dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "                        
                        " case when sum(coalesce(incidents.number_of_unjustified_allegations, 0)) + " 
                        " sum(coalesce(incidents.number_of_preventable_allegations, 0)) + "
                        " sum(coalesce(incidents.number_of_sustained_allegations, 0)) > 0 then 1 else 0 end as feature_column "
                        " from (select * from staging.events_hub where event_datetime > {} and dispatch_id is not null ) as events_hub " 
                        "  left join staging.incidents as incidents "
                        " on events_hub.event_id = incidents.event_id "
                        " group by 1 ").format(from_date)

### Basic Dispatch Features

class DummyFeature(abstract.DispatchFeature):
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


class RandomFeature(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = ("Random feature for testing")
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   random() AS feature_column "
                        "FROM staging.events_hub "
                        "GROUP BY dispatch_id")
        self.type_of_imputation = "mean"


class DivisionAssigned(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Division in which the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   unit_div as feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")

#TODO
#ALL CODE BELOW IS QUERYING NON-STAGING TABLES, FIX THIS ASAP!
class Latitude(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Latitude of the original dispatch"
        self.query = (  "SELECT "
                        " dispatch_id, "
                        " latitude as feature_column "
                        "FROM "
                        " staging.non_formatted_dispatches_data ")

class Longitude(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Longitude of the original dispatch"
        self.query = (  "SELECT "
                        " dispatch_id, "
                        " longitude as feature_column "
                        "FROM "
                        " staging.non_formatted_dispatches_data ")
class DispatchMinute(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Minute of the hour the dispatch occured"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(minute FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")

class DispatchHour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Hour during which the dispatch occurred (24 hour clock)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(hour FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")


class DispatchDayOfWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Day of week the dispatch occurred (Sunday=0)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(DOW FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")


class DispatchYearQuarter(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year quarter the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(QUARTER FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")

class DispatchMonth(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Month the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(MONTH FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")

class DispatchYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   extract(YEAR FROM event_datetime) AS feature_column "
                        "FROM "
                        "   staging.non_formatted_dispatches_data ")


class OriginalPriority(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Original priority code of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_priority_code) as feature_column "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime > {} and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.dispatch_id = dispatches.dispatch_id "
                        "GROUP BY 1").format(from_date)
#TODO beat

#TODO event_type_code

#TODO priority

#TODO dispatch role

#TODO dispatch delay

#TODO travel time

#TODO response time

#TODO at scene time

#TODO units assigned

#TODO units arrived

#TODO unit shift
