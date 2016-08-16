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

################
#    LABELS    #
################

class LabelSustained(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Binary label, 1 if this dispatch led to a sustained complaint"
        self.is_label = True
        self.query = (  "SELECT "
                        "    dispatch_id, "
                        "    CASE WHEN "
                        "           SUM(COALESCE(incidents.number_of_sustained_allegations, 0)) > 0 "
                        "         THEN 1 "
                        "         ELSE 0 "
                        "    END AS {} "
                        "FROM "
                        "   (SELECT * "
                        "    FROM staging.events_hub "
                        "    WHERE event_datetime BETWEEN '{}' AND '{}' "
                        "    AND event_type_code = 4 "
                        "    AND dispatch_id IS NOT NULL "
                        "   ) AS events_hub "
                        "LEFT JOIN staging.incidents AS incidents "
                        "  ON events_hub.event_id = incidents.event_id "
                        "GROUP BY 1 "
                        .format(self.feature_name, self.from_date, self.to_date))

class LabelUnjustified(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Binary label, 1 if this dispatch led to an unjustified use of force"
        self.is_label = True
        self.query = (  "SELECT "
                        "    dispatch_id, "
                        "    CASE WHEN SUM(COALESCE(incidents.number_of_unjustified_allegations, 0)) > 0 "
                        "         THEN 1 "
                        "         ELSE 0 "
                        "    END AS {} "
                        "FROM "
                        "   (SELECT * "
                        "    FROM staging.events_hub "
                        "    WHERE event_datetime BETWEEN '{}' AND '{}' "
                        "    AND event_type_code = 4 "
                        "    AND dispatch_id IS NOT NULL "
                        "   ) AS events_hub "
                        "LEFT JOIN staging.incidents AS incidents "
                        "  ON events_hub.event_id = incidents.event_id "
                        "GROUP BY 1 "
                        .format(self.feature_name, self.from_date, self.to_date))

class LabelPreventable(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Binary label, 1 if this dispatch led to a preventable accidents"
        self.is_label = True
        self.query = (  "SELECT "
                        "    dispatch_id, "
                        "    CASE WHEN SUM(COALESCE(incidents.number_of_preventable_allegations, 0)) > 0 "
                        "         THEN 1 "
                        "         ELSE 0 "
                        "    END AS {} "
                        "FROM "
                        "   (SELECT * "
                        "    FROM staging.events_hub "
                        "    WHERE event_datetime BETWEEN '{}' AND '{}' "
                        "    AND event_type_code = 4 "
                        "    AND dispatch_id IS NOT NULL "
                        "   ) AS events_hub "
                        "LEFT JOIN staging.incidents AS incidents "
                        "  ON events_hub.event_id = incidents.event_id "
                        "GROUP BY 1 "
                        .format(self.feature_name, self.from_date, self.to_date))

############################
#   TIME OF DAY FEATURES   #
############################

class DispatchMinute(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Minute of the hour the dispatch occured"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(minute FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)

class DispatchHour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Hour during which the dispatch occurred (24 hour clock)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(hour FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)


class DispatchDayOfWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Day of week the dispatch occurred (Sunday=0)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(DOW FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)


class DispatchYearQuarter(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year quarter the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(QUARTER FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)

class DispatchMonth(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Month the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(MONTH FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)

class DispatchYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(YEAR FROM event_datetime)) AS {} "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)

#########################################
#   DISPATCH CHARACTERISTICS FEATURES   # i.e. what priority/type is the dispatch?
#########################################

class OriginalPriority(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Original priority code of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_priority_code) as {} "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.feature_name, self.from_date, self.to_date)

class DispatchType(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Type of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_type)  as {} "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.feature_name, self.from_date, self.to_date)

class DispatchSubType(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Type of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_subtype)  as {} "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.feature_name, self.from_date, self.to_date)

class NumberOfUnitsAssigned(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of units assigned to dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(units_assigned)  as {} "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.feature_name, self.from_date, self.to_date)

class AverageOfficerTravelTime(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Average amount of time it takes the attending officers to arrive"
        self.query = (" SELECT "
                          " dispatch_id, "
                          " avg(travel_time_minutes) AS {} "
                      " FROM staging.events_hub AS a "
                      " INNER JOIN staging.dispatches AS b "
                      " ON a.event_id = b.event_id "
                      " WHERE a.event_datetime BETWEEN '{}' AND '{}' "
                      " AND a.event_type_code = 5 "
                      " AND travel_time_minutes < 60 "
                      " GROUP BY dispatch_id ").format(self.feature_name, self.from_date, self.to_date)

class MinimumOfficerTravelTime(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Minimum travel time out of the attending officers"
        self.query = (" SELECT "
                          " dispatch_id, "
                          " min(travel_time_minutes) AS {} "
                      " FROM staging.events_hub AS a "
                      " INNER JOIN staging.dispatches AS b "
                      " ON a.event_id = b.event_id "
                      " WHERE a.event_datetime BETWEEN '{}' AND '{}' "
                      " AND a.event_type_code = 5 "
                      " AND travel_time_minutes < 60 "
                      " GROUP BY dispatch_id ").format(self.feature_name, self.from_date, self.to_date)

class MaximumOfficerTravelTime(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Maximum travel time out of the attending officers"
        self.query = (" SELECT "
                          " dispatch_id, "
                          " max(travel_time_minutes) AS {} "
                      " FROM staging.events_hub AS a "
                      " INNER JOIN staging.dispatches AS b "
                      " ON a.event_id = b.event_id "
                      " WHERE a.event_datetime BETWEEN '{}' AND '{}' "
                      " AND a.event_type_code = 5 "
                      " AND travel_time_minutes < 60 "
                      " GROUP BY dispatch_id ").format(self.feature_name, self.from_date, self.to_date)

class DispatchCategory(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Dispatch category, e.g. OI or CI"
        self.is_categorical = True
        self.query = (" SELECT "
                          " department_defined_dispatch_id AS dispatch_id, "
                          " min(dispatch_category) AS {} "
                      " FROM staging.dispatches AS a "
                      " INNER JOIN staging.events_hub AS b "
                      " ON a.event_id = b.event_id "
                      " WHERE b.event_datetime BETWEEN '{}' AND '{}' "
                      " GROUP BY 1 ").format(self.feature_name, self.from_date, self.to_date)

################################
#   GENERAL HISTORY FEATURES   #
#           ARRESTS            #    # i.e. what has been happening in Charlotte over past few hours?
################################
# All arrests
class ArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the hour preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class ArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 6 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '6 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class ArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 12 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '12 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)


class ArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 24 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '24 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class ArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 48 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '48 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class ArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the week preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 week' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

# Felony Arrests
class FelonyArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the hour preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 hour' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class FelonyArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 6 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '6 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class FelonyArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 12 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '12 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class FelonyArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 24 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '24 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class FelonyArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 48 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '48 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class FelonyArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the week preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 week' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

# Drugs arrests
class DrugsArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the hour preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 hour' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class DrugsArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the 6 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '6 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class DrugsArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the 12 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '12 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class DrugsArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the 24 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '24 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class DrugsArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the 48 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '48 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class DrugsArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of drugs-related arrests made in the week preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 week' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE drugs_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

# Stolen vehicle arrests
class StolenVehicleArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the hour preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 hour' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class StolenVehicleArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the 6 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '6 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class StolenVehicleArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the 12 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '12 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class StolenVehicleArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the 24 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '24 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class StolenVehicleArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the 48 hours preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '48 hours' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class StolenVehicleArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of stolen vehicle related arrests made in the week preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 week' "
                                   " WHERE event_type_code = 3 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {} "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE stolen_vehicle_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

################################
#   GENERAL HISTORY FEATURES   #
#           DISPATCHES         #    # i.e. what has been happening in Charlotte over past few hours?
################################
#Dispatch time features - small time windows
class OfficersDispatchedInPast1Minute(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of unique officers sent on dispatches in minute preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '1 minute' "
                                   " WHERE event_type_code = 5 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {}"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedInPast15Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of unique officers sent on dispatches in the 15 minutes preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '15 minutes' "
                                   " WHERE event_type_code = 5 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {}"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedInPast30Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of unique officers sent on dispatches in the 30 minutes preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '30 minutes' "
                                   " WHERE event_type_code = 5 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {}"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)


class OfficersDispatchedInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of unique officers sent on dispatches in the 1 hour preceding the dispatch"
        self.query = ( " WITH recent_events AS "
                       "( SELECT "
                           " a.dispatch_id, "
                           " b.event_id "
                           " FROM staging.earliest_dispatch_time a "
                               " INNER JOIN staging.events_hub b "
                               " on b.event_datetime <= a.earliest_dispatch_datetime "
                               " and b.event_datetime >= a.earliest_dispatch_datetime - interval '60 minutes' "
                                   " WHERE event_type_code = 5 "
                                           " and earliest_dispatch_datetime between '{}' and '{}' "
                            " ) "
                        " SELECT dispatch_id, count(dispatch_id) as {}"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date, self.feature_name)

##################################
#   DISPATCHED OFFICER HISTORY   #
#         INTERNAL AFFAIRS       #
##################################
# Average
# Past 3 years
class OfficersDispatchedAverageUnjustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageJustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAveragePreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageNonPreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageSustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageUnsustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past Year
class OfficersDispatchedAverageUnjustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageJustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAveragePreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageNonPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageSustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageUnsustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past 6 months
class OfficersDispatchedAverageUnjustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageJustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAveragePreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageNonPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageSustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageUnsustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past month
class OfficersDispatchedAverageUnjustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageJustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAveragePreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageNonPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageSustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedAverageUnsustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     AVG(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Minimum
# Past 3 years
class OfficersDispatchedMinimumUnjustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unjustified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumJustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of justified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumPreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumNonPreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of non-preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumSustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of sustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumUnsustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unsustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past Year
class OfficersDispatchedMinimumUnjustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unjustified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumJustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of justified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumNonPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of non-preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumSustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of sustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumUnsustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unsustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past 6 months
class OfficersDispatchedMinimumUnjustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unjustified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumJustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of justified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumNonPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of non-preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumSustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of sustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumUnsustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unsustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past month
class OfficersDispatchedMinimumUnjustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unjustified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumJustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of justified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumNonPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of non-preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumSustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of sustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMinimumUnsustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum number of unsustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MIN(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Maximum
# Past 3 years
class OfficersDispatchedMaximumUnjustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unjustified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumJustifiedIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of justified incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumPreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumNonPreventableIncidentsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of non-preventable incidents occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumSustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of sustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumUnsustainedAllegationsInPast3Years(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unsustained allegations occuring in past 3 years for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '3 years' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past Year
class OfficersDispatchedMaximumUnjustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unjustified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumJustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of justified incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumNonPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of non-preventable incidents occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumSustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of sustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumUnsustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unsustained allegations occuring in past year for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 year' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past 6 months
class OfficersDispatchedMaximumUnjustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unjustified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumJustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of justified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumNonPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of non-preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumSustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of sustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumUnsustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unsustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '6 months' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

# Past month
class OfficersDispatchedMaximumUnjustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unjustified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unjustified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumJustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of justified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_justified_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumNonPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of non-preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_non_preventable_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumSustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of sustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_sustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)

class OfficersDispatchedMaximumUnsustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum number of unsustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " WITH dispatches_to_include AS "
                    "     (SELECT "
                    "         dispatch_id AS this_dispatch, officer_id AS this_officer,  "
                    "         dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    "     FROM staging.dispatch_geo_time_officer "
                    "     WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_history AS "
                    "     (SELECT "
                    "         * "
                    "     FROM dispatches_to_include AS a "
                    "     INNER JOIN staging.events_hub AS b "
                    "         ON b.officer_id = a.this_officer "
                    "         AND b.event_datetime < a.this_datetime "
                    "         AND b.event_datetime >= a.this_datetime - INTERVAL '1 month' "
                    "     INNER JOIN staging.incidents AS c "
                    "     ON b.event_id = c.event_id "
                    "     WHERE b.event_type_code = 4), "
                    " dispatch_officer_sums AS "
                    "     (SELECT "
                    "         this_dispatch, this_officer, SUM(coalesce(number_of_unsustained_allegations, 0)) AS allegations "
                    "     FROM officer_history "
                    "     GROUP BY 1,2) "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     MAX(allegations) AS {} "
                    " FROM dispatch_officer_sums "
                    " GROUP BY 1 ").format(self.from_date, self.to_date, self.feature_name)


########################################
#   OFFICER CHARACTERISTICS FEATURES   #
########################################

class AverageAgeOfRespondingOfficers(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average age of the officers dispatched"
     self.query = ( " SELECT "
                    "     dispatch_id, "
                    "     avg(age_in_years)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " round(extract(days from dispatch_datetime - date_of_birth)/365) as age_in_years "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class MaximumAgeOfRespondingOfficers(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The maximum age of the officers dispatched"
     self.query = ( " SELECT "
                    "     dispatch_id, "
                    "     max(age_in_years)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " round(extract(days from dispatch_datetime - date_of_birth)/365) as age_in_years "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class MinimumAgeOfRespondingOfficers(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The minimum age of the officers dispatched"
     self.query = ( " SELECT "
                    "     dispatch_id, "
                    "     min(age_in_years)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " round(extract(days from dispatch_datetime - date_of_birth)/365) as age_in_years "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

#TODO average time on force of responding officers

class HighestEducationLevelAmongRespondingOfficers(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The highest education level attained amongst all the responding officers"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " max(education)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.education_level_code as education "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_characteristics as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class LowestEducationLevelAmongRespondingOfficers(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The lowest education level attained amongst all the responding officers"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " min(education)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.education_level_code as education "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_characteristics as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersWithFourYearCollegeDegreeOrHigher(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers with a four-year college degree or higher"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " sum(case when education >= 4 then 1 else 0 end)/(count(education)+0.00001)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.education_level_code as education "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_characteristics as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersMale(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose gender is designated as male"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " sum(case when gender = 1 then 1 else 0 end)/(count(gender)+0.00001)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.gender_code as gender "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_characteristics as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersBlack(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose race is designated as African American"
     self.query = ( " select "
                    "     dispatch_id, "
                    "     sum(case when race_code = 1 then 1.0 else 0.0 end) / (count(dispatch_id)::float + 0.00001) as {} "
                    " from "
                    "     (select "
                    "         a.dispatch_id, "
                    "         a.officer_id, "
                    "         b.race_code "
                    "     from staging.dispatch_geo_time_officer as a "
                    "     inner join staging.officers_hub as b "
                    "     on a.officer_id = b.officer_id "
                    "     where a.dispatch_datetime between '{}' and '{}') as a "
                    " group by dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersHispanic(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose ethnicity is designated as Hispanic"
     self.query = ( " select "
                    "     dispatch_id, "
                    "     sum(case when ethnicity_code = 2 then 1.0 else 0.0 end) / (count(dispatch_id)::float + 0.00001) as {} "
                    " from "
                    "     (select "
                    "         a.dispatch_id, "
                    "         a.officer_id, "
                    "         b.ethnicity_code "
                    "     from staging.dispatch_geo_time_officer as a "
                    "     inner join staging.officers_hub as b "
                    "     on a.officer_id = b.officer_id "
                    "     where a.dispatch_datetime between '{}' and '{}') as a "
                    " group by dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersAsian(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose race is designated as Asian"
     self.query = ( " select "
                    "     dispatch_id, "
                    "     sum(case when race_code = 4 then 1.0 else 0.0 end) / (count(dispatch_id)::float + 0.00001) as {} "
                    " from "
                    "     (select "
                    "         a.dispatch_id, "
                    "         a.officer_id, "
                    "         b.race_code "
                    "     from staging.dispatch_geo_time_officer as a "
                    "     inner join staging.officers_hub as b "
                    "     on a.officer_id = b.officer_id "
                    "     where a.dispatch_datetime between '{}' and '{}') as a "
                    " group by dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersOtherRace(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose race is designated as a race other than black, white or Asian"
     self.query = ( " select "
                    "     dispatch_id, "
                    "     sum(case when race_code in (3,5,6,7) then 1.0 else 0.0 end) / (count(dispatch_id)::float + 0.00001) as {} "
                    " from "
                    "     (select "
                    "         a.dispatch_id, "
                    "         a.officer_id, "
                    "         b.race_code "
                    "     from staging.dispatch_geo_time_officer as a "
                    "     inner join staging.officers_hub as b "
                    "     on a.officer_id = b.officer_id "
                    "     where a.dispatch_datetime between '{}' and '{}') as a "
                    " group by dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersDivorcedOrSeparated(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose marital status is listed as divorced or separated"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " sum(case when status = 3 then 1 else 0 end)+sum(case when status = 4 then 1 else 0 end)/(count(status)+0.00001) AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.marital_status_code as status "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_marital as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfRespondingOfficersMarried(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The proportion of the responding officers whose marital status is married"
     self.query = ( " SELECT "
                    " dispatch_id, "
                    " sum(case when status = 2 then 1 else 0 end)/(count(status)+0.00001)  AS {}"
                    " FROM (SELECT a.dispatch_id, a.officer_id, "
                    " c.marital_status_code as status "
                    " FROM staging.dispatch_geo_time_officer as a "
                    " INNER JOIN staging.officers_hub as b "
                    " ON a.officer_id = b.officer_id "
                    " INNER JOIN staging.officer_marital as c "
                    " ON b.officer_id = c.officer_id "
                    " WHERE a.dispatch_datetime between '{}' and '{}') as a "
                    " GROUP BY dispatch_id").format(self.feature_name, self.from_date, self.to_date)


#TODO time difference between first and last arrival at scene

#TODO variance in travel time for responding officers

#TODO average time on shift of responding officers at time of dispatch

#TODO average number of dispatches attended in current shift by responding officers
#and do 12 hours, 24 hours, 1 week

#TODO average priority of dispatches attended in current shift by responding officers

#TODO average number of arrests in current shift by responding officers

#TODO average number of felony arrests in current shift by responding officers


#####################################
#   DEMOGRAPHIC FEATURES FROM ACS   #
#####################################

class MedianAgeInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median age of the population in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b01002001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianAgeOfMenInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median age of men in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b01002002 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianAgeOfWomenInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median age of women in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b01002003 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class UnweightedSampleCountOfPopulationInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Unweighted sample count of the population in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b00001001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b00001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class UnweightedSampleCountOfHousingUnitsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Unweighted sample count of housing units in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b00002001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b00002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageMenInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of men in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b01001002 / (c.b01001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageWomenInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of women in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b01001026 / (c.b01001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageWhiteInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of white race in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b02001002 / (c.b02001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b02001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageBlackInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of black race in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b02001003 / (c.b02001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b02001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageAsianInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of Asian race in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b02001005 / (c.b02001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b02001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageHispanicInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of Hispanic ethnicity in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b03001003 / (c.b03001001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b03001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class PercentageForeignBornInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Percentage of population born outside of the US in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b05002009 / (c.b05002001 + 0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b05002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)


class ProportionOfPopulationUnderAge18InCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population under the age of 18 in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b01001003+c.b01001004+c.b01001005+c.b01001006+c.b01001027+c.b01001028+c.b01001029+c.b01001030) / (c.b01001001 + 0.0001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b01001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationEnrolledInSchoolInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population over age 3 enrolled in school in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b14001002/(c.b14001001+0.0001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b14001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationOver25WithLessThanHighSchoolEducationInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population over age 25 who have less than a high school education in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    " ((c.b15003001)-(c.b15003017+c.b15003018+c.b15003019+c.b15003020+c.b15003021+c.b15003022+c.b15003023+c.b15003024+c.b15003025))/(c.b15003001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b15003 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationVeteransInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population who list themselves as veterans in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b21001002)/(c.b21001001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b21001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)


class ProportionOfPopulationWithIncomeBelowPovertyLevelInPastYearInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population who have had an income below poverty level in past year in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    " c.b17001002/(c.b17001001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b17001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationWithIncomeInPast12MonthsBelow45000DollarsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the population who have had an income below $45,000 (in 2014 inflation-adjusted dollars) in past year in census tract of dispatch origin"
     #Note that the 2014 median income in NC was $46,556, so this is really below state median income
     #except it excludes people who earn 45,000-46,556 as they are included in the next category 45-49
     #See http://www.deptofnumbers.com/income/north-carolina/
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    " (c.b19001002+c.b19001003+c.b19001004+c.b19001005+c.b19001006+c.b19001007+c.b19001008+c.b19001009)/(c.b19001001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b19001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianIncomeInPast12MonthsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median income (in 2014 inflation-adjusted dollars) in past year in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    " c.b06011001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b06011 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianHouseholdIncomeInPast12MonthsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median household income (in 2014 inflation-adjusted dollars) in past year in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    " c.b19013001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b19013 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfHouseholdsReceivingAssistanceOrFoodStampsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the households receiving cash public assistance or food stamps/SNAP in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b19058002/(c.b19058001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b19058 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfHousingUnitsVacantInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the housing units that are vacant in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25002003/(c.b25002001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfHousingUnitsOccupiedByOwnerInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of the housing units that are occupied by the owner in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25003002/(c.b25003001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25003 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)


class MedianYearStructureBuildInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median year that existing structures were built in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25035001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25035 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianYearRenterMovedIntoHousingUnitInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median year resident moved into current property (renter occupied properties) in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25039003 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25039 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianYearOwnerMovedIntoHousingUnitInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median year resident moved into current property (owner occupied properties) in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25039002 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25039 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianGrossRentInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median gross rent measured in 2014 inflation-adjusted dollars in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25064001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25064 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class MedianPropertyValueInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Median property value in dollars in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25077001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25077 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class LowerQuartilePropertyValueInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Lower quartile property value in dollars in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25076001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25076 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class UpperQuartilePropertyValueInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Upper quartile property value in dollars in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25078001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25078 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class AverageHouseholdSizeInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of people living in households in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b25010001 AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b25010 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfChildrenUnder18LivingWithSingleParentInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of children under the age of 18 livig with a single parent in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b23008008+c.b23008021)/(c.b23008001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b23008 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfChildrenUnder18LivingWithMotherInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of children under the age of 18 living with mother (and not father) in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b23008012+c.b23008025)/(c.b23008001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b23008 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationNeverMarriedInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of people over age 15 who report their marital status as never been married in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b12001003+c.b12001012)/(c.b12001001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b12001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationDivorcedOrSeparatedInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of people over age 15 who report their marital status as divorced or separated in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b12001007+c.b12001010+c.b12001016+c.b12001019)/(c.b12001001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b12001 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfPopulationWithoutHealthInsuranceInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of population with no health insurance in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     (c.b27020006+c.b27020017)/(c.b27020001+0.00001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b27020 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

class ProportionOfWomenWhoGaveBirthInPast12MonthsInCT(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Proportion of women aged between 15 and 50 who gave birth in the last 12 months in census tract of dispatch origin"
     self.query = ( " SELECT "
                    "     a.dispatch_id,  "
                    "     c.b13002002 / (c.b13002011+0.0001) AS {} "
                    " FROM staging.dispatch_geoid as a "
                    " INNER JOIN staging.earliest_dispatch_time AS b "
                    " ON a.dispatch_id = b.dispatch_id "
                    " INNER JOIN acs2013_5yr.b13002 AS c "
                    " ON a.acs_geoid_long = c.geoid "
                    " WHERE b.earliest_dispatch_datetime BETWEEN '{}' AND '{}' ").format(self.feature_name, self.from_date, self.to_date)

#########################################
#   SPACE-RESTRICTED HISTORY FEATURES   #
#                ARRESTS                #
#########################################

class ArrestsWithin1kmRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 1 km radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin1kmRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 1 km radius within the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '6 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin1kmRadiusInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 1 km radius within the past 12 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '12 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin1kmRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 1 km radius within the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '24 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin500mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 500 m radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin500mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 500 m radius within the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '6 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin500mRadiusInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 500 m radius within the past 12 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '12 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin500mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 500 m radius within the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '24 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin100mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 100 m radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin100mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 100 m radius within the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '6 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin100mRadiusInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 100 m radius within the past 12 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '12 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class ArrestsWithin100mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of arrests within a 100 m radius within the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.event_id, b.arrest_location, b.arrest_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.arrests_geo_time AS b "
                    "      ON b.arrest_datetime < a.this_datetime "
                    "      AND b.arrest_datetime >= a.this_datetime - interval '24 hours') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(event_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  arrest_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

#########################################
#   SPACE-RESTRICTED HISTORY FEATURES   #
#               DISPATCHES              #
#########################################

class DispatchesWithin1kmRadiusInPast15Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 1 km radius within the past 15 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '15 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin1kmRadiusInPast30Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 1 km radius within the past 30 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '30 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin1kmRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 1 km radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin500mRadiusInPast15Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 500 m radius within the past 15 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '15 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin500mRadiusInPast30Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 500 m radius within the past 30 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '30 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin500mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 500 m radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin100mRadiusInPast15Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 100 m radius within the past 15 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '15 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin100mRadiusInPast30Minutes(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 100 m radius within the past 30 minutes"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '30 minutes') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class DispatchesWithin100mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Number of dispatches within a 100 m radius within the past 1 hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, dispatch_location AS this_loc, earliest_dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time "
                    " WHERE earliest_dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.dispatch_location, b.earliest_dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time AS b "
                    "      ON b.earliest_dispatch_datetime < a.this_datetime "
                    "      AND b.earliest_dispatch_datetime >= a.this_datetime - interval '1 hour') "
                    " SELECT "
                    "     this_dispatch AS dispatch_id, "
                    "     COUNT(dispatch_id) AS {} "
                    " FROM time_restrained "
                    " WHERE ST_DWithin(this_loc,  dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

#############################################################
#   SPACE-RESTRICTED AND TIME-RESTRICTED OFFICER FEATURES   #
#                       DISPATCHES                          #
#############################################################

# Average
class AverageOfficerDispatchesWithin100mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 100m in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin100mRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 100m in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin100mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 100m in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin100mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 100m in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin100mRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 100m in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin500mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 500m in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin500mRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 500m in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin500mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 500m in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin500mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 500m in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin500mRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 500m in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin1kmRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 1 km in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin1kmRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 1km in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin1kmRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 1 km in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin1kmRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 1 km in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class AverageOfficerDispatchesWithin1kmRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Average number of times the officers on this dispatch have attended other dispatches within 1km in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " AVG(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

# Maximum
class MaximumOfficerDispatchesWithin100mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 100m in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin100mRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 100m in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin100mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 100m in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin100mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 100m in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin100mRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 100m in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin500mRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 500m in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin500mRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 500m in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin500mRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 500m in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin500mRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 500m in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin500mRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 500m in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0045) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin1kmRadiusInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 1 km in the past hour"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '1 hour'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin1kmRadiusInPast3Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 1km in the past 3 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '3 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin1kmRadiusInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 1 km in the past 6 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '6 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin1kmRadiusInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 1 km in the past 24 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '24 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)

class MaximumOfficerDispatchesWithin1kmRadiusInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "Maximum number of times the officers on this dispatch have attended other dispatches within 1km in the past 48 hours"
     self.query = ( " WITH dispatches_to_include AS "
                    " (SELECT "
                    "      dispatch_id AS this_dispatch, officer_id AS this_officer, "
                    "      dispatch_location AS this_loc, dispatch_datetime AS this_datetime "
                    " FROM staging.dispatch_geo_time_officer "
                    " WHERE dispatch_datetime BETWEEN '{}' AND '{}'), "
                    " officer_time_restrained AS "
                    " (SELECT "
                    "      a.this_dispatch, a.this_officer, a.this_loc, a.this_datetime, "
                    "      b.dispatch_id, b.officer_id, b.dispatch_location, b.dispatch_datetime "
                    " FROM dispatches_to_include AS a "
                    " INNER JOIN staging.dispatch_geo_time_officer AS b "
                    "      ON b.officer_id = a.this_officer "
                    "      AND b.dispatch_datetime < a.this_datetime "
                    "      AND b.dispatch_datetime >= a.this_datetime - INTERVAL '48 hours'), "
                    " officers_grouped AS "
                    " (SELECT  "
                    "      this_dispatch, "
                    "      this_officer, "
                    "      COUNT(officer_id) AS dispatch_count "
                    " FROM officer_time_restrained "
                    " WHERE ST_DWithin(this_loc, dispatch_location, 0.0009) "
                    " GROUP BY this_dispatch, this_officer) "
                    " SELECT "
                    " this_dispatch AS dispatch_id, "
                    " MAX(dispatch_count) AS {} "
                    " FROM officers_grouped "
                    " GROUP BY this_dispatch ").format(self.from_date, self.to_date, self.feature_name)
