mport logging
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
                        "    END AS feature_column "
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
                        .format(self.from_date, self.to_date))

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
                        "    END AS feature_column "
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
                        .format(self.from_date, self.to_date))

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
                        "    END AS feature_column "
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
                        .format(self.from_date, self.to_date))




#TODO
#ALL CODE BELOW IS QUERYING NON-STAGING TABLES, FIX THIS ASAP!

class DispatchMinute(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Minute of the hour the dispatch occured"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(minute FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)

class DispatchHour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Hour during which the dispatch occurred (24 hour clock)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(hour FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)


class DispatchDayOfWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Day of week the dispatch occurred (Sunday=0)"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(DOW FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)


class DispatchYearQuarter(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year quarter the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(QUARTER FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)

class DispatchMonth(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Month the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "  max(extract(MONTH FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)

class DispatchYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Year the dispatch occurred"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(extract(YEAR FROM event_datetime)) AS feature_column "
                        "FROM "
                        "   staging.events_hub where event_datetime between '{}' and '{}' and dispatch_id is not null "
                        "GROUP BY 1 ").format(self.from_date, self.to_date)


class OriginalPriority(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Original priority code of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_priority_code) as feature_column "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.from_date, self.to_date)


class DispatchType(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Type of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_type)  as feature_column "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.from_date, self.to_date)

class DispatchSubType(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.is_categorical = True
        self.description = "Type of dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(dispatch_original_subtype)  as feature_column "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.from_date, self.to_date)

class NumberOfUnitsAssigned(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of units assigned to dispatch"
        self.query = (  "SELECT "
                        "   dispatch_id, "
                        "   max(units_assigned)  as feature_column "
                        "FROM "
                        "   (select * from staging.events_hub where event_datetime between '{}' and '{}' "
                        "                                     and event_type_code = 5 "
                        "                                     and dispatch_id is not null ) as events_hub "
                        "   inner join staging.dispatches as dispatches "
                        "   on events_hub.event_id = dispatches.event_id "
                        "GROUP BY 1").format(self.from_date, self.to_date)

#TODO beat

#TODO dispatch role

#TODO dispatch delay

#TODO travel time

#TODO response time

#TODO at scene time

#TODO units assigned

#TODO units arrived

#TODO unit shift

#GENERAL TEMPORAL Features
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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)


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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column "
                            " FROM recent_events a "
                            " INNER JOIN staging.arrests b "
                            " on a.event_id = b.event_id "
                                " WHERE felony_flag = true "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)


#TEMPORAL Features
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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)


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
                        " SELECT dispatch_id, count(dispatch_id) as feature_column"
                            " FROM recent_events a "
                            " GROUP BY dispatch_id").format(self.from_date, self.to_date)

class OfficersDispatchedInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of unique officers sent on dispatches in the 6 hours preceding the dispatch"
        self.query = ( " SELECT "
                       "    dispatch_id, "
                       "    COUNT(dispatch_id) AS feature_column"
                       " FROM "
                       "    (SELECT "
                       "        dispatch_id, "
                       "        min(event_datetime) AS min_event_datetime  "
                       "    FROM staging.events_hub "
                       "    WHERE event_type_code = 5 AND dispatch_id IS NOT NULL "
                       "    AND event_datetime BETWEEN '{}' AND '{}' "
                       "    GROUP BY 1) AS a "
                       "    INNER JOIN "
                       "    (SELECT event_datetime FROM staging.events_hub WHERE event_type_code = 5 and event_datetime between '{}' and '{}') AS b "
                       "    ON b.event_datetime <= a.min_event_datetime AND b.event_datetime >= a.min_event_datetime - interval '6 hours'  "
                       " GROUP BY 1 ").format(self.from_date, self.to_date, self.from_date, self.to_date)

#DISPATCHED OFFICER SPECIFIC temporal
class OfficersDispatchedAverageUnjustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unjustified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unjustified_allegations,0)) as unjustified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageJustifiedIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(justified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_justified_allegations,0)) as justified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAveragePreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_preventable_allegations,0)) as preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageNonPreventableIncidentsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(non_preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_non_preventable_allegations,0)) as non_preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageSustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(sustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_sustained_allegations,0)) as sustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageUnsustainedAllegationsInPastYear(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past year for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unsustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unsustained_allegations,0)) as unsustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 year' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageUnjustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unjustified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unjustified_allegations,0)) as unjustified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageJustifiedIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(justified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_justified_allegations,0)) as justified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAveragePreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_preventable_allegations,0)) as preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageNonPreventableIncidentsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(non_preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_non_preventable_allegations,0)) as non_preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageSustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(sustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_sustained_allegations,0)) as sustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageUnsustainedAllegationsInPast6Months(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past 6 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unsustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unsustained_allegations,0)) as unsustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '6 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageUnjustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unjustified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unjustified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unjustified_allegations,0)) as unjustified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageJustifiedIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of justified incidents occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(justified_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_justified_allegations,0)) as justified_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAveragePreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_preventable_allegations,0)) as preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageNonPreventableIncidentsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of non-preventable incidents occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(non_preventable_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_non_preventable_allegations,0)) as non_preventable_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageSustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of sustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(sustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_sustained_allegations,0)) as sustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

class OfficersDispatchedAverageUnsustainedAllegationsInPast1Month(abstract.DispatchFeature):
    def __init__(self, **kwargs):
     abstract.DispatchFeature.__init__(self, **kwargs)
     self.description = "The average number of unsustained allegations occuring in past 1 months for officers dispatched"
     self.query = ( " SELECT "
                        " dispatch_id, "
                        " avg(unsustained_allegations) as feature_column "
                    "FROM "
                        " (SELECT "
                        " a.dispatch_id, "
                        " a.officer_id, "
                        " sum(coalesce(number_of_unsustained_allegations,0)) as unsustained_allegations "
                    " FROM "
                        " (SELECT "
                        " dispatch_id, "
                        " officer_id, "
                        " min(event_datetime) as min_event_datetime "
                    " FROM staging.events_hub where event_type_code = 5 and dispatch_id is not null and event_datetime between '{}' and '{}' "
                        " GROUP BY 1,2) as a "
                    " LEFT JOIN "
                        " ((SELECT * FROM staging.events_hub where event_type_code = 4) as c "
                    " INNER JOIN staging.incidents as d "
                        " on c.event_id = d.event_id) as b "
                        " on a.officer_id = b.officer_id and b.event_datetime <= a.min_event_datetime and b.event_datetime >= a.min_event_datetime - interval '1 months' "
                        " GROUP BY 1,2 ) as e "
                        " GROUP BY 1 ").format(self.from_date, self.to_date)

#TODO spatio-temporal, e.g. num dispatches in same division as ROs in past 1 hr
#e.g. average priority of dispatches in division in past k hrs.

#TODO Officer level fixed, e.g. average age, average education, etc.
