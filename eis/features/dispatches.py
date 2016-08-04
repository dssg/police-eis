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

#TEMPORAL Features
class ArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the hour preceding the dispatch"
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
                       "    LEFT JOIN "
                       "    (SELECT event_datetime FROM staging.events_hub WHERE event_type_code = 3) AS b "
                       "    ON b.event_datetime <= a.min_event_datetime AND b.event_datetime >= a.min_event_datetime - interval '1 hour'  "
                       " GROUP BY 1 ").format(self.from_date, self.to_date)

class ArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 6 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '6 hours') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class ArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 12 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '12 hours') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class ArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 24 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '24 hours') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class ArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 48 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '48 hours') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class ArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the week preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '1 week') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class ArrestsInPastMonth(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the month preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       " (select count(*) from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '1 month') and a.event_datetime) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPast1Hour(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the hour preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '1 hours') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPast6Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 6 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '6 hours') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPast12Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 12 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '12 hours') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPast24Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 24 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '24 hours') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPast48Hours(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the 48 hours preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '48 hours') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPastWeek(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the week preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '1 week') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)

class FelonyArrestsInPastMonth(abstract.DispatchFeature):
    def __init__(self, **kwargs):
        abstract.DispatchFeature.__init__(self, **kwargs)
        self.description = "Number of arrests made in the month preceding the dispatch"
        self.query = ( " SELECT "
                       " a.dispatch_id, "
                       "(SELECT count(*) from (select * from staging.events_hub as b "
                           " where b.event_type_code = 3 "
                           " and b.event_datetime between (a.event_datetime - interval '1 month') and a.event_datetime) "
                           " as c "
                           " inner join staging.arrests as d "
                           " on c.event_id = d.event_id "
                           " where d.felony_flag = true) as feature_column "
                       " FROM "
                       " (SELECT "
                       " dispatch_id, "
                       " min(event_datetime) as event_datetime "
                       " FROM staging.events_hub "
                           " where event_type_code = 5 and dispatch_id is not null "
                           " and event_datetime between '{}' and '{}' "
                       " GROUP by 1) as a ").format(self.from_date, self.to_date)
