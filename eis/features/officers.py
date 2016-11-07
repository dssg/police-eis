#!/usr/bin/env python
import pdb
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

### Officer labels.
class LabelSustained(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Binary label, 1 if an officer led to a sustained complaint"
        self.is_label = True
        self.query = ()
        self.query = (  "UPDATE features.{0} feature_table "
                        "SET {1} = staging_table.feature_column "
                        "FROM (     "
                        "       SELECT events_hub.officer_id, CASE WHEN SUM(COALESCE(incidents.number_of_sustained_allegations, 0)) > 0 "
                        "                               THEN 1 "  #
                        "                               ELSE 0 "  # COALESCE here maps NULL values to zero.
                        "       END AS sustained_flag from staging.events_hub LEFT JOIN staging.incidents "
                        "       ON events_hub.event_id=incidents.event_id "
                        "       WHERE events_hub.event_datetime >= '{2}'::date "
                        "       WHERE events_hub.event_datetime <= '{3}'::date "
                        "       GROUP BY officer_id ) AS staging_table"
                        " WHERE feature_table.officer_id = staging_table.officer_id "
                        " AND feature_table.fake_today = '{2}'::date "
                        .format(self.table_name,
                                self.feature_name,
                                self.fake_today,
                                self.to_date))

### Dummy instances of the abstract classes to use as templates.

class DummyFeature(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Dummy feature for testing 2016 schema")
        self.num_features = 1
        self.name_of_features = ["DummyFeature"]
        self.query = ("SELECT officer_id, COUNT(event_type_code) "
                      "FROM events_hub "
                      "WHERE event_type_code = 4 "
                      "GROUP BY officer_id")

class DummyCategoricalFeature(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Categorical dummy feature for testing 2016 schema")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officers_hub "
                      "         WHERE staging.officers_hub.race_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class TimeGatedDummyFeature(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Dummy time-gated feature for testing 2016 schema")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=3 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))

class TimeGatedCategoricalDummyFeature(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "absent",
                            4: "bereavement",
                            16: "family medical",
                            23: "leave without pay",
                            29: "sick non family",
                            30: "suspension",
                            31: "suspension without pay",
                            2: "admin" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Dummy time-gated categorical feature for testing 2016 schema")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_shifts "
                      "         WHERE staging.officer_shifts.shift_type_code = {4} "
                      "         AND start_datetime <= '{2}'::date "
                      "         AND start_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

# Actual features.

class ArrestMonthlyVariance(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The variance in the number of arrests an officer has made per month, time-gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.variance "
                      "FROM (   SELECT officer_id, variance(count) "
                      "         FROM ( "
                      "                 SELECT officer_id,  count(officer_id) "
                      "                 FROM staging.events_hub "
                      "                 WHERE event_type_code=3 "
                      "                 AND event_datetime <= '{2}'::date "
                      "                 AND event_datetime >= '{2}'::date - interval '{3}' "
                      "                 GROUP BY officer_id, date_trunc( 'month', event_datetime ) "
                      "             ) AS monthlyarrests  "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class ArrestMonthlyCOV(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The STDDEV/MEAN in the number of arrests an officer has made per month, time-gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.cov "
                      "FROM (   SELECT officer_id, "
                      "     CASE avg(count) "
                      "         WHEN 0 THEN 0  "
                      "         ELSE stddev(count) / avg(count) "
                      "     END as cov "
                      "         FROM ( "
                      "                 SELECT officer_id,  count(officer_id) "
                      "                 FROM staging.events_hub "
                      "                 WHERE event_type_code=3 "
                      "                 AND event_datetime <= '{2}'::date "
                      "                 AND event_datetime >= '{2}'::date - interval '{3}' "
                      "                 GROUP BY officer_id, date_trunc( 'month', event_datetime ) "
                      "             ) AS monthlyarrests  "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class DaysSinceLastAllegation(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Days since last allegation")
        self.num_features = 1
        self.name_of_features = ["DaysSinceLastAllegation"]
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.days "
                      "FROM (   SELECT officer_id, ABS( EXTRACT( DAY FROM MAX( event_datetime - '{2}'::date ) ) ) AS days "
                      "         FROM staging.incidents "
                      "         WHERE event_datetime < '{2}'::date "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )

class DaysSinceLastSustainedAllegation(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Days since last sustained allegation")
        self.num_features = 1
        self.name_of_features = ["DaysSinceLastSustainedAllegation"]
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.days "
                      "FROM (   SELECT officer_id, ABS( EXTRACT( DAY FROM MAX( event_datetime - '{2}'::date ) ) ) AS days "
                      "         FROM staging.incidents "
                      "         WHERE event_datetime < '{2}'::date "
                      "         AND final_ruling_code in (1,4,5) "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )

class NumberOfShiftsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'absent',
                            1: 'adjustment',
                            2: 'admin',
                            3: 'admin_leave',
                            4: 'bereavement',
                            5: 'break',
                            6: 'bubble',
                            7: 'canine',
                            8: 'comp_time',
                            9: 'court_jury',
                            10: 'premium',
                            11: 'mission',
                            12: 'double',
                            13: 'excused',
                            14: 'special',
                            15: 'outside',
                            16: 'family_medical',
                            17: 'holiday',
                            18: 'ebs',
                            19: 'training',
                            20: 'injured',
                            21: 'kelly',
                            22: 'doc_pay',
                            23: 'leave_without_pay',
                            24: 'light_duty_medical_disability',
                            25: 'military',
                            26: 'not_attended_court',
                            27: 'safer',
                            28: 'personal',
                            29: 'sick_non_family',
                            30: 'suspension',
                            31: 'suspension_with_pay',
                            32: 'unallocated',
                            33: 'vacation',
                            34: 'work',
                            35: 'work_holiday',
                            99: 'other'}
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of shifts of different types aggregated over time")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_shifts "
                      "         WHERE staging.officer_shifts.shift_type_code = {4} "
                      "         AND start_datetime <= '{2}'::date "
                      "         AND start_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumberOfSuspensionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 'hours_active_suspension': "Active Suspension",
                            'hours_inactive_suspension': "Inactive Suspension" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of time-gated suspensions by type (active or inactive)")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.incidents "
                      "         WHERE incidents.{4} > 0 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class TotalHoursOfSuspensionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 'hours_active_suspension': "Active Suspension",
                            'hours_inactive_suspension': "Inactive Suspension" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of time-gated suspensions by type (active or inactive)")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.sum "
                      "FROM (   SELECT officer_id, sum({4}) "
                      "         FROM staging.incidents "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumberOfArrestsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "On view arrest",
                            1: "Order for Arrest",
                            2: "Warrant for Arrest",
                            3: "Non Arrest" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of time-gated arrests by categorical type")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.arrests "
                      "         WHERE arrests.arrest_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumberOfArrestsON(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Sunday",
                            1: "Monday",
                            2: "Tuesday",
                            3: "Wednesday",
                            4: "Thursday",
                            5: "Friday",
                            6: "Saturday" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of time-gated arrests by day of week")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.arrests "
                      "         WHERE arrests.arrest_day_of_week = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumberOfSuspectsArrestedOfRaceType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of suspects arrested by race type, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.arrests "
                      "         WHERE arrests.suspect_race_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumberOfSuspectsArrestedOfEthnicityType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "non_hispanic",
                            2: "hispanic" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of suspects arrested by ethnicity type, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.arrests "
                      "         WHERE arrests.suspect_ethnicity_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

# These are *only* the interventions the stemmed directly from an incident
class TotalInterventionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Unknown",
                            1: "Counseling",
                            2: "Training",
                            3: "Suspension",
                            4: "Termination",
                            5: "Reprimand",
                            6: "Loss of vacation",
                            7: "No intervention required" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Total interventions of each type an officer has received as the result of an incident")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.incidents "
                      "         WHERE staging.incidents.intervention_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class IncidentCount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = "Number of investigable incidents"
        self.name_of_features = ["IncidentCount"]
        self.start_date = kwargs["fake_today"] - datetime.timedelta(days=365)
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=4 "
                      "         AND event_datetime <= '{}'::date "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{}'::date"
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format),
                                self.fake_today.strftime(time_format),
                                self.fake_today.strftime(time_format)))
        self.name_of_features = ["incident_count"]
        self.type_of_features = "categorical"
        self.set_null_counts_to_zero = True

class OfficerGender(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "male",
                            2: "female" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer gender, categorical")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_characteristics "
                      "         WHERE staging.officer_characteristics.gender_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class OfficerAge(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer age in years")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.age "
                      "FROM (   SELECT officer_id, extract(day from '{2}'::timestamp - date_of_birth)/365 AS age "
                      "         FROM staging.officers_hub"
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True

class OfficerRace(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer race, categorical")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officers_hub "
                      "         WHERE staging.officers_hub.race_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class OfficerEthnicity(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "non_hispanic",
                            2: "hispanic" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer ethnicity, categorical")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officers_hub "
                      "         WHERE staging.officers_hub.ethnicity_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class OfficerRank(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = {
                0: "Civilian",
                1: "Police Officer Trainee",
                2: "Police Officer",
                3: "Sergeant",
                4: "Master Patrolman",
                5: "Captain",
                6: "Lietenant",
                7: "Police Commander",
                8: "Deputy Chief",
                9: "Chief of Police" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer Rank")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_roles "
                      "         WHERE staging.officer_roles.rank_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class OfficerEducation(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "less_than_high_school",
                            1: "high_school",
                            2: "some_college",
                            3: "two_year_degree",
                            4: "four_year_degree",
                            5: "graduate_degree" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer Education")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_characteristics "
                      "         WHERE staging.officer_characteristics.education_level_code = {2} "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class AcademyScore(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Officer's score at the police academy")
        self.num_features = 1
        self.name_of_features = ["AcademyScore"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.score "
                      "FROM (   SELECT officer_id, score "
                      "         FROM staging.officer_trainings "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name ) )

class DivorceCount(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of divorces for the officer")
        self.num_features = 1
        self.name_of_features = ["DivorceCount"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.officer_marital "
                      "         WHERE marital_status_code = 4 "
                      "         AND last_modified <= '{}'::date "
                      "         GROUP BY officer_id ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )
        self.set_null_counts_to_zero = True

class MilesFromPost(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of miles that the officer lives from the post")
        self.num_features = 1
        self.name_of_features = ["MilesFromPost"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.miles_to_assignment "
                      "FROM (   SELECT officer_id, miles_to_assignment "
                      "         FROM staging.officer_addresses "
                      "         WHERE last_modified <= '{}'::date ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format) ) )

##############
## Field interview features
##############
class NumOfFieldInterviews(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of field interviews within a time window")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.field_interviews "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

# TODO: This is a little strange -- average hour isn't very meaningful with
# transitions around midnight... but it's how 2015 did it.
class AvgHourOfFieldInterviews(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average hour that field interviews are conducted")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.avg_hour "
                      "FROM (   SELECT officer_id, avg(date_part('hour',event_datetime)-12) as avg_hour"
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code = 2 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True # This is also strange, but again, matches 2015

class ModeHourOfFieldInterviews(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average hour that field interviews are conducted")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.mode "
                      "FROM (   SELECT officer_id, mode() within group (order by date_part('hour',event_datetime)-12) as mode"
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code = 2 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfFieldInterviewsByRace(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of field interviews within a time window, by race")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.field_interviews "
                      "         WHERE field_interviews.interviewed_person_race = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionOfFieldInterviewsByRace(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of field interviews within a time window, by race")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::Float / staging_table.total::Float "
                      "FROM (   SELECT officer_id, count(interviewed_person_race) as total, sum((interviewed_person_race = {4})::INT) as count"
                      "         FROM staging.field_interviews "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfFieldInterviewsByOutcome(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Arrest",
                            3: "Ban",
                            4: "None" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of field interviews within a time window, by outcome")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.field_interviews "
                      "         WHERE field_interviews.field_interview_outcome_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionOfFieldInterviewsByOutcome(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Arrest",
                            3: "Ban",
                            4: "None" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of field interviews within a time window, by outcome")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::Float / staging_table.total::Float "
                      "FROM (   SELECT officer_id, count(field_interviews.event_id) as total, sum((field_interview_outcome_code = {4})::INT) as count"
                      "         FROM staging.field_interviews "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfFieldInterviewsWithFlag(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 'searched_flag': "Did search",
                            'drugs_found_flag': "Found drugs",
                            'weapons_found_flag': "Found weapons" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of field interviews with a particular feature within a time window")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.field_interviews "
                      "         WHERE {4}"
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionOfFieldInterviewsWithFlag(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 'searched_flag': "Did search",
                            'drugs_found_flag': "Found drugs",
                            'weapons_found_flag': "Found weapons" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of field interviews with a search within a time window")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count({4}) as total, sum({4}::INT) as count"
                      "         FROM staging.field_interviews "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class ArrestCount(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of arrests an officer has made, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=3 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class MeanHoursPerShift(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of hours worked on a shift on average")
        self.num_features = 1
        self.name_of_features = ["MeanHoursPerShift"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.avg "
                      "FROM (   SELECT officer_id, AVG( EXTRACT( EPOCH from shift_length)/3600 )"
                      "         FROM staging.officer_shifts "
                      "         WHERE EXTRACT( EPOCH from shift_length)/3600 < 48 " # Discarding tremendous outliers (bad data).
                      "         AND stop_datetime < '{}'::date "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name,
                                self.fake_today.strftime(time_format)))

class SustainedRuleViolations(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of sustained rule violation over time")
        self.num_features = 1
        self.name_of_features = ["SustainedRuleViolations"]
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, sum(number_of_sustained_allegations) as count "
                      "         FROM staging.incidents "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class AllAllegations(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of allegations, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, sum(number_of_allegations) as count "
                      "         FROM staging.incidents "
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      # the following line must be removed when not-sworn officers are removed. GIANT HACK
                      "         AND officer_id IS NOT null "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumberOfIncidentsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = {
                            0: 'accident',
                            1: 'appearance',
                            2: 'bias_or_profiling',
                            3: 'chain_of_command',
                            4: 'conditions_of_employment',
                            5: 'conformance_to_rules',
                            6: 'courtesy_and_behaviour',
                            7: 'equipment',
                            8: 'gift_policy',
                            9: 'handling_of_civilians',
                            10: 'harassment_or_intimidation',
                            11: 'officer_injury',
                            12: 'promptness_or_absence',
                            13: 'pursuit',
                            14: 'quality_of_work',
                            15: 'raid',
                            16: 'standard_procedures',
                            17: 'substance_abuse',
                            18: 'traffic_laws',
                            19: 'unknown',
                            20: 'use_of_force'}
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Incident type categorical feature, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.incidents "
                      "         WHERE staging.incidents.grouped_incident_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class ComplaintToArrestRatio(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Ratio of complaints to arrests and officer has")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.complaintdensity "
                      "FROM ( SELECT num_arrests.officer_id, num_complaints.count/num_arrests.count complaintdensity FROM "
                      "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 3 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_arrests "
                        "full outer join "
                        "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 4 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_complaints "
                        "ON num_arrests.officer_id = num_complaints.officer_id "
                      ") AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

####################
### EIS FEATURES ###
####################

class TotalEISInterventionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Unknown",
                            1: "Counseling",
                            2: "Training",
                            3: "Suspension",
                            4: "Termination",
                            5: "Reprimand",
                            6: "Loss of vacation",
                            7: "No intervention required",
                            8: "Reassignment",
                            9: "Demotion" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Total interventions of each type an officer has received as a result of an EIS flag")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.department_eis_alerts eis "
                      "         WHERE eis.intervention_type = {4} "
                      "         AND date_created <= '{2}'::date "
                      "         AND date_created >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionEISFlagsWithIntervention(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of EIS flags that required interventions")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count(officer_id) as total, sum((intervention_type != 7)::INT) as count"
                      "         FROM staging.department_eis_alerts eis "
                      "         WHERE date_created <= '{2}'::date "
                      "         AND date_created >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class TotalEISFlagsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'Accident',
                            1: 'Complaint',
                            2: 'Injury',
                            3: 'Pursuit',
                            4: 'Sick Leave Frequency',
                            5: 'Sick Leave or Days Off',
                            6: 'Supv Initiated',
                            7: 'Use Of Force',
                            8: 'Combination',
                            9: 'Other' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Total interventions of each type an officer has received as a result of an EIS flag")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.department_eis_alerts eis "
                      "         WHERE eis.event_type = {4} "
                      "         AND date_created <= '{2}'::date "
                      "         AND date_created >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionEISFlagsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'Accident',
                            1: 'Complaint',
                            2: 'Injury',
                            3: 'Pursuit',
                            4: 'Sick Leave Frequency',
                            5: 'Sick Leave or Days Off',
                            6: 'Supv Initiated',
                            7: 'Use Of Force',
                            8: 'Combination',
                            9: 'Other' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of interventions of each type an officer has received as a result of an EIS flag")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count(officer_id) as total, sum((event_type = {4})::INT) as count "
                      "         FROM staging.department_eis_alerts eis "
                      "         WHERE date_created <= '{2}'::date "
                      "         AND date_created >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class OfficerMilitary(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Whether or not officer has had military experience")
        self.num_features = 1
        self.name_of_features = ["OfficerMilitary"]
        self.query = ("UPDATE features.{} feature_table "
                      "SET {} = staging_table.military_service_flag "
                      "FROM (   SELECT officer_id, military_service_flag::int "
                      "         FROM staging.officer_characteristics "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      .format(  self.table_name,
                                self.feature_name ) )
#######
## Extra duty
#######
class TotalOutsideEmploymentHours(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The total number of hours of extra duty the officer has worked (outside employment)")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.sum "
                      "FROM (   SELECT officer_id, sum(hours_on_shift) "
                      "         FROM staging.officer_outside_employment "
                      "         WHERE date_time <= '{2}'::date "
                      "         AND date_time >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                              self.COLUMN,
                              self.fake_today.strftime(time_format),
                              self.DURATION ))
        self.set_null_counts_to_zero = True

class ComplaintsPerHourWorked(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The rate of complaints per hour worked")
        self.query = ("UPDATE features.{0} feature_table "
            "SET {1} = staging_table.complaintstohours "
            "FROM ( SELECT  hours_worked.officer_id, num_complaints.count/hours_worked.hours complaintstohours FROM "
            "(SELECT officer_id, SUM( EXTRACT( EPOCH from shift_length)/3600 ) hours "
            "FROM staging.officer_shifts "
            "WHERE EXTRACT( EPOCH from shift_length)/3600 < 48 " # Discarding tremendous outliers (bad data).
            "AND stop_datetime < '{2}'::date "
            "GROUP BY officer_id) hours_worked "
            "full outer join "
            "(SELECT officer_id, COUNT(officer_id)::float "
            "FROM staging.events_hub "
            "WHERE event_type_code = 4 "
            "AND event_datetime <= '{2}'::date "
            "AND event_datetime >= '{2}'::date - interval '{3}' "
            "GROUP by officer_id) num_complaints "
            "ON hours_worked.officer_id = num_complaints.officer_id "
            ") AS staging_table "
            "WHERE feature_table.officer_id = staging_table.officer_id "
            "AND feature_table.fake_today = '{2}'::date"
            .format(  self.table_name,
                    self.COLUMN,
                    self.fake_today.strftime(time_format),
                    self.DURATION ))
        self.set_null_counts_to_zero = True

class UOFtoArrestRatio(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Ratio of uses of force per arrest ratio, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.uofdensity "
                      "FROM ( SELECT num_arrests.officer_id, num_uof.count/num_arrests.count uofdensity FROM "
                      "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 3 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_arrests "
                        "full outer join "
                        "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 7 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_uof "
                        "ON num_arrests.officer_id = num_uof.officer_id "
                      ") AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfUsesOfForceOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Taser dart or stun",
                            1: "Firearm",
                            2: "Canine",
                            3: "Pepper spray",
                            4: "Any other use of force" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of uses of force by type, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.use_of_force "
                      "         INNER JOIN staging.events_hub "
                      "         ON use_of_force.event_id = events_hub.event_id "
                      "         WHERE staging.use_of_force.use_of_force_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class CountUOFwithSuspectInjury(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "false": "False no suspect injured",
                            "true": "True suspect injured" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of uses of force by whether the suspect was injured, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.use_of_force "
                      "         INNER JOIN staging.events_hub "
                      "         ON use_of_force.event_id = events_hub.event_id "
                      "         WHERE staging.use_of_force.suspect_injury is {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class SuspectInjuryToUOFRatio(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Ratio of suspect injuries to uses of force that an officer has, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.uofdensity "
                      "FROM ( SELECT num_suspect_injuries.officer_id, num_suspect_injuries.count/num_uof.count uofdensity FROM "
                      "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.use_of_force "
                        "INNER JOIN staging.events_hub "
                        "ON use_of_force.event_id = events_hub.event_id "
                        "WHERE staging.use_of_force.suspect_injury is true "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_suspect_injuries "
                        "full outer join "
                        "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 7 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_uof "
                        "ON num_suspect_injuries.officer_id = num_uof.officer_id "
                      ") AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class CountUOFwithResistingArrest(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "false": "False suspect did not resist",
                            "true": "True suspect resisted" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of uses of force by whether the suspect resisted arrest, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.use_of_force "
                      "         INNER JOIN staging.events_hub "
                      "         ON use_of_force.event_id = events_hub.event_id "
                      "         WHERE staging.use_of_force.in_response_to_resisting_arrest is {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class ResistingArrestToUOFRatio(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Ratio of resisting arrest to uses of force that an officer has, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.uofdensity "
                      "FROM ( SELECT num_resisting.officer_id, num_resisting.count/num_uof.count uofdensity FROM "
                      "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.use_of_force "
                        "INNER JOIN staging.events_hub "
                        "ON use_of_force.event_id = events_hub.event_id "
                        "WHERE staging.use_of_force.in_response_to_resisting_arrest is true "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_resisting "
                        "full outer join "
                        "(SELECT officer_id, COUNT(officer_id)::float "
                        "FROM staging.events_hub "
                        "WHERE event_type_code = 7 "
                        "AND event_datetime <= '{2}'::date "
                        "AND event_datetime >= '{2}'::date - interval '{3}' "
                        "GROUP by officer_id) num_uof "
                        "ON num_resisting.officer_id = num_uof.officer_id "
                      ") AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfUnjustifiedUsesOfForceOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Taser dart or stun",
                            1: "Firearm",
                            2: "Canine",
                            3: "Pepper spray",
                            4: "Any other use of force" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of uses of unjustified force by type, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) FROM "
                              "( SELECT incidents.event_id, use_of_force_type_code  "
                              "	FROM staging.use_of_force "
                              "	INNER JOIN staging.incidents "
                              "	ON use_of_force.event_id = incidents.event_id  "
                              "	WHERE number_of_unjustified_allegations >0 ) "
                      "AS unjustified_force "
                      "INNER JOIN staging.events_hub "
                      "ON unjustified_force.event_id=events_hub.event_id "
                      "         WHERE unjustified_force.use_of_force_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class UnjustUOFInterventionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Unknown",
                            1: "Counseling",
                            2: "Training",
                            3: "Suspension",
                            4: "Termination",
                            5: "Reprimand",
                            6: "Loss of vacation",
                            7: "No intervention required" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of interventions of type X following an unjustified force, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) FROM "
                              "( SELECT incidents.event_id, intervention_type_code  "
                              "	FROM staging.use_of_force "
                              "	INNER JOIN staging.incidents "
                              "	ON use_of_force.event_id = incidents.event_id  "
                              "	WHERE number_of_unjustified_allegations >0 ) "
                      "AS unjustified_force "
                      "INNER JOIN staging.events_hub "
                      "ON unjustified_force.event_id=events_hub.event_id "
                      "         WHERE unjustified_force.intervention_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class UOFInterventionsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "Unknown",
                            1: "Counseling",
                            2: "Training",
                            3: "Suspension",
                            4: "Termination",
                            5: "Reprimand",
                            6: "Loss of vacation",
                            7: "No intervention required" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of interventions of type X following any use of force, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) FROM "
                              "( SELECT incidents.event_id, intervention_type_code  "
                              "	FROM staging.use_of_force "
                              "	INNER JOIN staging.incidents "
                              "	ON use_of_force.event_id = incidents.event_id ) "
                      "AS any_force "
                      "INNER JOIN staging.events_hub "
                      "ON any_force.event_id=events_hub.event_id "
                      "         WHERE any_force.intervention_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class PreventableAccidents(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of preventable accidents an officer has had, time-gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.incidents"
                      "         WHERE staging.incidents.grouped_incident_type_code=0 "
		      "		AND staging.incidents.number_of_preventable_allegations > 0" # NB!: does not account for multiple accidents that occurred at the same event
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True


# =====================================
# = Features related to traffic stops =
# =====================================

class NumOfTrafficStops(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code=1 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithSearch(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where a search occurred")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.searched_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithUseOfForce(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where force was used")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.use_of_force_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithArrest(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where an arrest was made")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.arrest_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithInjury(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where a driver or passenger was injured")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.injuries_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithOfficerInjury(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where the officer was injured")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.officer_injury_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsWithSearchRequest(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The number of traffic stops an officer has made where the officer requested consent for a search")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.search_consent_request_flag=true "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class FractionOfTrafficStopsWithSearchRequest(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("The fraction of traffic stops an officer has made where the officer requested consent for a search")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count(search_consent_request_flag) as total, sum(search_consent_request_flag::INT) as count"
                      "         FROM staging.traffic_stops"
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsByRace(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: "unknown",
                            1: "black",
                            2: "white",
                            3: "american_indian",
                            4: "asian",
                            5: "pacific_islander",
                            6: "other",
                            7: "mixed" }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stops made by race, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.stopped_person_race_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsByStopType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'investigative',
                            1: 'dwi',
                            2: 'parking_violation',
                            3: 'safety_violation',
                            4: 'regulatory_violation',
                            5: 'seatbelt_violation',
                            6: 'moving_violation',
                            7: 'equipment_violation',
                            8: 'checkpoint',
                            9: 'other' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stops made by the type of stop, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.stop_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsByStopResult(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'Arrest',
                            1: 'Citation',
                            2: 'Verbal warning',
                            3: 'Written warning',
                            4: 'Nothing' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stops made by the stop outcome, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.stop_outcome_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsBySearchReason(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'Consent',
                            1: 'Prob Cause',
                            2: 'Frisk',
                            3: 'Arrest',
                            4: 'Warrent' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stop searches made by the search justification, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.search_justification_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class FractionOfTrafficStopsBySearchReason(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0: 'Consent',
                            1: 'Prob Cause',
                            2: 'Frisk',
                            3: 'Arrest',
                            4: 'Warrent' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stop searches made by the search justification, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count(officer_id) as total, sum((search_justification_code = {4})::INT) as count"
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.searched_flag "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class NumOfTrafficStopsByInterestingSearchText(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        # This is a bit of a hack of the categorical officer feature to simply look up interesting* words
        # (* as deemed interesting by the 2015 team)
        self.categories = { 'crime': 'crime',
                            'suspicious': 'suspicious',
                            'marijuana': 'marijuana',
                            'consent': 'consent',
                            'all my stops': 'all my stops',
                            'area': 'area',
                            'drug': 'drug' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of traffic stop searches made by interesting words in the search justification narrative, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id)"
                      "         FROM staging.traffic_stops"
                      "         WHERE staging.traffic_stops.search_justification_narrative like '%%{4}%%' "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True




class FractionOfTrafficStopsByInterestingSearchText(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        # This is a bit of a hack of the categorical officer feature to simply look up interesting* words
        # (* as deemed interesting by the 2015 team)
        self.categories = { 'crime': 'crime',
                            'suspicious': 'suspicious',
                            'marijuana': 'marijuana',
                            'consent': 'consent',
                            'all my stops': 'all my stops',
                            'area': 'area',
                            'drug': 'drug' }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Fraction of traffic stop searches made by interesting words in the search justification narrative, time-gated periods")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count::FLOAT / staging_table.total::FLOAT "
                      "FROM (   SELECT officer_id, count(search_justification_narrative) as total, sum((search_justification_narrative like '%%{4}%%')::INT) as count"
                      "         FROM staging.traffic_stops"
                      "         WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date "
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

###############################################
##### Features for threshold-based EIS modeled
##### on CMPD's old EIS system
###############################################

class ThresholdAccidentFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 2 accidents in the past 180 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(grouped_incident_type_code) >= 2 as flag
                   FROM staging.incidents
                   WHERE staging.incidents.grouped_incident_type_code in (0,7)
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '180 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True

class ThresholdUOFFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 3 UOF incidents in the past 90 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(grouped_incident_type_code) >= 3 as flag
                   FROM staging.incidents
                   WHERE staging.incidents.grouped_incident_type_code = 20
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '90 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True

class ThresholdComplaintFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 3 accidents in the past 180 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(origination_type_code) >= 3 as flag
                   FROM staging.incidents
                   WHERE staging.incidents.origination_type_code in (0) -- this might also need to include type 1
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '180 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True

class ThresholdSickLeaveFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 3 sick leave days in the past 120 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(shift_type_code) >= 3 as flag
                   FROM staging.officer_shifts
                   WHERE officer_shifts.shift_type_code in (4, 13, 16, 28, 29)
                   AND start_datetime <= '{2}'::date
                   AND start_datetime >= '{2}'::date - interval '90 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True

class ThresholdInjuryFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 2 injuries in the past 180 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(grouped_incident_type_code) >= 2 as flag
                   FROM staging.incidents
                   WHERE staging.incidents.grouped_incident_type_code in (11)
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '180 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True


class ThresholdPursuitsFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 2 pursuits in the past 180 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT officer_id, count(grouped_incident_type_code) >= 2 as flag
                   FROM staging.incidents
                   WHERE staging.incidents.grouped_incident_type_code in (13)
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '180 days'
                   AND officer_id IS NOT null
                   GROUP BY officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True


class ThresholdCombinedFlag(abstract.OfficerFeature):
    def __init__(self, **kwargs):
        abstract.OfficerFeature.__init__(self, **kwargs)
        self.description = ("Flag if there have been more than 5 incidents or sick days in the past 180 days")
        self.query = ("""
            UPDATE features.{0} feature_table
            SET {1} = staging_table.flag::int
            FROM ( SELECT incidents.officer_id, count(grouped_incident_type_code) >= 5 as flag
                   FROM staging.incidents
                   inner join staging.officer_shifts
                   on incidents.officer_id = officer_shifts.officer_id
                   WHERE (
					staging.incidents.origination_type_code in (0)
					or
					staging.incidents.grouped_incident_type_code in (11)
					or
					staging.incidents.grouped_incident_type_code in (0,7)
					or
					staging.incidents.grouped_incident_type_code in (13)
					or
					staging.incidents.grouped_incident_type_code = 20
					or
					officer_shifts.shift_type_code in (4, 13, 16, 28, 29)
                   )
                   AND event_datetime <= '{2}'::date
                   AND event_datetime >= '{2}'::date - interval '180 days'
                   AND incidents.officer_id IS NOT null
                   GROUP BY incidents.officer_id
               ) AS staging_table
            WHERE feature_table.officer_id = staging_table.officer_id
            AND feature_table.fake_today = '{2}'::date
            """.format(self.table_name,
                    self.feature_name,
                    self.fake_today.strftime(time_format)))
        self.set_null_counts_to_zero = True


class DispatchTypeCount(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { 0 : '911',
                            2:  'theft',
                            3:  'domestic_disturb',
                            4:  'person_wweapon',
                            5:  'assault',
                            6:  'shooting',
                            7:  'suicide',
                            8:  'kidnap',
                            9:  'disorder',
                            10:  'rape_related',
                            11:  'safety_hazard',
                            12:  'want_officer',
                            13:  'criminal_vice',
                            14:  'intoxicated',
                            15:  'missing_person',
                            16:  'prisioner_escapee',
                            17:  'gang_activity',
                            18:  'injury',
                            19:  'riot',
                            20:  'suspucious',
                            21:  'stabbing',
                            22:  'other'
                            }
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of dispatches of different type aggregated over time")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.dispatches "
                      "         WHERE dispatch_type_code = {4} "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class PriorSustainedUnknownMajorAllegationsCount(abstract.TimeGatedOfficerFeature):
        def __init__(self, **kwargs):
            abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
            self.description = ("Number of prior sustained unknown major allegations per officer, time gated")
            self.query = ("UPDATE features.{0} feature_table "
                          "SET {1} = staging_table.count "
                          "FROM (   SELECT officer_id, count(officer_id) "
                          "         FROM staging.incidents "
                          "         WHERE event_datetime <= '{2}'::date "
                          "         AND event_datetime >= '{2}'::date - interval '{3}'"
                          "         AND (grouped_incident_type_code in ( 0, 2, 3, 4, 8, 9, 10, 11, 17, 20 ) "
                          "         AND final_ruling_code in (0, 1, 4, 5 ))"
                          "         GROUP BY officer_id "
                          "         ) AS staging_table"
                          " WHERE feature_table.officer_id = staging_table.officer_id"
                          " AND feature_table.fake_today = '{2}'::date"
                          .format(  self.table_name,
                                    self.COLUMN,
                                    self.fake_today.strftime(time_format),
                                    self.DURATION))
            self.set_null_counts_to_zero = True

class PriorMajorAllegationsCount(abstract.TimeGatedOfficerFeature):
        def __init__(self, **kwargs):
            abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
            self.description = ("Number of prior major allegations per officer, time gated")
            self.query = ("UPDATE features.{0} feature_table "
                          "SET {1} = staging_table.count "
                          "FROM (   SELECT officer_id, count(officer_id) "
                          "         FROM staging.incidents "
                          "         WHERE event_datetime <= '{2}'::date "
                          "         AND event_datetime >= '{2}'::date - interval '{3}'"
                          "         AND (grouped_incident_type_code in ( 0, 2, 3, 4, 8, 9, 10, 11, 17, 20 )) "
                          "         GROUP BY officer_id "
                          "         ) AS staging_table"
                          " WHERE feature_table.officer_id = staging_table.officer_id"
                          " AND feature_table.fake_today = '{2}'::date"
                          .format(  self.table_name,
                                    self.COLUMN,
                                    self.fake_today.strftime(time_format),
                                    self.DURATION))
            self.set_null_counts_to_zero = True

class PriorSustainedUnkownMinorAllegationsCount(abstract.TimeGatedOfficerFeature):
        def __init__(self, **kwargs):
            abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
            self.description = ("Number of prior sustained unknown minor allegations per officer, time gated")
            self.query = ("UPDATE features.{0} feature_table "
                          "SET {1} = staging_table.count "
                          "FROM (   SELECT officer_id, count(officer_id) "
                          "         FROM staging.incidents "
                          "         WHERE event_datetime <= '{2}'::date "
                          "         AND event_datetime >= '{2}'::date - interval '{3}'"
                          "         AND (grouped_incident_type_code in ( 1, 6, 16, 18, 12, 7, 14 ) "
                          "         AND final_ruling_code in (0, 1, 4, 5 ))"
                          "         GROUP BY officer_id "
                          "         ) AS staging_table"
                          " WHERE feature_table.officer_id = staging_table.officer_id"
                          " AND feature_table.fake_today = '{2}'::date"
                          .format(  self.table_name,
                                    self.COLUMN,
                                    self.fake_today.strftime(time_format),
                                    self.DURATION))
            self.set_null_counts_to_zero = True

class PriorMinorAllegationsCount(abstract.TimeGatedOfficerFeature):
        def __init__(self, **kwargs):
            abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
            self.description = ("Number of prior minor allegations per officer, time gated")
            self.query = ("UPDATE features.{0} feature_table "
                          "SET {1} = staging_table.count "
                          "FROM (   SELECT officer_id, count(officer_id) "
                          "         FROM staging.incidents "
                          "         WHERE event_datetime <= '{2}'::date "
                          "         AND event_datetime >= '{2}'::date - interval '{3}'"
                          "         AND (grouped_incident_type_code in ( 1, 6, 16, 18, 12, 7, 14 )) "
                          "         GROUP BY officer_id "
                          "         ) AS staging_table"
                          " WHERE feature_table.officer_id = staging_table.officer_id"
                          " AND feature_table.fake_today = '{2}'::date"
                          .format(  self.table_name,
                                    self.COLUMN,
                                    self.fake_today.strftime(time_format),
                                    self.DURATION))
            self.set_null_counts_to_zero = True


class NumOfUnjustifiedUsesOfForce(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of unjustified use of force, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "         SET {1} = staging_table.count"
                      "         FROM (SELECT officer_id, count(officer_id)"
                      "         FROM staging.incidents "
                      "         WHERE staging.incidents.grouped_incident_type_code = 20 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         AND number_of_unjustified_allegations > 0 "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True



class CountComplaintsTypeSource(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = {
                             1 : 'internal',
                             2 : 'external'}
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of complaints by type an officer had, time gated")
        self.query = ("""UPDATE features.{0} feature_table """
                      """SET {1} = staging_table.count """
                      """FROM (   SELECT officer_id, count(officer_id) """
                      """         FROM staging.events_hub """
                      """         JOIN staging.incidents """
                      """         ON events_hub.event_id = incidents.event_id"""
                      """         WHERE event_type_code = 6 """
                      """         AND event_datetime <= '{2}'::date """
                      """         AND event_datetime >= '{2}'::date - interval '{3}' """
                      """         AND origination_type_code = {4} """
                      """         GROUP BY officer_id """
                      """     ) AS staging_table """
                      """WHERE feature_table.officer_id = staging_table.officer_id """
                      """AND feature_table.fake_today = '{2}'::date """
                      .format( self.table_name,
                               self.COLUMN,
                               self.fake_today.strftime(time_format),
                               self.DURATION,
                               self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True


class ComplaintsCount(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of complaints an officer had, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT officer_id, count(officer_id) "
                      "         FROM staging.events_hub "
                      "         WHERE event_type_code = 6 "
                      "         AND event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class SustainedComplaintsCount(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Number of complaints an officer had, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT incidents.officer_id, count(incidents.officer_id) "
                      "         FROM staging.events_hub "
                      "         JOIN staging.incidents "
                      "         ON staging.events_hub.event_id = staging.incidents.event_id "
                      "         WHERE events_hub.event_type_code=6 "
                      "         AND events_hub.event_datetime <= '{2}'::date "
                      "         AND events_hub.event_datetime >= '{2}'::date - interval '{3}' "
                      "         AND final_ruling_code in (1,4,5) "
                      "         GROUP BY incidents.officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION ))
        self.set_null_counts_to_zero = True

class NumberOfComplaintsOfType(abstract.TimeGatedCategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = {
                            0: 'accident',
                            1: 'appearance',
                            2: 'bias_or_profiling',
                            3: 'chain_of_command',
                            4: 'conditions_of_employment',
                            5: 'conformance_to_rules',
                            6: 'courtesy_and_behaviour',
                            7: 'equipment',
                            8: 'gift_policy',
                            9: 'handling_of_civilians',
                            10: 'harassment_or_intimidation',
                            11: 'officer_injury',
                            12: 'promptness_or_absence',
                            13: 'pursuit',
                            14: 'quality_of_work',
                            15: 'raid',
                            16: 'standard_procedures',
                            17: 'substance_abuse',
                            18: 'traffic_laws',
                            19: 'unknown',
                            20: 'use_of_force'}
        abstract.TimeGatedCategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Complaint type categorical feature, time gated")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.count "
                      "FROM (   SELECT incidents.officer_id, count(incidents.officer_id) "
                      "         FROM staging.incidents "
                      "         INNER JOIN staging.events_hub"
                      "         ON incidents.event_id = events_hub.event_id"
                      "         WHERE staging.incidents.grouped_incident_type_code = {4} "
                      "         AND event_type_code=6 "
                      "         AND events_hub.event_datetime <= '{2}'::date "
                      "         AND events_hub.event_datetime >= '{2}'::date - interval '{3}' "
                      "         GROUP BY incidents.officer_id "
                      "     ) AS staging_table "
                      "WHERE feature_table.officer_id = staging_table.officer_id "
                      "AND feature_table.fake_today = '{2}'::date"
                      .format(  self.table_name,
                                self.COLUMN,
                                self.fake_today.strftime(time_format),
                                self.DURATION,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

## IMPORTANT: Change to point to staging
class OfficerAvgNeighborhoodFeatures1(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "Population_Density_2013": "density",
                            "Age_of_Residents_2013": "age",
                            "Black_Population_2010": "black_population",
                            "311_Calls_Log_2013": "311_calls",
                            "Household_Income_Log_2013": "household_income",
                            "Employment_Rate_2013": "employment_rate",
                            "Vacant_Land_Area_Log_2013": "vacant_land",
                            "Voter_Participation_2012": "voter_participation" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average demographics of arrests for each officer")
        self.query = ("""UPDATE features.{0} feature_table """
                      """SET {1} = staging_table.avg """
                      """FROM (   SELECT officer_id, avg(var_used) """
                      """         FROM staging.events_hub """
                      """         JOIN temp_arrest_npa as geo """
                      """         on geo.event_id = events_hub.event_id"""
                      """         JOIN (SELECT "NPA", "{2}" as var_used"""
                      """               FROM mecklenburg.npa2014_part_1) b"""
                      """               ON npa = b."NPA" """
                      """       GROUP BY officer_id """
                      """     ) AS staging_table """
                      """WHERE feature_table.officer_id = staging_table.officer_id """
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

## IMPORTANT: Change to point to staging
class OfficerAvgNeighborhoodFeatures2(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "Age_of_Death_2012": "age_of_death",
                            "Housing_Density_2013": "housing_density",
                            "Nuisance_Violations_Total_2013": "nuisance_violations",
                            "Violent_Crime_Rate_2013": "crime_rate",
                            "Sidewalk_Availability_2013": "sidewalk_availability",
                            "Foreclosures_2013": "foreclosures",
                            "Disorder_Call_Rate_Log_2013": "disorder_call" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average demographics of arrests for each officer")
        self.query = ("""UPDATE features.{0} feature_table """
                      """SET {1} = staging_table.avg """
                      """FROM (   SELECT officer_id, avg(var_used) """
                      """         FROM staging.events_hub """
                      """         JOIN temp_arrest_npa as geo """
                      """         on geo.event_id = events_hub.event_id"""
                      """         JOIN (SELECT "NPA", "{2}" as var_used"""
                      """               FROM mecklenburg.npa2014_part_2) b"""
                      """               ON npa = b."NPA" """
                      """       GROUP BY officer_id """
                      """     ) AS staging_table """
                      """WHERE feature_table.officer_id = staging_table.officer_id """
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True


## IMPORTANT: Change to point to staging
class OfficerAvgNeighborhoodPatrolFeatures1(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "Population_Density_2013": "density",
                            "Age_of_Residents_2013": "age",
                            "Black_Population_2010": "black_population",
                            "311_Calls_Log_2013": "311_calls",
                            "Household_Income_Log_2013": "household_income",
                            "Employment_Rate_2013": "employment_rate",
                            "Vacant_Land_Area_Log_2013": "vacant_land",
                            "Voter_Participation_2012": "voter_participation" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average demographics of patrols for each officer")
        self.query = ("""UPDATE features.{0} feature_table """
                      """SET {1} = staging_table.avg """
                      """FROM (   WITH patrols as ( """
                      """         SELECT complnt FROM cmpd_merged.ods_cad_events where patrol = 'Y'), """
                      """          patrols_npa as ( """
                      """         SELECT dispatch_id, npa, "{2}" as var_used  from patrols"""
                      """         JOIN temp_dispatch_npa"""
                      """             ON complnt = temp_dispatch_npa.dispatch_id"""
                      """             JOIN mecklenburg.npa2014_part_1 """
                      """                ON "NPA" = npa) """
                      """       SELECT officer_id, avg(var_used) """
                      """       FROM  staging.events_hub  """
                      """       JOIN patrols_npa """
                      """           on patrols_npa.dispatch_id = events_hub.dispatch_id """
                      """       GROUP BY officer_id """
                      """     ) AS staging_table """
                      """WHERE feature_table.officer_id = staging_table.officer_id """
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True


## IMPORTANT: Change to point to staging
class OfficerAvgNeighborhoodPatrolFeatures2(abstract.CategoricalOfficerFeature):
    def __init__(self, **kwargs):
        self.categories = { "Age_of_Death_2012": "age_of_death",
                            "Housing_Density_2013": "housing_density",
                            "Nuisance_Violations_Total_2013": "nuisance_violations",
                            "Violent_Crime_Rate_2013": "crime_rate",
                            "Sidewalk_Availability_2013": "sidewalk_availability",
                            "Foreclosures_2013": "foreclosures",
                            "Disorder_Call_Rate_Log_2013": "disorder_call" }
        abstract.CategoricalOfficerFeature.__init__(self, **kwargs)
        self.description = ("Average demographics of patrols for each officer")
        self.query = ("""UPDATE features.{0} feature_table """
                      """SET {1} = staging_table.avg """
                      """FROM (   WITH patrols as ( """
                      """         SELECT complnt FROM cmpd_merged.ods_cad_events where patrol = 'Y'), """
                      """          patrols_npa as ( """
                      """         SELECT dispatch_id, npa, "{2}" as var_used  from patrols"""
                      """         JOIN temp_dispatch_npa"""
                      """             ON complnt = temp_dispatch_npa.dispatch_id"""
                      """             JOIN mecklenburg.npa2014_part_2 """
                      """                ON "NPA" = npa) """
                      """       SELECT officer_id, avg(var_used) """
                      """       FROM  staging.events_hub  """
                      """       JOIN patrols_npa """
                      """           on patrols_npa.dispatch_id = events_hub.dispatch_id """
                      """       GROUP BY officer_id """
                      """     ) AS staging_table """
                      """WHERE feature_table.officer_id = staging_table.officer_id """
                      .format(  self.table_name,
                                self.COLUMN,
                                self.LOOKUPCODE ))
        self.set_null_counts_to_zero = True

class ComplimentsToComplaintsRatio(abstract.TimeGatedOfficerFeature):
    def __init__(self, **kwargs):
        abstract.TimeGatedOfficerFeature.__init__(self, **kwargs)
        self.description = ("Ratio of internal compliments to complaints and officer has")
        self.query = ("UPDATE features.{0} feature_table "
                      "SET {1} = staging_table.ratio "
                      "FROM (  "
                      "   WITH compliments as ( "
                      "      SELECT  officer_id, COUNT(officer_id)::float as num_compliments "
                      "      FROM staging.events_hub as e "
                      "         JOIN staging.officer_compliments as c "
                      "            ON e.event_id = c.event_id "
                      "      WHERE event_datetime <= '{2}'::date "
                      "         AND event_datetime >= '{2}'::date - interval '{3}' "
                      "      GROUP BY officer_id),  "
                      "   complaints as ( "
                      "      SELECT officer_id, COUNT(officer_id)::float  as num_complaints "
                      "      FROM staging.events_hub "
                      "      WHERE event_type_code = 6 "
                      "        AND event_datetime <= '{2}'::date "
                      "        AND event_datetime >= '{2}'::date - interval '{3}' "
                      "     GROUP BY officer_id )"
                      "   SELECT compliments.officer_id, num_compliments/num_complaints "
                      "   FROM  compliments "
                      "   FULL OUTHER JOIN complaints "
                      "   ON compliments.officer_id = complaints.officer_id"
                      .format(self.table_name,
                              self.COLUMN,
                              self.fake_today.strftime(time_format),
                              self.DURATION ))
        self.set_null_counts_to_zero = True
           
