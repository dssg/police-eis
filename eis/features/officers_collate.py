#!/usr/bin/env python
import pdb
import logging
import sys
import yaml
import datetime
import sqlalchemy
from enum import Enum
import sqlalchemy.sql.expression as ex
from sqlalchemy.sql import Select

from .. import setup_environment
from . import abstract

# from collate import collate
from collate.collate import collate

log = logging.getLogger(__name__)
try:
    _, tables = setup_environment.get_database()
except:
    pass

time_format = "%Y-%m-%d %X"


# magic loops for generating certain conditions
class AllegationSeverity(Enum):
    major = "grouped_incident_type_code in ( 0, 2, 3, 4, 8, 9, 10, 11, 17, 20 )"
    minor = "grouped_incident_type_code in ( 1, 6, 16, 18, 12, 7, 14 )"
    unknown = ""


class AllegationOutcome(Enum):
    sustained = "final_ruling_code in (1, 4, 5 )"
    unsustained = ""
    unknown = "final_ruling_code = 0"


# Super class for feature generation
class FeaturesBlock():
    def __init__(self, **kwargs):

        self.lookback_durations = kwargs["lookback_durations"]
        self.unit_id = ""
        self.from_obj = ""
        self.date_column = ""
        self.prefix = ""
        self.sub_query = False

    def _lookup_values_conditions(self, engine, column_code_name, lookup_table, fix_condition='', prefix=''):
        query = """select code, value from staging.{0}""".format(lookup_table)
        lookup_values = engine.connect().execute(query)
        dict_temp = {}
        for code, value in lookup_values:
            if fix_condition:
                dict_temp[prefix + '_' + value] = "({0} = {1} AND {2})::int".format(column_code_name,
                                                                                    code,
                                                                                    fix_condition)
            else:
                dict_temp[prefix + '_' + value] = "({0} = {1})::int".format(column_code_name, code)
        return dict_temp

    def feature_aggregations_to_use(self, feature_list, engine):
        feature_aggregations = self._feature_aggregations(engine)
        feature_aggregations_to_use = []
        log.debug(feature_list)
        for feature in feature_list:
            try:
                feature_aggregations_to_use.append(feature_aggregations[feature])
            except KeyError:
                log.info("WARNING: no feature aggregation for feature: {}".format(feature))
                sys.exit(1)
        return feature_aggregations_to_use

    def _feature_aggregations(self, engine):
        return {}

    def _sub_query(self, space_time_aggregation):
        return {}

    def build_space_time_aggregation(self, engine, as_of_dates, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list, engine)
        st = collate.SpacetimeAggregation(feature_aggregations_list,
                                          from_obj=self.from_obj,
                                          groups={'off': self.unit_id},
                                          intervals=self.lookback_durations,
                                          dates=as_of_dates,
                                          date_column=self.date_column,
                                          prefix=self.prefix,
                                          output_date_column="as_of_date",
                                          schema=schema)
        st.execute(engine.connect())

    def build_aggregation(self, engine, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list, engine)
        st = collate.Aggregation(feature_aggregations_list,
                                 from_obj=self.from_obj,
                                 groups={'off': self.groups},
                                 prefix=self.prefix,
                                 schema=schema)
        st.execute(engine.connect())


# --------------------------
# REPORTED INCIDENTS: 
# Only considers when the incident is reported not the outcome
# -------------------------
class IncidentsReported(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = ex.text('officer_id')
        self.from_obj = ex.text('staging.incidents')
        self.date_column = "report_date"
        self.lookback_durations = kwargs["lookback_durations"]
        self.prefix = 'ir'

    def _feature_aggregations(self, engine):
        return {
            'InterventionsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='intervention_type_code',
                                               lookup_table='lookup_intervention_types',
                                               prefix='InterventionsOfType'), ['sum']),

            'IncidentsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='grouped_incident_type_code',
                                               lookup_table='lookup_incident_types',
                                               prefix='IncidentsOfType'), ['sum']),

            'ComplaintsTypeSource': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='origination_type_code',
                                               lookup_table='lookup_complaint_origins',
                                               prefix='ComplaintsTypeSource'), ['sum']),

            'SuspensionsOfType': collate.Aggregate(
                {"SuspensionsOfType_active": "(hours_active_suspension > 0)::int",
                 "SuspensionsOfType_inactive": "(hours_inactive_suspension > 0)::int"}, ['sum']),

            'HoursSuspensionsOfType': collate.Aggregate(
                {"HoursSuspensionsOfType_active": "hours_active_suspension",
                 "HoursSuspensionsOfType_inactive": "hours_inactive_suspension"}, ['sum']),

            'AllAllegations': collate.Aggregate(
                {"AllAllegations": "number_of_allegations"}, ['sum']),

            'IncidentsOfSeverity': collate.Aggregate(
                {"IncidentsOfSeverity_major": "({})::int".format(AllegationSeverity['major'].value),
                 "IncidentsOfSeverity_minor": "({})::int".format(AllegationSeverity['minor'].value)}, ['sum']),

            'IncidentsSeverityUnknown': collate.Aggregate(
                {"IncidentsSeverityUnknown_major": "({0} and {1})::int".format(
                    AllegationSeverity['major'].value, AllegationOutcome['unknown'].value),
                    "IncidentsSeverityUnknown_minor": "({} and {})::int".format(
                        AllegationSeverity['minor'].value, AllegationOutcome['unknown'].value)}, ['sum']),

            'Complaints': collate.Aggregate(
                {"Complaints": "(origination_type_code is not null)::int"}, ['sum']),

            'DaysSinceLastAllegation': collate.Aggregate(
                {"DaysSinceLastAllegation": "{date} - report_date"}, ['min'])

        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: COMPLETED INCIDENTS
# Consider the outcome of the incident
# -------------------------------------------------------
class IncidentsCompleted(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = ex.text('officer_id')
        self.from_obj = ex.text('staging.incidents')
        self.date_column = 'date_of_judgment'
        self.lookback_durations = kwargs["lookback_durations"]
        self.prefix = 'IC'

    def _feature_aggregations(self, engine):
        return {
            'IncidentsByOutcome': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='final_ruling_code',
                                               lookup_table='lookup_final_rulings',
                                               prefix='IncidentsByOutcome'), ['sum']),

            'MajorIncidentsByOutcome': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='final_ruling_code',
                                               lookup_table='lookup_final_rulings',
                                               fix_condition=AllegationSeverity['major'].value,
                                               prefix='MajorIncidentsByOutcome'), ['sum']),

            'MinorIncidentsByOutcome': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='final_ruling_code',
                                               lookup_table='lookup_final_rulings',
                                               fix_condition=AllegationSeverity['minor'].value,
                                               prefix='MinorIncidentsByOutcome'), ['sum']),

            'DaysSinceLastSustainedAllegation': collate.Aggregate(
                {"DaysSinceLastSustainedAllegation": "{} - date_of_judgment"}, ['min'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: SHIFTS
# --------------------------------------------------------
class OfficerShifts(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.officer_shifts'
        self.date_column = 'stop_datetime'
        self.prefix = 'shifts'

    def _feature_aggregations(self, engine):
        return {
            'ShiftsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='shift_type_code',
                                               lookup_table='lookup_shift_types',
                                               prefix='ShiftsOfType'), ['sum']),

            'HoursPerShift': collate.Aggregate(
                {'HoursPerShift': '(EXTRACT( EPOCH from shift_length)/3600)'}, ['avg'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: ARRESTS
# --------------------------------------------------------
class OfficerArrests(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.arrests'
        self.date_column = 'event_datetime'
        self.prefix = 'arrests'

    def _feature_aggregations(self, engine):
        return {
            'Arrests': collate.Aggregate(
                {"Arrests": 'event_id'}, ['count']),

            'ArrestsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='arrest_type_code',
                                               lookup_table='lookup_arrest_types',
                                               prefix='ArrestsOfType'), ['sum']),

            'ArrestsON': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='arrest_day_of_week',
                                               lookup_table='lookup_days_of_week',
                                               prefix='ArrestsON'), ['sum']),

            'SuspectsArrestedOfRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='suspect_race_code',
                                               lookup_table='lookup_races',
                                               prefix='SuspectsArrestedOfRace'), ['sum']),
            'SuspectsArrestedOfEthnicity': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='suspect_ethnicity_code',
                                               lookup_table='lookup_ethnicities',
                                               prefix='SuspectsArrestedOfEthnicity'), ['sum'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: ARRESTS
# --------------------------------------------------------
class OfficerArrestsStats(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = "" \
                        "(SELECT" \
                        "officer_id," \
                        "count(officer_id)                   AS count_officer" \
                        "date_trunc('month', event_datetime) AS event_datetime" \
                        "FROM staging.arrests" \
                        "WHERE {collate_date} " \
                        "GROUP BY officer_id, date_trunc('month', event_datetime)) AS sub_query"
        self.date_column = 'event_datetime'
        self.prefix = 'arstat'
        self.sub_query = True

    def _feature_aggregations(self, engine):
        return {
            'ArrestMonthlyVariance': collate.Aggregate(
                {"ArrestMonthlyVariance": 'count_officer'}, ['variance']),

        }

    # add a sub query to perform the pre aggregation step
    def _sub_query(self, space_time_aggregation):
        x = 1

        select_sub = collate.make_sql_clause(""
                                             "officer_id,"
                                             "count(officer_id)  AS count_officer,"
                                             "date_trunc('month', event_datetime) as event_datetime", ex.text)
        from_sub = collate.make_sql_clause('staging.arrests', ex.text)
        group_by_sub = collate.make_sql_clause(self.unit_id, ex.literal_column)

        for group_by, sels in space_time_aggregation.get_selects().items():
            for sel in sels:
                # dynamically add the where clause for the sub_query for speed optimization
                where = sel._whereclause  # type: Select
                sub_query = ex.select(columns=[select_sub], from_obj=from_sub) \
                    .where(where) \
                    .group_by(group_by_sub).alias('sub_queryX')

                sel.correlate_except(sub_query)
                log.debug('Query : {}'.format(sel))
                y = 1

        # st = collate.SpacetimeAggregation(feature_aggregations_list,
        #                                   from_obj=self.from_obj,
        #                                   group_intervals={self.unit_id: self.lookback_durations},
        #                                   dates=as_of_dates,
        #                                   date_column=self.date_column,
        #                                   prefix=self.prefix,
        #                                   output_date_column="as_of_date")

        x = 1

        pass


# --------------------------------------------------------
# BLOCK: TRAFFIC STOPS
# --------------------------------------------------------
class TrafficStops(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.traffic_stops'
        self.date_column = 'event_datetime'
        self.prefix = 'TS'

    def _feature_aggregations(self, engine):
        return {
            'TrafficStopsWithSearch': collate.Aggregate(
                {"TrafficStopsWithSearch": '(searched_flag = true)::int'}, ['sum']),

            'TrafficStopsWithUseOfForce': collate.Aggregate(
                {"TrafficStopsWithUseOfForce": '(use_of_force_flag = true)::int'}, ['sum']),

            'TrafficStops': collate.Aggregate(
                {"TrafficStops": 'event_id'}, ['count']),

            'TrafficStopsWithArrest': collate.Aggregate(
                {"TrafficStopsWithArrest": '(arrest_flag = true)::int'}, ['sum']),

            'TrafficStopsWithInjury': collate.Aggregate(
                {"TrafficStopsWithInjury": '(injuries_flag = true)::int'}, ['sum']),

            'TrafficStopsWithOfficerInjury': collate.Aggregate(
                {"TrafficStopsWithOfficerInjury": '(officer_injury_flag=true)::int'}, ['sum']),

            'TrafficStopsWithSearchRequest': collate.Aggregate(
                {"TrafficStopsWithSearchRequest": 'search_consent_request_flag::int'}, ['sum', 'avg']),

            'TrafficStopsByRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stopped_person_race_code',
                                               lookup_table='lookup_races',
                                               prefix='TrafficStopsByRace'), ['sum']),

            'TrafficStopsByStopType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stop_type_code',
                                               lookup_table='lookup_traffic_stop_type',
                                               prefix='TrafficStopsByStopType'), ['sum']),

            'TrafficStopsByStopResult': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stop_outcome_code',
                                               lookup_table='lookup_traffic_stop_outcome_type',
                                               prefix='TrafficStopsByStopResult'), ['sum']),

            'TrafficStopsBySearchReason': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='search_justification_code',
                                               lookup_table='lookup_search_justifications',
                                               prefix='TrafficStopsBySearchReason'), ['sum'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: FIELD INTERVIEWS
# --------------------------------------------------------
class FieldInterviews(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.field_interviews'
        self.date_column = 'event_datetime'
        self.prefix = 'fi'

    def _feature_aggregations(self, engine):
        return {
            'FieldInterviews': collate.Aggregate(
                {"FieldInterviews": 'event_id'}, ['sum']),

            'HourOfFieldInterviews': collate.Aggregate(
                {"HourOfFieldInterviews": "date_part('hour',event_datetime)-12"}, ['avg']),

            'FieldInterviewsByRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='interviewed_person_race',
                                               lookup_table='lookup_races',
                                               prefix='FieldInterviewsByRace'), ['sum', 'avg']),

            'FieldInterviewsByOutcome': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='field_interview_outcome_code',
                                               lookup_table='lookup_field_interview_outcomes',
                                               prefix='FieldInterviewsByOutcome'), ['sum', 'avg']),

            'FieldInterviewsWithFlag': collate.Aggregate(
                {"FieldInterviewsWithFlag_searched": 'searched_flag::int',
                 "FieldInterviewsWithFlag_drugs": 'drugs_found_flag::int',
                 "FieldInterviewsWithFlag_weapons": 'weapons_found_flag::int'}, ['sum', 'avg']),

            'InterviewsType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='field_interview_type_code',
                                               lookup_table='lookup_field_interview_types',
                                               prefix='InterviewsType'), ['sum'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: USE OF FORCE
# --------------------------------------------------------
class UseOfForce(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.use_of_force as u left join staging.incidents as i using (event_id, event_datetime, officer_id)'
        self.date_column = 'event_datetime'
        self.prefix = 'uof'

    def _feature_aggregations(self, engine):
        return {
            'UsesOfForceOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='use_of_force_type_code',
                                               lookup_table='lookup_use_of_force_type',
                                               prefix='UsesOfForceOfType'), ['sum']),

            'UnjustifiedUsesOfForceOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='use_of_force_type_code',
                                               lookup_table='lookup_use_of_force_type',
                                               fix_condition='number_of_unjustified_allegations >0',
                                               prefix='UnjustifiedUsesOfForceOfType'), ['sum']),

            'UnjustUOFInterventionsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='intervention_type_code',
                                               lookup_table='lookup_intervention_types',
                                               fix_condition='number_of_unjustified_allegations >0',
                                               prefix='UnjustUOFInterventionsOfType'), ['sum']),

            'OFwithSuspectInjury': collate.Aggregate(
                {"OFwithSuspectInjury": '(suspect_injury)::int'}, ['sum', 'avg'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: DISPATCHES
# --------------------------------------------------------
class Dispatches(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.dispatches'
        self.date_column = 'event_datetime'
        self.prefix = 'dispatch'

    def _feature_aggregations(self, engine):
        return {
            'DispatchType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='dispatch_type_code',
                                               lookup_table='lookup_dispatch_types',
                                               prefix='DispatchType'), ['sum']),

            'DispatchInitiatiationType': collate.Aggregate(
                {"DispatchInitiatiationType_ci": "(dispatch_category = 'CI')::int",
                 "DispatchInitiatiationType_oi": "(dispatch_category = 'OI')::int",
                 "DispatchInitiatiationType_oi": "(dispatch_category = 'AL')::int"}, ['sum'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: EIS ALERTS
# --------------------------------------------------------
class EISAlerts(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.department_eis_alerts'
        self.date_column = 'date_created'
        self.prefix = 'eis'

    def _feature_aggregations(self, engine):
        return {
            'EISInterventionsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='intervention_type',
                                               lookup_table='lookup_intervention_types',
                                               prefix='EISInterventionsOfType'), ['sum']),

            'EISFlagsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='event_type',
                                               lookup_table='lookup_eis_flag_types',
                                               prefix='EISFlagsOfType'), ['sum', 'avg']),
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: OFFICER CHARACTERISTICS
# --------------------------------------------------------
class OfficerCharacteristics(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.groups = 'officer_id'
        self.from_obj = ex.text('staging.officers_hub '
                                'left outer join staging.officer_characteristics '
                                '   using (officer_id) '
                                'left outer join staging.officer_trainings '
                                '   using (officer_id) '
                                'left outer join staging.officer_roles '
                                '   using (officer_id) ')
        self.prefix = 'oc'

    def _feature_aggregations(self, engine):
        return {
            'DummyOfficerGender': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='gender_code',
                                               lookup_table='lookup_genders',
                                               prefix='DummyOfficerGender'), ['max']),

            'DummyOfficerRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='race_code',
                                               lookup_table='lookup_races',
                                               prefix='DummyOfficerRace'), ['max']),

            'DummyOfficerEthnicity': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='ethnicity_code',
                                               lookup_table='lookup_ethnicities',
                                               prefix='DummyOfficerEthnicity'), ['max']),

            #'OfficerAge': collate.Aggregate(
            #{"OfficerAge": "extract(day from '{date}'::timestamp - date_of_birth)/365"}, ['max']),
            #
            'DummyOfficerEducation': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='education_level_code',
                                               lookup_table='lookup_education_levels',
                                               prefix='DummyOfficerEducation'), ['max']),

            'DummyOfficerMilitary': collate.Aggregate(
                {"DummyOfficerMilitary": 'military_service_flag::int'}, ['max']),

            'AcademyScore': collate.Aggregate(
                {"AcademyScore": 'score'}, ['max']),

            'DummyOfficerRank': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='rank_code',
                                               lookup_table='lookup_ranks',
                                               prefix='DummyOfficerRank'), ['max'])
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_aggregation(engine, feature_list, schema)
