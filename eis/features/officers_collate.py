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

from collate import collate

# from collate.collate import collate

log = logging.getLogger(__name__)

time_format = "%Y-%m-%d %X"


class AllegationOutcome(Enum):
    sustained = "final_ruling_code in (1, 4, 5 )"
    unsustained = "final_ruling_code in (2, 3, 6, 7, 8)"
    unknown = "final_ruling_code = 0"


# Super class for feature generation
class FeaturesBlock():
    def __init__(self, **kwargs):

        self.lookback_durations = kwargs["lookback_durations"]
        self.unit_id = ""
        self.from_obj = ""
        self.date_column = ""
        self.prefix_space_time = ""
        self.prefix_space_time_lookback = ""
        self.prefix_agg = ""
        self.prefix_sub = ""
        self.prefix_post = ""
        self.prefix = []
        self.join_table = None
        self.from_obj_sub = ""
        self.n_jobs = kwargs['n_cpus']

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

    def _group_category_conditions_str(self, engine, column_name, table, fix_condition='', prefix='', schema='staging'):
        query = """select {column_name} from {schema}.{table} GROUP BY {column_name} """.format(
            schema=schema,
            table=table,
            column_name=column_name)
        group_categories = engine.connect().execute(query)
        dict_temp = {}
        for row in group_categories:
            value = list(row)[0]

            name = value.strip().replace(' ', '_').lower()
            if fix_condition:
                dict_temp[prefix + '_' + name] = "({0} = '{1}' AND {2})::int".format(column_name,
                                                                                     value,
                                                                                     fix_condition)
            else:
                dict_temp[prefix + '_' + name] = "({0} = '{1}')::int".format(column_name, value)
        return dict_temp

    def feature_aggregations_to_use(self, feature_list, feature_aggregations):
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

    def _feature_aggregations_space_time_lookback(self, engine):
        return {}

    def _feature_aggregations_space_time(self, engine):
        return {}

    def _feature_aggregations_sub(self, engine):
        return {}

    def _sub_query(self):
        return {}

    def build_post_features(self, engine, feature_list, schema):
        return {}

    # time based aggregation with time intervals
    def build_space_time_aggregation_lookback(self, engine, as_of_dates, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list,
                                                                     self._feature_aggregations_space_time_lookback(
                                                                         engine))
        st = collate.SpacetimeAggregation(feature_aggregations_list,
                                          from_obj=self.from_obj,
                                          groups={'id': self.unit_id},
                                          intervals=self.lookback_durations,
                                          dates=as_of_dates,
                                          date_column=self.date_column,
                                          prefix=self.prefix_space_time_lookback,
                                          output_date_column="as_of_date",
                                          schema=schema)

        st.execute_par(setup_environment.get_database, self.n_jobs)

    # time based aggregation without time intervals
    def build_space_time_aggregation(self, engine, as_of_dates, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list,
                                                                     self._feature_aggregations_space_time(
                                                                         engine))
        st = collate.SpacetimeAggregation(feature_aggregations_list,
                                          from_obj=self.from_obj,
                                          groups={'id': self.unit_id},
                                          intervals={'id': ["all"]},
                                          dates=as_of_dates,
                                          date_column=self.date_column,
                                          prefix=self.prefix_space_time,
                                          output_date_column="as_of_date",
                                          schema=schema)
        st.execute_par(setup_environment.get_database, self.n_jobs)

    # time based aggregation with time intervals and a sub query
    def build_space_time_sub_query_aggregation(self, engine, as_of_dates, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list,
                                                                     self._feature_aggregations_sub(engine))
        st = collate.SpacetimeSubQueryAggregation(feature_aggregations_list,
                                                  from_obj=self.from_obj_sub,
                                                  groups={'id': self.unit_id},
                                                  intervals=self.lookback_durations,
                                                  dates=as_of_dates,
                                                  date_column=self.date_column,
                                                  prefix=self.prefix_sub,
                                                  output_date_column="as_of_date",
                                                  schema=schema,
                                                  sub_query=self._sub_query(),
                                                  join_table=self.join_table)

        st.execute_par(setup_environment.get_database, self.n_jobs)

    # time based aggregation with time intervals and a sub query
    def build_aggregation(self, engine, feature_list, schema):
        feature_aggregations_list = self.feature_aggregations_to_use(feature_list, self._feature_aggregations(engine))
        st = collate.Aggregation(feature_aggregations_list,
                                 from_obj=self.from_obj,
                                 groups={'id': self.unit_id},
                                 prefix=self.prefix_agg,
                                 schema=schema)
        st.execute_par(setup_environment.get_database, self.n_jobs)

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        # check if a space-time feature was selected with lookback
        list_space_time_lookback = [x for x in feature_list if
                                    x in set(self._feature_aggregations_space_time_lookback(engine).keys())]
        if list_space_time_lookback:
            self.build_space_time_aggregation_lookback(engine, as_of_dates, list_space_time_lookback, schema)
            self.prefix.append(self.prefix_space_time_lookback)

        # check if a sub-query feature was selected
        list_space_time_sub = [x for x in feature_list if
                               x in set(self._feature_aggregations_sub(engine).keys())]
        if list_space_time_sub:
            self.build_space_time_sub_query_aggregation(engine, as_of_dates, list_space_time_sub, schema)
            self.prefix.append(self.prefix_sub)

        # check if an  aggregate feature was selected
        list_agg = [x for x in feature_list if x in set(self._feature_aggregations(engine).keys())]
        if list_agg:
            self.build_aggregation(engine, list_agg, schema)
            self.prefix.append(self.prefix_agg)

        # check if a space-time feature was selected
        list_space_time = [x for x in feature_list if
                           x in set(self._feature_aggregations_space_time(engine).keys())]
        if list_space_time:
            self.build_space_time_aggregation(engine, as_of_dates, list_space_time, schema)
            self.prefix.append(self.prefix_space_time)

        if not self.prefix:
            log.info("WARNING: no feature aggregation for features: {}".format(feature_list))
            sys.exit(1)


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
        self.prefix_space_time_lookback = 'ir'
        self.prefix_space_time = 'irAG'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'InterventionsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='intervention_type_code',
                                               lookup_table='lookup_intervention_types',
                                               prefix='InterventionsOfType'), ['sum', 'avg']),

            'IncidentsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='grouped_incident_type_code',
                                               lookup_table='lookup_incident_types',
                                               prefix='IncidentsOfType'), ['sum', 'avg']),

            'IncidentsOfTypeDep': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='department_defined_policy_type',
                                                    schema='staging',
                                                    table='incidents',
                                                    prefix='IncidentsOfTypeDep'), ['sum', 'avg']),

            'ComplaintsTypeSource': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='origination_type_code',
                                               lookup_table='lookup_complaint_origins',
                                               prefix='ComplaintsTypeSource',
                                               fix_condition='origination_type_code NOTNULL'), ['sum', 'avg']),

            'SuspensionsOfType': collate.Aggregate(
                {"SuspensionsOfType_active": "(hours_active_suspension > 0)::int",
                 "SuspensionsOfType_inactive": "(hours_inactive_suspension > 0)::int"}, ['sum', 'avg']),

            'HoursSuspensionsOfType': collate.Aggregate(
                {"HoursSuspensionsOfType_active": "hours_active_suspension",
                 "HoursSuspensionsOfType_inactive": "hours_inactive_suspension"}, ['sum', 'avg']),

            'AllAllegations': collate.Aggregate(
                {"AllAllegations": "number_of_allegations"}, ['sum', 'avg']),

        }

    def _feature_aggregations_space_time(self, engine):
        return {

            'DaysSinceLastAllegation': collate.Aggregate(
                {"DaysSinceLastAllegation": "EXTRACT( DAY FROM ('{collate_date}' - report_date))"}, ['min'])

        }


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
        self.prefix_space_time_lookback = 'ic'
        self.prefix_space_time = 'icAG'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'IncidentsByOutcome': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='final_ruling_code',
                                               lookup_table='lookup_final_rulings',
                                               prefix='IncidentsByOutcome'), ['avg', 'sum']),

            'ComplaintsTypeSourceOutSustained': collate.Aggregate(
                self._lookup_values_conditions(
                    engine, column_code_name='origination_type_code',
                    lookup_table='lookup_complaint_origins',
                    prefix='ComplaintsTypeSourceOutSustained',
                    fix_condition=AllegationOutcome.sustained.value + ' and origination_type_code NOTNULL'),
                ['sum', 'avg']),

            'ComplaintsTypeSourceOutUnSustained': collate.Aggregate(
                self._lookup_values_conditions(
                    engine, column_code_name='origination_type_code',
                    lookup_table='lookup_complaint_origins',
                    prefix='ComplaintsTypeSourceOutUnSustained',
                    fix_condition=AllegationOutcome.unsustained.value + ' and origination_type_code NOTNULL'),
                ['sum', 'avg']),

            'ComplaintsTypeSourceOutUnknown': collate.Aggregate(
                self._lookup_values_conditions(
                    engine, column_code_name='origination_type_code',
                    lookup_table='lookup_complaint_origins',
                    prefix='ComplaintsTypeSourceOutUnknown',
                    fix_condition=AllegationOutcome.unknown.value + ' and origination_type_code NOTNULL'),
                ['sum', 'avg']),

            'IncidentsOfTypeOutSust': collate.Aggregate(
                self._lookup_values_conditions(engine,
                                               column_code_name='grouped_incident_type_code',
                                               lookup_table='lookup_incident_types',
                                               fix_condition=AllegationOutcome.sustained.value,
                                               prefix='IncidentsOfTypeOutSust'), ['sum', 'avg']),

            'IncidentsOfTypeUnSust': collate.Aggregate(
                self._lookup_values_conditions(engine,
                                               column_code_name='grouped_incident_type_code',
                                               lookup_table='lookup_incident_types',
                                               fix_condition=AllegationOutcome.unsustained.value,
                                               prefix='IncidentsOfTypeUnSust'), ['sum', 'avg']),

            'IncidentsOfTypeUnknown': collate.Aggregate(
                self._lookup_values_conditions(engine,
                                               column_code_name='grouped_incident_type_code',
                                               lookup_table='lookup_incident_types',
                                               fix_condition=AllegationOutcome.unknown.value,
                                               prefix='IncidentsOfTypeUnknown'), ['sum', 'avg']),

            'IncidentsOfTypeDepOutSust': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='department_defined_policy_type',
                                                    schema='staging',
                                                    table='incidents',
                                                    fix_condition=AllegationOutcome.sustained.value,
                                                    prefix='IncidentsOfTypeDepOutSust'), ['sum', 'avg']),

            'IncidentsOfTypeDepUnSust': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='department_defined_policy_type',
                                                    schema='staging',
                                                    table='incidents',
                                                    fix_condition=AllegationOutcome.unsustained.value,
                                                    prefix='IncidentsOfTypeDepUnSust'), ['sum', 'avg']),

            'IncidentsOfTypeDepUnknown': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='department_defined_policy_type',
                                                    schema='staging',
                                                    table='incidents',
                                                    fix_condition=AllegationOutcome.unknown.value,
                                                    prefix='IncidentsOfTypeDepUnknown'), ['sum', 'avg']),

        }

    def _feature_aggregations_space_time(self, engine):
        return {

            'DaysSinceLastCompletedAllegation': collate.Aggregate(
                {"DaysSinceLastCompletedAllegation": "EXTRACT(DAY FROM ('{collate_date}' - date_of_judgment))"},
                ['min']),

        }


# --------------------------------------------------------
# BLOCK: SHIFTS
# --------------------------------------------------------
class OfficerShifts(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.officer_shifts'
        self.date_column = 'stop_datetime'
        self.prefix_space_time_lookback = 'shifts'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'ShiftsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='shift_type_code',
                                               lookup_table='lookup_shift_types',
                                               prefix='ShiftsOfType'), ['sum', 'avg']),

            'HoursPerShift': collate.Aggregate(
                {'HoursPerShift': '(EXTRACT( EPOCH from shift_length)/3600)'}, ['avg', 'sum'])
        }


# --------------------------------------------------------
# BLOCK: ARRESTS
# --------------------------------------------------------
class OfficerArrests(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.arrests'
        self.from_obj_sub = 'sub_query'
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'arrests'
        self.prefix_sub = 'arstat'
        self.join_table = 'staging.arrests'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'Arrests': collate.Aggregate(
                {"Arrests": 'event_id'}, ['count']),

            'ArrestsOfType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='arrest_type_code',
                                               lookup_table='lookup_arrest_types',
                                               prefix='ArrestsOfType'), ['sum', 'avg']),

            'ArrestsON': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='arrest_day_of_week',
                                               lookup_table='lookup_days_of_week',
                                               prefix='ArrestsON'), ['sum', 'avg']),

            'SuspectsArrestedOfRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='suspect_race_code',
                                               lookup_table='lookup_races',
                                               prefix='SuspectsArrestedOfRace'), ['sum', 'avg']),
            'SuspectsArrestedOfEthnicity': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='suspect_ethnicity_code',
                                               lookup_table='lookup_ethnicities',
                                               prefix='SuspectsArrestedOfEthnicity'), ['sum', 'avg']),

            'ArrestsCrimeType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='ucr4_grouped_code',
                                               lookup_table='lookup_ucr4_grouped_dispatch_types',
                                               prefix='ArrestsCrimeType'), ['sum', 'avg'])
        }

    def _feature_aggregations_sub(self, engine):
        return {
            'ArrestMonthlyVariance': collate.Aggregate(
                {"ArrestMonthlyVariance": 'count_officer'}, ['variance']),

            'ArrestMonthlyCV': collate.Aggregate(  # TODO
                {"ArrestMonthlyCOV": 'count_officer'}, ['cv'])
        }

    # add a sub query to perform the pre aggregation step
    def _sub_query(self):
        select_sub = collate.make_sql_clause(""
                                             "officer_id,"
                                             "count(officer_id)  AS count_officer,"
                                             "date_trunc('month', event_datetime) as event_datetime", ex.text)
        from_sub = collate.make_sql_clause('staging.arrests', ex.text)
        group_by_sub = collate.make_sql_clause("officer_id, date_trunc('month', event_datetime) ", ex.text)

        sub_query = ex.select(columns=[select_sub], from_obj=from_sub) \
            .group_by(group_by_sub)

        return sub_query


# --------------------------------------------------------
# BLOCK: TRAFFIC STOPS
# --------------------------------------------------------
class TrafficStops(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.traffic_stops'
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'ts'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'TrafficStopsWithSearch': collate.Aggregate(
                {"TrafficStopsWithSearch": '(searched_flag = true)::int'}, ['sum', 'avg']),

            'TrafficStopsWithUseOfForce': collate.Aggregate(
                {"TrafficStopsWithUseOfForce": '(use_of_force_flag = true)::int'}, ['sum', 'avg']),

            'TrafficStops': collate.Aggregate(
                {"TrafficStops": 'event_id'}, ['count']),

            'TrafficStopsWithArrest': collate.Aggregate(
                {"TrafficStopsWithArrest": '(arrest_flag = true)::int'}, ['sum', 'avg']),

            'TrafficStopsWithInjury': collate.Aggregate(
                {"TrafficStopsWithInjury": '(injuries_flag = true)::int'}, ['sum', 'avg']),

            'TrafficStopsWithOfficerInjury': collate.Aggregate(
                {"TrafficStopsWithOfficerInjury": '(officer_injury_flag=true)::int'}, ['sum', 'avg']),

            'TrafficStopsWithSearchRequest': collate.Aggregate(
                {"TrafficStopsWithSearchRequest": 'search_consent_request_flag::int'}, ['sum', 'avg']),

            'TrafficStopsByRace': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stopped_person_race_code',
                                               lookup_table='lookup_races',
                                               prefix='TrafficStopsByRace'), ['sum', 'avg']),

            'TrafficStopsByStopType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stop_type_code',
                                               lookup_table='lookup_traffic_stop_type',
                                               prefix='TrafficStopsByStopType'), ['sum', 'avg']),

            'TrafficStopsByStopResult': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='stop_outcome_code',
                                               lookup_table='lookup_traffic_stop_outcome_type',
                                               prefix='TrafficStopsByStopResult'), ['sum', 'avg']),

            'TrafficStopsBySearchReason': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='search_justification_code',
                                               lookup_table='lookup_search_justifications',
                                               prefix='TrafficStopsBySearchReason'), ['sum', 'avg'])
        }


# --------------------------------------------------------
# BLOCK: FIELD INTERVIEWS
# --------------------------------------------------------
class FieldInterviews(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.field_interviews'
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'fi'

    def _feature_aggregations_space_time_lookback(self, engine):
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
                                               prefix='InterviewsType'), ['sum', 'avg']),

            'ModeHourOfFieldInterviews': collate.Aggregate(
                {"ModeHourOfFieldInterviews": ""}, 'mode', "date_part('hour',event_datetime)-12")
        }


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
                {"OFwithSuspectInjury": '(suspect_injury)::int'}, ['sum'])
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
        self.from_obj = ex.text(
            'staging.dispatches LEFT JOIN staging.dispatch_geo_time_officer using(dispatch_id, officer_id)')
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'dispatch'
        self.prefix_post = 'dispatchPOST'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'DispatchType': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='dispatch_final_type_code',
                                               lookup_table='lookup_dispatch_types',
                                               prefix='DispatchType'), ['sum', 'avg']),

            'DispatchInitiatiationType': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='dispatch_category',
                                                    schema='staging',
                                                    table='dispatches',
                                                    prefix='DispatchInitiatiationType'), ['sum', 'avg']),

            'DispatchDivision': collate.Aggregate(
                self._group_category_conditions_str(engine,
                                                    column_name='division_id',
                                                    schema='staging',
                                                    table='dispatch_geo_time_officer',
                                                    prefix='DispatchDivision'), ['sum']),
        }

    def build_post_features(self, engine, feature_list, schema):
        if 'DispatchMovement' in feature_list:
            feature = 'DispatchDivision'
            db_conn = engine.raw_connection()

            # Store Procedure that returns the name of the columns that map to the feature by time window
            window_columns_query = ("""SELECT * 
                                    FROM public.get_columns_by_time_window('{schema}',
                                                              '{prefix}_aggregation',
                                                              '{feature}')""".format(schema=schema,
                                                                                     prefix=self.prefix_space_time_lookback,
                                                                                     feature=feature))
            cur = db_conn.cursor()
            cur.execute(window_columns_query)
            columns_by_windows = cur.fetchall()
            db_conn.close()

            # calculate dispatch movement rate (sum_all - greatest) / greatest for every time window
            rates = {}
            for window, column in columns_by_windows:
                new_col = '{prefix}_id_{window}_DispatchMovement_rate'.format(prefix=self.prefix_post,
                                                                              window=window)
                sum_all = " + ".join('"{}"'.format(x) for x in column)
                greatest = 'GREATEST({})::real'.format(", ".join('"{}"'.format(x) for x in column))
                rates[new_col] = 'round((({sum_all} - {greatest}) / {greatest})::numeric, 3)  AS "{new_col}" '.format(
                    sum_all=sum_all,
                    greatest=greatest,
                    new_col=new_col)
                # Create table {prefix_post}_aggregation
            drop_query = ("""DROP TABLE if exists {schema}."{prefix_post}_aggregation" """.format(schema=schema,
                                                                                                  prefix_post=self.prefix_post))

            query_create_rates = (""" CREATE TABLE {schema}."{prefix_post}_aggregation" AS (
                                       SELECT {unit},
                                              as_of_date,
                                              {rates_func} 
                                       FROM {schema}."{prefix}_aggregation"); """.format(schema=schema,
                                                                                         prefix_post=self.prefix_post,
                                                                                         unit=self.unit_id,
                                                                                         prefix=self.prefix_space_time_lookback,
                                                                                         rates_func=", ".join(
                                                                                             ['{rate}'.format(rate=rate)
                                                                                              for col, rate in
                                                                                              rates.items()])))
            self.prefix.append(self.prefix_post)
            db_conn = engine.raw_connection()
            cur = db_conn.cursor()
            cur.execute(drop_query)
            cur.execute(query_create_rates)
            db_conn.commit()
            db_conn.close()
            log.debug('Build post {schema}.{prefix}_aggregation  features table'.format(schema=schema,
                                                                                        prefix=self.prefix_post))


# --------------------------------------------------------
# BLOCK: OFFICER EMPLOYMENT
# --------------------------------------------------------
class OfficerEmployment(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = 'staging.officer_outside_employment'
        self.date_column = 'date_time'
        self.prefix_space_time_lookback = 'outemp'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {

            'OutsideEmploymentHours': collate.Aggregate(
                {"OutsideEmploymentHours": "hours_on_shift"}, ['sum', 'avg'])
        }


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
                                               prefix='EISFlagsOfType'), ['sum']),
        }

    def build_collate(self, engine, as_of_dates, feature_list, schema):
        self.build_space_time_aggregation(engine, as_of_dates, feature_list, schema)


# --------------------------------------------------------
# BLOCK: OFFICER CHARACTERISTICS
# --------------------------------------------------------
class OfficerCharacteristics(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = ex.text('staging.officers_hub '
                                'left outer join staging.officer_characteristics '
                                '   using (officer_id) '
                                'left outer join staging.officer_trainings '
                                '   using (officer_id) '
                                'left outer join staging.officer_roles '
                                '   using (officer_id) '
                                'left outer join staging.officer_marital '
                                '   using (officer_id) '
                                )
        self.prefix_agg = 'ocND'
        self.prefix_space_time = 'ocAG'
        self.date_column = 'date_of_birth'

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

            'DummyOfficerEducation': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='education_level_code',
                                               lookup_table='lookup_education_levels',
                                               prefix='DummyOfficerEducation'), ['max']),

            'DummyOfficerMarital': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='marital_status_code',
                                               lookup_table='lookup_marital_statuses',
                                               prefix='DummyOfficerMarital'), ['max']),

            'DummyOfficerMilitary': collate.Aggregate(
                {"DummyOfficerMilitary": 'military_service_flag::int'}, ['max']),

            'AcademyScore': collate.Aggregate(
                {"AcademyScore": 'score'}, ['max']),

            'DummyOfficerRank': collate.Aggregate(
                self._lookup_values_conditions(engine, column_code_name='rank_code',
                                               lookup_table='lookup_ranks',
                                               prefix='DummyOfficerRank'), ['max'])
        }

    def _feature_aggregations_space_time(self, engine):
        return {

            'OfficerAge': collate.Aggregate(
                {"OfficerAge": "EXTRACT( DAY FROM ('{collate_date}' - date_of_birth)/365)"}, ['max'])

        }


# --------------------------------------------------------
# BLOCK: DEMOGRAPHICS BY ARRESTS
# --------------------------------------------------------

class DemographicNpaArrests(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = ex.text('staging.arrests_geo_time_officer_npa a '
                                ' JOIN staging.demographics_npa d ON a.npa = d.npa and a.year = d.year+1')
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'demarrests'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'Arrests311Call': collate.Aggregate(
                {"Arrests311Call": '"311_calls"'}, ['avg']),
            'Arrests311Requests': collate.Aggregate(
                {"Arrests311Requests": '"311_requests"'}, ['avg']),
            'PopulationDensity': collate.Aggregate(
                {"PopulationDensity": 'population_density'}, ['avg']),
            'AgeOfResidents': collate.Aggregate(
                {"AgeOfResidents": 'age_of_residents'}, ['avg']),
            'BlackPopulation': collate.Aggregate(
                {"BlackPopulation": 'black_population'}, ['avg']),
            'HouseholdIncome': collate.Aggregate(
                {"HouseholdIncome": 'household_income'}, ['avg']),
            'EmploymentRate': collate.Aggregate(
                {"EmploymentRate": 'employment_rate'}, ['avg']),
            'VacantLandArea': collate.Aggregate(
                {"VacantLandArea": 'vacant_land_area'}, ['avg']),
            'VoterParticipation': collate.Aggregate(
                {"VoterParticipation": 'voter_participation'}, ['avg']),
            'AgeOfDeath': collate.Aggregate(
                {"AgeOfDeath": 'age_of_death'}, ['avg']),
            'HousingDensity': collate.Aggregate(
                {"HousingDensity": 'housing_density'}, ['avg']),
            'NuisanceViolations': collate.Aggregate(
                {"NuisanceViolations": 'nuisance_violations'}, ['avg']),
            'ViolentCrimeRate': collate.Aggregate(
                {"ViolentCrimeRate": 'violent_crime_rate'}, ['avg']),
            'PropertyCrimeRate': collate.Aggregate(
                {"PropertyCrimeRate": 'property_crime_rate'}, ['avg']),
            'SidewalkAvailability': collate.Aggregate(
                {"SidewalkAvailability": 'sidewalk_availability'}, ['avg']),
            'Foreclosures': collate.Aggregate(
                {"Foreclosures": 'foreclosures'}, ['avg']),
            'DisorderCallRate': collate.Aggregate(
                {"DisorderCallRate": 'disorder_call_rate'}, ['avg']),
        }


class OfficerCompliments(FeaturesBlock):
    def __init__(self, **kwargs):
        FeaturesBlock.__init__(self, **kwargs)
        self.unit_id = 'officer_id'
        self.from_obj = ex.text('staging.officer_compliments')
        self.date_column = 'event_datetime'
        self.prefix_space_time_lookback = 'compliments'

    def _feature_aggregations_space_time_lookback(self, engine):
        return {
            'Compliments': collate.Aggregate(
                {"Compliments": 'event_id'}, ['count'])
        }
