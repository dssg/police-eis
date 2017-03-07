import pandas as pd
import logging
import pdb

log = logging.getLogger(__name__)


class FeatureLoader():

    def __init__(self, features, features_table, labels_config, labels, labels_table, prediction_window, officer_past_activity_window):
        '''
        Args:
            features (list): list of features to use for the matrix
            feature_table (str : name of the features table in the db
            labels_config (dict): config file of the conditions for each label
            labels (dict): labels dictionary to use from the config file
            prediction_window (str) : prediction window to use for the label generation
            officer_past_activity_window (str): window for conditioning which officers to use given an as_of_date
        '''

        self.features = features
        self.features_table = features_table
        self.labels_config = labels_config
        self.labels = labels
        self.labels_table = labels_table
        self.prediction_window = prediction_window
        self.officer_past_activity_window = officer_past_activity_window
        self.flatten_label_keys = [item for sublist in self.labels for item in sublist]

    def _tree_conditions(self, nested_dict, parent=[], conditions=[]):
        '''
        Function that returns a list of conditions from the labels config file
        looping recursively through each tree
        Args:
            nested_dict (dict): dictionary for each of the keys in the labels_config file
            parent (list): use in the recursive function to append the parent to each tree
            conditions (list): use in the recursive mode to append all the conditions
        '''
        if isinstance(nested_dict, dict):
            column = nested_dict['COLUMN']
            for value in nested_dict['VALUES']:
                parent_temp = parent.copy()
                if isinstance(value, dict):
                    for key in value.keys():
                        parent_temp.append('{col}:{val}'.format(col=column, val=key))
                        self._tree_conditions(value[key], parent_temp, conditions)
                else:
                    parent_temp.append('{col}:{val}'.format(col=column, val=value))
                    conditions.append('{{{parent_temp}}}'.format(parent_temp=",".join(parent_temp)))
        return conditions

    def _get_event_type_columns(self, nested_dict, list_events=[]):
        if isinstance(nested_dict, dict):
            list_events.append(nested_dict['COLUMN'])
            for val in nested_dict['VALUES']:
                if isinstance(val, dict):
                    for key in val.keys():
                        self._get_event_type_columns(val[key], list_events)
        return list_events

    def get_query_labels(self, as_of_dates_to_use):
        '''
        '''

        # SUBQUERIES of arrays of conditions
        sub_query = []
        event_type_columns = set()
        for key in self.flatten_label_keys:
            condition = key.lower()
            list_conditions = self._tree_conditions(self.labels_config[key], parent=[], conditions=[])
            sub_query.append(" {condition}_table as "
                            "    ( SELECT  "
                            "          unnest(ARRAY{list_conditions}) as {condition}_condition )"
                            .format(condition=condition,
                                    list_conditions=list_conditions))
            # event type
            event_type_columns.update(self._get_event_type_columns(self.labels_config[key], []))

        # JOIN subqueries
        sub_queries = ", ".join(sub_query)
        sub_queries = ("WITH {sub_queries}, "
                       " all_conditions as "
                       "    (SELECT * "
                       "     FROM {cross_joins})"
                       .format(sub_queries=sub_queries,
                               cross_joins=" CROSS JOIN ".join([key.lower() + '_table' for key in self.flatten_label_keys])))

        # CREATE AND AND OR CONDITIONS
        and_conditions = []
        for and_labels in self.labels:
            or_conditions = []
            for label in and_labels:
                or_conditions.append("event_type_array::text[] @> {key}_condition::text[]".format(key=label.lower()))
            and_conditions.append(" OR ".join(or_conditions))
        conditions = " AND ".join('({and_condition})'.format(and_condition=and_condition) for and_condition in and_conditions)

        # QUERY OF AS OF DATES
        query_as_of_dates = (" as_of_dates as ( "
                            "select unnest(ARRAY{as_of_dates}::timestamp[]) as as_of_date) "
                            .format(as_of_dates=as_of_dates_to_use))

        # DATE FILTER
        query_filter = ("group_events as ( "
                        "SELECT officer_id,  "
                        "       event_id, "
                        "       array_agg(event_type::text ||':'|| value::text ORDER BY 1) as event_type_array, "
                        "       min(event_datetime) as min_date, "
                        "       max(event_datetime) filter (where event_type in "
                        "                          (SELECT unnest(ARRAY{event_types}))) as max_date "
                        "FROM features.{labels_table}  "
                        "GROUP BY officer_id, event_id  "
                        "), date_filter as ( "
                        " SELECT  officer_id, "
                        "        as_of_date, "
                        "        event_type_array "
                        " FROM group_events "
                        " JOIN  as_of_dates ON "
                        " min_date > as_of_date and max_date <= as_of_date + INTERVAL '{prediction_window}') "
                        .format(event_types=list(event_type_columns),
                                labels_table=self.labels_table,
                                prediction_window=self.prediction_window))

        query_select_labels = (" labels as ( "
                               "  SELECT officer_id, "
                               "        as_of_date, "
                               "        1 as outcome "
                               " FROM date_filter "
                               " JOIN all_conditions ON "
                               "   {conditions} "
                               " GROUP by as_of_date, officer_id)"
                               .format(conditions=conditions))

        # CONCAT all parts of query
        query_labels = ("{sub_queries}, "
                        "{as_of_dates}, "
                        "{query_filter}, "
                        "{query_select}".format(sub_queries=sub_queries,
                                                as_of_dates=query_as_of_dates,
                                                query_filter=query_filter,
                                                query_select=query_select_labels))
        return query_labels

    def get_dataset(self, as_of_dates_to_use, db_conn):
        '''
        This function returns dataset and labels to use for training / testing
        It is splitted in two queries:
            - features_subquery: which joins the features table with labels table
            - query_active: using the first table created in query_labels, and returns it only
                            for officers that are have any activity given the officer_past_activity_window

        '''

        # convert features to string for querying while replacing NULL values with ceros in sql
        features_coalesce = ", ".join(['coalesce("{0}",0) as {0}'.format(feature) for feature in self.features])
        features_list_string = ", ".join(['{}'.format(feature) for feature in self.features])

        # JOIN FEATURES AND LABELS
        query_features_labels = ( " {labels_subquery}, "
                              " features_labels AS ( "
                              "    SELECT officer_id, "
                              "           as_of_date, "
                              "           {features_coalesce}, "
                              "           coalesce(outcome,0) as outcome "
                              "    FROM features.{features_table} "
                              "    LEFT JOIN labels "
                              "    USING (as_of_date, officer_id) "
                              "    WHERE {features_table}.as_of_date in ( SELECT as_of_date from as_of_dates)) "
                              .format(labels_subquery=self.get_query_labels(as_of_dates_to_use),
                                      features_coalesce=features_coalesce,
                                      features_table=self.features_table))

        # We only want to train and test on officers that have been active (any logged activity in events_hub)
        # NOTE: it uses the feature_labels created in query_labels
        query_active =  (""" SELECT officer_id, as_of_date, {features}, outcome """
                        """ FROM features_labels as f, """
                        """        LATERAL """
                        """          (SELECT 1 """
                        """           FROM staging.events_hub e """
                        """           WHERE f.officer_id = e.officer_id """
                        """           AND e.event_datetime + INTERVAL '{window}' > f.as_of_date """
                        """           AND e.event_datetime <= f.as_of_date """
                        """            LIMIT 1 ) sub; """
                        .format(features=features_list_string,
                                window=self.officer_past_activity_window))

        # join both queries together and load data
        query = (query_features_labels + query_active)

        all_data = pd.read_sql(query, con=db_conn)

        ## TODO: remove all zero value columns
        #all_data = all_data.loc[~(all_data[features_list]==0).all(axis=1)]

        all_data = all_data.set_index('officer_id')
        log.info('length of data_set: {}'.format(len(all_data)))
        log.info('as of dates used: {}'.format( all_data['as_of_date'].unique()))
        log.info('number of officers with adverse incident: {}'.format( all_data['outcome'].sum() ))
        return all_data

