/*
Function for using the model group table. This function requires a table like
-----------
CREATE TABLE results.model_groups
(
  model_group_id    SERIAL PRIMARY KEY,
  model_type        TEXT,
  model_parameters  JSONB,
  prediction_window TEXT,
  feature_list      TEXT []
);
-----------
A call like:
-----------
SELECT
  get_model_group_id(model_type, model_parameters, (config -> 'prediction_window') :: TEXT,
                     ARRAY(SELECT jsonb_array_elements_text(config -> 'officer_features')
                           ORDER BY 1) :: TEXT []),
  *
FROM results.models;
-----------
populates the table and returns the IDs
*/
CREATE OR REPLACE FUNCTION get_model_group_id(in_model_type        TEXT, in_model_parameters JSONB,
                                              in_prediction_window TEXT,
                                              in_feature_list      TEXT [])
  RETURNS INTEGER AS
$BODY$
DECLARE
  model_group_return_id INTEGER;
BEGIN
  --Obtain an advisory lock on the table to avoid double execution
  PERFORM pg_advisory_lock(60637);

  -- Check if the model_group_id exists, if not insert the model parameters and return the new value
  SELECT *
  INTO model_group_return_id
  FROM results.model_groups
  WHERE
    model_type = in_model_type AND model_parameters = in_model_parameters AND prediction_window = in_prediction_window
    AND feature_list = ARRAY(Select unnest(in_feature_list) ORDER BY 1);
  IF NOT FOUND
  THEN
    INSERT INTO results.model_groups (model_group_id, model_type, model_parameters, prediction_window, feature_list)
    VALUES (DEFAULT, in_model_type, in_model_parameters, in_prediction_window, ARRAY(Select unnest(in_feature_list) ORDER BY 1))
    RETURNING model_group_id
      INTO model_group_return_id;
  END IF;

  -- Release the lock again
  PERFORM pg_advisory_unlock(60637);


  RETURN model_group_return_id;
END;

$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;



/*
Function for reading active features from each features tablesblock
-----------
-----------
A call like:
-----------
SELECT *
FROM 
  get_active_bock_features(schema_name,
                          table_block_name,
                          ARRAY features_list,
                          ARRAY timegated_feature_lookback_duration);
-----------
*/
CREATE OR REPLACE FUNCTION get_active_block_features(schema_name TEXT,
                                                    table_block_name TEXT,
                                                    features text[],
                                                    timegated_feature_lookback_duration text[])
RETURNS  TABLE (
 col_avaliable text[],
 col_missing text[]
 ) AS
$$
BEGIN
RETURN QUERY
        WITH full_list AS (
                SELECT column_name AS column_name
                FROM information_schema.columns
            WHERE table_schema = schema_name
                 AND table_name =  table_block_name
           ), list_cut AS (
                SELECT
                  regexp_matches(column_name, '_id_(P\d+\w|all)?[_]?([A-Z][A-Za-z0-9]+)_') AS array_col,
                  column_name
                FROM full_list
                WHERE column_name LIKE '%%_id_%%'
            )
              , db_avaliable_features AS (
                SELECT
                  CASE WHEN array_col [1] NOTNULL
                    THEN array_col [1] :: TEXT || '_' || array_col [2] :: TEXT
                  ELSE array_col [2] :: TEXT END AS db_created_features,
                  column_name,
                  array_col
                FROM list_cut
            ), selected_columns AS (
                SELECT unnest(
                    features )
            ), selected_timewindow AS (
                SELECT unnest(timegated_feature_lookback_duration)
            ), non_timewindow_colums AS (
                SELECT db_created_features
                FROM db_avaliable_features
                WHERE array_col [1] ISNULL
                GROUP BY db_created_features
            ), all_colums AS (
                SELECT replace(db_created_features, 'all_', '') AS db_created_features
                FROM db_avaliable_features
                WHERE array_col [1] = 'all'
                GROUP BY db_created_features
            ), requested_features AS (
              SELECT 
                     unnest(f) AS original_feature,
                     unnest(t) || '_' || unnest(f) AS r_columns
              FROM selected_timewindow t CROSS JOIN selected_columns f
              EXCEPT -- exclude all features from the cross product that do not have a time-window
              SELECT
                     f.db_created_features AS original_feature,
                     unnest(t) || '_' || f.db_created_features AS r_columns
              FROM selected_timewindow t CROSS JOIN non_timewindow_colums f
              EXCEPT -- exclude all features from the cross product that do have the window all
              SELECT 
                      f.db_created_features AS original_feature,
                     unnest(t) || '_' || f.db_created_features AS r_columns
                     
              FROM selected_timewindow t CROSS JOIN all_colums f
              UNION (-- add all the single features that are requested which  did not have a time-window in the database
                SELECT 
                    db_created_features AS original_feature,
                    db_created_features AS r_columns
                FROM non_timewindow_colums
                INTERSECT
                SELECT 
                  unnest(f) AS original_features,     
                unnest(f) AS r_columns
                FROM selected_columns f
              )
                            UNION (
                SELECT
                       ('all_' || r_columns) AS original_features,
                       ('all_' || r_columns) AS r_columns
                FROM (
                       SELECT db_created_features AS r_columns
                       FROM all_colums
                       INTERSECT
                       SELECT unnest(f) AS r_columns
                       FROM selected_columns f
                     ) s
              )
            ), final_avaliable_colums AS (
                SELECT array_agg(column_name :: TEXT
                       ORDER BY 1) AS col_avaliable
                FROM requested_features r
                  JOIN db_avaliable_features a ON r.r_columns = a.db_created_features
            ), final_missing_columns AS (
                SELECT array_agg(DISTINCT original_feature
                      ) AS col_missing
                FROM requested_features r
                  LEFT JOIN db_avaliable_features a ON r.r_columns = a.db_created_features
                WHERE a.column_name ISNULL
            )
            SELECT
              (SELECT *
               FROM final_avaliable_colums),
              (SELECT *
               FROM final_missing_columns);

end; $$
LANGUAGE 'plpgsql';


/*
Function that for a specific features returns the names of all columns
by time window
-----------
-----------
A call like:
-----------
SELECT *
FROM
   get_columns_by_time_window(schema_name,
                          table_features__name,
                          feature);
-----------
*/
CREATE OR REPLACE FUNCTION public.get_columns_by_time_window(schema_name TEXT,
                                                        table_features_name TEXT,
                                                        feature text
                                                        )
RETURNS  TABLE (
 time_window text,
 column_names text[]
 ) AS
$$
BEGIN
RETURN QUERY
     WITH full_list AS (
               /* get all columns from the specified feature table*/
                 SELECT column_name AS column_name
                 FROM information_schema.columns
                 WHERE table_schema = schema_name
                       AND table_name = table_features_name
             ), list_cut AS (
               /* seperate the full name into parts, e.g IR_officer_id_1d_IncidentsSeverityUnknown_major_sum ->
                * {1d,IncidentsSeverityUnknown}*/
                 SELECT
                   regexp_matches(column_name, '_id_(P\d+\w)_([A-Z][A-Za-z]+)_') AS array_col,
                   column_name
                 FROM full_list
                 WHERE column_name LIKE '%_id_%'
             ), db_avaliable_features AS (
               /* convert to string for matching: e.g 1d_IncidentsSeverityUnknown  */
                 SELECT
                   array_col [1] :: TEXT  t_window,
                   column_name::TEXT
                 FROM list_cut
                 WHERE array_col [2] :: TEXT = feature
             )
             select t_window, array_agg(column_name) 
             from db_avaliable_features
             GROUP BY t_window;
end; $$
LANGUAGE 'plpgsql';


/*
Function for mapping the name of the column to a feature dictionary and time aggregation
dictionary for explaining the name
-----------
-----------
A call like:
-----------
select * from get_feature_complete_description('column_name',
                                        json.dumps(features_config['feature_names'])::JSON,
                                        json.dumps(features_config['time_aggregations'])::JSON,
                                        json.dumps(features_config['metrics'])::JSON)
-----------
Returns a Table with:
column_original_name |  feature_long_name  |  of_type  |  time_aggregation  |  metric_used

*/
CREATE OR REPLACE FUNCTION public.get_feature_complete_description (column_name TEXT,
                                                            feature_dict JSON,
                                                             time_agg_dict JSON,
                                                              metric_dict JSON)
 RETURNS TABLE (
column_original_name TEXT,
metric_name TEXT,
feature_long_name TEXT,
of_type TEXT,
time_aggregation TEXT
)
AS $$
BEGIN
 RETURN QUERY
            SELECT
                   t1.column_name as column_original_name,
                   metrics.value::text as metric_name,
                   features.value::text as feature_long_name,
                   t1.of_type::text,
                   time_aggregations.value::text as time_aggregation
            from ( select column_name,
                   array_col [1] as block,
                   array_col [2] as time_window_agg,
                   array_col [3] as feature,
                   array_col [4] as of_type,
                   array_col [5] as metric_used
                   from (
                   select regexp_matches(column_name, '(.+)_id_([pP]\d+\w|all)?[_]?([A-Za-z0-9]+)[_]?(.+)?_(sum|avg|max|mode|rate)')
                     AS array_col
                   )  list_cut
                   ) t1
            LEFT JOIN ( SELECT *
                           FROM json_each(feature_dict::json) )features
            on UPPER(features.key) = UPPER(t1.feature)
            LEFT JOIN (SELECT *
                           FROM json_each(time_agg_dict::json)) time_aggregations
            on UPPER(time_aggregations.key) = UPPER(t1.time_window_agg)
            LEFT JOIN (SELECT *
                           FROM json_each(metric_dict::json)) metrics
            on metrics.key = t1.metric_used ;
END; $$

LANGUAGE 'plpgsql';;

/*
FUNCTION THAT POPULATES THE PRODUCTION.TIME_DELTA
inserts changes in relative rank over the last day, week, month, quarter, and year
*/
CREATE OR REPLACE FUNCTION production.populate_time_delta()
  RETURNS BOOLEAN AS
$$
BEGIN
  DELETE FROM production.time_delta;
  INSERT INTO production.time_delta (model_id, entity_id, as_of_date,
                                     last_day, last_week, last_month, last_quarter, last_year)
    SELECT
      a.model_id,
      a.entity_id,
      a.as_of_date,
      a.rank_pct - lag(a.rank_pct, 1)
      OVER (PARTITION BY a.entity_id
        ORDER BY a.as_of_date) AS last_day,
      a.rank_pct - lag(a.rank_pct, 7)
      OVER (PARTITION BY a.entity_id
        ORDER BY a.as_of_date) AS last_week,
      a.rank_pct - lag(a.rank_pct, 30)
      OVER (PARTITION BY a.entity_id
        ORDER BY a.as_of_date) AS last_month,
      a.rank_pct - lag(a.rank_pct, 91)
      OVER (PARTITION BY a.entity_id
        ORDER BY a.as_of_date) AS last_quarter,
      a.rank_pct - lag(a.rank_pct, 365)
      OVER (PARTITION BY a.entity_id
        ORDER BY a.as_of_date) AS last_year
    FROM production.predictions AS a
      INNER JOIN staging.officers_hub AS b ON a.entity_id = b.officer_id;
  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';

/*
FUNCTION THAT POPULATES THE PRODUCTION.individual_importances
Currently a random draw from the top30 features from the feature_importance table
*/
CREATE OR REPLACE FUNCTION production.populate_individual_importances(choosen_model_group_id INTEGER,choosen_as_of_date DATE)
  RETURNS BOOLEAN AS
$$
BEGIN
  DELETE FROM production.individual_importances
  WHERE model_id = (
    SELECT model_id
    FROM production.models AS a
      INNER JOIN production.predictions AS b USING (model_id)
    WHERE a.model_group_id = choosen_model_group_id AND b.as_of_date = choosen_as_of_date
    GROUP BY model_id) AND as_of_date = choosen_as_of_date;
  INSERT INTO production.individual_importances (model_id, as_of_date, entity_id, risk_1, risk_2, risk_3, risk_4, risk_5)
    WITH sub AS (
        SELECT
          a.model_id,
          b.as_of_date,
          b.entity_id
        FROM production.models AS a
          INNER JOIN production.predictions AS b USING (model_id)
          INNER JOIN staging.officers_hub AS c ON b.entity_id = c.officer_id
        WHERE a.model_group_id = choosen_model_group_id and b.as_of_date=choosen_as_of_date
        GROUP BY
          model_id,
          as_of_date,
          entity_id
    ), importance_list AS (
        SELECT *
        FROM sub,
          LATERAL (
          SELECT feature
          FROM production.feature_importances f
          WHERE sub.model_id = f.model_id
                AND rank_abs < 30
          ORDER BY random()
          LIMIT 5
          ) a
    ), officer_aggregates AS (
        SELECT
          model_id,
          as_of_date,
          entity_id,
          array_agg(feature) AS risk_array
        FROM importance_list
        GROUP BY
          model_id,
          as_of_date,
          entity_id
    )
    SELECT
      model_id,
      as_of_date,
      entity_id,
      risk_array [1] AS risk_1,
      risk_array [2] AS risk_2,
      risk_array [3] AS risk_3,
      risk_array [4] AS risk_4,
      risk_array [5] AS risk_5
    FROM officer_aggregates;
  RETURN TRUE;
END;
$$
LANGUAGE 'plpgsql';