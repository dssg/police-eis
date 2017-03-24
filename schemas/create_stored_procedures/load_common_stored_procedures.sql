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
REATE OR REPLACE FUNCTION get_active_bock_features(schema_name TEXT,
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
                  regexp_matches(column_name, '_id_(\d+\w|all)?[_]?([A-Z][A-Za-z0-9]+)_') AS array_col,
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
