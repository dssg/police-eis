DROP SCHEMA IF EXISTS results CASCADE;
CREATE SCHEMA results;

-- model group table for uniquely identifying similar models run at different time periods
CREATE TABLE results.model_groups
(
  model_group_id    SERIAL PRIMARY KEY,
  model_type        TEXT,
  model_parameters  JSONB,
  prediction_window TEXT,
  feature_list      TEXT []
);

-- model table containing each of the models run.
CREATE TABLE results.models (
  model_id              SERIAL PRIMARY KEY,
  model_group_id        INT REFERENCES results.model_groups (model_group_id),
  run_time              TIMESTAMP,
  batch_run_time        TIMESTAMP,
  model_type            TEXT,
  model_parameters      JSONB,
  model_comment         TEXT,
  batch_comment         TEXT,
  config                JSONB,
  pickle_file_path_name TEXT
);

-- predictions corresponding to each model.
CREATE TABLE results.predictions (
  model_id    INT REFERENCES results.models (model_id),
  as_of_date  TIMESTAMP,
  unit_id     BIGINT,
  unit_score  NUMERIC,
  label_value INT,
  rank_abs    INT,
  rank_pct    REAL
);

-- evaluation table containing metrics for each of the models run.
CREATE TABLE results.evaluations (
  model_id  INT REFERENCES results.models (model_id),
  metric    TEXT,
  parameter TEXT,
  value     NUMERIC,
  comment   TEXT
);

-- data table for storing pickle blobs.
CREATE TABLE results.data (
  model_id    INT REFERENCES results.models (model_id),
  pickle_blob BYTEA
);

-- feature_importance table for storing a json with feature importances
CREATE TABLE results.feature_importances (
  model_id           INT REFERENCES results.models (model_id),
  feature            TEXT,
  feature_importance NUMERIC
);

-- individual feature importance
CREATE TABLE results.individual_importances (
  model_id INT REFERENCES results.models (model_id),
  unit_id  BIGINT,
  risk_1   TEXT,
  risk_2   TEXT,
  risk_3   TEXT,
  risk_4   TEXT,
  risk_5   TEXT
);
