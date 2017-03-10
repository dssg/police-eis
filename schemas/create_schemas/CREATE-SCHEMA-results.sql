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
  model_id          SERIAL PRIMARY KEY,
  model_group_id    INT REFERENCES results.model_groups (model_group_id),
  run_time          TIMESTAMP,
  batch_run_time    TIMESTAMP,
  model_type        TEXT,
  model_parameters  JSONB,
  model_comment     TEXT,
  batch_comment     TEXT,
  config            JSONB,
  test              BOOL,
  model_hash        VARCHAR(36) UNIQUE,
  train_matrix_uuid VARCHAR(36)
);

-- predictions corresponding to each model.
CREATE TABLE results.predictions (
  model_id    INT REFERENCES results.models (model_id),
  as_of_date  TIMESTAMP,
  entity_id   BIGINT,
  score       REAL,
  label_value INT,
  rank_abs    INT,
  rank_pct    REAL,
  matrix_uuid VARCHAR(36)
);

CREATE INDEX ON results.predictions (model_id);
CREATE INDEX ON results.predictions (as_of_date);
CREATE INDEX ON results.predictions (model_id, as_of_date);

-- evaluation table containing metrics for each of the models run.
CREATE TABLE results.evaluations (
  model_id   INT REFERENCES results.models (model_id),
  metric     TEXT,
  parameter  TEXT,
  value      REAL,
  comment    TEXT,
  as_of_date TIMESTAMP
);

CREATE INDEX ON results.evaluations (model_id);
CREATE INDEX ON results.evaluations (as_of_date);
CREATE INDEX ON results.evaluations (model_id, as_of_date);

-- feature_importance table for storing a json with feature importances
CREATE TABLE results.feature_importances (
  model_id           INT REFERENCES results.models (model_id),
  feature            TEXT,
  feature_importance NUMERIC,
  rank_abs           INT,
  rank_pct           REAL
);

CREATE INDEX ON results.feature_importances (model_id);

-- individual feature importance
CREATE TABLE results.individual_importances (
  model_id  INT REFERENCES results.models (model_id),
  entity_id BIGINT,
  risk_1    TEXT,
  risk_2    TEXT,
  risk_3    TEXT,
  risk_4    TEXT,
  risk_5    TEXT
);

CREATE INDEX ON results.individual_importances (model_id);

