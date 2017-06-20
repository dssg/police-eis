/*
This schema contains the tables that will be updated daily and populate scores
for production
*/

DROP SCHEMA IF EXISTS production CASCADE;
CREATE SCHEMA production;

-- model table containing each of the models run.
CREATE TABLE production.models (
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
  train_matrix_uuid VARCHAR(36),
  train_end_time    TIMESTAMP,
  experiment_hash   TEXT
);


-- feature_importance table for storing a json with feature importances
CREATE TABLE production.feature_importances (
  model_id           INT REFERENCES production.models (model_id),
  feature            TEXT,
  feature_importance REAL,
  rank_abs           INT,
  rank_pct           REAL
);

CREATE INDEX ON production.feature_importances (model_id);


--  stores the predictions from model
CREATE TABLE production.predictions (
  model_id    INT REFERENCES production.models (model_id),
  as_of_date  TIMESTAMP,
  entity_id   BIGINT,
  score       REAL,
  label_value INT,
  rank_abs    INT,
  rank_pct    REAL,
  matrix_uuid VARCHAR(36)
);

CREATE INDEX ON production.predictions (as_of_date);
CREATE INDEX ON production.predictions (entity_id,as_of_date);

-- changes in rank over time from the predictions table
CREATE TABLE production.time_delta (
  model_id  INT REFERENCES production.models (model_id),
 entity_id   BIGINT,
 as_of_date TIMESTAMP,
 last_day  NUMERIC,
 last_week  NUMERIC,
 last_month NUMERIC,
 last_quarter NUMERIC,
 last_year  NUMERIC
);

CREATE INDEX ON production.time_delta (as_of_date,entity_id);

--  information for when predictions are  reviewed 
CREATE TABLE production.review_audit (
  model_id    INT REFERENCES production.models (model_id),
  entity_id   BIGINT,
  rank_abs    INT,
  rank_pct    REAL,
  review_status TEXT,		-- this needs to be updated later for the specific types 
  as_of_date  TIMESTAMP,
  date_reviewed TIMESTAMP, 
  date_created TIMESTAMP DEFAULT now()
);

CREATE INDEX ON production.review_audit (date_reviewed, model_id);


CREATE TABLE production.individual_importances(
  model_id    INT REFERENCES production.models (model_id),
  as_of_date  TIMESTAMP,
  entity_id   BIGINT,
  risk_1      TEXT,
  risk_2      TEXT,
  risk_3      TEXT,
  risk_4      TEXT,
  risk_5      TEXT
);  

CREATE INDEX ON production.individual_importances (as_of_date);
CREATE INDEX ON production.individual_importances (entity_id,as_of_date);
