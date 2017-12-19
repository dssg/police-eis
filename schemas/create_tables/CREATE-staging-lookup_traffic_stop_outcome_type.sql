DROP TABLE IF EXISTS staging.lookup_traffic_stop_outcome_type;
CREATE TABLE staging.lookup_traffic_stop_outcome_type (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
