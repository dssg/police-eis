DROP TABLE IF EXISTS staging.lookup_traffic_stop_type;
CREATE TABLE staging.lookup_traffic_stop_type (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
