DROP TABLE IF EXISTS staging.lookup_event_types;
CREATE TABLE staging.lookup_event_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
