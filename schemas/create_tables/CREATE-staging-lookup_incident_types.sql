DROP TABLE IF EXISTS staging.lookup_incident_types;
CREATE TABLE staging.lookup_incident_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
