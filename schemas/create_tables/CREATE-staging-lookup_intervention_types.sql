DROP TABLE IF EXISTS staging.lookup_intervention_types;
CREATE TABLE staging.lookup_intervention_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
