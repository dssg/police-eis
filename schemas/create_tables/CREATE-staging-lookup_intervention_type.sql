DROP TABLE IF EXISTS staging.lookup_intervention_type;
CREATE TABLE staging.lookup_intervention_type (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
