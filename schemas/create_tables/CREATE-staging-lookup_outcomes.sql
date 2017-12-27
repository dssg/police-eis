DROP TABLE IF EXISTS staging.lookup_outcomes;
CREATE TABLE staging.lookup_outcomes (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
