DROP TABLE IF EXISTS staging.lookup_races;
CREATE TABLE staging.lookup_races (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
