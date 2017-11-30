DROP TABLE IF EXISTS staging.lookup_injury_types;
CREATE TABLE staging.lookup_injury_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
