DROP TABLE IF EXISTS staging.lookup_hazards;
CREATE TABLE staging.lookup_hazards (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
