DROP TABLE IF EXISTS staging.lookup_education_levels;
CREATE TABLE staging.lookup_education_levels (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
