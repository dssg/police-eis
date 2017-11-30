DROP TABLE IF EXISTS staging.lookup_ethnicities;
CREATE TABLE staging.lookup_ethnicities (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
