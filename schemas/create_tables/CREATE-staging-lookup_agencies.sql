DROP TABLE IF EXISTS staging.lookup_agencies;
CREATE TABLE staging.lookup_agencies (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
