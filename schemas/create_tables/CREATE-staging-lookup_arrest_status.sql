DROP TABLE IF EXISTS staging.lookup_arrest_status;
CREATE TABLE staging.lookup_arrest_status (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
