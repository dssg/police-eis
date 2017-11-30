DROP TABLE IF EXISTS staging.lookup_days_of_week;
CREATE TABLE staging.lookup_days_of_week (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
