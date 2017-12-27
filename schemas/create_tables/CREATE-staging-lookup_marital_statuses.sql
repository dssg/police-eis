DROP TABLE IF EXISTS staging.lookup_marital_statuses;
CREATE TABLE staging.lookup_marital_statuses (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
