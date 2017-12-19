DROP TABLE IF EXISTS staging.lookup_departments;
CREATE TABLE staging.lookup_departments (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
