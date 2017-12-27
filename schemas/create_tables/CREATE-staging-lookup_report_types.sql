DROP TABLE IF EXISTS staging.lookup_report_types;
CREATE TABLE staging.lookup_report_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
