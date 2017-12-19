DROP TABLE IF EXISTS staging.lookup_duty_statuses;
CREATE TABLE staging.lookup_duty_statuses (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
