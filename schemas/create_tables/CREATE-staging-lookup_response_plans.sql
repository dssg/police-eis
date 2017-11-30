DROP TABLE IF EXISTS staging.lookup_response_plans;
CREATE TABLE staging.lookup_response_plans (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
