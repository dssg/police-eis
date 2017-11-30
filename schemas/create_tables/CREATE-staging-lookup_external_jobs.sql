DROP TABLE IF EXISTS staging.lookup_external_jobs;
CREATE TABLE staging.lookup_external_jobs (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
