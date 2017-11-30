DROP TABLE IF EXISTS staging.lookup_field_interview_outcomes;
CREATE TABLE staging.lookup_field_interview_outcomes (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
