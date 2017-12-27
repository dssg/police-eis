DROP TABLE IF EXISTS staging.lookup_field_interview_types;
CREATE TABLE staging.lookup_field_interview_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
