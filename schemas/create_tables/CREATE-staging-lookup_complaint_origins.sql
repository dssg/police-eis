DROP TABLE IF EXISTS staging.lookup_complaint_origins;
CREATE TABLE staging.lookup_complaint_origins (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
