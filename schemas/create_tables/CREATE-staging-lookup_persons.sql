DROP TABLE IF EXISTS staging.lookup_persons;
CREATE TABLE staging.lookup_persons (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
