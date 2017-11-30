DROP TABLE IF EXISTS staging.lookup_arrest_types;
CREATE TABLE staging.lookup_arrest_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
