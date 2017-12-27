DROP TABLE IF EXISTS staging.lookup_unit_types;
CREATE TABLE staging.lookup_unit_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
