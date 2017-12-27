DROP TABLE IF EXISTS staging.lookup_origination_types;
CREATE TABLE staging.lookup_origination_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
