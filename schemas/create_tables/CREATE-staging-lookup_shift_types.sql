DROP TABLE IF EXISTS staging.lookup_shift_types;
CREATE TABLE staging.lookup_shift_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
