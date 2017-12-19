DROP TABLE IF EXISTS staging.lookup_use_of_force_type;
CREATE TABLE staging.lookup_use_of_force_type (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
