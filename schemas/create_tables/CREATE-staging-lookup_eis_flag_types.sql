DROP TABLE IF EXISTS staging.lookup_eis_flag_types;
CREATE TABLE staging.lookup_eis_flag_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
