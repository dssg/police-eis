DROP TABLE IF EXISTS staging.lookup_reprimand_types;
CREATE TABLE staging.lookup_reprimand_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
