DROP TABLE IF EXISTS staging.lookup_charges;
CREATE TABLE staging.lookup_charges (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
