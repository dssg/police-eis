DROP TABLE IF EXISTS staging.lookup_ncic;
CREATE TABLE staging.lookup_ncic (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
