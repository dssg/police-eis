DROP TABLE IF EXISTS staging.lookup_nib;
CREATE TABLE staging.lookup_nib (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
