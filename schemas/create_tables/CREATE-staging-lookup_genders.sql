DROP TABLE IF EXISTS staging.lookup_genders;
CREATE TABLE staging.lookup_genders (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
