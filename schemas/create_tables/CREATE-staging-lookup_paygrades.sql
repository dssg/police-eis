DROP TABLE IF EXISTS staging.lookup_paygrades;
CREATE TABLE staging.lookup_paygrades (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
