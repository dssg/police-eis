DROP TABLE IF EXISTS staging.lookup_dipatch_priorities;
CREATE TABLE staging.lookup_dipatch_priorities (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
