DROP TABLE IF EXISTS staging.lookup_ranks;
CREATE TABLE staging.lookup_ranks (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
