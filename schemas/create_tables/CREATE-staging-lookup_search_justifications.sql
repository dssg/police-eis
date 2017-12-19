DROP TABLE IF EXISTS staging.lookup_search_justifications;
CREATE TABLE staging.lookup_search_justifications (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
