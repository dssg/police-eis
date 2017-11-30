DROP TABLE IF EXISTS staging.lookup_ucr4_codes;
CREATE TABLE staging.lookup_ucr4_codes (
  code        VARCHAR PRIMARY KEY, --
  value       VARCHAR,
  description VARCHAR             --
);
