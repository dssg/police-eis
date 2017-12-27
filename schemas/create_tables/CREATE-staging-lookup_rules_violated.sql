DROP TABLE IF EXISTS staging.lookup_rules_violated;
CREATE TABLE staging.lookup_rules_violated (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
