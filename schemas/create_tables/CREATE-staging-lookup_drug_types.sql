DROP TABLE IF EXISTS staging.lookup_drug_types;
CREATE TABLE staging.lookup_drug_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
