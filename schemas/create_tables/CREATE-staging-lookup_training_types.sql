DROP TABLE IF EXISTS staging.lookup_training_types;
CREATE TABLE staging.lookup_training_types (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
