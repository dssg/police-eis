DROP TABLE IF EXISTS staging.lookup_rulings;
CREATE TABLE staging.lookup_rulings (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
