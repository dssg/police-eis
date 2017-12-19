DROP TABLE IF EXISTS staging.lookup_final_rulings;
CREATE TABLE staging.lookup_final_rulings (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
