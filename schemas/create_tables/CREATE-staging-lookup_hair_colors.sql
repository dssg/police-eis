DROP TABLE IF EXISTS staging.lookup_hair_colors;
CREATE TABLE staging.lookup_hair_colors (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
