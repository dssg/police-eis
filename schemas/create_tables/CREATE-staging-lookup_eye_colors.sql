DROP TABLE IF EXISTS staging.lookup_eye_colors;
CREATE TABLE staging.lookup_eye_colors (
  code        INT PRIMARY KEY, --
  value       VARCHAR, --
  description VARCHAR             --
);
