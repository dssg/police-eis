DROP TABLE IF EXISTS staging.officer_shifts;
CREATE UNLOGGED TABLE staging.officer_shifts (
  shift_id              SERIAL, --unique shift id
  officer_id            INT REFERENCES staging.officers_hub (officer_id) ON DELETE CASCADE, --officer id
  start_datetime        TIMESTAMP, --datetime when the shift started
  stop_datetime         TIMESTAMP, --datetime when the shift ended
  shift_length          INTERVAL, --the length of the shift
  shift_bodycam_code    INT, --
  shift_overtime_code   INT, --
  shift_type_code       INT, --
  shift_pay_multiplier  FLOAT, --
  shift_body_armor_code INT, --
  car_code              INT, --
  weapon_type_code      INT, --
  unit_type_code        VARCHAR, --e.g. bomb squad, k9, swat, csi, mounted, cycle
  hazard_code           INT,
  police_area_id        INT,
  start_stop_range      TSRANGE
);
