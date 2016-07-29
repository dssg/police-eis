DROP TABLE IF EXISTS staging.officer_shifts;
CREATE UNLOGGED TABLE staging.officer_shifts (
	shift_id                                                              serial,                --unique shift id
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	start_datetime                                                        timestamp,          --datetime when the shift started
	stop_datetime                                                         timestamp,          --datetime when the shift ended
	shift_length                                                          interval,           --the length of the shift
	shift_bodycam_code                                                    int,                --
	shift_overtime_code                                                   int,                --
	shift_type_code                                                       int,                --
	shift_pay_multiplier                                                  float,              --
	shift_body_armor_code                                                 int,                --
	car_code                                                              int,                --
	weapon_type_code                                                      int,                --
	unit_type_code                                                        varchar,                --e.g. bomb squad, k9, swat, csi, mounted, cycle
	hazard_code                                                           int,
	police_area_id														  int
);
