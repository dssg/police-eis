DROP TABLE IF EXISTS staging.charges;
CREATE TABLE staging.charges (
	/*	department_defined_arrest_id                                          varchar references staging.arrests(department_defined_arrest_id),            --There will be multiples of arrest_id, as one arrest usually leads to multiple charges
	*/
	department_defined_charge_id                                          varchar,            --
	charge_code                                                           int,                --
	accepted_by_magistrate_code                                           int,                --Decision about whether to take charge to court in first place
	ruling_code                                                           int                 --The outcome after plea/court
);
