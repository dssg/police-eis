DROP TABLE IF EXISTS staging.arrests; 
CREATE UNLOGGED TABLE staging.arrests (
	event_id                                                              int references staging.events_hub(event_id) on delete cascade,                --Primary key
	department_defined_arrest_id                                          varchar,                --Native key
	department_defined_arrest_number                                      int,                --Department internal arrest number
	arrest_type_code                                                      int,                --Type of arrest (e.g. on sight, warrant)
	arresting_agency_code                                                 int,                --agency who executed the arrest
	arrest_status_code                                                    int,                --Status of the arrest
	arrest_day_of_week                                                    int,                --Day of the week
	booked_datetime                                                       timestamp,          --Date and time of booking
	suspect_person_id                                                     int,                --ID of person arrested
	suspect_age_at_arrest                                                 int,                --Age at time of arrest
	suspect_race_code                                                     int,                --race of suspect
	suspect_ethnicity_code                                                int,                --ethnicity of suspect
	suspect_gender_code                                                   int,                --gender of suspect
	suspect_height_inches                                                 int,                --height of suspect in inches
	suspect_weight_pounds                                                 int,                --weight of suspect in pounds
	suspect_hair_color_code                                               int,                --hair color of suspect
	suspect_eye_color_code                                                int,                --eye color of suspect
	ucr4_code                                                             int,                --Universal Crime Reporting Code
	ncic_code                                                             int,                --National Crime Information Center Codes
	nibrs_code                                                            int,                --National Incident-Based Reporting System (NIBRS)
	felony_flag                                                           bool,               --Arrested for a felony
	drugs_flag															  bool,
	stolen_vehicle_flag													  bool,
	resisted_arrest_flag                                                  bool,               --did the suspect resist arrest?
	assaulted_officer_flag                                                bool,               --did the suspect assault the officer?
	officer_injured_flag                                                  bool,               --Was an officer injured
	arrest_address_id                                                     int,                --Address of arrest
	arrest_finest_area_id                                                 int,                --id of the smallest geographic unit we have police department shape information for
	suspect_address_id                                                    int,                --Address of suspect
	assisting_officer_id                                                  int,                --the id of the officer assisting with the arrest
	transporting_officer_id                                               int,                --the id of the officer who transported the arrestee
	narrative                                                             text,               --description of the arrest
	shift_code															  int,				  --the shift on which the arrest took place (e.g. 1, 2, 3)
	date_created														  timestamp,		  --date the entry was first created
	date_modified														  timestamp  		  --date the entry was last modified
);
