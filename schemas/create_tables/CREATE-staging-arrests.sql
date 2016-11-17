DROP TABLE IF EXISTS staging.arrests;
CREATE UNLOGGED TABLE staging.arrests (
  event_id                         INT PRIMARY KEY REFERENCES staging.events_hub (event_id) ON DELETE CASCADE, --Primary key
  department_defined_arrest_id     VARCHAR, --Native key
  department_defined_arrest_number INT, --Department internal arrest number
  arrest_type_code                 INT, --Type of arrest (e.g. on sight, warrant)
  arresting_agency_code            INT, --agency who executed the arrest
  arrest_status_code               INT, --Status of the arrest
  arrest_day_of_week               INT, --Day of the week
  booked_datetime                  TIMESTAMP, --Date and time of booking
  suspect_person_id                INT, --ID of person arrested
  suspect_age_at_arrest            INT, --Age at time of arrest
  suspect_race_code                INT, --race of suspect
  suspect_ethnicity_code           INT, --ethnicity of suspect
  suspect_gender_code              INT, --gender of suspect
  suspect_height_inches            INT, --height of suspect in inches
  suspect_weight_pounds            INT, --weight of suspect in pounds
  suspect_hair_color_code          INT, --hair color of suspect
  suspect_eye_color_code           INT, --eye color of suspect
  ucr4_code                        INT, --Universal Crime Reporting Code
  ncic_code                        INT, --National Crime Information Center Codes
  nibrs_code                       INT, --National Incident-Based Reporting System (NIBRS)
  felony_flag                      BOOL, --Arrested for a felony
  drugs_flag                       BOOL,
  stolen_vehicle_flag              BOOL,
  resisted_arrest_flag             BOOL, --did the suspect resist arrest?
  assaulted_officer_flag           BOOL, --did the suspect assault the officer?
  officer_injured_flag             BOOL, --Was an officer injured
  arrest_address_id                BIGINT, --Address of arrest
  arrest_finest_area_id            INT, --id of the smallest geographic unit we have police department shape information for
  suspect_address_id               INT, --Address of suspect
  assisting_officer_id             INT, --the id of the officer assisting with the arrest
  transporting_officer_id          INT, --the id of the officer who transported the arrestee
  narrative                        TEXT, --description of the arrest
  shift_code                       INT, --the shift on which the arrest took place (e.g. 1, 2, 3)
  date_created                     TIMESTAMP, --date the entry was first created
  date_modified                    TIMESTAMP, --date the entry was last modified
  event_datetime                  TIMESTAMP,
  officer_id                      INT REFERENCES staging.officers_hub (officer_id)
);
