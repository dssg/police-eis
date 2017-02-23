DROP TABLE IF EXISTS staging.complaints;
CREATE  TABLE staging.complaints (
  event_id                          INT  REFERENCES staging.events_hub (event_id), -- This is not a primary key: event that the complaint maps to in the events hub. It is possible that an officer violated multiple directives at once which will results in an row by directive violated. -> TODO fix
  complaint_id                      INT, --the unique id of the complaint. If there are multiple officers for the same complaint, they will share the same id. The pk of this table is the combination of complaint_id and accused_officer_id
  accusing_officer_id               INT, --the officer who is doing the complaining, if internal
  complaint_type                    TEXT, --the type of incident being complained about e.g. uniform, profanity
  complaint_narrative               TEXT, --raw text describing the complaint
  datetime                          DATE, --the datetime the complaint was filed
  department_defined_complaint_id   VARCHAR, --mapping to the original complaint id
  processed_flag                    BOOL, --whether or not the complaint has been processed yet. If the complaint_id is present in the internal_affairs_investigations.complaint_id column, then it is considered processed
  complaint_origin_code             INT, --whether complaint is internal or external
  accusing_citizen_hash             INT, --may chuck this. This is a unique id for the member of the public that filed a complaint.
  accusing_citizen_race_code        INT, --
  accusing_citizen_ethnicity_code   INT, --
  accusing_citizen_gender_code      INT, --
  accusing_citizen_age_at_complaint INT, --
  event_datetime                    TIMESTAMP,
  officer_id                        INT REFERENCES staging.officers_hub (officer_id)
);
