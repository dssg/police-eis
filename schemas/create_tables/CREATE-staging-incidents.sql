DROP TABLE IF EXISTS staging.incidents;
CREATE  TABLE staging.incidents (
  event_id                                   INT PRIMARY KEY REFERENCES staging.events_hub (event_id) ON DELETE CASCADE, --links to event_hub
  investigation_id                           INT, --auto generated id for the investigation
  department_defined_investigation_id        TEXT, -- the investigation number from the department
  department_defined_policy_type             TEXT, -- the kind of department policy that was (allegedly) violated
  department_defined_incident_type_code      INT, --if there is already an internal set of groupings for incident type within the department
  department_defined_policy_violated_raw     TEXT, -- the policy number of the (alleged) violation, from the department
  department_defined_policy_violated_cleaned TEXT, --  the policy number from the department that has been cleaned to then be look-upable to generate the grouped_incident_type_code
  grouped_incident_type_code                 INT, --the commonly- agreed upon set of groupings for incident type (e.g. 'substance abuse', 'uof')
  number_of_allegations                      INT, --if multiple allegations of the same incident_type_code are made for the same event (e.g. multiple uniform violations), they will be aggregated into one row. This column shows how many of these allegations there were
  number_of_justified_allegations            INT, --if incident type can be classed into justified or unjustified (e.g. UOF), this shows the number of these. If not, it is a null
  number_of_unjustified_allegations          INT, --as above, for un-justified allegations
  number_of_preventable_allegations          INT, --as above, for preventable allegations
  number_of_non_preventable_allegations      INT, --as above, for un-preventable allegations
  number_of_unfounded_allegations            INT, --as above, for unfounded allegations
  number_of_sustained_allegations            INT, --as above, for sustained allegations
  number_of_unsustained_allegations          INT, --as above, for unsustained allegations
  number_of_exonerated_allegations           INT, --as above, for exonerated allegations
  number_of_unknown_outcome_allegations      INT, --as above, for unknown outcome
  origination_type_code                      INT, --the origination of the complaint e.g. email, letter, in person, phone call, automatic (procedural), automatic(computer), automatic(taser). Maps to a lookup table
  investigation_start_date                   TIMESTAMP, --Start date of the investigation
  investigation_end_date                     TIMESTAMP, --End date of the investigation
  investigating_officer_id                   INT, --Id of the officer who was investigating the complaint
  tactics_concerns_flag                      BOOL, -- whether or not there were tactical concerns
  safety_concerns_flag                       BOOL, -- whether or not there were safety concerns
  communications_concerns_flag               BOOL, --whether or not there were communications concenrs
  report_date                                TIMESTAMP, --The date the report of the incident was taken
  intervention_type_code                     INT, --the kind of intervention, maps to lookup_intervention_type
  days_suspended                             INT, --number of days suspended, if any.
  concerns_type_code                         INT, --if there were concerns over e.g. tactics, safety. The type of concern maps to a lookup table
  hours_active_suspension                    INT, --number of days suspended, if any.
  hours_inactive_suspension                  INT, --number of days suspended, if any.
  training_id                                INT, --this links to the training table, which describes what training an officer received as a result of the investigation
  date_of_judgment                          TIMESTAMP, --date that a judgement was reached
  final_ruling_code                          INT, -- the code for what the final ruling was
  date_intervention_received                 TIMESTAMP, --date that an intervention was received. May duplicated what is seen in the training table.
  reprimand_type_code                        INT, --the kind of repirmand, e.g. verbal, written, none etc
  reprimand_narrative                        TEXT, --free text field for the reprimand given to the officer, if one is given
  officer_explanation_narrative              TEXT, --free text field for officer explanation, if one exists
  general_narrative                          TEXT, --free text field describing the investigation
  last_modified                              TIMESTAMP, --timestamp for when the table was last updated
  department_source_table                    TEXT, --free text field showing which source table the data comes from
  event_datetime                             TIMESTAMP,
  officer_id                                 INT REFERENCES staging.officers_hub (officer_id)
);
