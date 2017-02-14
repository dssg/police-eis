DROP TABLE IF EXISTS staging.officer_trainings; 
CREATE  TABLE staging.officer_trainings (
	training_id                                                           int,                --auto generated id for the specific instance of training
	officer_id                                                            int references staging.officers_hub(officer_id) on delete cascade,                --officer id
	training_type_code                                                    int,                --code corresponding to type of training
	start_date                                                            timestamp,          --date the training was started
	completion_date                                                       timestamp,          --date the training was completed
	score                                                                 int,                --numerical score, if the training included a numerical evalution
	pass_fail_flag                                                        bool,               --whether the training was successfully completed
	precipitated_by_event_id                                              int                 --event id of the event that led to the officer requiring this training
);
