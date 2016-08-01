DROP SCHEMA IF EXISTS results CASCADE;
CREATE SCHEMA results;

-- model table containing each of the models run.
CREATE TABLE results.models(
	model_id                    									serial primary key,
  run_time                    									timestamp,
  batch_run_time              									timestamp,
	config 				        												json,
  pickle_file                 									bytea
);

-- predictions corresponding to each model.
CREATE TABLE results.predictions(
		model_id                    								int references results.models(model_id),
    unit_id                     								int,
    unit_score                  								numeric,
    label_value                 								int
);

-- evaluation table containing metrics for each of the models run.
CREATE TABLE results.evaluations(
		model_id                   									int references results.models(model_id),
		evaluation																	numeric,
		metric				             									text,
		metric_parameter														text,
		comment																			text
);
