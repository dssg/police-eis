# Police Early Intervention System (EIS)

![Build Status](https://travis-ci.org/dssg/police-eis.svg)
[![Documentation Status](https://readthedocs.org/projects/police-eis/badge/?version=latest)](http://police-eis.readthedocs.org/en/latest/?badge=latest)

This is a data-driven Early Intervention System (EIS) for police departments. The system uses a police department's data to predict which officers are likely to have an adverse interaction with the public. An adverse incident can be defined on a department by department basis, but typically includes unjustified uses of force, officer injuries, preventable accidents and sustained complaints. This is done such that additional training, counseling and other resources can be provided to the officer _before_ any adverse interactions occur.

## How to Run the Pipeline
The pipeline has two main configurations. In the **modelling** configuration, there are three distinct steps.

### Build features

`python3 -m eis.run --config officer_config.yaml --labels labels_config.yaml --buildfeatures`

In this stage, features and labels are built for the time durations specified in the config files.
Features are stored in the features schema defined by `schema_feature_blocks` in the config file. Labels are stored in the table specificed by the `officer_labels_table` in the config.

### Generate matrices

`python3 -m eis.run --config officer_config.yaml --labels labels_config.yaml --generatematrices`

All possible configurations of the train/test splits are saved. They are saved in the directory specified by `project_path` in the config.

### Run models

`python3 -m eis.run --config officer_config_collate_daily.yaml --labels labels_config.yaml`

Running the pipeline with no flags will complete the modeling run. The pipeline first checks to see if the feature building and matrix generation stages have been completed. If not, these processes are run before the modeling run of the pipeline.

The results schema is populated in this stage. The schema includes the tables:
* evaluations: metrics and values for each model (ex. precision@100)
* experiments: stores the config (JSON) for each experiment hash
* feature_importances: for each model, gives feature importance values as well as rank (abs and pct)
* individual_importances: stores 5 risk factors for each officer per model
* model_groups: feature list, model config, model parameters
* models: stores all information pertinent to each model
* predictions: for each model, stores the risk scores per officer

After the model runs are completed and a model is picked, the **production** setup lets the user run a specific model group and score the list of active officers for a provided date.

`python3 -m eis.run --config officer_config.yaml --labels labels_config.yaml --production --modelgroup 5709 --date 2015-02-22`

The production schema will be populated at this stage. The schema includes the tables:
* models: information about the models run
* feature_importances: for each model, gives feature importance values as well as rank (abs and pct)
* individual_importances: gives five risk factors contributing to an officer's risk score at a given date
* predictions: gives the risk score for each officer per model
* time_delta: shows the changes in risk score for the officers over time


## Quickstart Documentation

Our modeling pipeline has some prerequisites and structure documentation:

1.  [Configure the Machine](docs/config.md).
2.  [Documentation about the structure and contents of the repositories](docs/repository_documentation.md).
3.  [Setup Database Connection](docs/database_connection.md).

### After the prerequisites and requirements are met, the full pipeline can be run ([pipeline documentation](docs/repositories_dependencies_and_pipeline.md)).

![Process](docs/tableProces.png)

Once the pipeline has been run, the results can be visualized using the [webapp](https://github.com/dssg/tyra).

Deprecated Documentation Quick Links:


* [DEPRECATED: Read The Docs](https://police-eis.readthedocs.org/en/latest/)
* [DEPRECATED: Install](https://police-eis.readthedocs.org/en/latest/quickstart.html).

## Issues

Please use [Github's issue tracker](https://github.com/dssg/police-eis/issues/new) to report issues and suggestions.

## Contributors

* 2016: Tom Davidson, Henry Hinnefeld, Sumedh Joshi, Jonathan Keane, Joshua Mausolf, Lin Taylor, Ned Yoxall, Joe Walsh (Technical Mentor), Jennifer Helsby (Technical Mentor), Allison Weil (Project Manager)
* 2015: Jennifer Helsby, Samuel Carton, Kenneth Joseph, Ayesha Mahmud, Youngsoo Park, Joe Walsh (Technical Mentor), Lauren Haynes (Project Manager).
