# Police Early Intervention System (EIS) 

![Build Status](https://magnum.travis-ci.com/dssg/police-eis.svg)

This is a data-driven Early Intervention System (EIS) for police departments. The system uses a police department's data to predict which officers are likely to have an adverse interaction with the public. This is done such that additional training, counseling and other resources can be provided to the officer _before_ any adverse interactions occur. 

Authors: Samuel Carton, Kenneth Joseph, Ayesha Mahmud, Youngsoo Park, Joe Walsh, Lauren Haynes, Jennifer Helsby. 

## Installation

Initial setup is performed via two configuration files, one that contains database credentials, and one that contains configuration unique to the given police department:

* Database credentials are stored in a YAML file `default_profile` in the root directory. Use `example_default_profile` as a template:

```
PGPORT: 65535
PGHOST: "example.com"
PGDATABASE: "example"
PGUSER: "janedoe"
PGPASSWORD: "supersecretpassword"
DBCONFIG: "example_police_dept.yaml"
```

* `DBCONFIG` refers to a configuration file containing details of the individual police department, such as unit/district names and what data sources exist for feature generation `example_police_dept.yaml`. 


## Using the EIS

Details of experiments, e.g. ranges of hyperparameters as well as features to be included, are stored in a YAML file - example in `default.yaml`. Configure your experiments as you wish and then:

```
In [1]: from eis import experiment

In [2]: experiment.main()
2015-11-05 14:53:11,853 - Police EIS: Loaded experiment file
...things happen...
2015-11-05 15:22:52,702 - Police EIS: Training model: RandomForest with {'depth': 20, 'n_estimators': 50, 'criterion': 'entropy'}
2015-11-05 15:22:53,037 - Police EIS: Saving pickled results...
2015-11-05 15:22:53,038 - Police EIS: Training model: RandomForest with {'depth': 20, 'n_estimators': 100, 'criterion': 'entropy'}
2015-11-05 15:22:53,616 - Police EIS: Saving pickled results...
2015-11-05 15:22:53,617 - Police EIS: Done!

In [3]: 

```