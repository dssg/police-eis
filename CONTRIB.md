# Contributing

## Adding a feature

Presuming the requisite tables exist in the relevant schema in the database, add a feature by:

* Add the name of the feature (or group of features) you are going to define in the (default) experiment config file. Set this to true in `default.yaml`:

```
features:
    height_weight: True  # Height, weight, gender of the officer
    education: True  # Education level of officer
    experience: True  # Experience level of officer (number of years)
    my_new_feature: True  # My new feature
```

* Write a class that defines this feature in one of the feature definition files in `eis/features/` (e.g. `officers.py` for the officer-level experiment). This is where you will define the SQL query that pulls the feature out of the database as well as details of the processing that should be done. 

* Add the feature name from the config file with the name of the new class in `eis/features/class_map.py` in the dict `class_lookup`. 

* Finally, add this feature group name in `evaluation/ioutils.py` in `feature_summary()` such that the evaluation webapp is aware of this new feature/group of features for evaluation and plotting purposes.

## Running tests

Tests are stored in `tests`, run in the root directory or in the `tests` directory using: 

`
nosetests -v
`