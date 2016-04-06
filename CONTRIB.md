# Contributing

## Making Changes

If you want to make changes, please first make a branch (alternatively fork the repo), make the minimal changes necessary to implement the new feature, then run the test suite with `nosetests -v`. Travis CI will also run your tests when your submit your PR so please make sure these pass. For style, use [Google's Python docstring style](https://sphinxcontrib-napoleon.readthedocs.org/en/latest/example_google.html) for documenting your code. If everything looks okay then submit a PR. 

## Adding a feature

Presuming the requisite tables exist in the database, add a feature by doing each of the following steps:

* Add the name of the feature (or group of features) you are going to define in the (default) experiment config file. This should be placed underneath `features` within whatever data source is used to derive the feature `name_of_data_source`. This is used for cycling over different feature sets. Set the value of your new feature `my_new_feature` to true in `default.yaml`:

```
features:
    name_of_data_source:
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
