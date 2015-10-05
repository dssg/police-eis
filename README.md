# Police Early Intervention System (EIS) 

![Build Status](https://magnum.travis-ci.com/dssg/police-eis.svg?token=ANCRkxBQHot6B85pDms5)

This is a data-driven Early Intervention System (EIS) for police departments. The system analyzes police personnel and predicts the risk of an officer having a negative interaction with the public such that additional training, counseling and other resources can be provided to the officer before a negative interaction occurs. 

# Installation

Local environment variables are stored in a YAML file `default_profile` in the root directory. Use `example_default_profile` as a template:

```
PGPORT: 65535
PGHOST: "example.com"
PGDATABASE: "example"
PGUSER: "janedoe"
PGPASSWORD: "supersecretpassword"
DBCONFIG: ""
```

# Using the EIS

Details of experiments, e.g. parameters, are stored in a YAML file - example in `default.yaml`. 

# Running tests

Tests are stored in `tests`, run in the root directory or in the `tests` directory using: 

`
nosetests -v
`