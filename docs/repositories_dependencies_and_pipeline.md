# Repositories, dependencies, and pipeline management

This document describes the general structure and process for the full `police-eis` project.

## What are the repos?

There are a number of repositories that are and have been used in the course of developing the police early intervention system. This document is designed to describe the purpose and content of each. There is [detailed documentation about each of the repositories used.](./repository_documentation.md)

### How do I get them?

For the `police-eis` repository, simply run:

`git clone --recursive https://github.com/dssg/police-eis.git`

For the `police-eis-private` repository, change your directory to be in the root level of the `police-eis` repository, and then run:

`git clone --recursive https://github.com/dssg/police-eis-private.git`

The `--recursive` flag is important because it will make sure to clone any submodules that are included in the repositories (currently this is only [pg_tools](https://github.com/jonkeane/pg_tools)).

## Dependencies

### 1. [Drake](https://github.com/Factual/drake)
Only required for loading the Nashville data (see below).

### 2. Python 3.4+
For package dependencies, see requirements.txt

### 3. [Luigi](https://github.com/spotify/luigi)

#### pg_tools
[pg_tools](https://github.com/jonkeane/pg_tools) needs to exist in the repositories when you run any of the luigi population scripts. If you cloned the repositories with the `--recursive` flag, you should have them already. If you don't already have them, or you're getting errors that luigi cannot find pg_tools, you can try recloning pg_tools with the following commands:

```
cd [path to police-eis repo]\schemas\pg_tools
git submodule init
git submodule update
```

and for the `police-eis-private` repo:

```
cd [path to police-eis repo]\police-eis-private\schemas\pg_tools
git submodule init
git submodule update
```

## Pipeline

### Raw to ETL

We use Drake to transfer the raw data from the department to the ETL schema (only the MNPD data uses this system). To run this process use the following command:

`The right drake command here.`

### ETL to staging

We use luigi to move data from the ETL schema to the staging schema. [Full documentation for this process (including repopulation) is available.](nashville_staging_population_and_management.md)

Much, but not all, of the ETL to staging process has been automated with a bash script called ['run_luigi.sh' in the `police-eis/schemas/` directory.](../schemas/run_luigi.sh)

### Staging to features

To generate features, set the configuration of features (and labels) in a configuration file that has the same form  as `example_officer_config.yaml`. And then from the `police-eis` directory, run:

`python -m eis.run [configuration file name] --buildfeatures`

### Features to results

Once the features have been built, you can run all of the models with:

`python -m eis.run [configuration file name]`

### Model interrogation, plotting, and analysis

The models are stored in the `results` schema. Within this schema, there are a number of tables:

1. `models`  
  contains all of the model information and configuration details
1. `evaluations`  
  contains a number of metrics for each model. Each row is a metric for one model. The `model_id` is a foreign key to `model_id` in the `models` table.
1. `predictions`  
  contains the predictions for each unit (officer or dispatch) that was predicted for each model. The `model_id` is a foreign key to `model_id` in the `models` table.
1. `data`  
  contains model pickle files that can be read back into python if necessary.

#### Reading model configuration information

The entire configuration file for every model run is stored in `results.models.config` as a JSON object, which can be queried directly in PostgreSQL.  An example of such a query is shown here:

```sql
select * from results.models where config->>'fake_today' = '01May2014'
```

#### Reading pickles

In order to use a fit model in python (for visualization of ROC curves, model inspection, making predictions with a fitted model, etc.), simply query the database for the pickle, and then use `pickle.loads()`

```python
import pickle

[sql-cursor].execute("SELECT pickle_blob FROM results.data WHERE model_id = 20")
result, = cur.fetchone()
data = pickle.loads(result)
```
