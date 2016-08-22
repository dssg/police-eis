Quickstart
==========

This page describes how to get up and running using the police early intervention system from the installation of the software, setup of the datasets (once cleaning and importing the data into a centralized database has been completed), and model generation and selection.

Installation
------------

To begin modeling your data, please consult the [repository dependencies and requirements page](docs/repositories_dependencies_and_pipeline.md). 

Once your system is configured, the data will need to be loaded into the common police format. 


Model Pipeline Setup
-----

To set up a new police department with the early intervention system, you will need to write some configuration files that define the data sources available, how the code should connect to the database, and what features you want to create.


Database Connection and Data Definition
---------------------------------------

Initial setup is performed via two configuration files, one that contains database credentials, and one that contains configuration unique to the given police department:

* ####Database Connection: 
   
   Database credentials are stored in a YAML file ```default_profile``` in the root directory. Use ``example_default_profile`` as a template:

   ```YAML
      PGPORT: 65535
      PGHOST: "example.com"
      PGDATABASE: "example"
      PGUSER: "janedoe"
      PGPASSWORD: "supersecretpassword"
      DBCONFIG: "example_police_dept.yaml"
   ```

   * ``DBCONFIG`` refers to a configuration file containing details of the individual police department, such as unit/district names and what data sources exist for feature generation ``example_police_dept.yaml``.

* ####Example Police Department Setup

   The example police department needs a configuration file to specify the name of the staging schema and the names of the arrests table, internal investigation table, and officers hub table in the staging schema. This should be located in ``example_police_dept.yaml``.
   
   ```YAML
      schema: "staging"
      arrest_charges_table: "arrests"
      si_table: "internal_affairs_investigations"
      officer_table: "officers_hub"
   ```
   
   


Running Models
--------------

To run models on this new dataset, edit ``default.yaml``.

Details of experiments, e.g. ranges of hyperparameters as well as features to be included, are stored in a YAML file - example in ``default.yaml``. Configure your experiments as you wish and then::
    python -m eis.run default.yaml

The following flowchart describes the process of model generation in relation to the ETL and feature generation work:

.. image:: ../images/pipeline.png

The model code takes a config file with lists of window sizes, dates to use as "fake todays" for temporal cross validation, model types, and lists of hyperparameters (to search), feature groups, and runs every possible combination.

At the moment, some of the feature generation work must be done by hand in adapting the SQL queries to the schema used for a given police department's dataset. In addition, new features can be added as described in the following section.

.. note:: This pipeline covers the steps from when the data is dumped into a centralized database for analysis.

Adding Features
---------------

To add a new feature or make other code changes, you should follow the instructions in ``contributing.md``.
