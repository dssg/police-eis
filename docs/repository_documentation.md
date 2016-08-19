# [DSSG](https://dssg.uchicago.edu "Data Science for Social Good") and [DSaPP](https://dsapp.uchicago.edu/ "The Center for Data Science and Public Policy") Police Early Intervention System repository structure

There are a number of repositories that are and have been used in the course of developing the police early intervention system. This document is designed to describe the purpose and content of each.

## General pipeline
The general pipeline for data is as follows.

![Process](tableProces.png)

## [`police-eis`](https://github.com/dssg/police-eis)
This is the main open source, public repository for the Police EIS project. This code generally operates on the pipeline from the staging schema on.

It includes:
* SQL scripts that fully define the Common Police Schema
* Code (currently implemented with [Luigi](https://github.com/spotify/luigi)) to create an empty schema that conforms to the Common Police Schema, as well as populates all relevant lookup tables.
* Code to generate features from a populated Common Police Schema database. The `features` schema is created automatically in the feature generation process.
* Code to run a number of machine learning models on these features.
* Code to evaluate, store, and visualize the results from these models. The `results` schema is created and populated automatically in the model running process.
* A web application to inspect select metrics of the models that are stored in the `results` schema.

## [`police-eis-private`](https://github.com/dssg/police-eis-private)
This is the repository that contains department-specific information for the process of going from raw department data to `ETL` schemas and then the population from the (department specific) `ETL` schemas to the `staging` schema. Currently this repository is private because it includes references to specific column names in the raw data from the departments.

It includes:
* [Drake](https://www.factual.com/blog/introducing-drake-a-kind-of-make-for-data) scripts to load raw data into `ETL` schemas (or other processes that are department specific).
* SQL scrips and some python cleanup scripts that transform data from `ETL` schemas to then be populated into the `staging` schemas

This repository should sit in the root directory of the [`police-eis`](https://github.com/dssg/police-eis) repository described above.

## [`police`](https://github.com/dssg/police)
This repsoitory is mostly a hold over from previous iterations of the project. It does, however, contain some exploratory data analysis code that includes some department-specific information. This repository should be reserved for exploratory data analysis or historical code if at all possible. It should not include any pipeline code if at all possible.
