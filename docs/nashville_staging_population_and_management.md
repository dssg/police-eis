# Managing staging schemas

The `staging` schema should not be in flux or have changes made on the fly. Instead a development schema (_eg_ `staging_dev`) should be used to test new population code. This allows the feature generation code and model code to rely on what is in the `staging` schema.

When the development staging schema is in a (reliable|stable) state, it should be moved to `staging` and then the development staging schema should be recreated (with the Luigi commands below).

You must have a credentials file called `luigi.cfg` for any of the luigi scripts to run. Because these luigi scripts are located in both the `police-eis` and `police-eis-private` repositories you will need two copies: one at `police-eis/schemas/luigi.cfg` and another at `police-eis-private/schemas/luigi.cfg`. The form of this credentials file is given in an example file `luigi_example.cfg`. Use the same format here, but with your credentials to allow luigi to connect to the postgres database.

## Repopulating staging_dev without touching staging

To drop and recreate the entire `staging_dev` schema only, run the following postgres commands:

```
DROP SCHEMA IF EXISTS staging_dev CASCADE;
CREATE SCHEMA staging_dev;
```

If you made edits to an existing table population script, and want to test it on this table alone, use:

```
TRUNCATE TABLE staging_dev.table_name;
```

to drop the values from just that table.

## Move staging_dev to staging

To move the `staging_dev` schema to `staging` issue the following postgres commands. It will also create a new, empty `staging_dev` schema that will then be populated in the next step.

```
DROP SCHEMA IF EXISTS staging CASCADE;
ALTER SCHEMA staging_dev RENAME TO staging;
CREATE SCHEMA staging_dev;
```

## Rerun luigi to create and populate the `staging_dev` schema

Run the following commands (in the `police-eis/schemas/` directory) to create the staging table structure and populate lookup tables. Before running Luigi, confirm that you have the correct credentials located in the file `police-eis/schemas/luigi.cfg ` an example of the format for the credentials are given in `luigi_example.cfg`.

```
PYTHONPATH='' luigi --module setupStaging PopulateLookupTables --table-file ./populate_tables/lookup/lookup_tables.yaml --schema staging_dev --CreateAllStagingTables-create-tables-directory ./create_tables  --local-scheduler
```

After the staging schema is created, run the following commands (in the `[police-eis/]police-eis-private/schemas/`<sup id="a1">[1](#f1)</sup> directory) to insert the stored procedures and populate the tables with all available data. Before running Luigi, confirm that you have the correct credentials located in the file `[police-eis/]police-eis-private/schemas/luigi.cfg ` an example of the format for the credentials are given in `luigi_example.cfg`.

```
PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateStoredProcedures --schema staging_dev --local-scheduler
PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateAllStagingTables --schema staging_dev --populate-tables-directory ./populate_tables/mnpd --local-scheduler
```
<hr/>
<b id="f1"><sup>1</sup></b> The `[police-eis/]` in the path is to show the canonical location of the `police-eis-private` repo being in the root directory of the `police-eis` repo. If it is located elsewhere in your setup, you should use that directory instead. [↩](#a1)
