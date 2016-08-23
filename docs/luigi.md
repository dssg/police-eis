# luigi pipeline management

As described in [the main pipeline documentation](repositories_dependencies_and_pipeline.md), [luigi](https://github.com/spotify/luigi/) is used to create and setup a blank `staging` schema and then to populate this `staging` schema from the `etl` schema.

## Basics of luigi

The [luigi documentation is robust](https://luigi.readthedocs.io/en/latest/) and pretty complete. luigi is setup around configuring `tasks` that have a number of properties. Each task is defined by making a class that inherits from the abstract `luigi.Task`, and further defines the things that need to be done before the task, the things that need to be done when the task is run, and the things that need to be true for the task to be considered finished successfully.

We use a number of custom classes from the [`pg_tools` package](https://github.com/jonkeane/pg_tools). This package defines a number of targets for postgres tables, columns, etc.

## basic `luigi.Task`

The basic building block of luigi is a *task*. There is a more conceptual and possibly easier to understand toy example following this technical overview section.

Every task should have three things (`output`, `requires`, and `run`), which are all defined as bound methods. These methods will be executed in this order: the `output` is checked to see if the output already exists, the `requires` are run, and then the `run` method runs the task itself.

1. `def output(self)`  
  This should return a class that inherits from `luigi.target.Target`. This class defines the check that is run to determine if the task has (already) completed successfully. This target can be anything from checking if a file exists or if a table in a postgres database is not empty. Most of our `output`s are custom types from `pg_tools`.

  *Example:* the following `output` will check if the table `schema_name.table_name` exists; if it does not exist luigi will run the `requires` and `run` methods.
  ```
  def output(self):
    return pg_tools.PGTableTarget(table_name, schema_name)
  ```  

1. `def requires(self)`  
  This returns (or yields, if there is more than one dependency) luigi task objects that should be completed before the current task is run.

  *Example:* the following `requires` will yield (and spawn) tasks to create a table for each `file` in the `files` list  before the task it is part of is considered complete.
  ```
  def requires(self):
    for file in files:
      yield CreateTable(script=file, table=table_name, schema=schema.name)
  ```  

1. `def run(self)`  
  Finally, the `run` method is what needs to be executed for the task to run.

  *Example:* the following `run` will read in a script (`sql_script`) and execute it. This uses a `pg_tools` helper function `pgw.execute()` which executes a postgres query.
  ```
  def run(self):
    with open(sql_script, 'r') as myfile:
      sql_query=myfile.read()
      self.pgw.execute(sql_query)
  ```

## Toy example for writing luigi task dependencies

Imagine that we have a task: *keep Lin happy* this might be accomplished with a variety of things, but the thing that is critical is Lin having a chai latte. In order to have a chai latte, one must *make a chai latte*. Each of these two tasks have different requirements (dependencies).

The task of keeping Lin happy is successful when Lin is happy, or in luigi, the *target* of the task *keep Lin happy* would be a query of Lin's happiness that returns true if Lin is happy. If this target returns true, then the task is not run (since Lin is already happy, there's no need to do anything else).

```python
class LinIsHappy(luigi.target.Target):
  def exists(self):
    if lin.happiness:
      return True
    else:
      return False
```

Now that we have the target to determine if Lin is happy (`LinIsHappy`), we include that in the task `KeepLinHappy`. This task first checks the target `LinIsHappy` in the `output` method and if it returns `True`, nothing else is done, the task is complete. If, however, it returns `False`, then the dependencies specified in the `requires` method are run. For the `KeepLinHappy` task, we depend on having made a chai latte, so we return the task `MakeChaiLatte` as a dependency of the `KeepLinHappy` task. After the dependency is completed, we can execute the `run` method, which gives the chai latte to Lin, which will set her happiness to true.

```python
class KeepLinHappy(luigi.Task):
  def output(self):
    return LinIsHappy(self)

  def requires(self):
    return MakeChaiLatte(self)

  def run(self):
    give(what=ChaiLatte, to=Lin)
    lin.happiness = True
```

Now we have to define the `MakeChaiLatte` task, along with a target defining `ChaiLatteExists`. If the Chai Latte exists, there's no reason to make another one, we can simply give the extant Chai Latte to Lin.

```python
class ChaiLatteExists(luigi.target.Target):
  def exists(self):
    if have(ChaiLatte):
      return True
    else:
      return False

class MakeChaiLatte(luigi.Task):
  def output(self):
    return ChaiLatteExists(self)

  def requires(self):
    return GetChaiLatteIngredients(self)

  def run(self):
    chai = mix([milk, chai tea, ice])
    ChaiLatte = pour(cup, chai)
```
Again, for the `MakeChaiLatte` we might have further dependencies like the task `GetChaiLatteIngredients` (not shown here). After those dependencies are satisfied, we can mix the chai together, and then pour it into a coup, instantiating the `ChaiLatte`, which will then be given to Lin.

## wrapper tasks `luigi.WrapperTask`

Wrapper tasks are like regular tasks, however they don't have a `output` or `run` method. Instead they only have a `requires` method. This `requires` should yield (a number of) luigi task objects.

## luigi parameters

luigi accepts parameters that are passed to each task class (which all directly or indirectly inherit from the `luigi.Task` class). This are defined at the top level of the task class. These are helpful (and used in our code to) pass directories for files to parse, new schema names, etc.

```
class NewTask(luigi.Task):
  param = luigi.Parameter(default="")
```
The code above will define a parameter `param` for the task `NewTask`, which defaults to an empty string if not supplied.

## priority

luigi allows for priority to be set for each task. `priority`, like `run`, `requires`, and `output`, is a reserved method in luigi, so all you should have to do is make a `self.priority` attribute for your custom task class, and luigi will do the rest for you. luigi will respect dependency order before it respects priority, but if a set of tasks are yielded in one wrapper task, priority can be used to make some of those tasks to be completed before others. This does not guarantee an execution order, but it will try (and if luigi is running on a single machine, on a single core, with a single worker it should guarantee the order).  Higher numbers are higher priorities. If priority for a task needs to be set dynamically (that is, you are using an abstract class that is used to generate many similar tasks), you can use an `@property` decorator at the top of the class.

```
class CreateTable(pg_tools.PostgresTask):
  @property
  def priority(self):
      return(prioritize_tables(self.table))
```

The above code will call the function prioritize_tables, which returns the priority (as an integer) that that specific table creation should be given. This number is taken from a Python dictionary of table names mapped to priority numbers.

## `pg_tools`

We use a number of custom classes from the [`pg_tools` package](https://github.com/jonkeane/pg_tools). This package defines a number of targets for postgres tables, columns, etc. Most communication with postgress can be made through a `PostgresWrangler` (often called `pgw`) object that is a wrapper for `SQLAlchemy`. `pg_tools` includes a number of postgres targets, as well as postgres task types that include a `PostgresWrangler` to make connections to Postgres easier (and to handle authentication once).

## Create `staging` and populate lookup tables

The luigi code to create the `staging` schema and populate all lookup tables is located at [`police-eis/schemas/setupStaging.py`](../schemas/setupStaging.py).

Since the `PopulateLookupTables` task depends on the `CreateAllStagingTables` task, you only need to specify `PopulateLookupTables` and luigi will make sure all staging tables are created first.

An example command you might run is:
`PYTHONPATH='' luigi --module setupStaging PopulateLookupTables --table-file ./populate_tables/lookup/lookup_tables.yaml --schema staging_dev --CreateAllStagingTables-create-tables-directory ./create_tables  --local-scheduler`

### Constructing a luigi call in bash

1. `PYTHONPATH=''` you always need this to call luigi from bash
1. `luigi --module [filename without the .py extension]`
1. `SingleTaskToRun` e.g. `PopulateLookupTables`
1. parameters (see below)
1. `--local-scheduler` this tells luigi to run the command locally

### Parameters
The following parameters must be both specified in the [`police-eis/schemas/setupStaging.py`](../schemas/setupStaging.py) file, and passed to the bash command that calls luigi:

1. `--table-file   ./populate_tables/lookup/lookup_tables.yaml`  
  This is the yaml file that contains the information to be placed into the lookup tables.
1. `--schema staging_dev`    
  This is the name of the schema where the tables will be created (canonically `staging_dev`, but can be anything)
1. `--CreateAllStagingTables-create-tables-directory ./create_tables`    
  This is the directory that contains all of the create table SQL statement files that define the names, columns, and data types for each table to be created. The files must be named in the format: `CREATE-staging-[table_name].sql`

### Globals
The following global objects and functions are used in the [`police-eis/schemas/setupStaging.py`](../schemas/setupStaging.py) file:

1. `schema_in_file`  
  This is a string that is the schema that is specified throughout the create table scripts and populate lookup data. This is the string that is replaced by the schema specified in the `--schema` parameter.
1. `table_priorities`
  This is a dictionary that gives priorities for tables that must be created early in the process (due to foreign key constraints). The keys are the table names, and the values are integers, >1 where the larger the number the higher the priority given.
1. `prioritize_tables()`
  This is a function that takes a table name and returns a priority number based on the table_priorities.


### Tasks in setupStaging

1. `CreateTable()`  task
  This task creates a table given a `script`, `table`(name), and `schema`. The task is considered complete if the table exists (which is determined by `pg_tools.PGTableTarget`). If the table doesn't exist the script is executed.

1. `CreateAllStagingTables()`  task
  This task finds all of the create table sql scripts in the directory provided by the parameter `--CreateAllStagingTables-create-tables-directory`<sup id="a1">[1](#f1)</sup>, and yields a new create table task for every create table script in that directory. This wrapper task is considered successfully completed when all of the tasks in its `requires` method have completed successfully.

1. `PopulateLookupTables()`  task
  This task populates the lookup tables. It requires that the `CreateAllStagingTables` task is complete before it runs. It then reads the data in the yaml file provided by the `--table-file` parameter, drops the information in those lookup tables, and then repopulates the lookup tables.

  Because this is a quick operation we have not yet defined an output target for this task that checks if it has already completed. This means that any time this task is run it will always repopulate the lookup tables.

## Creating stored procedures and populating `staging` tables from `etl`

The luigi code to create the stored procedures and populate that `staging` schema is located at [`[police-eis]/police-eis-private/schemas/populateStagingFromMNPD.py`](https://github.com/dssg/police-eis-private/blob/master/schemas/populateStagingFromMNPD.py).

There are two main tasks (`PopulateStoredProcedures` and `PopulateAllStagingTables`) which don't currently have dependencies setup in luigi (so they must be run one after the other).

An example command you might run is:
`PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateStoredProcedures --schema staging_dev --local-scheduler`

followed by:
`PYTHONPATH='' luigi --module populateStagingFromMNPD PopulateAllStagingTables --schema staging_dev --populate-tables-directory ./populate_tables/mnpd --local-scheduler`

### Parameters
The following parameters must be both specified in the [`[police-eis]/police-eis-private/schemas/populateStagingFromMNPD.py`](https://github.com/dssg/police-eis-private/blob/master/schemas/populateStagingFromMNPD.py) file, and passed to the bash command that calls luigi:

1. `--schema staging_dev`  (both tasks)  
  This is the name that of the schema where the tables will be created (canonically `staging_dev`, but can be anything)
1. `--populate-tables-directory ./populate_tables/mnpd`  (`PopulateAllStagingTables` task only)
  This is the directory that contains all population scripts for each table. The files must be named in the format: `POPULATE-staging-[table_name].sql`

### Globals
The following global objects and functions are used in the [`[police-eis]/police-eis-private/schemas/populateStagingFromMNPD.py`](https://github.com/dssg/police-eis-private/blob/master/schemas/populateStagingFromMNPD.py) file:

1. `schema_in_file`  
  This is a string that is the schema that is specified throughout the create table scripts and populate lookup data. This is the string that is replaced by the schema specified in the `--schema` parameter.
1. `table_priorities`
  This is a dictionary that gives priorities for tables that must be created early in the process (due to foreign key constraints). The keys are the table names, and the values are integers, >1 where the larger the number the higher the priority given.
1. `prioritize_tables()`
  This is a function that takes a table name and returns a priority number based on the table_priorities.
1. `tables_and_cleanup_scripts`  
  This is a dictionary that has the tables that need to have cleanup scripts run on them after they are populated. The keys are the table name, and the values are lists, the first element is the (relative) path to the cleanup script that must be imported and the second element is the column that will be populated when the cleanup has run successfully. For more information see the `PopulateTableWithCleanUp` task below.

### Tasks in the populateStagingFromMNPD file

### Create stored procedures

1. `PopulateStoredProcedures()` task   
  This task takes a script that contains all of the stored procedure creation code and executes it. Currently, this will drop and recreate all stored procedures whenever it is run.

### Populate `staging`

1. `PopulateTable()`  task  
  This task takes a script that populates a table and executes it. The task is successful when the table is not empty (which is checked with the `pg_tools` target class `pg_tools.PGNonEmptyTableTarget()`). If the table is empty, the population script is run, if the table is not empty the task is considered finished, and nothing is run.

1. `PopulateTableWithCleanUp()`  task  
  This task requires that the table first be populated with a `PopulateTable` task, once that has completed successfully, it reads in the python script specified in the `tables_and_cleanup_scripts` dictionary, and then runs the function `main()` that is specified in that script. The `main()` function in the cleanup script must run all of the code needed for the cleanup to work. In general, you can pass the PostgresWrangler object (frequently called `pgw`) to this function to make postgres calls without having to worry about additional authentication issues. The cleanup is successful when there are any number of non-null values in the column that was specified in the `tables_and_cleanup_scripts` dictionary. This means one must be careful with the order that this cleanup is run.

1. `PopulateAllStagingTables()`  task  
  This task takes a list of files from the directory specified in the `--populate-tables-directory` parameter This task yields a `PopulateTableWithCleanUp` task if there is an entry in the `tables_and_cleanup_scripts` dictionary for that table, and if not it yields a `PopulateTable` task.

<hr/>
<b id="f1"><sup>1</sup></b> It should be noted that the parameter is passed on the command line with the task name prepended to the parameter name. So the `CreateAllStagingTables` task has a parameter `create_tables_directory` which is specified on the command line with `--CreateAllStagingTables-create-tables-directory [path to directory]`. Further, due to luigi limitations any underscores in parameter names must be replaced with dashes on the command line (that is, this parameter is refered to in the luigi script as `create_tables_directory` but on the command line as `create-tables-directory` ). [↩](#a1)
