import luigi
import luigi.postgres
import pg_tools.pg_tools as pg_tools
import os, re, yaml
import pandas as pd

### to run creating the full staging schema
# you can specify a new schema name (eg staging_dev) for testing
# creating all staging tables
# PYTHONPATH='' luigi --module setupStaging CreateAllStagingTables --create-tables-directory ./create_tables/ --schema staging_dev --local-scheduler
# create and populate all staging tables
# PYTHONPATH='' luigi --module setupStaging PopulateLookupTables --table-file ./populate_tables/lookup/lookup_tables.yaml --schema staging_dev --CreateAllStagingTables-create-tables-directory ./create_tables  --local-scheduler
###

schema_in_file="staging"

# This is a dictionary of prioirities for specific table names to request that
# luigi creates some tables before others. The higher the number, the higher the
# priority. If there is no entry here, the priority will be 0
table_priorities = {
    'officers_hub': 1000,
    'events_hub': 999,
    'addresses': 998
    }

def prioritize_tables(table_name, table_priorities=table_priorities):
    """ Set priority based on table name

    :param str table_name: table name to return a priority for
    :param dict table_priorities: dictionary of table priorities
    :return int: priority from dictionary, 0 if no entry

    """
    try:
        priority = table_priorities[table_name]
    except KeyError:
        priority = 0
    return(priority)

class CreateTable(pg_tools.PostgresTask):
    script = luigi.Parameter()
    table = luigi.Parameter()
    schema = luigi.Parameter(default="")
    @property
    def priority(self):
        return(prioritize_tables(self.table))


    def curr_schema_name(self):
        """
        Returns the current schema name to use
        """
        if self.schema != "":
            return(self.schema)
        else:
            return(schema_in_file)

    def output(self):
        # check if table already exists
        # if table already exists do nothing (currently it will always run?)
        return pg_tools.PGTableTarget(self.table, self.curr_schema_name())

    def run(self):
        # read script in, replace the staging schema
        with open(self.script, 'r') as myfile:
            sql_query=myfile.read()

        # if the schema is not an empty string, replace all instances
        if self.schema != "":
            # "[.]" matches just and only a period
            sql_query = re.sub(schema_in_file+"[.]", self.curr_schema_name()+".", sql_query)

        self.pgw.execute(sql_query)

class CreateAllStagingTables(luigi.WrapperTask):
    priority = 99 # set a low priority so that the officers hub is created first
    create_tables_directory = luigi.Parameter()
    schema = luigi.Parameter(default="")

    def requires(self):
        files = os.listdir(self.create_tables_directory)
        files = [x for x in files if "CREATE" in x and "SCHEMA" not in x]
        for f in files:
            print(f)
            name = str(f)
            name = name.split('-'+schema_in_file+'-')
            name =  name[1]
            name = name.split('.sql')
            table_name = name[0]
            yield CreateTable(script=os.path.join(self.create_tables_directory, f), table=table_name, schema=self.schema)

class PopulateLookupTables(pg_tools.PostgresTask):
    table_file = luigi.Parameter()
    schema = luigi.Parameter(default="")

    def read_table_file(self, table_file):
        """
        Reads in the table dictionary yaml file

        :param str table_file: path to the table file yaml
        """
        # read the lookup table data from the yaml file
        with open(table_file) as infile:
            table_dict = yaml.load(infile.read())

        return(table_dict)

    def requires(self):
        return [CreateAllStagingTables(schema=self.schema),PopulateStoredProcedures(schema=self.schema)]

    def run(self):
        table_dict = self.read_table_file(self.table_file)

        # populate each table
        for table_name, contents in table_dict.items():

            # remove all rows from the table, if any are already present
            sql_query = """DELETE FROM {}.{}""".format(self.schema, table_name)
            self.pgw.execute(sql_query)

            table_df = pd.DataFrame(contents['rows'], columns=contents['columns'])

            table_df.to_sql(table_name, self.pgw.engine, index=False, schema=self.schema, if_exists='append')
            print('done with {}'.format(table_name))


class PopulateStoredProcedures(pg_tools.PostgresTask):
    script = luigi.Parameter(default="./create_stored_procedures/load_common_stored_procedures.sql")
    schema = luigi.Parameter(default="")

    def curr_schema_name(self):
        """
        Returns the current schema name to use
        """
        if self.schema != "":
            return (self.schema)
        else:
            return (schema_in_file)


    def run(self):
        # a bit of a hack with sed to replace schema names, but if the file is
        # read into python, there is escape character terribleness that breaks
        # everything.
        shell_output = self.pgw.shell(
            "sed 's/{}\./{}\./g' {} | psql".format(schema_in_file, self.curr_schema_name(), self.script))
        print(shell_output)



if __name__ == '__main__':
    luigi.run()
