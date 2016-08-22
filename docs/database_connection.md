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
