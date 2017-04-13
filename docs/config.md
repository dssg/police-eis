## Machine

The analysis for this research was conducted on a Ubuntu 14.04.4 LTS machine hosted on an AWS EC2 server. A full description of the machine used can be found [here](machine_configuration/).

The specific dependencies are as follows:


## Dependencies

### 1. [Drake](https://github.com/Factual/drake)
Only required for loading the Nashville data (see below).

### 2. Python 3.5+
For package dependencies, see [requirements.txt](../requirements.txt)

### 3. [Luigi](https://github.com/spotify/luigi)

### 4. [pg_tools](https://github.com/jonkeane/pg_tools)
This is a repository that provides modules for helping with the staging process. It needs to be installed in both repositories



```
cd [path to police-eis repo]\schemas
git clone https://github.com/jonkeane/pg_tools.git
```

and for the `police-eis-private` repo:

```
cd [path to police-eis repo]\police-eis-private\schemas\pg_tools
git clone https://github.com/jonkeane/pg_tools.git
```

## Getting the Repositories

### What are the repos?

There are a number of repositories that are and have been used in the course of developing the police early intervention system. This document is designed to describe the purpose and content of each. There is [detailed documentation about each of the repositories used.](./repository_documentation.md)


####[Proceed to Database Connection](./database_connection.md).
