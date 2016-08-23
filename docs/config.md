## Machine

The analysis for this research was conducted on a Ubuntu 14.04.4 LTS machine hosted on an AWS EC2 server. A full description of the machine used can be found [here](machine_configuration/)

The specific dependencies are as follows:

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
Once the machine is configured, the repositories can be cloned.

## Getting the Repositories

### What are the repos?

There are a number of repositories that are and have been used in the course of developing the police early intervention system. This document is designed to describe the purpose and content of each. There is [detailed documentation about each of the repositories used.](./repository_documentation.md)

### How do I get them?

For the `police-eis` repository, simply run:

`git clone --recursive https://github.com/dssg/police-eis.git`

For the `police-eis-private` repository, change your directory to be in the root level of the `police-eis` repository, and then run:

`git clone --recursive https://github.com/dssg/police-eis-private.git`

The `--recursive` flag is important because it will make sure to clone any submodules that are included in the repositories (currently this is only [pg_tools](https://github.com/jonkeane/pg_tools)).

Documentation about the repository structure is [here](./repository_documentation.md).
