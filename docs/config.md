The analysis for this research was conducted on a Ubuntu 14.04.4 LTS machine hosted on an AWS EC2 server. A full description of the machine used can be found [here](machine_configuration/)

The specific dependencies are as follows:

Dependencies

1. Drake

Only required for loading the Nashville data (see below).

2. Python 3.4+

For package dependencies, see [requirements.txt](link)

3. Luigi

pg_tools

pg_tools needs to exist in the repositories when you run any of the luigi population scripts. If you cloned the repositories with the --recursive flag, you should have them already. If you don't already have them, or you're getting errors that luigi cannot find pg_tools, you can try recloning pg_tools with the following commands:

cd [path to police-eis repo]\schemas\pg_tools
git submodule init
git submodule update
and for the police-eis-private repo:

cd [path to police-eis repo]\police-eis-private\schemas\pg_tools
git submodule init
git submodule update

Once the machine is configured, the repositories can be cloned:

Documentation about the repository structure is here(link).
