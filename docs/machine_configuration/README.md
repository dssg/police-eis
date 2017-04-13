# Machine configuration

Based on an Amazon EC2 Ubuntu 14.02 image the following packages needed to be installed with the corresponding command sequence

```
* sudo add-apt-repository "deb https://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main"
* wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - 
* sudo aptitude update
* sudo aptitude install postgresql-9.5
* sudo aptitude install postgresql-9.5-postgis-2.3
* sudo aptitude install postgresql-server-dev-9.5
* sudo aptitude install python3-pip
* sudo aptitude install libfreetype6-dev
* sudo aptitude install libblas-dev
* sudo aptitude install liblapack-dev
* sudo aptitude install libatlas-base-dev
* sudo aptitude install libatlas-dev
* sudo aptitude install gfortran
* sudo aptitude install g++-4.4
* sudo aptitude install default-jre
* sudo aptitude install s3cmd
* sudo aptitude install parallel
* sudo aptitude install dos2unix

```




