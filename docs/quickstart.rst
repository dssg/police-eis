.. police-eis documentation master file, created by
   sphinx-quickstart on Wed Mar 30 12:28:33 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Quickstart
==========

This page describes how to get up and running using the police early intervention system from the installation of the software, setup of the datasets (once cleaning and importing the data into a centralized database has been completed), and model generation and selection. 

Installation
============

Git clone the repository and install with `setup.py`: 

   ``python setup.py install``

Setup
=====

To set up a new police department with the early intervention system, you will need to write some configuration files that define the data sources available, how the code should connect to the database, and what features you want to create.

Database Connection and Data Definition


Running Models
==============

To run models on this new dataset, edit `default.yaml`.


Adding Features
===============

To add a new feature, you should follow the instructions in contributing.md. 

.. toctree::
   :maxdepth: 2
   intro
   quickstart

