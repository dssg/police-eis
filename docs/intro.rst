Introduction
============

This document describes the main functions of the software *without* going into detail into the technical details of how to set up the software. Please refer to :doc:`quickstart` for that information. 

Predicting Police Misconduct
============================

The goal of the system is to provide a data-driven way for police departments to monitor their officers.

The nomenclature that we will use is that of *adverse incidents*: these are the incidents that both the police department and the population that is policed would like to minimize. Typically this would include events like sustained complaints and unjustified uses of force, though the specific definition that is most useful depends on the police department. For example, predicting sustained complaints is predicted on a robust internal affairs process to determine whether of not complaints are sustained as well as a robust complaint process. 

Features
========

In order to dynamically determine from a police department which behaviors are predictive of future adverse incidents, we build many *features*. These features are behaviors that we believe may be predictive of going on to have an adverse incident in the future. 

.. toctree::
   :maxdepth: 2
   intro
   quickstart

 
