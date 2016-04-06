Introduction
============

This document describes the main functions of the software *without* going into detail into the technical details of how to set up the software. Please refer to :doc:`quickstart` for that information. 

Predicting Adverse Incidents
----------------------------

The goal of the system is to provide a data-driven way for police departments to monitor their officers.

The nomenclature that we will use is that of *adverse incidents*: these are the incidents that both the police department and the population that is policed would like to minimize. Typically this would include events like sustained complaints and unjustified uses of force, though the specific definition that is most useful depends on the police department. For example, predicting sustained complaints is predicted on a robust internal affairs process to determine whether of not complaints are sustained as well as a robust complaint process. 

Individual Level Prediction
---------------------------

The individual level prediction predicts on an officer by officer basis the risk that the officer will have an adverse incident in the next time period. Individual feature weights are produced for each officer, allowing an analyst to understand why an officer has been assigned a given risk score. In order to dynamically determine from a police department which behaviors are predictive of future adverse incidents, we build many *features*. These features are behaviors that we believe may be predictive of going on to have an adverse incident in the future. 

Individual feature importances for officers with high risk scores (for example data):

.. image:: https://raw.githubusercontent.com/dssg/police-eis/master/images/example_individual_feature_importances.png

Group Level Aggregation
-----------------------

Group-level aggregation enables an analyst to examine the average risk of individual divisions and units. The evaluation webapp enables an analyst to see whether there are officers within a unit or division with unusually low or high risk scores.

Group prediction table (for example data):

.. image:: https://raw.githubusercontent.com/dssg/police-eis/master/images/group_level.png


.. toctree::
   :maxdepth: 2
   intro
   quickstart

 
