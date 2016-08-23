#Extracting Results for the Webapp

Currently, a development version of a webapp exists to visualize the technical results of the models such as the precision-recall curves, area under the curve (AUC), confusion matrices, and feature importances.

The webapp does not directly build these plots from the results schema. Instead, the results schema is queried for the top models using various constraints, and the required pickle files are returned to a local directory for the top models.

##Returning the Top Models

Returning the top models is achieved using the script [`prepare.py`](../prepare.py) located in the root directory of the `police-eis` repository.
