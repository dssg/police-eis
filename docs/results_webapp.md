#Extracting Results for the Webapp

Currently, a development version of a webapp exists to visualize the technical results of the models such as the precision-recall curves, area under the curve (AUC), confusion matrices, and feature importances.

The webapp does not directly build these plots from the results schema. Instead, the results schema is queried for the top models using various constraints, and the required pickle files are returned to a local directory for the top models.

##Returning the Top Models

Returning the top models is achieved using the script [`prepare.py`](../prepare.py) located in the root directory of the [`police-eis`](../) repository.

The script `prepare.py` is run from the command line and has several required arguments and a few available options. These are all specified at the command line.

####Required Arguments:

  ```python
  timestamp:      models run on or after given timestamp
                  example: '2016-08-03'
                  
  metric:         metric to be optimized
                  example: 'precision@'
  ```

####Options:
  
      --parameter:
      --p:          parameter value or threshold if any
                    default=None
                    example: '10.0'
                    
      --number:
      --n:          maximum number of desired results
                    default = 25
      
      --directory:  
      --d:          the directory the results should be returned
                    default = 'results/'
  ```
