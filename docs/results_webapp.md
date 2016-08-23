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
                  
     metric:          metric to be optimized
                      example: 'precision@'
  ```

####Options:
  ```python
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

####Examples:

`python prepare.py '2016-08-03' 'auc' ` returns the top models on or after 2016-08-03 with the best auc. By default, this returns up to 25 models into the `'/results/'` directory.

`python prepare.py '2016-08-22' 'precision@' -p '10.0' -n 5 -d 'example_directory/'` returns the top 5 models run on or after 2016-08-22 with the best precision at the top 10% of the distribution. The results are returned to `'example_directory'`. 


