#Extracting Results for the Webapp

Currently, a development version of a webapp exists to visualize the technical results of the models such as the precision-recall curves, area under the curve (AUC), confusion matrices, and feature importances.

The webapp does not directly build these plots from the results schema. Instead, the results schema is queried for the top models using various constraints, and the required pickle files are returned to a local directory for the top models.

## Webapp Setup

### Installation

`apt-get install gunicorn nginx supervisor apache2-utils`

### Password

`sudo htpasswd -c /etc/apache2/.htpasswd [modelusername]`

### Supervisor

Put this in `/etc/supervisor/conf.d/police.conf`

```
[program:police]
command = gunicorn -w 5 -b 127.0.0.1:8000 webapp:app
directory = ~/[directory with cloned police-eis repo]/police-eis/evaluation/
user = [machine username]
```

Restart supervisor: `sudo service supervisor restart`

Now you can run `sudo supervisorctl status police` to see the status of the
webapp. Supervisor will also restart the webapp if it dies for some reason.

### nginx

`cd /etc/nginx/sites-enabled`

Get rid of default nginx:
`sudo rm /etc/nginx/sites-enabled/default`

Add new police:
`sudo vi /etc/nginx/sites-available/police.conf`:

```
server {
    auth_basic "Restricted";
    auth_basic_user_file /etc/apache2/.htpasswd;

    access_log /var/log/nginx/access.log;
    error_log /var/log/nginx/error.log;
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Forward-For $proxy_add_x_forwarded_for;
    }
}
```

Soft link:
```
sudo ln -s /etc/nginx/sites-available/police.conf
/etc/nginx/sites-enabled/police
```

Restart nginx: `sudo service nginx restart`

The webapp will be hosted at your designed server name at `[your machine name].io` with username `[modelusername]` and password `[secretpassword]`.

Because the webapp only visualizes the pickle files for requested top models, you must next request the top models from the database before you can view the results in the webapp.


##Returning the Top Models

Returning the top models is achieved using the script [`prepare.py`](../prepare.py) located in the root directory of the [`police-eis`](../) repository.

The script `prepare.py` is run from the command line and has several required arguments and a few available options. These are all specified at the command line.

####Required Arguments:

  ```Python
     timestamp:      models run on or after given timestamp
                     example: '2016-08-03'
                  
     metric:         metric to be optimized
                     example: 'precision@'
  ```

####Options:
  ```Python
      parameter:
      p:            parameter value or threshold if any
                    default=None
                    example: '10.0'
                    
      number:
      n:            maximum number of desired results
                    default = 25
      
      directory:  
      d:            the directory the results should be returned
                    default = 'results/'
  ```

####Examples:

`python prepare.py '2016-08-03' 'auc' ` returns the top models on or after 2016-08-03 with the best auc. By default, this returns up to 25 models into the `'/results/'` directory.

`python prepare.py '2016-08-22' 'precision@' -p '10.0' -n 5 -d 'example_directory/'` returns the top 5 models run on or after 2016-08-22 with the best precision at the top 10% of the distribution. The results are returned to `'example_directory'`. 


##Modifying the Webapp

Code for the actual webapp lives in [evaluation/](../evaluation/), particularly the files [run_webapp.py](../evaluation/run_webapp.py) and the code in [evaluation/webapp/](../evaluation/webapp/). 

This code is a [Flask webapp](http://flask.pocoo.org) and can be modified locally. 
