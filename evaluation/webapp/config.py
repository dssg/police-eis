#!/usr/bin/python3
# this config file sets the current directory for a given machine without hardcoding the path
import os
cwd = os.getcwd()
results_folder = str(cwd.replace('evaluation', 'results/')).replace("'",'"')
