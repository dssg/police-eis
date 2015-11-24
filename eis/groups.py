mport numpy as np
import numpy as np
import pandas as pd
import logging
import sys
import pickle
import pdb
import datetime

from eis import dataset

log = logging.getLogger(__name__)

def aggregate(id, prediction):
    """
    Take officer level predictions and produce risk scores for 
    divisons and units.
    """

    pass

    return {'units': unitpreds,
            'divisions': divpreds}
