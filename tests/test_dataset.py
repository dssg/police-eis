import pandas as pd
import numpy as np
import pdb
from nose.tools import assert_equals

from eis import dataset


class TestFeatureFunctions:
    def test_categorical_basic(self):
        values = {'education_level_cleaned': 
            ['bachelor', 'highschool', 'highschool']}
        df = pd.DataFrame(values)

        df, featurenames = dataset.convert_categorical(df)
        assert sum(df['is_highschool']) == 2

    def test_categorical_none(self):
        values = {'education_level_cleaned': 
            ['bachelor', None, 'highschool']}
        df = pd.DataFrame(values)

        df, featurenames = dataset.convert_categorical(df)
        assert sum(df['is_highschool']) == 1
