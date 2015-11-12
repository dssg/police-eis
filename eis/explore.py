import numpy as np
import pdb
import pandas as pd
import yaml
import logging
import sys
import datetime
import seaborn as sns
import matplotlib.pyplot as plt


sns.set(rc={"figure.figsize": (12, 10)})
sns.set(style="white", palette="muted", color_codes=True)


def generate_feat_plot(class0, class1, name):
    timestamp = datetime.datetime.now().isoformat()

    blimits = bins = np.linspace(np.min(list(class0) + list(class1)),
                                 np.max(list(class0) + list(class1)), 30)

    fig, ax = plt.subplots()
    ax.hist(class0, alpha=0.30, bins=blimits, label='Not Adverse')
    ax.hist(class1, alpha=0.30, bins=blimits, color='red', label='Adverse')
    ax.set_xlabel(name)
    ax.set_ylabel('Number of Officers in bin')
    ax.legend()

    fig.savefig('dists/{}_distribution_{}.png'.format(name.replace('/',''), timestamp))
    plt.close(fig)

    fig, ax = plt.subplots()
    ax.hist(class0, alpha=0.30, bins=blimits, label='Not Adverse')
    ax.hist(class1, alpha=0.30, bins=blimits, color='red', label='Adverse')
    ax.set_yscale('log')
    ax.set_xlabel(name)
    ax.set_ylabel('Number of Officers in bin')
    ax.legend()

    fig.savefig('dists/{}_logdistribution_{}.png'.format(name.replace('/',''), timestamp))
    plt.close(fig)


def make_all_dists(exp_data):
    """
    This function makes adverse / not adverse distributions of the features
    for exploratory reasons.
    """
    train_x = exp_data["train_x"]
    train_y = exp_data["train_y"]
    names = train_x.columns

    for each in names:
        this_feature = train_x[each].values
        ind_0, = np.where(train_y == 0)
        ind_1, = np.where(train_y == 1)

        class0_dist = this_feature[ind_0]
        class1_dist = this_feature[ind_1]

        generate_feat_plot(class0_dist, class1_dist, each)

    return None