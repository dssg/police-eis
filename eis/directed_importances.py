import pickle
import sklearn
import random
import numpy as np
import pandas as pd

def main():

	# Read in the trained model and the data matrix

	model = pickle.load(open('/home/jhelsby/police-eis/results/police_eis_results_2016-01-28T16:08:23.560204.pkl','rb'))
	dat = pickle.load(open('/home/jhelsby/police-eis/audits/audit_2016-01-28T16:08:23.560204.pkl','rb'))


	# Determine number of features, as well as their min and max

	X = dat['test_x']
	rf = model['modelobj']
	feat_max = np.amax(X,axis=0)
	feat_min = np.amin(X,axis=0)
	n_feat = len(feat_min)


	# Generate random samples, run the trained model on it, and join

	random_X = np.array([[random.uniform(feat_min[i],feat_max[i]) for i in range(n_feat)] for _ in range(100000)])
	random_y = rf.predict_proba(random_X)[:,1]
	mc_mat = np.c_[random_X,random_y]


	# Perform the Monte Carlo integration

	risk_score_curves = []
	for i in range(len(feat_max)):
	    ssize = (feat_max[i] - feat_min[i])/50.
	    probs = []
	    for j in range(50):
	        sub_mat = mc_mat[ (mc_mat[:,i]>feat_min[i]+j*ssize) & (mc_mat[:,i]<feat_min[i]+(j+1)*ssize) , : ]
	        probs.append([np.sum(sub_mat[:,-1])/len(sub_mat[:,-1]),np.std(sub_mat[:,-1])])
	    risk_score_curves.append(probs)


	# Generate directed importances and return it

	master = np.array(risk_score_curves)
	directed_importances = []
	for i in range(n_feat):
	    directed_importances.append(master[i,-1,0]-master[i,0,0])
	out = np.c_[model['feature_importances_names'],directed_importances]
	return out

if __name__ == "__main__":
    main()