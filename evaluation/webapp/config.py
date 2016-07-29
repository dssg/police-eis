import sys, os, glob
# to be displayed in overview page
results_folder = "../../results/"


def get_files(folder_name):

	#Get Files in Folder
	folder = str(folder_name)
	os.chdir(folder)
	pickle_files = glob.glob("*.pkl")

	for pickle in pickle_files:
		print ("Analyzing file ", pickle, "...")

#get_files(results_folder)
