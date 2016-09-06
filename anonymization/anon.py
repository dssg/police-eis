"""This program anonymizes police reports, removing names of police officers
    and replacing them with hash codes. It also removes police badge numbers.
    This will work for all officers specified in the input file.

    Inputs:

    names.xls
    An excel spreadsheet containing columns for first_name, last_name,
    middle_name, and a hash_code for each officer.

    reports.xls
    An excel spreadsheet with a column called narratives, where each row
    is a police report.

    Outputs:

    A csv containing a single column with anonymized reports.
    """

import pandas as pd
import string
import re
import random
import csv

#Defining prefixes, list is not exhaustive!
ROLES = ['Chief', 'Deputy Chief','DC','Major','Mjr','Mgr', 'Inspector','Insp',
        'Commander','Captain','Cpt','Cptn','Lieutenant','Ltn','Sergeant',
        'Sgt','Detective','Det','Corporal','Corp','Crp','Police Officer',
        'Officer', 'PO','Ofcr','Ofr', 'Ofc','Offc','Offr','IOfficer','IOfc',
        'Patrol Officer','Cadet','R/O','RO','Trainee', 'Agent', 'Agnt',
        'ICE','ICE Agent', 'SA','SAS','ICE', 'Manager','Mgr', 'Technician',
        'Examiner', 'Assistant', 'DA', 'District Attorney', 'Volunteer',
        'Investigator', 'CDPC', 'Patrol Officer','Analyst', 'Intern',
        'FBI Agent', 'FBI', 'Marshall', 'Probation Officer','ATF Agent', 'ATF',
        'Reserve', 'Attorney', 'Contractor' ,'MCSO','MCSD', 'Medic', 'Dispatcher']

#Getting string of all punctuation
PUNCT = string.punctuation
#Getting lowercase alphabet
ALPHABET = string.ascii_lowercase

def preprocess_reports(reports):
    """Takes a list of reports and returns a list of reports
    where all text is lowercase and whitespace has been stripped."""
    for i in range(0, len(reports)-1):
        t = reports[i]
        t = str(t)
        t = t.lower()
        t = re.sub( '\s+', ' ',  t).strip()
        reports[i] = t
    return reports

def preprocess_name_strings(list_of_names):
    """This takes a list of names and returns a list of names
    where all text is lowercase, whitespace is stripped, and punctuation
    has been removed."""
    processed_list_of_names = []
    for name in list_of_names:
        #If name is a nan, add an empty string
        if isinstance(name, float) == True:
            processed_list_of_names.append('')
        #else name is a valid string
        else:
            #Strip whitespace
            name = re.sub( '\s+', ' ',  name).strip()
            #Converting to lowercase
            name = name.lower()
            #Remove punctuation
            name = ''.join(x for x in name if x not in PUNCT)
            processed_list_of_names.append(name)
    return processed_list_of_names

#TO DO
##Some officers will share the same surnames. Add the capacity to resolve
##potential conflicts using other ID.
##Potential solution: add a lookup table with officers with the same surname
##If officer has a match in lookup table then use shift/incident info to
##identify the correct officer.

def construct_name_dictionary(NAMES, PUNCT, ALPHABET):
    """
    This function reads in the table of names,
    where columns are 'first_name', 'last_name',
    'middle_name', and [not included currently ]'hash'.

    Returns a dictionary with common permuations of each
    name as keys and hashes as values.

    Where the permutation is uncertain, e.g. guessing the
    middle initial, a question make is appended to the hash.

    NAMES MUST MAP TO TRUE HASHES.
    """
    first_names = NAMES['first_name'].tolist()
    first_names = preprocess_name_strings(first_names)
    last_names = NAMES['last_name'].tolist()
    last_names = preprocess_name_strings(last_names)
    middle_names = NAMES['middle_name'].tolist()
    middle_names = preprocess_name_strings(middle_names)
    ## hashes = NAMES['hashes'].tolist()
    assert len(first_names) == len(last_names) == len(middle_names)
    #Creating a python dictionary object to store names
    name_dict = {}
    fake_hash=10000
    for person in range(0, len(first_names)-1):
            #fake_hash = hashes[person] # Uncomment if using real hash
            first_name = first_names[person]
            last_name = last_names[person]
            middle_name = middle_names[person]
            first_last = first_name + ' ' + last_name
            name_dict[last_name] = 'ID'+str(fake_hash)
            if len(first_name) >=1 and len(middle_name) >=1:
                first_initial_middle_initial_last = first_name[0]+middle_name[0]+' '+last_name
                name_dict[first_initial_middle_initial_last] = 'ID'+str(fake_hash)
            if len(first_name) >= 1:
                name_dict[first_last] = 'ID'+str(fake_hash)
                first_initial_last = first_name[0] + ' ' + last_name
                name_dict[first_initial_last] = 'ID'+str(fake_hash)
            if len(middle_name) >= 1:
                first_middle_initial_last = first_name + ' ' + middle_name[0] + ' ' + last_name
                name_dict[first_middle_initial_last] = 'ID'+str(fake_hash)
                fake_hash+=1 # Comment out if using real hash
            else:
                for letter in ALPHABET:
                    first_middle_initial_last = first_name + ' ' + letter + ' ' + last_name
                    name_dict[first_middle_initial_last] = 'ID'+str(fake_hash)+'?'
                fake_hash+=1
    all_names = list(name_dict.keys())
    all_names.sort()
    return name_dict, all_names

def tolist(text):
    """This is a helper function that converts a report to a list of words
    and catenates any officer first initial to the following entry in the
    list. e.g. ['J', 'Smith'] becomes ['J Smith'].
    ['J','R', 'Smith'] becomes ['JR Smith']
    Returns a list of words."""
    text_as_list = text.split()
    #We join any single letters to following word if it is a name
    for j in range(0, len(text_as_list)-1):
        try:
            word = text_as_list[j]
            if len(word) == 1:
                if text_as_list[j+1] in all_names:
                    #If word afterwards is a name.
                    #then catenate j with the name and remove j
                    next_word = text_as_list[j+1]
                    text_as_list[j+1] = word+' '+next_word
                    text_as_list.pop(j)
                if text_as_list[j+2] in all_names:
                    #If word 2 afterwards is a name and next word is a single
                    #character, then catenate j with j+1 and the name
                    middle_word = text_as_list[j+1]
                    if len(middle_word) == 1:
                        name = text_as_list[j+2]
                        text_as_list[j+2] = word+middle_word+' '+name
                        text_as_list.pop(j+1)
                        text_as_list.pop(j)
        except IndexError:
            break
    return text_as_list

def remove_by_prefix(text, ROLES, ALPHABET, all_names, name_dict):
    """
    This function searches the text for strings in ROLES. If a string is found
    we then search for officer names following the string,
    e.g. "officer smith, officer jones, ..."
    If a match is found we then replace this instance, and any other instance
    of the officers name in the string with the corresponding hash code.

    Commented out is code to search for all combinations of first initials with
    officer surnames.

    Returns a string with officer names found using the above
    procedure replaced with hashes.
    """

    for r in ROLES:
        if text.startswith(r.lower()) or " "+r.lower()+" " in text:
            for n in all_names:
                if r.lower()+" "+n.lower()+" " in text:
                    #e.g."officer smith"
                    #replace with hash for officer smith
                    text = text.replace(n, name_dict[n])

    ##ADDITIONAL VERIFICATION
    #Uses list comprehension to check for some other possible
    #permutations
    text_as_list = tolist(text)
    for role in ROLES:
        if role.lower() in text_as_list:
            role_indices = [i for i, x in enumerate(text_as_list) if x == role.lower()]
            for i in role_indices:
                try:
                    if text_as_list[i+1] in all_names:
                        name = text_as_list[i+1]
                        text_as_list[i+1] = name_dict[name]
                    elif text_as_list[i+2] in all_names:
                        name = text_as_list[i+2]
                        text_as_list[i+2] = name_dict[name]
                except IndexError:
                    break
    text = ' '.join(text_as_list)
    return text

def remove_by_prefix_plural(text, ROLES, ALPHABET, all_names, name_dict):
    """This function takes a text and looks for plurals of each role in ROLES.
    e.g. Officers. If a match is found the words following the plural are
    then checked and are replaced with hashes if they correspond to officer
    names."""
    HASHES = list(name_dict.values())
    #Convert text to list of words
    text_as_list = tolist(text)
    #Main function
    for r in ROLES:
        plural = r.lower()+'s'
        if plural in text_as_list:
            #Get index of every instance of plural
            plural_indices = [i for i, x in enumerate(text_as_list) if x == plural]
            #For each index
            for plural_index in plural_indices:
                k = 1
                word = "and" #assigning word as and
                while word == "and" or word in all_names or word in ROLES or word in HASHES \
                and plural_index+k <= len(text_as_list)-1:
                    try:
                        #Set word as the next word
                        word = text_as_list[plural_index+k]
                        k+=1
                        #If the word is a name, get all indices for it
                        if word in all_names:
                            name = word
                            name_indices = [i for i, x in enumerate(text_as_list) if x == name]
                            #replace every instance with a hash
                            for n in name_indices:
                                text_as_list[n] = name_dict[name]
                        #If a name is not in the name list then maybe replace it with a blank
                    except IndexError:
                        break
    text = ' '.join(text_as_list)
    return text

def anonymize(text, ROLES, PUNCT, ALPHABET, all_names, name_dict):
    """
    Input is a string with sensitive information.
    Output is a string with each police officer's name
    and badge number replaced with a hash.
    Uncertain matches are appended by a '?'.
    """

    #Begin by removing potential badge numbers
    #This will replace the form (#999) or #999 or (999)
    #with the term BADGENUM
    text= re.sub(r'\([#0-9]*?\)', 'BADGENUM',text)
    text= re.sub('#[0-9-]+', 'BADGENUM',text)
    text= re.sub('code[0-9-]+', 'BADGENUM',text)
    text= re.sub('code no.[0-9-]+', 'BADGENUM',text)
    text= re.sub('code number [0-9-]+', 'BADGENUM',text)
    text= re.sub('[0-9]{4-5}', 'BADGENUM', text)
    #text= re.sub('p[0-9-]+', 'BADGENUM',text)
    #Then remove all punctuation
    text = ''.join(x for x in text if x not in PUNCT)
    #remove names by prefix
    text = remove_by_prefix(text, ROLES, ALPHABET, all_names, name_dict)
    #then remove names following plurals
    text = remove_by_prefix_plural(text, ROLES, ALPHABET, all_names, name_dict)
    return text

if __name__ == '__main__':
    #IMPORTING DATA
    NAMES = input("Please type the name of the file containing officer details (e.g. names_test.xls) and press Enter: ")
    REPORTS = input("Please type the name of the file containing narrative reports and press Enter: ")
    #Loading the data
    print("LOADING AND PROCESSING RAW DATA...")
    NAMES = pd.read_excel(NAMES)
    REPORTS = pd.read_excel(REPORTS)
    #Transform data to list using column titles
    reports = REPORTS['narrative'].tolist()
    reports = preprocess_reports(reports)
    names = construct_name_dictionary(NAMES, PUNCT, ALPHABET)
    name_dict = names[0]
    all_names = names[1]

    #Uncomment the 3 lines below to process a specific number of reports
    ##number = input("Please enter the number of reports you wish to anonymize: ")
    ##number = int(number)
    ##reports = random.sample(reports, number)
    ##reports = random.sample(reports, 20)
    count_down = len(reports)
    print("ANONYMIZING ",count_down, " REPORTS")
    #anonymize text
    anon_texts = []
    for r in range(0, len(reports)-1):
        anon_r = anonymize(reports[r], ROLES,PUNCT, ALPHABET, all_names, name_dict)
        anon_texts.append(anon_r)
        count_down-=1
        print(count_down, " REPORTS LEFT")

    with open('anon_reports.csv', 'a') as output_file:
        writer = csv.writer(output_file, delimiter=',')
        for i in range(0, len(anon_texts)-1):
            print("ORIGINAL TEXT: ", reports[i].lower())
            print("ANONYMIZED TEXT: ",anon_texts[i])
            print('\n')
            writer.writerow([anon_texts[i]])
