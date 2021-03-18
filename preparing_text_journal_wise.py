import pandas as pd
import os
from collections import defaultdict
import pickle
r"C:\Users\Public\Downloads\elsapy\Final_Approach\pdf_available_df.xlsx"

def extract_pickle_path():
    pickle_path = r"C:\Users\Public\Downloads\elsapy\Final_Approach\Pickle_Object"
    pickle_objects = []
    for root, directories, files in os.walk(pickle_path, topdown=True):
        for i in files:
            if ".pkl" in i :

                print(os.path.join(root, i))
                pickle_objects.append(os.path.join(root, i))
    return pickle_objects

def extract_full_text_objects():
    pickle_path = r"C:\Users\Public\Downloads\elsapy\Final_Approach\Lemmatised_Text"
    pickle_objects = []
    for root, directories, files in os.walk(pickle_path, topdown=True):
        for i in files:

            if ".pkl" in i:

                print(os.path.join(root, i))
                pickle_objects.append(os.path.join(root, i))
    return pickle_objects







def journal_df(journal_name):
    lemma_path = extract_full_text_objects()
    doi_path = extract_pickle_path()
    parent_dir = r'C:\Users\Public\Downloads\elsapy\Final_Approach\Full_Text_With_Lemma_Text'
    main_df = pd.read_excel(r'C:\Users\Public\Downloads\elsapy\Final_Approach\pdf_available_df.xlsx')
    main_df=main_df[main_df['prism:publicationName']==journal_name]
    main_df.reset_index(drop=True)
    col=['doi', 'link', 'index', 'full_text', 'lemma_text',
       'prism:publicationName', 'dc:title', 'prism:doi', 'prism:url',
       'dc:identifier', 'prism:coverDate', 'citedby-count', 'Year']


    for k, v in zip(doi_path, lemma_path):
        df = pd.read_pickle(k)
        with open(v, 'rb') as f:
            dct=pickle.load(f)
            lemma_text=list(dct.values())[0]
            # print(lemma_text[:2])
            df['lemma_text'] = lemma_text


        df = df.merge(main_df, how="inner", left_on='doi', right_on='prism:doi')
        if df.shape[0]>0:
            print(len(df['lemma_text'][0]))
            df.reset_index(drop=True,inplace=True)
            df.to_pickle(os.path.join(parent_dir,"{}_{}".format(journal_name,v.split("\\")[-1])))



if __name__=='__main__':
    journal_names=['Water Resources Research',
                   'Water Resources Management',
                   'Sustainable Water Resources Management',
                   'Canadian Water Resources Journal',
                   'Journal of Water Resources Planning and Management',
                   'Water Environment Research',
                   'Integrated Environmental Assessment and Management']
    print(len(journal_names))
    for journal_name in journal_names:
        journal_df(journal_name)
        print("{} Completed".format(journal_name))
