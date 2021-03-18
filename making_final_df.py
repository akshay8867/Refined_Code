import time
from collections import defaultdict
import concurrent.futures
import pandas as pd
import gensim
from gensim.utils import lemmatize, simple_preprocess
import os
import pickle

full_text_dict=defaultdict(str)
## Creating pdf_links_df
def pdf_link():
    pdf_links_df=pd.DataFrame(columns=['doi','link','index'])
    with open('available_pdf_link.txt') as f:
        for line in f.readlines():
            doi=line.split(",")[0]
            link=line.split(",")[1]
            ind=line.split(",")[-1].replace("\n","")
            df_=pd.DataFrame([{'doi':doi,'link':link,'index':ind}])
            df_.reset_index(drop=True,inplace=True)
            pdf_links_df=pd.concat([pdf_links_df,df_],axis=0)

    # pdf_links_df.head()
    pdf_links_df.reset_index(drop=True,inplace=True)
    print(pdf_links_df.shape)
    return pdf_links_df



## Creating pdf_locations_df
def text_file():
    sample_text_files_path=r"C:\Users\Public\Downloads\elsapy\Final_Approach\SampleTextFiles"
    pdf_locations_df=pd.DataFrame(columns=['index','file_path'])
    for (root,dirs,files) in os.walk(sample_text_files_path, topdown=True):
        if len(files)==1 and files[0].split('.')[-1]=='txt':
    #         already_downloaded_text_files.append(root+"\\"+ files[0])
    #         print(root.split('Sample')[-1])
            file_path=root+"\\"+ files[0]
            index=root.split('Sample')[-1]
            df_=pd.DataFrame([{'index':index,'file_path':file_path}])
            pdf_locations_df=pd.concat([pdf_locations_df,df_],axis=0)

    pdf_locations_df.reset_index(drop=True,inplace=True)
    print(pdf_locations_df.shape)
    return pdf_locations_df
## Extracting test from text file
def extract_text(link):
    global full_text_dict
    try:

        lines=""
        with open(link[1],'r',encoding='utf-8') as f:
            print(link[0])
            for line in f.readlines():
                lines+=line
            sent = gensim.utils.simple_preprocess(str(lines), deacc=True)
            # full_text_dict[link[0]]=sent
            return (link[0],sent)
    except Exception as e:
        # full_text_dict[link[0]]=
        return (link[0],"Unable to open the text file")


if __name__=="__main__":
    t1=time.perf_counter()
    # df1 = pd.read_excel('pdf_available_df.xlsx')
    # print("df1 created")
    pdf_links_df=pdf_link()
    print("pdf_links created")
    pdf_locations_df=text_file()
    print("pdf_locations created")
    pdf_links_df = pdf_links_df.merge(pdf_locations_df, how="inner", left_on='index', right_on='index')
    pdf_list = [(link[0], link[1]) for link in pdf_links_df[['index', 'file_path']].values.tolist()]
    print(len(pdf_list))
    i = 0
    while i < len(pdf_list):
        if i + 1000 < len(pdf_list):
            print("hello")
            with concurrent.futures.ThreadPoolExecutor(100) as executor:
                results = executor.map(extract_text, pdf_list[i:i+1000])
                pdf_link=pd.DataFrame(columns=["index","full_text"])

                for x in results:
                    df_=pd.DataFrame([{'index':x[0],"full_text":x[1]}])
                    pdf_link=pd.concat([pdf_link,df_],axis=0)
                pdf_link.reset_index(drop=True,inplace=True)
                test_df = pdf_links_df.merge(pdf_link, how="inner", on='index')
                test_df = test_df.drop(['file_path'], axis=1)
                path=r'C:\Users\Public\Downloads\elsapy\Final_Approach\Pickle_Object'
                test_df.to_pickle(path+ "\\"+'full_text_df{}.pkl'.format(i))
                i=i+1000
        else:
            print("bye")
            print(len(pdf_list[i:]))
            with concurrent.futures.ThreadPoolExecutor(100) as executor:
                results = executor.map(extract_text, pdf_list[i:])
                pdf_link = pd.DataFrame(columns=["index", "full_text"])

                for x in results:

                    df_ = pd.DataFrame([{'index': x[0], "full_text": x[1]}])
                    pdf_link = pd.concat([pdf_link, df_], axis=0)
                pdf_link.reset_index(drop=True, inplace=True)
                print(pdf_link.head())
                test_df = pdf_links_df.merge(pdf_link, how="inner", on='index')
                print(test_df.head())
                test_df = test_df.drop(['file_path'], axis=1)
                path = r'C:\Users\Public\Downloads\elsapy\Final_Approach\Pickle_Object'
                test_df.to_pickle(path+ "\\"+'full_text_df{}.pkl'.format(i))
            break


    # global full_text_df
    # full_text_df = pd.DataFrame({'index': list(full_text_dict.keys()), 'Full_Text': list(full_text_dict.values())})
    # print("full_text_df created")
    # test_df = pdf_links_df.merge(full_text_df, how="inner", on='index')
    # test_df = test_df.drop(['file_path'], axis=1)
    # test_df = df1.merge(test_df, how="inner", left_on="prism:doi", right_on="doi")
    # test_df.reset_index(drop=True, inplace=True)
    # # test_df = test_df.drop(['file_path'], axis=1)
    # test_df = df1.merge(test_df, how="inner", left_on="prism:doi", right_on="doi")
    # test_df.reset_index(drop=True, inplace=True)
    # test_df=test_df.drop(['doi'], axis=1)
    # test_df.to_pickle('full_text_df.pkl')
    t2 = time.perf_counter()
    print(f'Finished in {(t2-t1)/60} minutes')

