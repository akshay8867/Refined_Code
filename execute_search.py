### This file would be used to call the scopus API and fetch the results
import validators

from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
import json
import requests
import time
import concurrent.futures
import time
import os
import pandas as pd
import shutil
from collections import defaultdict
import urllib.request
from bs4 import BeautifulSoup


available_pdf_dict = defaultdict(str)
missing_pdf_dict=defaultdict(str)
still_missing_pdf_dict=defaultdict(str)
opener = urllib.request.build_opener()
opener.addheaders = [('Connection', 'keep-alive'), ('Accept-Charset', 'ISO-8859-1,utf-8;q=0.7,*;q=0.3'),
                     ('Accept-Encoding', 'none'), ('Accept-Language', 'en-US,en;q=0.8'),
                     ('Accept', 'application/vnd.crossref.unixsd+xml'), ('Referer', 'https://cssspritegenerator.com'),
                     ('User-Agent',
                      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11')]

hdr = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
         'Referer': 'https://cssspritegenerator.com',
         'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
         'Accept-Encoding': 'none',
         'Accept-Language': 'en-US,en;q=0.8',
'Connection': 'keep-alive'}

i=0

con_file = open("config.json")
config = json.load(con_file)
con_file.close()

client = ElsClient(config['apikey'])
journal_names=['Water Resources Research',
                   'Water Resources Management','Water Research','Advances in Water Resources',
                   'Canadian Water Resources Journal','Journal of Water Resources',
                   'Sustainable Water Resources Management','Water Environment Research',
                   'Journal of Water Resources Planning and Management',
                   'Journal of hydrology','Integrated Environmental Assessment and Management',
                   'Environmental Impact Assessment Review',' Annual Review of Environment and Resources',
                  'Journal of Environmental Management','Environmental modelling and software']

selected_columns = ['prism:publicationName', 'dc:title', 'prism:doi', 'prism:url', 'dc:identifier',
                            'prism:coverDate', 'citedby-count','Year']

combined_research_papers = pd.DataFrame(columns=selected_columns)


def execute_search(query):
    selected_columns1 = ['prism:publicationName', 'dc:title', 'prism:doi', 'prism:url', 'dc:identifier',
                         'prism:coverDate', 'citedby-count']
    global journal_names
    global client
    try:
        client = ElsClient(config['apikey'])
        doc_srch = ElsSearch(query, 'scopus')
        doc_srch.execute(client, get_all=True)

        print("doc_srch has", len(doc_srch.results), "results.")
        print(doc_srch.hasAllResults())

        results_df = doc_srch.results_df[selected_columns1]

        results_df = results_df[results_df['prism:publicationName'].isin(journal_names)]
        results_df['Year'] = results_df['prism:coverDate'].apply(lambda x: x.year)
        print("{} was completed".format(query))
        time.sleep(1)


        return results_df

    except Exception as e:
        global queries_with_no_result
        queries_with_no_result.append(query)
        print("{}:{} not completed".format(e, query))
        return str(e)


def query_creation():
    queries = []
    pubyears = [i for i in range(2020, 1969, -1)]
    global journal_names
    for journal_name in journal_names:

        for pubyear in pubyears:
            try:
                query = "EXACTSRCTITLE ({" + journal_name + "})" + "AND PUBYEAR = {}".format(pubyear)
                #                 print(query)
                queries.append(query)
            except Exception as e:
                print(e)
    return queries

def execute_results():
    t1 = time.perf_counter()
    queries=query_creation()
    # t2 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(9) as executor:
        #     results=executor.map(hjhs,doi_list)
        results = executor.map(execute_search, queries)

    t2 = time.perf_counter()

    print(f'Finished in {t2 - t1} seconds')
    global combined_research_papers

    for df in results:
        if isinstance(df, str) == False:
            combined_research_papers = pd.concat([combined_research_papers, df.reset_index(drop=True)],
                                                 ignore_index=True, axis=0)
            combined_research_papers.drop_duplicates(subset=['dc:title'], inplace=True)
            combined_research_papers = combined_research_papers.reset_index(drop=True)

            combined_research_papers.to_excel(
                os.path.join(os.getcwd(), "Final_Approach", "combined_research_papers.xlsx"),
                index=False)

    m = r"C:\Users\Public\Downloads\RefinedCode"

    available_jorunal_list=['Water Resources Research',
     'Water Resources Management',
     'Water Environment Research',
     'Journal of Water Resources Planning and Management',
     'Canadian Water Resources Journal',
     'Integrated Environmental Assessment and Management',
     'Sustainable Water Resources Management']
    available_df=combined_research_papers[combined_research_papers['prism:publicationName'].isin(available_jorunal_list)].shape
    return available_df




def available_extract_links(doi):
    global i
    path = 'http://dx.doi.org/{}'.format(doi)
    try:
        i = i + 1
        print(i)
        r = opener.open(path)

        #         print (r.info()['Link'])

        m = r'rel="item"'
        n = r'rel="canonical"'
        ck = True
        #         print(r.info()['Link'])
        for x in r.info()['Link'].split(';'):
            if ((x.split(',')[0].strip() == m.strip() or x.split(',')[0].strip() == n.strip()) and ('pdf' in x)
                    and ck):
                # print("Item found:{}".format(str(x)))
                pdf_url = x.split(',')[1][2:-1]
                #                 print("1")

                available_pdf_dict[doi] = pdf_url
                #                 f.write(("{}:{}\n".format(doi,pdf_url)))

                ck = False
                print(pdf_url)
                return (doi, pdf_url)
        missing_pdf_dict[doi] = "Full link not found"
        return (doi, "Full link not found")

    except Exception as e:
        missing_pdf_dict[doi] = "Full link not found"
        print("Full link not found")
        return (doi, "Full link not found")


def available_write_links(args):
    f=args[2]
#     i=i+1
#     print(i)
    f.write(("{}:{}\n".format(args[0],args[1])))


def bs4(doi):
    global still_missing_pdf_dict
    try:
        doi = doi
        path = 'http://dx.doi.org/{}'.format(doi)
        response = requests.get(path, headers=hdr)
        soup = BeautifulSoup(response.text, 'html.parser')
        check = True
        for link in soup.find_all('a'):
            if 'pdf' in str(link.get('href')):
                if 'epdf' not in str(link.get('href')) and 'pdf' in str(link.get('href')) and check:
                    if validators.url(link.get('href')):
                        print(link.get('href'))
                        available_pdf_dict[doi] = link.get('href')
                        print(link.get('href'))
                        #                             f.write(("{}:{}\n".format(doi,link.get('href'))))
                        return (doi, link.get('href'))
                    else:
                        starting_url = response.url.split('/d')[0]
                        link1 = starting_url + link.get('href')
                        print(link1)
                        available_pdf_dict[doi] = link1
                        #                             f.write(("{}:{}\n".format(doi,link1)))
                        return (doi, link1)
        still_missing_pdf_dict[doi] = "Full link not found"
    except Exception as e:
        still_missing_pdf_dict[doi] = "Full link not found"
        print("{}:Full link not found".format(doi))
        return (doi, "Full link not found")


def fetch_doi(available_df):
    links_available_journals = ['Water Resources Research', 'Water Resources Management', 'Water Environment Research',
                                'Journal of Water Resources Planning and Management',
                                'Canadian Water Resources Journal',
                                'Integrated Environmental Assessment and Management',
                                'Sustainable Water Resources Management']


    path = r"C:\Users\Public\Downloads\RefinedCode"
    t1 = time.perf_counter()

    for journal in links_available_journals:
        print("{} started".format(journal))
        doi_list = list(available_df[available_df['prism:publicationName'] == journal]['prism:doi'].values)
        #     print(doi_list)

        with concurrent.futures.ThreadPoolExecutor(50) as executor:
            results = executor.map(available_extract_links, doi_list)

        f = open(path + "{}.txt".format(journal), 'a+')
        pdf_link = []
        for x in results:
            pdf_link.append((x[0],x[1],f))


        with concurrent.futures.ThreadPoolExecutor(50) as executor:
            results = executor.map(available_write_links, pdf_link)
        f.close()
        print("{} completed".format(journal))

    print("completed")
    t2 = time.perf_counter()
    print(f'Finished in {(t2 - t1) / 60} minutes')


    still_missing_pdf_dict = defaultdict(str)
    t1 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(50) as executor:
        results = executor.map(bs4, list(missing_pdf_dict.keys()))
        pdf_link = []
        f = open(path + "doi_missing.txt", 'a+')
        for x in results:
            pdf_link.append(x[0],x[1],f)


        with concurrent.futures.ThreadPoolExecutor(50) as executor:
            results = executor.map(available_write_links, pdf_link)
        f.close()
        print("{} completed".format(journal))

    t2 = time.perf_counter()
    print(f'Finished in {(t2 - t1) / 60} minutes')
    print("completed")
    with open("available_pdf_link.txt", 'w') as f:
        for k, v in enumerate(available_pdf_dict.items()):
            f.write('{},{},{}\n'.format(v[0], v[1], k))



