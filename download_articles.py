from Final_Approach.processing_doc import  *
from Final_Approach.test_logic_sample import  *
import time
from collections import defaultdict
import concurrent.futures

def dwnld(remaining_articles_to_download):
    i = 0
    while i < len(remaining_articles_to_download):

        if i + 5 < len(remaining_articles_to_download):
            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                results = executor.map(test_logic_sample, remaining_articles_to_download[i:i + 5])
            pdf_link = []

            for x in results:
                pdf_link.append(x)

            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                results = executor.map(processing_doc, pdf_link)

            i = i + 5
        else:
            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                results = executor.map(test_logic_sample, remaining_articles_to_download[i:])
            pdf_link = []

            for x in results:
                pdf_link.append(x)

            with concurrent.futures.ThreadPoolExecutor(10) as executor:
                results = executor.map(processing_doc, pdf_link)
            break



final_pdf_list=[]
with open(r"C:\Users\Public\Downloads\elsapy\Final_Approach\available_pdf_link.txt",'r') as f:
    for line in f.readlines():
        doi=line.split(",")[0]
        link=line.split(",")[1]
        ind=line.split(",")[-1].replace("\n","")
        final_pdf_list.append((doi,link,ind))
print(final_pdf_list[:2])
print(len(final_pdf_list))

sample_text_files_path=r"C:\Users\Public\Downloads\elsapy\Final_Approach\SampleTextFiles"
already_downloaded_text_files=[]
for (root,dirs,files) in os.walk(sample_text_files_path, topdown=True):
    if len(files)==1 and files[0].split('.')[-1]=='txt':
#         print(root.split('Sample')[-1])
        already_downloaded_text_files.append(root.split('Sample')[-1])




t1=time.perf_counter()
file_location_test=defaultdict(str)
file_location_text=defaultdict(str)

empty_folders=[]
sample_pdf_files_path=r"C:\Users\Public\Downloads\elsapy\Final_Approach\Download"

for (root,dirs,files) in os.walk(sample_pdf_files_path, topdown=True):
    if len(files)==0:
        empty_folders.append(root.split('Sample')[-1])


sample_pdf_files_path=r"C:\Users\Public\Downloads\elsapy\Final_Approach\Download"
pdf_files_completely_downloaded=[]
pdf_files_incompletely_downloaded=[]
for (root,dirs,files) in os.walk(sample_pdf_files_path, topdown=True):
    if len(files)==1:
        if files[0].split('.')[-1]=='pdf':
            pdf_files_completely_downloaded.append(root.split('Sample')[-1])
        if files[0].split('.crdownload')[-1] == '':
            pdf_files_incompletely_downloaded.append(root.split('Sample')[-1])

        # empty_folders.append(root.split('Sample')[-1])
print(len(pdf_files_incompletely_downloaded))
print(pdf_files_incompletely_downloaded)
print(len(pdf_files_completely_downloaded))
print(pdf_files_completely_downloaded[:5])


remaining_articles_to_download=[]
for x in final_pdf_list:
    # print(x[2])
    if x[2] not in ("27930") and  x[2] not in (already_downloaded_text_files +empty_folders[1:]) :
        # print(x[2])
        remaining_articles_to_download.append(x)
print(len(remaining_articles_to_download))

# if x[2] not in ("4162","4404","4405","4512","4782","4787","5010","5007","5181","17137") and





print(len(final_pdf_list))
print(len(already_downloaded_text_files))
print(len(empty_folders[1:]))

print(empty_folders)
a=dwnld(remaining_articles_to_download)

