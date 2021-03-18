import io
import os

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from pdfminer.high_level import extract_text
from _collections import defaultdict
import shutil
import time
import math

file_location_text=defaultdict(str)

import signal
import functools

# class TimedOutExc(Exception):
#     """
#     Raised when a timeout happens
#     """
#
# def timeout(timeout):
#     """
#     Return a decorator that raises a TimedOutExc exception
#     after timeout seconds, if the decorated function did not return.
#     """
#
#     def decorate(f):
#
#         def handler(signum, frame):
#             raise TimedOutExc()
#
#         @functools.wraps(f)  # Preserves the documentation, name, etc.
#         def new_f(*args, **kwargs):
#
#             old_handler = signal.signal(signal.SIGALRM, handler)
#             signal.alarm(timeout)
#
#             result = f(*args, **kwargs)  # f() always returns, in this scheme
#             print(result)
#
#             signal.signal(signal.SIGALRM, old_handler)  # Old signal handler is restored
#             signal.alarm(0)  # Alarm removed
#
#             return result
#
#         return new_f
#
#     return decorate
#
# @timeout(120)
def processing_doc(args):
    # try:
        rsrcmgr = PDFResourceManager()
        codec = 'utf-8'
        laparams = LAParams()

        outstream = io.StringIO()
        laparams = LAParams()
        rsrcmgr = PDFResourceManager(caching=True)
        device = TextConverter(rsrcmgr, outstream, laparams=laparams,
                               imagewriter=None)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        password = ""
        maxpages = 0
        caching = True
        pagenos = set()
        path = args[1]
        if args[1] == "Full text not found":
            file_location_text[args[0]] = "Full text not found"
            return 0
        folder_name = args[1].split("\\")[-2]
        file_name = args[1].split("\\")[-1].split(".pdf")[0] + ".txt"
        print(folder_name)
        print(file_name)
        m = r"C:\Users\Public\Downloads\elsapy\Final_Approach\SampleTextFiles"
        print(os.path.join(m, folder_name))
        if os.path.exists(os.path.join(m, folder_name)):
            shutil.rmtree(os.path.join(m, folder_name))
            print("Directory has been deleted")

        if not os.path.exists(os.path.join(m, folder_name)):
            os.makedirs(os.path.join(m, folder_name))

        write_file = m + "\\" + folder_name + "\\" + file_name
        print(write_file)

        try:

            with open(path, 'rb') as fp:
                text = extract_text(fp)
                print("{}:{}".format(folder_name,text[:5] ))
                with open(write_file, 'w+', encoding="utf-8") as wp:
                    wp.write(text)
                file_location_text[args[0]] = write_file
            if os.path.exists("\\".join(path.split("\\")[:-1])):
                shutil.rmtree("\\".join(path.split("\\")[:-1]))
                print("Directory has been deleted")
                return 1
        except Exception as e:
            print(e)
            return e

    # except TimedOutExc:
    #     with open("unsucessful_conversion.txt",'a+') as f:
    #         print("Unsuccessful conversion:{}".format(args[1]))
    #         f.write("Unsuccessful conversion:{}\n".format(args[1]))
    #     return 0

#             data_dict[args[0]] = outstream.getvalue()
#             data_dict[args[0]] = [word for word in gensim.utils.simple_preprocess(data_dict[args[0]], deacc=True) if
#                               (word not in stop_words)]
#             data_dict[args[0]] = [word for word in data_dict[args[0]] if nltk.pos_tag([word])[0][1] in ["NN", "VB"]]
#             return 1





