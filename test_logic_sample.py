import shutil
import pickle
from selenium import webdriver
import threading
import time
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import os
from _collections import defaultdict
import concurrent.futures
import time
file_location_test=defaultdict(str)

def test_logic_sample(link):
    print(link[2])
    try:
        m = r"C:\Users\Public\Downloads\elsapy\Final_Approach\Download\Sample{}".format(link[2])

        if os.path.exists(m):
            shutil.rmtree(m)
            print("Directory has been deleted")

        if not os.path.exists(m):
            os.makedirs(m)

        opts = Options()
        # opts.add_argument(r'user-data-dir=C:\Users\aksha\AppData\Local\Google\Chrome\User Data')
        opts.add_argument('--profile-directory=Profile 2')
        # opts.add_argument('--window-size=1280,800')
        opts.add_argument('--headless')
        opts.add_argument('--no-sandbox')
        prefs = {"profile.default_content_settings.popups": 0, "download.default_directory": m,
                 "directory_upgrade": True}
        opts.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(executable_path=r'C:\Users\aksha\Downloads\chromedriver_win32\chromedriver.exe',
                                  options=opts)
        driver.get(link[1])
        time.sleep(1)
        file_location_test[link[0]] = m + "\\" + sorted(os.listdir(m), key=lambda x: os.path.getmtime(m + "\\" + x))[-1]
        print(link[2])
        #         print(file_location[link[0]])

        fileends = "crdownload"
        while "crdownload" == fileends:
            time.sleep(2)
            files = sorted(os.listdir(m), key=lambda x: os.path.getmtime(m + "\\" + x))[-1]
            filename = m + "\\" + files
            print(files)
            file_location_test[link[0]] = m + "\\" + files

            if "crdownload" in files:
                print("hello")
                fileends = "crdownload"
            else:
                fileends = "none"
        driver.quit()
        return (link[0], file_location_test[link[0]])
    except Exception as e:
        file_location_test[link[0]] = "Full text not found"
        print("Full text not found")

        return (link[0], file_location_test[link[0]])


