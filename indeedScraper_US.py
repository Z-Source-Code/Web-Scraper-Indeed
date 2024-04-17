from bs4 import BeautifulSoup # For webscraping
from lxml import etree as et # Parsing and creating xml data
from csv import writer # Store data as a csv file written out
import time # In general to use with timing our function calls to Indeed
from time import sleep # Assist with creating incremental timing for our scraping to seem more human
import pandas as pd # Dataframe stuff
from random import randint # Random integer for more realistic timing for clicks, buttons and searches during scraping
from datetime import datetime

# Selenium 4:
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By

# Check version I am running
print(selenium.__version__)

# Allows you to cusotmize: ingonito mode, maximize window size, headless browser, disable certain features, etc
option= webdriver.ChromeOptions()

# Going undercover:
option.add_argument("--incognito")


# Define job and location search keywords
job_search_keyword = ['Data Science', 'Business Analyst', 'Data Engineer', 
                      'Machine Learning Engineer']

# Define Locations of Interest
# location_search_keyword = ['Texas', 'New York']
location_search_keyword = ['Houston', 'Chicago', 'Atlanta', 'Philadelphia', 
                           'Orlando', 'Los Angeles', 'Dallas', 'Las Vegas', 
                           'Washington', 'Texas', 'New York','Remote']

# Finding location, position, radius=35 miles, sort by date and starting page
paginaton_url = 'https://www.indeed.com/jobs?q={}&l={}&radius=100&filter=0&sort=date&start={}'
# paginaton_url = 'https://www.indeed.com/jobs?q={}&l={}&filter=0&sort=date&start={}'

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),
                         options=option)

# First let's try to find number of jobs for a given posting


job_=job_search_keyword[0].replace(" ", "+")

total_loc = len(location_search_keyword)

log = []

for loc in range(11, total_loc):
    print ()
    print('===============================================================')
    print('Location ' + str(loc+1) + ' of ' + str(total_loc) + ': ' + location_search_keyword[loc])
    start = time.time()
    location_=location_search_keyword[loc].replace(" ", "+")

    driver.get(paginaton_url.format(job_,location_,0))

    sleep(randint(2, 6))

    p=driver.find_element(By.CLASS_NAME,'jobsearch-JobCountAndSortPane-jobCount').text

    p = p.replace(",","")

    # Max number of pages for this search! 
    max_iter_pgs=int(p.split(' ')[0])//15 
    rem = rem = int(p.split(' ')[0])%15

    if (rem > 0):
        max_iter_pgs += 1        

    end = time.time()

    part1_log = 'part one ===> ' + str(end - start) + ' seconds to complete Query!'
    print(part1_log)
    print('-----------------------')
    print('Max Iterable Pages for this search: ',max_iter_pgs)
    print('jobs :', p)

    # Pagination
    start = time.time()

    job_lst=[]

    sleep(randint(2, 6))

    for i in range(0,max_iter_pgs):
        print ('Page ', str(i+1), ' of ', str(max_iter_pgs), ' from location ', str(loc+1), ': ' ,location_ , ', total locations = ', str(total_loc))
        driver.get(paginaton_url.format(job_,location_,i*10))    
        
        sleep(randint(2, 4))

        job_page = driver.find_element(By.ID,"mosaic-jobResults")
        jobs = job_page.find_elements(By.CLASS_NAME,"job_seen_beacon") # return a list

        for jj in jobs:
            job_title = jj.find_element(By.CLASS_NAME,"jobTitle")

            col1_job_title = job_title.text
            col2_href1 = job_title.find_element(By.CSS_SELECTOR,"a").get_attribute("href")
            col3_id = job_title.find_element(By.CSS_SELECTOR,"a").get_attribute("id")
            col4_company_name = jj.find_element(By.CLASS_NAME,"company_location").text.split('\n')[0] #company_name
            col5_company_location = jj.find_element(By.CLASS_NAME,"company_location").text.split('\n')[1] #company_location
            col6_posted_date = jj.find_element(By.XPATH,'//*[@id="mosaic-provider-jobcards"]/ul/li[1]/div/div/div/div/div/table/tbody/tr/td[1]/div[3]/div[2]/span[1]').text.split('\n')[1]

            id_x = col3_id[4:]
            col7_href2 = 'https://www.indeed.com/viewjob?jk=' + id_x

            job_lst.append([col1_job_title, col2_href1, col3_id, col4_company_name,
                        col5_company_location, col6_posted_date, col7_href2])

    job_x = job_.replace("+","_")
    location_x = location_.replace("+","_")
    today = datetime.now()

    file_name = job_x + '_' + location_x +  '_'  + str(today.year) + '_' + str(today.month) + '_' + str(today.day)   
    file_part1 = 'RawData/' + file_name + '_part_'+ '1.csv'

    cols = ['job_title', 'href_1', 'id', 'cocmpany_name', 'company_location', 'posted_date', 'href_2']

    df = pd.DataFrame(job_lst, columns=cols)
    df.to_csv(file_part1, sep=',', index=False, encoding='utf-8')

    end = time.time()

    #driver.quit()

    part2_log = 'part two ===> ' + str(end - start) + ' seconds to complete Query!'
    print(part2_log)

    job_description_list_02=[]
    descr_link_lst = df['href_2'].tolist()

    start = time.time()

    index = 0

    num_jobs = len(descr_link_lst)

    for link in descr_link_lst:

        index += 1
        print ('Job ', str(index), ' of ', str(num_jobs), ' from location ', str(loc+1), ': ' ,location_ , ', total locations = ', str(total_loc))
        print(link)

        driver.get(link)

        sleep(randint(2, 5))
        try:
            desc = "" + str(driver.find_element(By.ID,"jobDescriptionText").text)
            job_description_list_02.append(desc)
            #job_description_list_02.append(driver.find_element(By.XPATH,'//*[@id="jobDescriptionText"]').text)
        except: 
            print('none')    
            job_description_list_02.append(None)

    df['job_description'] = job_description_list_02

    file_part2 = 'RawData/' + file_name + '.csv'
    df.to_csv(file_part2, sep=',', index=False, encoding='utf-8')

    end = time.time()

    part3_log = 'part three ===> ' + str(end - start) + 'seconds to complete Query!'

    print(part3_log)

    log.append([location_, part1_log, max_iter_pgs, part2_log, len(jobs), part3_log])

driver.quit()

cols_log = ['location', 'part1_log', 'max_iter_pgs', 'part2_log', 'num_jobs', 'part3_log']

df_log = pd.DataFrame(log, columns=cols_log)
today2 = datetime.now()
log_file = 'RawData/' + 'log' +  '_'  + str(today2.year) + '_' + str(today2.month) + '_' + str(today2.day) + '.csv'
df_log.to_csv(log_file, sep=',', index=False, encoding='utf-8')
