# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 15:16:21 2018

@author: jweiss
"""

# Importing built-in libraries (no need to install these)
import re
import os
from time import gmtime, strftime
from datetime import datetime, timedelta
import unicodedata
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import requests
import bs4 as bs
from lxml import html
from tqdm import tqdm

def MapTickerToCik(tickers):
    url = 'http://www.sec.gov/cgi-bin/browse-edgar?CIK={}&Find=Search&owner=exclude&action=getcompany'
    cik_re = re.compile(r'.*CIK=(\d{10}).*')
    cik_dict = {}
    for ticker in tqdm(tickers):
        results = cik_re.findall(requests.get(url.format(ticker)).text)
        if len(results):
            cik_dict[str(ticker).lower()] = str(results[0])
    return cik_dict

def WriteLogFile(log_file_name, text):
    with open(log_file_name, "a") as log_file:
        log_file.write(text)

    return

def Scrape10K(browse_url_base, filing_url_base, doc_url_base, cik, log_file_name):
    
    '''
    Scrapes all 10-Ks and 10-K405s for a particular 
    CIK from EDGAR.
    
    Parameters
    ----------
    browse_url_base : str
        Base URL for browsing EDGAR.
    filing_url_base : str
        Base URL for filings listings on EDGAR.
    doc_url_base : str
        Base URL for one filing's document tables
        page on EDGAR.
    cik : str
        Central Index Key.
    log_file_name : str
        Name of the log file (should be a .txt file).
        
    Returns
    -------
    None.
    
    '''
    
    # Check if we've already scraped this CIK
    try:
        os.mkdir(cik)
    except OSError:
        print("Already scraped CIK", cik)
        return
    
    # If we haven't, go into the directory for that CIK
    os.chdir(cik)
    
    print('Scraping CIK', cik)
    
    # Request list of 10-K filings
    res = requests.get(browse_url_base % cik)
    
    # If the request failed, log the failure and exit
    if res.status_code != 200:
        os.chdir('..')
        os.rmdir(cik) # remove empty dir
        text = "Request failed with error code " + str(res.status_code) + \
               "\nFailed URL: " + (browse_url_base % cik) + '\n'
        WriteLogFile(log_file_name, text)
        return

    # If the request doesn't fail, continue...
    
    # Parse the response HTML using BeautifulSoup
    soup = bs.BeautifulSoup(res.text, "lxml")

    # Extract all tables from the response
    html_tables = soup.find_all('table')
    
    # Check that the table we're looking for exists
    # If it doesn't, exit
    if len(html_tables) < 3:
        os.chdir('..')
        return
    
    # Parse the Filings table
    filings_table = pd.read_html(str(html_tables[2]), header=0)[0]
    filings_table['Filings'] = [str(x) for x in filings_table['Filings']]

    # Get only 10-K and 10-K405 document filings
    filings_table = filings_table[(filings_table['Filings'] == '10-K') | (filings_table['Filings'] == '10-K405')]

    # If filings table doesn't have any
    # 10-Ks or 10-K405s, exit
    if len(filings_table)==0:
        os.chdir('..')
        return
    
    # Get accession number for each 10-K and 10-K405 filing
    filings_table['Acc_No'] = [x.replace('\xa0',' ')
                               .split('Acc-no: ')[1]
                               .split(' ')[0] for x in filings_table['Description']]

    # Iterate through each filing and 
    # scrape the corresponding document...
    for index, row in filings_table.iterrows():
        
        # Get the accession number for the filing
        acc_no = str(row['Acc_No'])
        
        # Navigate to the page for the filing
        docs_page = requests.get(filing_url_base % (cik, acc_no))
        
        # If request fails, log the failure
        # and skip to the next filing
        if docs_page.status_code != 200:
            os.chdir('..')
            text = "Request failed with error code " + str(docs_page.status_code) + \
                   "\nFailed URL: " + (filing_url_base % (cik, acc_no)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue

        # If request succeeds, keep going...
        
        # Parse the table of documents for the filing
        docs_page_soup = bs.BeautifulSoup(docs_page.text, 'lxml')
        docs_html_tables = docs_page_soup.find_all('table')
        if len(docs_html_tables)==0:
            continue
        docs_table = pd.read_html(str(docs_html_tables[0]), header=0)[0]
        docs_table['Type'] = [str(x) for x in docs_table['Type']]
        
        # Get the 10-K and 10-K405 entries for the filing
        docs_table = docs_table[(docs_table['Type'] == '10-K') | (docs_table['Type'] == '10-K405')]
        
        # If there aren't any 10-K or 10-K405 entries,
        # skip to the next filing
        if len(docs_table)==0:
            continue
        # If there are 10-K or 10-K405 entries,
        # grab the first document
        elif len(docs_table)>0:
            docs_table = docs_table.iloc[0]
        
        docname = docs_table['Document']
        
        # If that first entry is unavailable,
        # log the failure and exit
        if str(docname) == 'nan':
            os.chdir('..')
            text = 'File with CIK: %s and Acc_No: %s is unavailable' % (cik, acc_no) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue       
        
        # If it is available, continue...
        
        # Request the file
        file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname))
        
        # If the request fails, log the failure and exit
        if file.status_code != 200:
            os.chdir('..')
            text = "Request failed with error code " + str(file.status_code) + \
                   "\nFailed URL: " + (doc_url_base % (cik, acc_no.replace('-', ''), docname)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue
        
        # If it succeeds, keep going...
        
        # Save the file in appropriate format
        if '.txt' in docname:
            # Save text as TXT
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.txt'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()
        else:
            # Save text as HTML
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.html'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()
        
    # Move back to the main 10-K directory
    os.chdir('..')
        
    return

def Scrape10Q(browse_url_base, filing_url_base, doc_url_base, cik, log_file_name):
    
    '''
    Scrapes all 10-Qs for a particular CIK from EDGAR.
    
    Parameters
    ----------
    browse_url_base : str
        Base URL for browsing EDGAR.
    filing_url_base : str
        Base URL for filings listings on EDGAR.
    doc_url_base : str
        Base URL for one filing's document tables
        page on EDGAR.
    cik : str
        Central Index Key.
    log_file_name : str
        Name of the log file (should be a .txt file).
        
    Returns
    -------
    None.
    
    '''
    
    # Check if we've already scraped this CIK
    try:
        os.mkdir(cik)
    except OSError:
        print("Already scraped CIK", cik)
        return
    
    # If we haven't, go into the directory for that CIK
    os.chdir(cik)
    
    print('Scraping CIK', cik)
    
    # Request list of 10-Q filings
    res = requests.get(browse_url_base % cik)
    
    # If the request failed, log the failure and exit
    if res.status_code != 200:
        os.chdir('..')
        os.rmdir(cik) # remove empty dir
        text = "Request failed with error code " + str(res.status_code) + \
               "\nFailed URL: " + (browse_url_base % cik) + '\n'
        WriteLogFile(log_file_name, text)
        return
    
    # If the request doesn't fail, continue...

    # Parse the response HTML using BeautifulSoup
    soup = bs.BeautifulSoup(res.text, "lxml")

    # Extract all tables from the response
    html_tables = soup.find_all('table')
    
    # Check that the table we're looking for exists
    # If it doesn't, exit
    if len(html_tables)<3:
        print("table too short")
        os.chdir('..')
        return
    
    # Parse the Filings table
    filings_table = pd.read_html(str(html_tables[2]), header=0)[0]
    filings_table['Filings'] = [str(x) for x in filings_table['Filings']]

    # Get only 10-Q document filings
    filings_table = filings_table[filings_table['Filings'] == '10-Q']

    # If filings table doesn't have any
    # 10-Ks or 10-K405s, exit
    if len(filings_table)==0:
        os.chdir('..')
        return
    
    # Get accession number for each 10-K and 10-K405 filing
    filings_table['Acc_No'] = [x.replace('\xa0',' ')
                               .split('Acc-no: ')[1]
                               .split(' ')[0] for x in filings_table['Description']]

    # Iterate through each filing and 
    # scrape the corresponding document...
    for index, row in filings_table.iterrows():
        
        # Get the accession number for the filing
        acc_no = str(row['Acc_No'])
        
        # Navigate to the page for the filing
        docs_page = requests.get(filing_url_base % (cik, acc_no))
        
        # If request fails, log the failure
        # and skip to the next filing    
        if docs_page.status_code != 200:
            os.chdir('..')
            text = "Request failed with error code " + str(docs_page.status_code) + \
                   "\nFailed URL: " + (filing_url_base % (cik, acc_no)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue
            
        # If request succeeds, keep going...
        
        # Parse the table of documents for the filing
        docs_page_soup = bs.BeautifulSoup(docs_page.text, 'lxml')
        docs_html_tables = docs_page_soup.find_all('table')
        if len(docs_html_tables)==0:
            continue
        docs_table = pd.read_html(str(docs_html_tables[0]), header=0)[0]
        docs_table['Type'] = [str(x) for x in docs_table['Type']]
        
        # Get the 10-K and 10-K405 entries for the filing
        docs_table = docs_table[docs_table['Type'] == '10-Q']
        
        # If there aren't any 10-K or 10-K405 entries,
        # skip to the next filing
        if len(docs_table)==0:
            continue
        # If there are 10-K or 10-K405 entries,
        # grab the first document
        elif len(docs_table)>0:
            docs_table = docs_table.iloc[0]
        
        docname = docs_table['Document']
        
        # If that first entry is unavailable,
        # log the failure and exit
        if str(docname) == 'nan':
            os.chdir('..')
            text = 'File with CIK: %s and Acc_No: %s is unavailable' % (cik, acc_no) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue       
        
        # If it is available, continue...
        
        # Request the file
        file = requests.get(doc_url_base % (cik, acc_no.replace('-', ''), docname))
        
        # If the request fails, log the failure and exit
        if file.status_code != 200:
            os.chdir('..')
            text = "Request failed with error code " + str(file.status_code) + \
                   "\nFailed URL: " + (doc_url_base % (cik, acc_no.replace('-', ''), docname)) + '\n'
            WriteLogFile(log_file_name, text)
            os.chdir(cik)
            continue
            
        # If it succeeds, keep going...
        
        # Save the file in appropriate format
        if '.txt' in docname:
            # Save text as TXT
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.txt'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()
        else:
            # Save text as HTML
            date = str(row['Filing Date'])
            filename = cik + '_' + date + '.html'
            html_file = open(filename, 'a')
            html_file.write(file.text)
            html_file.close()
        
    # Move back to the main 10-Q directory
    os.chdir('..')
        
    return

# Get lists of tickers from NASDAQ, NYSE, AMEX
nasdaq_tickers = pd.read_csv('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download')
nyse_tickers = pd.read_csv('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download')
amex_tickers = pd.read_csv('https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download')

# Drop irrelevant cols
nasdaq_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)
nyse_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)
amex_tickers.drop(labels='Unnamed: 8', axis='columns', inplace=True)

# Create full list of tickers/names across all 3 exchanges
tickers = list(set(list(nasdaq_tickers['Symbol']) + list(nyse_tickers['Symbol']) + list(amex_tickers['Symbol'])))
cik_dict = MapTickerToCik(tickers)
ticker_cik_df = pd.DataFrame.from_dict(data=cik_dict, orient='index')
ticker_cik_df.reset_index(inplace=True)
ticker_cik_df.columns = ['ticker', 'cik']
ticker_cik_df['cik'] = [str(cik) for cik in ticker_cik_df['cik']]

# Keep first ticker alphabetically for duplicated CIKs
ticker_cik_df = ticker_cik_df.sort_values(by='ticker')
ticker_cik_df.drop_duplicates(subset='cik', keep='first', inplace=True)

pathname_10k = 'C:/Users/jweiss/Desktop/10k//'
pathname_10q = 'C:/Users/jweiss/Desktop/10q//'

# Run the function to scrape 10-Ks

# Define parameters
browse_url_base_10k = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=10-K'
filing_url_base_10k = 'http://www.sec.gov/Archives/edgar/data/%s/%s-index.html'
doc_url_base_10k = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s'

# Set correct directory
os.chdir(pathname_10k)

# Initialize log file
# (log file name = the time we initiate scraping session)
time = strftime("%Y-%m-%d %Hh%Mm%Ss", gmtime())
log_file_name = 'log '+time+'.txt'
with open(log_file_name, 'a') as log_file:
    log_file.close()

# Iterate over CIKs and scrape 10-Ks
for cik in tqdm(ticker_cik_df['cik']):
    Scrape10K(browse_url_base=browse_url_base_10k, 
          filing_url_base=filing_url_base_10k, 
          doc_url_base=doc_url_base_10k, 
          cik=cik,
          log_file_name=log_file_name)


# Run the function to scrape 10-Qs
# Define parameters
browse_url_base_10q = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=%s&type=10-Q&count=1000'
filing_url_base_10q = 'http://www.sec.gov/Archives/edgar/data/%s/%s-index.html'
doc_url_base_10q = 'http://www.sec.gov/Archives/edgar/data/%s/%s/%s'

# Set correct directory (fill this out yourself!)
os.chdir(pathname_10q)

# Initialize log file
# (log file name = the time we initiate scraping session)
time = strftime("%Y-%m-%d %Hh%Mm%Ss", gmtime())
log_file_name = 'log '+time+'.txt'
log_file = open(log_file_name, 'a')
log_file.close()

# Iterate over CIKs and scrape 10-Ks
for cik in tqdm(ticker_cik_df['cik']):
    Scrape10Q(browse_url_base=browse_url_base_10q, 
          filing_url_base=filing_url_base_10q, 
          doc_url_base=doc_url_base_10q, 
          cik=cik,
          log_file_name=log_file_name)
















































































