from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import time
import json
import glob
import os
import re


def github_repo_files(repo_url, ext='.csv'):
  '''
  Get the max date of files in a github repository.
    Parameters:
      repo (url): full repository url containing data files
      ext (str): filename extension to search
    Returns:
      dictionary of valid data urls and the max date 

  '''
  ext = ext.lower() # make lowercase

  regex_url = re.compile(
              r'^(?:http|ftp)s?://' # http:// or https://
              r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
              r'localhost|' #localhost...
              r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
              r'(?::\d+)?' # optional port
              r'(?:/?|[/?]\S+)$', re.IGNORECASE)

  # check if valid url
  if not(re.match(regex_url, repo_url) is None):
    raw_url = repo_url.replace('https://github.com/', 'https://raw.githubusercontent.com/').replace('tree/', '')
    
    # make request
    r = requests.get(repo_url)
    if r.status_code==200:
      soup = BeautifulSoup(r.text, 'html.parser')
      github_data_files = [a.text for a in soup.find_all('a') if ext in a.text]
      if len(github_data_files)>0:
        github_data_urls = [os.path.join(raw_url, f) for f in github_data_files]
        max_file_date = str(max([int(f.split('_')[-1].split('.')[-2]) for f in github_data_files]))
        max_date = (datetime.strptime(max_file_date, '%Y%m%d')).strftime('%Y-%m-%d')
        
        return {'github_data_urls':github_data_urls, 'max_date':max_date}
      
      else:
        raise Exception(f'No files with {ext} in {repo_url}')
    else:
      raise Exception(f'Invalid URL 400 Bad Request: {repo_url}')
  else:
    raise Exception(f'MissingSchema: {repo_url}')


def get_yf_data(list_of_symbols, yf=None, start_date=None, end_date=None, verbose=False, sleep=0):
  '''
  Get stock data using yFinance.
    Parameters:
      yf (obj): yf library
      list_of_symbols (list): list of symbols/tickers
      start_date (str): defaults to None, 'YYYY-MM-DDDD'
      end_date (str): defaults to None, 'YYYY-MM-DDDD'
      verbose (bool): show symbol output
      sleep (int): time between symbol request
    Returns:
      dataframe of entererd symbols
  '''

  if type(list_of_symbols)!=list:
    raise Exception(f'Invalid data type for list_of_symbols: {type(list_of_symbols)}')

  if yf is None:
    raise Exception(f'yfinance object yf is None, import yfinance as yf')

  dfs_list = []
  cnt = 1
  for s in list_of_symbols:
    if verbose:
      print(f'{cnt}/{len(list_of_symbols)} {s}')
      cnt += 1
    ticker = yf.Ticker(s)
    df_ticker = ticker.history(period='max', start=start_date, end=end_date).reset_index()
    df_ticker.insert(1, 'Symbol', s)
    dfs_list.append(df_ticker)
    time.sleep(sleep)

  df = pd.concat(dfs_list, sort=False)
  return df

def export_data(df_results, filename, export_path=None):
  '''
  Export results as csv to data folder.
  '''
  # check dates
  min_date = datetime.strftime(df_results['Date'].min(), '%Y%m%d')
  max_date = datetime.strftime(df_results['Date'].max(), '%Y%m%d')
  
  filename_ = f"{filename}_{min_date}_{max_date}.csv"

  try:
    df_results.to_csv(os.path.join(export_path, filename_), index=False)
  except:
    print(f'Invalid export_path {export_path}, exporting to current directory {os.getcwd()}')
    df_results.to_csv(filename_, index=False)

  print(filename_)

def symbols_dict(create_new=True, path_to_files=None, export_json=True):
  '''
  Create a dictionary object of csv file containing symbol data.
   Parameters:
    create_new (bool): create new dictionary from csv files otherwise, import previous if exist
    path_to_files (str): path with csv symbols, defaults to None
    export_json (bool): export dictionary as .json, defaults to True
  '''
  json_filename = 'symbols.json'

  if path_to_files is None:
    raise Exception(f'Invalid path_to_files {path_to_files}')

  if create_new:
    dict_ = dict()
    for f in glob.glob(os.path.join(path_to_files, '*csv')):
      filename = os.path.split(f)[-1].split('.')[0]
      dict_[filename] = dict()
      df = pd.read_csv(f)
      for s in df['symbol']:
        dict_[filename][s] = dict()
        for c in df.columns[1:]:
          dict_[filename][s][c] = df[df['symbol']==s][c].values[0]

    if export_json:
      with open(os.path.join(path_to_files, json_filename), 'w') as fp:
        json.dump(dict_, fp)

  else:
    print(f'Importing previous {json_filename} file')
    if json_filename in os.listdir(path_to_files):
      with open(os.path.join(path_to_files, json_filename)) as fp:
        dict_ = json.load(fp) 
    else:
      raise Exception(f'No such file or directory {os.path.join(ASSETS_PATH, json_filename)}')

  return dict_