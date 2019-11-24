#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import urllib
import sqlite3 as sqlite
from bs4 import BeautifulSoup
import os
import zipfile
import time
import csv
import sys
import subprocess

csv.field_size_limit(sys.maxsize)

#set cleanup to False if you don't want to delete the
#zipped archives and TSV files after they've been added to the DB.
cleanup = True
get_descs = True


#sets the path to the python script's location
#the DB will be created in that same directory
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


#these URLs aren't listed publicly on the download page. 
#If you send patentsview an email they will send you a link to the files 
#Add that URL below
detailed_desc_url = None


def get_urls(url):
    '''takes patentsview downloads website url, parses out the .tsv.zip files
    that contain the patent tables, returns a list of urls
    for those files'''
    urls = []
    req = urllib.request.Request(url) 
    html  = urllib.request.urlopen(req)
    data = html.read() 
    data = data.decode()
    soup = BeautifulSoup(data, 'lxml')
    for link in soup.findAll('a', href=True):
        link = link.get('href')
        if link.endswith('.zip'):
            urls.append(link)
    return urls

  
def download_file(url):
    '''when downloading large files s3 sometimes
    resets the connection. This caused problems with Python's 
    request and urllib libraries. Calling wget or curl diretly
    from python worked better. Takes url and filename, 
    downloads file at url and writes it to cwd'''
    count = 0
    arg = 'curl -C - -O %s'%url
    while count < 10:    
        try:
            subprocess.run(arg, check = True, shell = True)
            print("Finished Downloading "+url)
            break
        except:
            print('Error downloading '+url)
            time.sleep(30)
            count += 1
            
            
def clean_file(filename):
    '''fixes tsv files that have extra quotation marks in them
    a few of the patentsview files have """""""data"""""" format
    which breaks the pandas csv parser. Returns name of new file.'''
    fixed_name = filename+'2'
    infile = open(os.getcwd()+'/'+filename, 'r', encoding='utf-8')
    outfile = open(os.getcwd()+'/'+fixed_name, 'w', encoding='utf-8')
    for line in infile:
        line = line.replace('"','')
        outfile.write(line)
    infile.close()
    outfile.close()
    return fixed_name
            
            
def extract_names(url):
    '''takes url and returns the appropriate file name and db table name'''
    filename = url.split('/')[-1]
    tablename = filename.split('.')[0]
    if 'detail-desc' in tablename:
        tablename = 'description'    
    return filename, tablename
    
    
def unzip_file(filename):
    '''takes filename and unzips file to cwd
    returns the name of the unzipped tsv file'''
    zf = zipfile.ZipFile(filename)
    tsv_name = zf.filelist[0].filename
    try: #some files use proprietary deflate64 compression, so handle resulting errors
        zf.extractall()
        zf.close()
    except:
        print('Error unzipping, trying subprocess method')
        arg = 'unzip %s' %filename + ' -d ' + os.getcwd()
        subprocess.run(arg, check = True, shell = True)
    return tsv_name
    
    
def make_column_args(header):
    '''takes a TSV header as a row, and returns a string of the 
    column names and datatypes (assumes all text) to use in creating
    a new table'''
    header = ['"'+c+'"' for c in header]
    columns = ' TEXT, '.join(header)
    columns = '(' + columns + ' TEXT)'
    return columns
    
    
def write_to_db(tsv_name, tablename):
    '''takes name of unzipped tsv file and db table name and uses pandas 
    csv_read to read into dataframe and subsequently write to db'''
    #parse out column names
    infile = open(os.getcwd()+'/'+tsv_name, 'r', encoding = 'utf-8')
    reader = csv.reader(infile, delimiter = '\t')
    
    count = 0
    for row in reader:
        count += 1
        if count == 1: #on header, make a new table if this one doesn't exist
            rowsize = len(row) #check length against this to skip malformed rows
            columns = make_column_args(row)
            cur.execute('''CREATE TABLE IF NOT EXISTS %s %s''' %(tablename, columns))
            continue
        if len(row) != rowsize:
            continue
        cur.execute("INSERT INTO " + tablename + " VALUES (" + ",".join(len(row) * ["?"]) + ")", row)
        if count % 100000 == 0:
            print('Inserting row '+str(count)+' into '+tablename)
            conn.commit()
    conn.commit()
    
    
def download_and_parse_tsv(url):
    '''takes url to a zipped tsv file, downloads, extracts,
    reads into a pandas df, and then subsequently writes to sqlite db'''
    
    filename, tablename = extract_names(url)
    print('\nDownloading '+filename)
    
    #download and save file
    download_file(url)

    #unzip file
    tsv_name = unzip_file(filename)

    #read file into pandas df and write to db
    write_to_db(tsv_name, tablename)

    #track processed files
    cur.execute('''INSERT INTO processed VALUES (?)''',[url])
    conn.commit()
    processed.append(url)
    
    #cleanup zipped file and tsv file
    if cleanup == True:
        os.remove(filename)
        os.remove(os.getcwd()+'/'+tsv_name)
    
def add_indices():
    '''adds indexes to the db on patent numbers and universal ids
    other indexes might be desired depending on how you plan on using the data'''
    print("Adding indices")
    tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    for table in tables:
        table = table[0]
        reader = cur.execute("SELECT * FROM {}".format(table))
        columns = [c[0] for c in reader.description]
        for column in columns:
            if column == 'uuid' or column == 'patent_id' or column == 'id':
                cur.execute("""CREATE INDEX IF NOT EXISTS {} ON {} ({} ASC)""".format(column+table+'_index',table, column))
        conn.commit()
        
def make_processed_list():
    '''makes table in db to track downloaded files in case
    process is interrupted and needs to start from the middle'''
    cur.execute('''CREATE TABLE IF NOT EXISTS processed (
            url text)''')
    processed = cur.execute('''SELECT url FROM processed''').fetchall()
    processed = [u[0] for u in processed]
    return processed

if __name__ == "__main__":
    data_url = 'http://www.patentsview.org/download/'       
    urls = get_urls(data_url)
    conn = sqlite.connect('patent_db2.sqlite')
    cur = conn.cursor()
    processed = make_processed_list()

    if get_descs == True:
        urls = urls + get_urls(detailed_desc_url)
    count = 0
    for url in urls:
        if url in processed:
            continue
        download_and_parse_tsv(url)
        count += 1
        print(str(count)+" files processed. Files remaining: "+str(len(urls)-len(processed)))
    add_indices()                     

    conn.commit()
    conn.close()
