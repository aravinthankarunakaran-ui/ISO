#import required libraries for finding bank name and preparing summary
import PyPDF2 
import re
from pathlib import Path
from PIL import Image
import imagehash
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import pandas as pd
from dateparser.search import search_dates
import ast
import csv
import collections
import datetime as dti
import dateutil.parser as dtparse
from PyPDF2 import PdfFileReader, PdfFileWriter
import numpy as np
from tika import parser as psr
import json
from dateparser.search import search_dates
import ast
import collections
import itertools
import pikepdf
from pikepdf import Pdf
from functools import reduce 
import joblib
import numpy
import math
import dateutil.parser
from datetime import datetime
from operator import itemgetter
import os
from copy import deepcopy
from flask import current_app as app
import pandas.io.common
import camelot
import fitz
from datetime import timedelta
import glob
from glob import glob as glb
import difflib
import cv2
import shutil
from difflib import SequenceMatcher
from fnmatch import fnmatch
workingdir = os.getcwd()
#directories of required files
dependencydir = os.path.join(workingdir , 'static/iso_dependencies/')
#directories of required files
uploaddir = os.path.join (workingdir , 'static/uploads/')

# read line by line contents of a pdf using tika
def read_pdf(FileName):
    lines = []
    PDF_Parse = psr.from_file(FileName)
    lines.append(PDF_Parse ['content'])
    return lines

def decrypt_pdf(filename):
    pdfa = fitz.open(filename)
    pdfa.save(str(filename) + '_decrypted_statement.pdf', False)
    pdf = str(filename)+'_decrypted_statement.pdf'
    return pdf

def template_check(pdf):
    print("pdf",pdf)
    data = pd.read_csv(dependencydir + "Funder_list.csv",on_bad_lines='skip',encoding='cp1252')
    template = []
    for i in range(0,len(data)):
        if data['Keyword'][i] in contents[0]:
            template = data['Funder'][i]
            break
    if template == []:
        template = 'No format found'
    return template

#search string and find page numbers using fitz library
def findtextinpdf(file,text):
    doc = fitz.open(file)
    PageFound = []
    for i in range(0,len(doc)):
        page = doc[i]
        for j in range(0,len(text)):
            if page.searchFor(text[j]):
                PageFound.append(i)
    PageFound = list(dict.fromkeys(PageFound))
    PageFound = np.add(PageFound, 1).tolist()
    return PageFound

#to fetch column names for dataframes
def column_search(table,type_column):
#     print(bankName,formats,table)
    for i in range(0,len(type_column)):
        if type_column['Table_Names'][i]==table:
            columns = type_column['Column_Names'][i]
    columns = ast.literal_eval(columns)
    return columns

#to fetch keys to be searched in order to find pages and table names for each format
def type_search(table,type_column):
    for i in range(0,len(type_column)):
        if type_column['Table_Names'][i]==table:
            keysearch = type_column['keySearch'][i]
            Table_areas = type_column['Table_Area'][i]
            column_split = type_column['Column_Split'][i]
    return keysearch,Table_areas,column_split

#functions to extract tables for given column splits
def table_extract(pdf,table,type_column):
    keysearch,Table_areas,column_split = type_search(table,type_column)
    if keysearch!='Nil':
        keysearch = ast.literal_eval(keysearch)
        pagelist = findtextinpdf(pdf,keysearch)
    else:
        pagelist = []
    tablelist = []
    Table_areas = ast.literal_eval(Table_areas)
    column_split = ast.literal_eval(column_split)
    if len(pagelist)!=0:
        for m in range(0,len(pagelist)):
            tablelist.append(camelot.read_pdf(pdf,pages = str(pagelist[m]),flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split))
    else:
        tablelist.append(camelot.read_pdf(pdf,pages = 'all',flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split))
    tablelist = [j for i in tablelist for j in i]
    #print(len(tablelist))
    return tablelist

def exact_df(bank_found):  
    ispresent_start = bank_found['Start_1'][0]
    ispresent_col_start = bank_found['Start_Col_1'][0]
    ispresent_end = bank_found['End_1'][0]
    ispresent_col_end = bank_found['End_Col_1'][0]
    return ispresent_start, ispresent_col_start,ispresent_end, ispresent_col_end

def extract_table_content(table,table_name,type_column):
    type_column = type_column[type_column.Table_Names.str.contains(table_name,na=False)]
    type_column.reset_index(drop=True, inplace=True) 
    frames = pd.DataFrame()
    for p in range(0,len(table)):
        df = table[p].df
        #fetch column names according to number of columns from db
        df.columns = column_search(table_name,type_column)
        ispresent_start, ispresent_col_start,ispresent_end, ispresent_col_end = exact_df(type_column)
        m = "aa"
        for j in range(0,len(df)):
            if str(ispresent_start) in re.sub(' +', ' ', str(df[str(ispresent_col_start)][j])) or str(ispresent_start) in re.sub('\s+',' ',str(df[str(ispresent_col_start)][j])):
            #check condition if start key is present , if present, get the index in a variable
                m = j
                break            
#             print(m)
        if m!="aa" and m!=0:
            df = df.drop(df.index[0:m+1])
            df.reset_index(drop=True, inplace=True)
        n = "aa"
        for j in range(0,len(df)):
            if str(ispresent_end) in re.sub(' +', ' ', str(df[str(ispresent_col_end)][j])) or str(ispresent_end) in re.sub('\s+', ' ', str(df[str(ispresent_col_end)][j])):
            #check condition if start key is present , if present, get the index in a variable
                n = j
                break   

        if n!="aa":
            df = df.drop(df.index[n:len(df)])
            df.reset_index(drop=True, inplace=True)
        #if m=="aa":
         #   df = pd.DataFrame()
        frames = pd.concat([frames,df],ignore_index= True)
    #to fill empty cells in a dataframe with a different value/string
    def convert_fill(df):
        df = df.stack()
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df.fillna('ENOAH', inplace=True)
        df = df.unstack()
        return df
    frames = convert_fill(frames)
    return frames

def Business_Information_Normal(df,keyword,column_names,value_location,valueposts,req_val):
    # keyword = str(keyword).replace(' ','')
    if keyword == 'Nil':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    df.to_csv('Fundamerica.csv')
    columns = ast.literal_eval(column_names)
    valueranges = ast.literal_eval(valueposts)
    locations = ast.literal_eval(value_location)
    #print("columns,valueranges",columns,valueranges)
    for m in range(0,len(columns)):
        column_name = columns[m]
        for k in range(0,len(locations)):
            value_location = locations[k]
            for t in range(0,len(valueranges)):
                value = valueranges[t]
                print("column_name,value",column_name,value)
                for i in range(0,len(df)):
                    print("keyword,df[column_name][i]",keyword,df[column_name][i])
                    print("key in column",re.sub(' +', ' ', str(df[column_name][i])))
                    if str(keyword).lower() in re.sub(' +', ' ', str(df[column_name][i])).lower():
                        print("keyword22,df[column_name][i]",keyword,df[column_name][i])
                        df.loc[i,column_name] = str(df.loc[i,column_name]).replace('_','')
                        if value_location == 'same':
                            if value == "present+1":
                                required_field = df[column_name][i+1]
                                break
                            if value == "present-1" and not 'State of Incorp:' in str(df['Col_2'][i-1]):
                                required_field = df[column_name][i-1]
                                break
                            elif value == "present-1" and 'State of Incorp:' in str(df['Col_2'][i-1]) and keyword != 'Business Legal Name:':
                                required_field = df[column_name][i-1]
                                break
                            if value == "keypresent":
                                required_field = str(df[column_name][i]).replace(keyword,'')
                                required_field = str(required_field).lower().replace(keyword.lower(),'')
                                print('bigthing........',required_field)
                                if 'enoah' in str(required_field):
                                    required_field = 'ENOAH'
                                break
                            if value == "present+2":
                                if (i + 2) < len(df):#28/07
                                    required_field = df[column_name][i+2]
                                    if 'DATE OF BIRTH:' in str(required_field) and keyword == 'BUSINESS START DATE:' and req_val == 'Date': # condition only for National Business Capital
                                        required_field = df['Col_4'][i+1]
                                    break
                            if value == "present+3":
                                required_field = df[column_name][i+3]
                                break
                            if value == "present-3":
                                required_field = df[column_name][i-3]
                                break
                            if value == "present-2":
                                required_field = df[column_name][i-2]
                                break
                            if value == "ispresent":
                                required_field = df[column_name][i]
                                break
                        else:
                            if value == "present+1":
                                if (i + 1) < len(df):
                                    required_field = df[value_location][i+1]
                                else:
                                    print(f"Warning: Index {i+1} out of range for DataFrame of length {len(df)}")
                                    required_field = 'ENOAH'
                                break
                            if value == "present-1":
                                if (i - 1) >= 0:
                                    required_field = df[value_location][i-1]
                                else:
                                    print(f"Warning: Index {i-1} out of range for DataFrame of length {len(df)}")
                                    required_field = 'ENOAH'
                                break
                            if value == "present+2":
                                if (i + 2) < len(df):
                                    required_field = df[value_location][i+2]
                                else:
                                    print(f"Warning: Index {i+2} out of range for DataFrame of length {len(df)}")
                                    required_field = 'ENOAH'
                                break
                            if value == "present+3":
                                required_field = df[value_location][i+3]
                                break
                            if value == "present+4":
                                required_field = df[value_location][i+4]
                                break
                            if value == "present+7":
                                required_field = df[value_location][i+7]
                                break
                            if value == "present+8":
                                required_field = df[value_location][i+8]
                                break
                            if value == "present-3":
                                required_field = df[value_location][i-3]
                                break
                            if value == "present-2":
                                required_field = df[value_location][i-2]
                                break
                            if value == "keypresent":
                                required_field = str(df[value_location][i]).replace('Federal Tax ID:','').replace('Federal TaxID:','').replace('Federal Tax Identiï¬cation Number:','').replace(' Identiﬁcation Number','').replace(keyword,'')
                                required_field = str(required_field).lower().replace(keyword.lower(),'')
                                print('bigthing11111........',required_field)
                                if 'industry:' in str(df[value_location][i]) and 'Business Address:' in str(df[column_names][i]):
                                    required_field = str(df[column_names][i]).replace('Business Address:','')
                                    print('bigthing222........',required_field)
                                if 'enoah' in str(required_field):
                                    required_field = 'ENOAH'
                                break
                            if value == "ispresent":
                                required_field = df[value_location][i]
                                break
                required_field = str(required_field).replace('Number (9 digits):','')
                
                if keyword == 'DBA:' and 'City:' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Legal Name & DBA:' and 'Legal Entity' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Doing Business as (DBA)' and 'Business Address' == str(required_field):
                    required_field='ENOAH'
                if keyword == 'Business Legal Name:' and 'Legal Entity' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Physical Street Address:' and 'Billing Street Address' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Business Legal Name:' and 'Type of Business' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Business Address' and 'DBA' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'DBA:' and 'Business Address' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Legal Business Name:' and 'Legal Entity' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'DBA' and 'Campaign' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Physical Street Address:' and 'Billing Street Address:' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Business DBA' and 'Primary Business' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Legal Business Name' and 'Legal Business Name' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Business DBA Name:' and ('City' in required_field or 'Physical Street Address:' in required_field):
                    required_field='ENOAH' 
                if keyword == 'DBA:' and 'City:' in required_field:
                    required_field='ENOAH'
                if keyword == 'DBA:' and 'State:' in required_field:
                    required_field='ENOAH'
                if keyword == 'Physical Street Address:' and str(required_field) == 'City:':
                    required_field='ENOAH'
                if keyword == 'Federal Tax ID#:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Physical Location' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'date under' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Federal Tax ID:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Business Start Date:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Business Start Date:' and 'Contractor Industry:' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Business DBA:' and  re.search('[0-9]',str(required_field)) and not re.search('[a-zA-Z]',str(required_field)):
                    required_field = 'ENOAH'
                if keyword == 'Physical Address:' and '@' in str(required_field):
                    required_field='ENOAH'
                if keyword == 'Ownership' and req_val == 'Date' and re.search('[a-zA-Z]',str(required_field)): # only for skyline funding
                    required_field = str(re.sub('[a-zA-Z%,()]', '',str(required_field)))
                if keyword == 'Business Legal Name' and 'Merchant' in str(required_field):
                    required_field = re.sub('[^A-Za-z0-9&]+', ' ', str(required_field))
                required_field = str(required_field).replace('(“merchant’):','').replace('Merchant','')
                required_field = str(required_field).replace('*','').replace('[ t:a:o ]','')
                required_field = str(required_field).replace(':','')
                required_field = str(required_field).replace('#','')
                required_field = str(required_field).replace('(','')
                required_field = str(required_field).replace(')','')
                print("FIRST ITERATION",keyword,required_field)
                if str(required_field)!='ENOAH' and str(required_field).strip()!='':
                    break
            if keyword == 'Legal Name & DBA:' and 'Legal Entity' in str(required_field):
                required_field='ENOAH'
            print("SECOND ITERATION",keyword,required_field)
            if str(required_field)!='ENOAH' and str(required_field).strip()!='':
                break
    # print("THIRD ITERATION",keyword,required_field)
    if keyword == 'Year in Business' and len(required_field)<4:
        given_years = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
        currentTimeDate = int(datetime.now().strftime('%Y'))
        required_field = (int(currentTimeDate)) - int(given_years)
    if keyword == 'Business DBA Name:' and ('City' in required_field or 'Physical Street Address:' in required_field):
        required_field='ENOAH'
    if keyword == 'DBA:' and 'City:' in required_field:
        required_field='ENOAH'
    if keyword == 'DBA:' and 'State:' in required_field:
        required_field='ENOAH'
    if keyword == 'DBA Name:' and 'Business Name:' in required_field:
        required_field = 'ENOAH'

    required_field = str(required_field).replace('n/a','').replace('(If different than above):','').replace('Type of Business Entity select one','')
    required_field = str(required_field).replace('N/A','').replace('\u0000 ','').replace('Street Address','')
    required_field = str(required_field).replace('Ownerxdsxship:','').replace('Ownership','').replace('business  start date','')
    required_field = str(required_field).replace('City:',' ').replace('_','').replace('Physical  Address','').replace('physical  address','')
    required_field = str(required_field).replace('Merchant','').replace('•','').replace('[ tao ]','').replace('”merchant”','').replace('“merchant”','')
    required_field = str(required_field).replace('Date  Business Established MM/DD/YYYY','').replace('United States US','')
    required_field = str(required_field).replace('Primary Business Structure','').replace('DBA','').replace('Dba','').replace('BUSINESS INFO','').replace('Physical Address','')
    # Legal name replaced for Loanspark LLC funder
    required_field = str(required_field).replace('l Name','').replace('Legal Nam','').replace('l Nam','').replace(':','')
    required_field = str(required_field).replace('(MM/YYYY):','').replace('DBA:','').replace('D/B/A:','').replace('D/B/A','')
    if keyword == 'Business Established:' and 'Years' in required_field and 'Month' in required_field:
        required_field='ENOAH'
    ### This lines is for 'Lend On Capital' Business Street address issuse fixing:
    if keyword == 'Street' and ('Address' in df[column_name][i-1] or 'Address' in df[column_name][i-2]):
        value = str(df[column_name][i-1]+''+df[column_name][i-2]+''+str(required_field))
        value = str(value).replace('Business Address','').replace('BusinessÂ Address','').replace('Business ','').replace('Business','').replace('Address','').replace('ENOAH','')
        if not re.search('[0-9]',str(value)[1]) and not re.search('[a-zA-Z]',str(value)[1]):
            required_field = value[0]+value[2:]
        else:
            required_field = value

    if req_val == 'Industry':
        required_field = str(required_field).replace('industry type','').replace('Products Sold','').replace('Existing Balances if Applicable','').replace('Business Financial Information','')
        #'Cap Front', Atlas One Group,Genesis Merchant Services,Tinaz Enterprises Llc 
    if req_val == 'Business_Email':
        required_field = str(required_field).replace('amount requested','').replace('Amount Requested','').replace('Business Description','')
        #Bobby B Consulting, Ace Capital Funding Source
    if req_val == 'Purpose':
        required_field = str(required_field).replace('credit card batches three months ago','').replace('Is the Business Seasonal?','').replace('Enter text here','')
        #'CAPITAL QUICKLY','Advance Funds Network, Closer Capital
    if req_val == 'Loan_Amount':
        required_field = str(required_field).replace('credit card sales three months ago $','').replace('approximate credit score','')
        #'CAPITAL QUICKLY',DME Merchants

    return required_field
def Owner_Information_Normal(df,keyword,column_names,value_location,valueposts,req_val,owner_status):
    #print("keyword,columns",keyword,column_names)
    # keyword = str(keyword).replace(' ','')
    df.to_csv('firstname.csv')
    if keyword == 'Nil':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    
    columns = ast.literal_eval(column_names)
    valueranges = ast.literal_eval(valueposts)
    locations = ast.literal_eval(value_location)
    for m in range(0,len(columns)):
        column_name = columns[m]
        for k in range(0,len(locations)):
            value_location = locations[k]
            for t in range(0,len(valueranges)):
                value = valueranges[t]
                for i in range(0,len(df)):
                    #print(keyword,re.sub(' +', ' ', str(df[column_name][i])))
                    if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
                        if owner_status == 'Owner #1':
                            if value_location == 'same':
                               
                                if value == "present+1":
                                    required_field = df[column_name][i+1]
                                    break
                                if i > 0 and value == "present-1":#31/07

                                    required_field = df[column_name][i-1]
                                    break
                                if value == "keypresent":
                                    required_field = str(df[column_name][i]).replace(keyword,'')
                                    break
                                if value == "ispresent":
                                    required_field = df[column_name][i]
                                    break
                                if value == "present+2":
                                    required_field = df[column_name][i+2]
                                    break
                                if value == "present-2":
                                    required_field = df[column_name][i-2]
                                    break
                                if value == "present+3":
                                    required_field = df[column_name][i+3]
                                    break
                                if value == "present-3":
                                    required_field = df[column_name][i-3]
                                    break
                            else:
                                
                                if value == "present+1" and i+1 < len(df):
                                    required_field = df[value_location][i+1]
                                    break
                                if value == "present-1" and i-1 >= 0:
                                    required_field = df[value_location][i-1]
                                    break
                                if value == "present+2" and i+2 < len(df):
                                    required_field = df[value_location][i+2]
                                    break
                                if value == "present+3" and i+3 < len(df):
                                    required_field = df[value_location][i+3]
                                    break
                                if value == "present-3" and i-3 >= 0:
                                    required_field = df[value_location][i-3]
                                    break

                                if value == "present-2" and i-2 >= 0:
                                    required_field = df[value_location][i-2]
                                    break
                                if value == "keypresent" and i < len(df):
                                    required_field = str(df[value_location][i]).replace(keyword,'').replace(':','')
                                    break
                                if value == "ispresent" and i < len(df):
                                    required_field = df[value_location][i]
                                    break
                        else:
                            if value_location == 'same':
                                if value == "present+1":
                                    required_field = df[column_name][i+1]
                                if  i > 0 and value == "present-1":
                                    required_field = df[column_name][i-1]
                                if value == "keypresent":
                                    required_field = str(df[column_name][i]).replace(keyword,'')
                                if value == "present+2":
                                    required_field = df[column_name][i+2]
                                if value == "present+3":
                                    required_field = df[column_name][i+3]
                                if value == "present-3":
                                    required_field = df[column_name][i-3]

                                if value == "present-2":
                                    required_field = df[column_name][i-2]
                                if value == "ispresent":
                                    required_field = df[column_name][i]
                            else:
                                if value == "present+1" and i + 1 < len(df):
                                    required_field = df[value_location][i+1]
                                    break
                                if value == "present-1" and i - 1 >= 0:
                                    required_field = df[value_location][i-1]
                                if value == "present+2" and i + 2 < len(df):
                                    required_field = df[value_location][i+2]
                                if value == "present+3" and i + 3 < len(df):
                                    required_field = df[value_location][i+3]
                                if value == "present-3" and i - 3 >= 0:
                                    required_field = df[value_location][i-3]

                                if value == "present-2" and i - 2 >= 0:
                                    required_field = df[value_location][i-2]
                                if value == "keypresent":
                                    required_field = str(df[value_location][i]).replace(keyword,'')
                                if value == "ispresent":
                                    required_field = df[value_location][i]
                         
                if keyword == 'Home Address:' and not re.search('[0-9]',str(required_field)):
                    required_field = 'ENOAH'
                if keyword == 'Date of Birth:' and 'Mobile' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Date of Birth:' and 'Owner' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == '% of Ownership' and 'City' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'DBA:' and 'Legal Name' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Business Ownership %:' and 'Home Address' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Principle Owner Name:' and 'Ownership' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Last Name' and 'City' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Applicant:' and "Home" in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Name:' and 'Primary' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Home Address' and 'State' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == '% of Ownership' and not re.search('[0-9]',str(required_field)):
                    required_field = 'ENOAH'
                if keyword == 'DOB:' and 'I'in str(required_field):
                    required_field = str(required_field).replace('I','')
                if keyword == 'State:' and 'SSN:'== str(required_field):
                    required_field = str(df['Col_1'][i+2])
                if keyword == 'Ownership %:' and 'Email:' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Name:' and 'Home Address:' in str(required_field):
                    required_field = 'ENOAH'
                if keyword == 'Email:' and not '.com' in str(required_field) and 'Cell' in str(required_field):
                    required_field = str(df['Col_2'][i-1])
                if keyword == 'Phone:' and 'Cell' in str(required_field) and not re.search('[0-9]',str(required_field)):
                    required_field = str(df['Col_1'][i-1])
                if keyword == 'NUMBER' and 'XX-XXXX' in required_field:
                    required_field = 'ENOAH'
                if keyword == 'Street Address:' and ('Property Owner Information' in required_field or 'Owned Outright' in required_field):
                    required_field = 'ENOAH'
                if keyword == 'Name:' and '/Owner' in str(required_field):
                    mystring = required_field
                    keyword = 'Owner'
                    before_keyword,keyword,after_keyword = mystring.partition(keyword)
                    required_field = after_keyword
                # if keyword != 'Ownership %:':
                #     required_field = str(required_field).replace('.','')

                required_field = str(required_field).replace('Title: MR','')
                required_field = str(required_field).replace('Title:','').replace('\u0000 ','')
                required_field = str(required_field).replace('Officer','')
                required_field = str(required_field).replace('Owner','').replace('DL#','')
                required_field = str(required_field).replace('OWNER INFO','').replace('OWNER','').replace('Corporate Oï¬ƒcer/','').replace('Corporate Officer/','').replace('Corporate Oﬃcer','')
                required_field = str(required_field).replace('owner','').replace('SSN:','').replace('Social Security Number:','')
                required_field = str(required_field).replace('*','')
                required_field = str(required_field).replace(':','')
                required_field = str(required_field).replace('#','')
                required_field = str(required_field).replace('DOB','')

                if str(required_field)!='ENOAH' and str(required_field).strip()!='':
                    break
            if str(required_field)!='ENOAH' and str(required_field).strip()!='':
                break
    if (req_val == 'Email1' or req_val == 'Email2') and not '@' in required_field:
        required_field = 'ENOAH'
    required_field = str(required_field).replace('First Name','').replace('Last Name','').replace('Name','')
    required_field = str(required_field).replace('Title:','').replace('Cell','')
    required_field = str(required_field).replace('Owner','')
    required_field = str(required_field).replace('O?cer','').replace(':','').replace('•','')
    required_field = str(required_field).replace('O?cer','')
    required_field = str(required_field).replace('OWNER INFO','').replace('OWNER','')
    required_field = str(required_field).replace('managing partner','').replace('Managing Member','').replace('Realtor / Investor','')
    required_field = str(required_field).replace('President','').replace('United States (US)','')
    required_field = str(required_field).replace('CEO','').replace('...','.')
    required_field = str(required_field).replace('M.D','').replace('Date of','').replace('Date Of','')
    required_field = str(required_field).replace('n/a','').replace('_','')
    required_field = str(required_field).replace('N/A','').replace('E-mail','').replace('Address:','').replace('Address','').replace('Authorizations','')
    if req_val!='Date_of_Birth1' and req_val!='Date_of_Birth2':
        required_field = str(required_field).replace('/','')
    if keyword == 'Date of' and ('C' in required_field[0] or 'c' in required_field[0]) and req_val == 'Date_of_Birth1' and valueposts=='same': # condition for Alternated Financing Funder only
        required_field = required_field[1:]    
    print(keyword,required_field)
    return required_field
# def Owner_Information_Normal(df,keyword,column_names,value_location,valueposts,req_val,owner_status):
#     #print("keyword,columns",keyword,column_names)
#     if keyword == 'Nil':
#         required_field = 'ENOAH'
#     required_field = 'ENOAH'
#     columns = ast.literal_eval(column_names)
#     valueranges = ast.literal_eval(valueposts)
#     locations = ast.literal_eval(value_location)
#     for m in range(0,len(columns)):
#         column_name = columns[m]
#         for k in range(0,len(locations)):
#             value_location = locations[k]
#             for t in range(0,len(valueranges)):
#                 value = valueranges[t]
#                 for i in range(0,len(df)):
#                     #print(keyword,re.sub(' +', ' ', str(df[column_name][i])))
#                     if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
#                         if owner_status == 'Owner #1':
#                             if value_location == 'same':
#                                 if value == "present+1":
#                                     required_field = df[column_name][i+1]
#                                     break
#                                 if value == "present-1":
#                                     required_field = df[column_name][i-1]
#                                     break
#                                 if value == "keypresent":
#                                     required_field = str(df[column_name][i]).replace(keyword,'')
#                                     break
#                                 if value == "ispresent":
#                                     required_field = df[column_name][i]
#                                     break
#                                 if value == "present+2":
#                                     required_field = df[column_name][i+2]
#                                     break
#                                 if value == "present-2":
#                                     required_field = df[column_name][i-2]
#                                     break
#                                 if value == "present+3":
#                                     required_field = df[column_name][i+3]
#                                     break
#                                 if value == "present-3":
#                                     required_field = df[column_name][i-3]
#                                     break
#                             else:
#                                 if value == "present+1":
#                                     required_field = df[value_location][i+1]
#                                     break
#                                 if value == "present-1":
#                                     required_field = df[value_location][i-1]
#                                     break
#                                 if value == "present+2":
#                                     required_field = df[value_location][i+2]
#                                     break
#                                 if value == "present+3":
#                                     required_field = df[value_location][i+3]
#                                     break
#                                 if value == "present-3":
#                                     required_field = df[value_location][i-3]
#                                     break

#                                 if value == "present-2":
#                                     required_field = df[value_location][i-2]
#                                     break
#                                 if value == "keypresent":
#                                     required_field = str(df[value_location][i]).replace(keyword,'').replace(':','')
#                                     break
#                                 if value == "ispresent":
#                                     required_field = df[value_location][i]
#                                     break
#                         else:
#                             if value_location == 'same':
#                                 if value == "present+1":
#                                     required_field = df[column_name][i+1]
#                                 if value == "present-1":
#                                     required_field = df[column_name][i-1]
#                                 if value == "keypresent":
#                                     required_field = str(df[column_name][i]).replace(keyword,'')
#                                 if value == "present+2":
#                                     required_field = df[column_name][i+2]
#                                 if value == "present+3":
#                                     required_field = df[column_name][i+3]
#                                 if value == "present-3":
#                                     required_field = df[column_name][i-3]

#                                 if value == "present-2":
#                                     required_field = df[column_name][i-2]
#                                 if value == "ispresent":
#                                     required_field = df[column_name][i]
#                             else:
#                                 if value == "present+1" and i + 1 < len(df):
#                                     required_field = df[value_location][i+1]
#                                 if value == "present-1" and i - 1 >= 0:
#                                     required_field = df[value_location][i-1]
#                                 if value == "present+2" and i + 2 < len(df):
#                                     required_field = df[value_location][i+2]
#                                 if value == "present+3" and i + 3 < len(df):
#                                     required_field = df[value_location][i+3]
#                                 if value == "present-3" and i - 3 >= 0:
#                                     required_field = df[value_location][i-3]

#                                 if value == "present-2" and i - 2 >= 0:
#                                     required_field = df[value_location][i-2]
#                                 if value == "keypresent":
#                                     required_field = str(df[value_location][i]).replace(keyword,'')
#                                 if value == "ispresent":
#                                     required_field = df[value_location][i]

#                 if keyword == 'Home Address:' and not re.search('[0-9]',str(required_field)):
#                     required_field = 'ENOAH'
#                 if keyword == 'Date of Birth:' and 'Mobile' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Date of Birth:' and 'Owner' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == '% of Ownership' and 'City' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'DBA:' and 'Legal Name' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Business Ownership %:' and 'Home Address' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Principle Owner Name:' and 'Ownership' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Last Name' and 'City' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Applicant:' and "Home" in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Name:' and 'Primary' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Home Address' and 'State' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == '% of Ownership' and not re.search('[0-9]',str(required_field)):
#                     required_field = 'ENOAH'
#                 if keyword == 'DOB:' and 'I'in str(required_field):
#                     required_field = str(required_field).replace('I','')
#                 if keyword == 'State:' and 'SSN:'== str(required_field):
#                     required_field = str(df['Col_1'][i+2])
#                 if keyword == 'Ownership %:' and 'Email:' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Name:' and 'Home Address:' in str(required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Email:' and not '.com' in str(required_field) and 'Cell' in str(required_field):
#                     required_field = str(df['Col_2'][i-1])
#                 if keyword == 'Phone:' and 'Cell' in str(required_field) and not re.search('[0-9]',str(required_field)):
#                     required_field = str(df['Col_1'][i-1])
#                 if keyword == 'NUMBER' and 'XX-XXXX' in required_field:
#                     required_field = 'ENOAH'
#                 if keyword == 'Street Address:' and ('Property Owner Information' in required_field or 'Owned Outright' in required_field):
#                     required_field = 'ENOAH'
#                 if keyword == 'Name:' and '/Owner' in str(required_field):
#                     mystring = required_field
#                     keyword = 'Owner'
#                     before_keyword,keyword,after_keyword = mystring.partition(keyword)
#                     required_field = after_keyword
#                 # if keyword != 'Ownership %:':
#                 #     required_field = str(required_field).replace('.','')

#                 required_field = str(required_field).replace('Title: MR','')
#                 required_field = str(required_field).replace('Title:','').replace('\u0000 ','')
#                 required_field = str(required_field).replace('Officer','')
#                 required_field = str(required_field).replace('Owner','').replace('DL#','')
#                 required_field = str(required_field).replace('OWNER INFO','').replace('OWNER','').replace('Corporate Oï¬ƒcer/','').replace('Corporate Officer/','').replace('Corporate Oﬃcer','')
#                 required_field = str(required_field).replace('owner','').replace('SSN:','').replace('Social Security Number:','')
#                 required_field = str(required_field).replace('*','')
#                 required_field = str(required_field).replace(':','')
#                 required_field = str(required_field).replace('#','')
#                 required_field = str(required_field).replace('DOB','')

#                 if str(required_field)!='ENOAH' and str(required_field).strip()!='':
#                     break
#             if str(required_field)!='ENOAH' and str(required_field).strip()!='':
#                 break
#     if (req_val == 'Email1' or req_val == 'Email2') and not '@' in required_field:
#         required_field = 'ENOAH'
#     required_field = str(required_field).replace('First Name','').replace('Last Name','').replace('Name','')
#     required_field = str(required_field).replace('Title:','').replace('Cell','')
#     required_field = str(required_field).replace('Owner','')
#     required_field = str(required_field).replace('O?cer','').replace(':','').replace('•','')
#     required_field = str(required_field).replace('O?cer','')
#     required_field = str(required_field).replace('OWNER INFO','').replace('OWNER','')
#     required_field = str(required_field).replace('managing partner','').replace('Managing Member','').replace('Realtor / Investor','')
#     required_field = str(required_field).replace('President','').replace('United States (US)','')
#     required_field = str(required_field).replace('CEO','').replace('...','.')
#     required_field = str(required_field).replace('M.D','').replace('Date of','').replace('Date Of','')
#     required_field = str(required_field).replace('n/a','').replace('_','')
#     required_field = str(required_field).replace('N/A','').replace('E-mail','').replace('Address:','').replace('Address','').replace('Authorizations','')
#     if req_val!='Date_of_Birth1' and req_val!='Date_of_Birth2':
#         required_field = str(required_field).replace('/','')
#     if keyword == 'Date of' and ('C' in required_field[0] or 'c' in required_field[0]) and req_val == 'Date_of_Birth1' and valueposts=='same': # condition for Alternated Financing Funder only
#         required_field = required_field[1:]    
#     print(keyword,required_field)
#     return required_field

def Business_Information_Address(df,keyword,column_names,value_location,valueposts,req_val,specialchar,position):
    if keyword == 'Nil':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    columns = ast.literal_eval(column_names)
    valueranges = ast.literal_eval(valueposts)
    locations = ast.literal_eval(value_location)
    for m in range(0,len(columns)):
        column_name = columns[m]
        for k in range(0,len(locations)):
            value_location = locations[k]
            for t in range(0,len(valueranges)):
                value = valueranges[t]
                for i in range(0,len(df)):
                    if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
                        if specialchar == 'None':
                            if value_location == 'same':
                                if value == "present+1":
                                    required_field = df[column_name][i+1]
                                    break
                                if value == "present+2":
                                    required_field = df[column_name][i+2]
                                    break
                                if value == "present+3":
                                    required_field = df[column_name][i+3]
                                    break
                                if value == "present+4":
                                    required_field = df[column_name][i+4]
                                    break
                                if value == "present-2":
                                    required_field = df[column_name][i-2]
                                    break
                                if value == "present-3":
                                    required_field = df[column_name][i-3]
                                    break
                                if value == "present-1":
                                    required_field = df[column_name][i-1]
                                    break
                                if value == "keypresent":
                                    required_field = str(df[column_name][i]).replace(keyword,'')
                                    break
                                if value == "ispresent":
                                    required_field = df[column_name][i]
                                    break
                                if value == 'Trusted keypresent+1':
                                    req= str(df[column_name][i]) + '' + str(df[column_name][i+1])+ '' + str(df[column_name][i+2])
                                    required_field = str(req).replace('Zip','').replace('ENOAH','')
                            else:
                                if value == "present+1":
                                    required_field = df[value_location][i+1]
                                    break
                                if value == "present+2":
                                    required_field = df[value_location][i+2]
                                    break
                                if value == "present+3":
                                    required_field = df[value_location][i+3]
                                    break
                                if value == "present-2":
                                    required_field = df[column_name][i-2]
                                    break
                                if value == "present-3":
                                    required_field = df[column_name][i-3]
                                    break

                                if value == "present-1":
                                    required_field = df[value_location][i-1]
                                    break
                                if value == "ispresent":
                                    required_field = df[value_location][i]
                                    break
                                if value == "keypresent":
                                    required_field = str(df[value_location][i]).replace(keyword,'')
                                    break
                        elif specialchar == ',':
                            if value_location == 'same':
                                if value == "present+1":
                                    required_field = df[column_name][i+1].split(',')
                                if value == "present+2":
                                    required_field = df[column_name][i+2].split(',')
                                if value == "present+3":
                                    required_field = df[column_name][i+3].split(',')
                                if value == "present-2":
                                    required_field = df[column_name][i-2].split(',')
                                if value == "present-3":
                                    required_field = df[column_name][i-3].split(',')

                                if value == "present-1":
                                    required_field = df[column_name][i-1].split(',')
                                if value == "keypresent":
                                    required_field = str(df[column_name][i]).replace(keyword,'')
                                if value == "ispresent":
                                    required_field = df[column_name][i]
                                if len(required_field)>2 and 'unit' in required_field[-2]:
                                    required_field = required_field[0:-2]
                                n = len(required_field)
                                #print(req_val,[required_field])
                                if req_val == 'City' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = required_field[n-int(position)]
                                    break
                                if req_val == 'State' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                    break
                                if req_val == 'Zip' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                    if required_field!=[]:
                                        required_field = required_field[0]
                                        break
                            else:
                                if value == "present+1":
                                    required_field = df[value_location][i+1].split(',')
                                if value == "present-1":
                                    required_field = df[value_location][i-1].split(',')
                                if value == "keypresent":
                                    required_field = str(df[value_location][i]).replace(keyword,'').split(',')
                                if value == "present+2":
                                    required_field = df[value_location][i+2].split(',')
                                if value == "present+3":
                                    required_field = df[value_location][i+3].split(',')
                                if value == "present-2":
                                    required_field = df[value_location][i-2].split(',')
                                if value == "present-3":
                                    required_field = df[value_location][i-3].split(',')
                                if value == "ispresent":
                                    required_field = df[value_location][i].split(',')

                                if len(required_field)>2 and 'unit' in required_field[-2]:
                                    required_field = required_field[0:-2]
                                #print(req_val,[required_field])

                                n = len(required_field)
                                if req_val == 'City' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = required_field[n-int(position)]
                                    break
                                if req_val == 'State' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                    break
                                if req_val == 'Zip' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                    if required_field!=[]:
                                        required_field = required_field[0]
                                        break
                        else:
                            if value_location == 'same':
                                if value == "present+1":
                                    required_field = df[column_name][i+1].split()
                                if value == "present-1":
                                    required_field = df[column_name][i-1].split()
                                if value == "present-2":
                                    required_field = df[column_name][i-2].split()
                                if value == "present-3":
                                    required_field = df[column_name][i-3].split()
                                if value == "present+2":
                                    required_field = df[column_name][i+2].split()
                                if value == "present+3":
                                    required_field = df[column_name][i+3].split()

                                if value == "keypresent":
                                    required_field = str(df[column_name][i]).replace(keyword,'').split()
                                if value == "ispresent":
                                    required_field = df[column_name][i].split()

                                if len(required_field)>2 and 'unit' in required_field[-2]:
                                    required_field = required_field[0:-2]
                                #print(req_val,[required_field])
                                n = len(required_field)
                                if req_val == 'City' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = required_field[n-int(position)]
                                    break
                                if req_val == 'State' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>= position:
                                        required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                    break
                                if req_val == 'Zip' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                    if required_field!=[]:
                                        required_field = required_field[0]
                                        break
                            else:
                                if value == "present+1":
                                    required_field = df[value_location][i+1].split()
                                    if req_val == 'Zip' and keyword == 'Business Address' and ('Rent or Own' in str(df[column_name][i+3]) or 'Business Phone Number' in str(df[column_name][i+3])): #condition only for Get pop cash 
                                        required_field = df[value_location][i+2].split()
                                if value == "present-1":
                                    required_field = df[value_location][i-1].split()
                                if value == "present+2":
                                    required_field = df[value_location][i+2].split()
                                if value == "present-2":
                                    required_field = df[value_location][i-2].split()
                                if value == "present+3":
                                    required_field = df[value_location][i+3].split()
                                if value == "present-3":
                                    required_field = df[value_location][i-3].split()
                                if value == "present+4":
                                    required_field = df[value_location][i+4].split()
                                if value == "present-4":
                                    required_field = df[value_location][i-4].split()
                                if value == "present+5":
                                    required_field = df[value_location][i+5].split()
                                if value == "present-5":
                                    required_field = df[value_location][i-5].split()
                                if value == "keypresent":
                                    required_field = df[value_location][i].replace(keyword,'').split()
                                if value == "ispresent":
                                    required_field = df[value_location][i].split()
                                if len(required_field)>2 and 'unit' in required_field[-2]:
                                    required_field = required_field[0:-2]
                                n = len(required_field)
                                if req_val == 'City' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if position is not None and not pd.isna(position):
                                        if n>=int(position):
                                            required_field = required_field[n-int(position)]
                                        else:
                                            print(f"Warning: 'n' is less than 'position' for City. Skipping adjustment.")
                                            break
                                    else:
                                        print("Error: 'position' is NaN or invalid.")
                                        break
                                else:
                                    print("Error: Invalid or empty 'required_field' for City.")
                                    break
                                if req_val == 'State' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                    break
                                if req_val == 'Zip' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                    if n>=int(position):
                                        required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                    if required_field!=[]:
                                        required_field = required_field[0]
                                    break
                if keyword == 'City:' and re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Zip Code:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Zip:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Zip' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if req_val == 'State':
                    required_field = re.sub('[0-9]','',str(required_field))
                if req_val == 'Zip':
                    required_field = re.sub('[a-zA-Z]','',str(required_field))
                if req_val == 'Zip':
                    required_field = re.sub('[a-zA-Z#:]'," ",str(required_field))    
                if keyword == 'City:' and 'City:' in required_field:
                    required_field = 'ENOAH'
                if keyword == 'City' and 'State' in str(required_field):
                    required_field=str(required_field).replace('State','')
                if keyword == 'City, State Zip:' and 'Fax' in str(required_field):
                    required_field='ENOAH'
                required_field = str(required_field).replace('*','')
                required_field = str(required_field).replace(':','')
                required_field = str(required_field).replace('#','')
                required_field = str(required_field).replace("[]",'ENOAH')
                required_field = str(required_field).replace("['ENOAH']",'ENOAH')
                required_field = str(required_field).replace("['']",'ENOAH')
                # print("final req field",required_field,type(required_field))
                if str(required_field)!='ENOAH' and str(required_field).strip()!='' and required_field!=['ENOAH'] and required_field!=[''] and required_field != '['']' and required_field!=[]:
                    break
            if str(required_field)!='ENOAH' and str(required_field).strip()!='' and required_field!=['ENOAH'] and required_field!=[''] and required_field!=[] and required_field != '['']':
                break
    required_field = str(required_field).replace("['ENOAH']",'ENOAH').replace('_','')
    required_field = str(required_field).replace("['Na']",'ENOAH').replace('Physical Address','')
    required_field = str(required_field).replace("['Same']",'ENOAH').replace('•','')
    required_field = str(required_field).replace("[]",'ENOAH').replace('['']','ENOAH').replace(':','')
    required_field = str(required_field).replace("\u2610 Manufacturer/Wholesaler",'ENOAH').replace('\u0000 ','')
    return required_field
    
def Owner_Information_Address(df,keyword,column_names,value_location,valueposts,req_val,specialchar,position,owner_status):
    if keyword == 'Nil':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    columns = ast.literal_eval(column_names)
    valueranges = ast.literal_eval(valueposts)
    locations = ast.literal_eval(value_location)
    #print('columns,valueranges',columns,valueranges,keyword)
    for m in range(0,len(columns)):
        column_name = columns[m]
        for k in range(0,len(locations)):
            value_location = locations[k]
            for t in range(0,len(valueranges)):
                value = valueranges[t]
                for i in range(0,len(df)):
                    if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
                        if owner_status == "Owner #1":
                            if specialchar == 'None':
                                if value_location == 'same':
                                    if value == "present+1":
                                        required_field = df[column_name][i+1]
                                        break
                                    if value == "present+2":
                                        required_field = df[column_name][i+2]
                                        break
                                    if value == "present+3":
                                        required_field = df[column_name][i+3]
                                        break
                                    if value == "present-2":
                                        required_field = df[column_name][i-2]
                                        break
                                    if value == "present-3":
                                        required_field = df[column_name][i-3]
                                        break

                                    if value == "present-1":
                                        required_field = df[column_name][i-1]
                                        break
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'')
                                        break
                                    if value == "ispresent":
                                        required_field = df[column_name][i]
                                        break
                                else:
                                    if value == "present+1":
                                        required_field = df[value_location][i+1]
                                        break
                                    if value == "present+2":
                                        required_field = df[value_location][i+2]
                                        break
                                    if value == "present+3":
                                        required_field = df[value_location][i+3]
                                        break
                                    if value == "present+4":
                                        required_field = df[value_location][i+4]
                                        break
                                    if value == "present+5":
                                        required_field = df[value_location][i+5]
                                        break
                                    if value == "present+6":
                                        required_field = df[value_location][i+6]
                                        break
                                    if value == "present-2":
                                        required_field = df[value_location][i-2]
                                        break
                                    if value == "present-3":
                                        required_field = df[value_location][i-3]
                                        break
                                    if value == "present-1":
                                        required_field = df[value_location][i-1]
                                        break
                                    if value == "ispresent":
                                        required_field = df[value_location][i]
                                        break
                                    if value == "keypresent":
                                        required_field = str(df[value_location][i]).replace(keyword,'')
                                        break
                            elif specialchar == ',':
                                if value_location == 'same':
                                    if value == "present+1":
                                        required_field = df[column_name][i+1].split(',')
                                    if value == "present+2":
                                        required_field = df[column_name][i+2].split(',')
                                    if value == "present+3":
                                        required_field = df[column_name][i+3].split(',')
                                    if value == "present-2":
                                        required_field = df[column_name][i-2].split(',')
                                    if value == "present-3":
                                        required_field = df[column_name][i-3].split(',')

                                    if value == "present-1":
                                        required_field = df[column_name][i-1].split(',')
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'')
                                        required_field = str(required_field).split(',')
                                    if value == "ispresent":
                                        required_field = df[column_name][i].split(',')
                                    n = len(required_field)
                                    #print(req_val,[required_field])
                                    if req_val == 'City1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = required_field[n-int(position)]
                                            break
                                    if req_val == 'State1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                            break
                                    if req_val == 'Zip1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                        if required_field!=[]:
                                            required_field = required_field[0]
                                            break
                                else:
                                    if value == "present+1":
                                        required_field = df[value_location][i+1].split(',')
                                    if value == "present-1":
                                        required_field = df[value_location][i-1].split(',')
                                    if value == "keypresent":
                                        required_field = str(df[value_location][i]).replace(keyword,'').split(',')
                                    if value == "present+2":
                                        required_field = df[value_location][i+2].split(',')
                                    if value == "present+3":
                                        required_field = df[value_location][i+3].split(',')
                                    if value == "present-2":
                                        required_field = df[value_location][i-2].split(',')
                                    if value == "present-3":
                                        required_field = df[value_location][i-3].split(',')

                                    if value == "ispresent":
                                        required_field = df[value_location][i].split(',')
                                    n = len(required_field)
                                    if req_val == 'City1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = required_field[n-int(position)]
                                            break
                                    if req_val == 'State1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                            break
                                    if req_val == 'Zip1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                        if required_field!=[]:
                                            required_field = required_field[0]
                                            break
                            else:
                                if value_location == 'same':
                                    if value == "present+1":
                                        required_field = df[column_name][i+1].split()
                                    if value == "present+2":
                                        required_field = df[column_name][i+2].split()
                                    if value == "present+3":
                                        required_field = df[column_name][i+3].split()
                                    if value == "present-2":
                                        required_field = df[column_name][i-2].split()
                                    if value == "present-3":
                                        required_field = df[column_name][i-3].split()

                                    if value == "present-1":
                                        required_field = df[column_name][i-1].split()
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'').split()
                                    if value == "ispresent":
                                        required_field = df[column_name][i].split()
                                    n = len(required_field)
                                    if req_val == 'City1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = required_field[n-int(position)]
                                            break
                                    if req_val == 'State1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n >= position:
                                            required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                            break
                                    if req_val == 'Zip1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if position is not None and not pd.isna(position):
                                            if n >= int(position):
                                                required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                                if required_field:  
                                                    required_field = required_field[0]
                                            else:
                                                print(f"Warning: 'n' is less than 'position' for Zip1. Skipping adjustment.")
                                        else:
                                            print("Error: 'position' is NaN or invalid.") 
                                    else:
                                        print("Error: Invalid or empty 'required_field' for Zip1.")                           
                                     #   if required_field!=[]:
                                     #       required_field = required_field[0]
                                     #       break
                                else:
                                    if value == "present+1":
                                        required_field = str(df[value_location][i+1]).split()
                                    if value == "present+2":
                                        required_field = df[value_location][i+2].split()
                                    if value == "present+3":
                                        required_field = df[value_location][i+3].split()
                                    if value == "present-2":
                                        required_field = df[value_location][i-2].split()
                                    if value == "present-3":
                                        required_field = df[value_location][i-3].split()

                                    if value == "present-1":
                                        required_field = df[value_location][i-1].split()
                                    if value == "keypresent":
                                        required_field = str(df[value_location][i]).replace(keyword,'').split()
                                    if value == "ispresent":
                                        required_field = df[value_location][i].split()
                                    n = len(required_field)
                                    if req_val == 'City1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if position is not None and not pd.isna(position):
                                            if n>=int(position):
                                                required_field = required_field[n-int(position)]
                                            else:
                                                print(f"Warning: 'n' is less than 'position' for City. Skipping adjustment.")
                                                break  # Exit the loop if further processing isn't meaningful
                                        else:
                                            print("Error: 'position' is NaN or invalid.")
                                            break  # Exit the loop if position is invalid
                                    else:
                                        print("Error: Invalid or empty 'required_field' for City.")
                                        break
                                    if req_val == 'State1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                            break
                                    if req_val == 'Zip1' and (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']):
                                        if n>=int(position):
                                            required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                        if required_field!=[]:
                                            required_field = required_field[0]
                                            break
                        else:
                            if specialchar == 'None':
                                if value_location == 'same':
                                    if value == "present+1":
                                        print('welcome......',keyword,df[column_name][i+1],df[column_name][i+2])
                                        required_field = df[column_name][i+1]
                                    if value == "present+2":
                                        required_field = df[column_name][i+2]
                                    if value == "present+3":
                                        required_field = df[column_name][i+3]
                                    if value == "present-2":
                                        required_field = df[column_name][i-2]
                                    if value == "present-3":
                                        required_field = df[column_name][i-3]
                                    if value == "present+4":
                                        required_field = df[value_location][i+4]
                                    if value == "present+5":
                                        required_field = df[value_location][i+5]
                                    if value == "present+6":
                                        required_field = df[value_location][i+6]
                                    if value == "present-1":
                                        required_field = df[column_name][i-1]
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'')
                                    if value == "ispresent":
                                        required_field = df[column_name][i]
                                else:
                                    if value == "present+1":
                                        if len(df)<i+1:
                                            print('welcome1111......',keyword,df[column_name][i+1],df[column_name][i+2])
                                            required_field = df[value_location][i+1]
                                        else:
                                            required_field = df[value_location][i+1]
                                    if value == "present+2":
                                        print('welcome2222......',keyword,df[column_name][i+1],df[column_name][i+2])
                                        required_field = df[value_location][i+2]
                                    if value == "present+3":
                                        required_field = df[value_location][i+3]
                                    if value == "present-2":
                                        required_field = df[value_location][i-2]
                                    if value == "present-3":
                                        required_field = df[value_location][i-3]
                                    if value == "present-1":
                                        required_field = df[value_location][i-1]
                                    if value == "keypresent":
                                        required_field = str(df[value_location][i]).replace(keyword,'')
                                    if value == "ispresent":
                                        required_field = df[value_location][i]
                            elif specialchar == ',':
                                if value_location == 'same':
                                    if value == "present+1":
                                        required_field = df[column_name][i+1].split(',')
                                    if value == "present+2":
                                        required_field = df[column_name][i+2].split(',')
                                    if value == "present+3":
                                        required_field = df[column_name][i+3].split(',')
                                    if value == "present-2":
                                        required_field = df[column_name][i-2].split(',')
                                    if value == "present-3":
                                        required_field = df[column_name][i-3].split(',')
                                    if value == "present-1":
                                        required_field = df[column_name][i-1].split(',')
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'').split(',')
                                    if value == "ispresent":
                                        required_field = df[column_name][i].split(',')
                                    n = len(required_field)
                                    if (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']) and len(required_field)>2:
                                        if req_val == 'City2':
                                            if n>=int(position):
                                                required_field = required_field[n-int(position)]
                                        if req_val == 'State2':
                                            if n>=int(position):
                                                required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                        if req_val == 'Zip2':
                                            if n>=int(position):
                                                required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                            if required_field!=[]:
                                                required_field = required_field[0]
                                    else:
                                        required_field = 'ENOAH'
                                else:
                                    if value == "present+1":
                                        required_field = df[column_name][i+1].split(',')
                                    if value == "present+2":
                                        required_field = df[column_name][i+2].split(',')
                                    if value == "present+3":
                                        required_field = df[column_name][i+3].split(',')
                                    if value == "present-2":
                                        required_field = df[column_name][i-2].split(',')
                                    if value == "present-3":
                                        required_field = df[column_name][i-3].split(',')
                                    if value == "present-1":
                                        required_field = df[column_name][i-1].split(',')
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'').split(',')
                                    if value == "ispresent":
                                        required_field = df[column_name][i].split(',')
                                    n = len(required_field)
                                    if (required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*']) and len(required_field)>2:
                                        if req_val == 'City2':
                                            if n>=int(position):
                                                required_field = required_field[n-int(position)]
                                        if req_val == 'State2':
                                            if n>=int(position):
                                                required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                        if req_val == 'Zip2':
                                            if n>=int(position):
                                                required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                            if required_field!=[]:
                                                required_field = required_field[0]
                                    else:
                                        required_field = 'ENOAH'
                            else:
                                if value_location == 'same':
                                    if value == "present+1":
                                        required_field = df[column_name][i+1].split()
                                    if value == "present+2":
                                        required_field = df[column_name][i+2].split()
                                    if value == "present+3":
                                        required_field = df[column_name][i+3].split()
                                    if value == "present-2":
                                        required_field = df[column_name][i-2].split()
                                    if value == "present-3":
                                        required_field = df[column_name][i-3].split()

                                    if value == "present-1":
                                        required_field = df[column_name][i-1].split()
                                    if value == "keypresent":
                                        required_field = str(df[column_name][i]).replace(keyword,'').split()
                                    if value == "ispresent":
                                        required_field = df[column_name][i].split()
                                    n = len(required_field)
                                    if ((required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*'])) and len(required_field)>2:
                                        if req_val == 'City2':
                                            if n>=int(position):
                                                required_field = required_field[n-int(position)]
                                        if req_val == 'State2':
                                            if n>=int(position):
                                                required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                        if req_val == 'Zip2':
                                            if n>=int(position):
                                                required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                            if required_field!=[]:
                                                required_field = required_field[0]
                                    else:
                                        required_field = 'ENOAH'
                                else:
                                    if value == "present+1":
                                        required_field = str(df[value_location][i+1]).split()
                                    if value == "present+2":
                                        required_field = df[value_location][i+2].split()
                                    if value == "present+3":
                                        required_field = df[value_location][i+3].split()
                                    if value == "present-2":
                                        required_field = df[value_location][i-2].split()
                                    if value == "present-3":
                                        required_field = df[value_location][i-3].split()

                                    if value == "present-1":
                                        required_field = df[value_location][i-1].split()
                                    if value == "keypresent":
                                        required_field = str(df[value_location][i]).replace(keyword,'').split()
                                    if value == "ispresent":
                                        required_field = df[value_location][i].split()
                                    n = len(required_field)
                                    if ((required_field!=['ENOAH'] and required_field!=[] and required_field!=[''] and required_field!=['*'])) and len(required_field)>2:
                                        if req_val == 'City2':
                                            if position is not None and not pd.isna(position):
                                                try:
                                                    if n>=int(position):
                                                        required_field = required_field[n-int(position)]
                                                    else:
                                                        print("Skipping adjustment")
                                                except (ValueError, TypeError) as e:
                                                    print(e)
                                                    required_field = 'ENOAH'
                                            else:
                                                print("position is NaN or invalid")
                                                required_field = 'ENOAH'
                                        if req_val == 'State2':
                                            if position is not None and not pd.isna(position) and n >= int(position):
                                                required_field = re.sub('[0-9]',  ' ', str(required_field[n-int(position)]))
                                            else:
                                                required_field = 'ENOAH'
                                        if req_val == 'Zip2':
                                            if position is not None and not pd.isna(position) and n >= int(position):
                                                required_field = re.findall('\d+', str(required_field[n-int(position)]))
                                                if required_field:
                                                    required_field = required_field[0]
                                            else:
                                                required_field = 'ENOAH'
                                    else:
                                        required_field = 'ENOAH'
                if keyword == 'City:' and re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if keyword == 'Zip Code:' and not re.search('[0-9]',str(required_field)):
                    required_field='ENOAH'
                if req_val == 'State1' :
                    required_field = re.sub('[0-9]','',str(required_field))
                if req_val == 'Zip1' :
                    required_field = re.sub('[a-zA-Z]','',str(required_field))

                required_field = str(required_field).replace('*','')
                required_field = str(required_field).replace(':','')
                required_field = str(required_field).replace('#','')
                required_field = str(required_field).replace("[]",'ENOAH')
                required_field = str(required_field).replace("['']",'ENOAH')
                print("owner req field",required_field)
                if str(required_field)!='ENOAH' and str(required_field).strip()!='' and required_field!=['ENOAH'] and required_field!=[''] and required_field!=[]:
                    break
            if str(required_field)!='ENOAH' and str(required_field).strip()!='' and required_field!=['ENOAH'] and required_field!=[''] and required_field!=[]:
                break
    if keyword == 'City' and required_field == 'Street Address':
        required_field = 'ENOAH'
    if keyword == 'City:' and 'Date of Birth:' in str(required_field):
        required_field = 'ENOAH'
    required_field = str(required_field).replace("['Na']",'ENOAH').replace('_','')
    required_field = str(required_field).replace("[]",'ENOAH').replace(':','').replace('•','')
    required_field = str(required_field).replace("['ENOAH']",'ENOAH').replace('Last Name','ENOAH').replace('Home Address','').replace('Email:','').strip()
    return required_field

def extract_business_info_template(Funder,table,table_name,type_column):
    print(Funder,table,table_name,type_column)
    df = extract_table_content(table,table_name,type_column)
    requirements = pd.read_csv(dependencydir + 'Template_Business.csv',on_bad_lines='skip',encoding='cp1252')
    state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv',on_bad_lines='skip',encoding='cp1252')
    data = requirements[requirements.Funders.str.contains(Funder,na=False)]
    data.reset_index(drop=True, inplace=True)
    for i in range(0,len(df)):
        if 'LegaI' in str(df['Col_1'][i]) or 'LegaI' in str(df['Col_2'][i]) or 'Legai' in str(df['Col_2'][i]):
            df['Col_1'][i] = str(df['Col_1'][i]).replace('LegaI','Legal')
            df['Col_2'][i] = str(df['Col_2'][i]).replace('LegaI','Legal')
            df['Col_2'][i] = str(df['Col_2'][i]).replace('Legai','Legal')
    if Funder == 'Crestmont Capital':#13/06
        for i in range(0,len(df)):
            if df['Col_1'][i] == 'Business address' and df['Col_1'][i+1] == 'ENOAH' and df['Col_2'][i+1] != 'ENOAH':
                df['Col_2'][i] = str(df['Col_2'][i])+' '+df['Col_2'][i+1]
    if Funder == '_Cap_Front':#17/06
        for i in range(0,len(df)):
            if df['Col_1'][i] == 'Business Address:' and df['Col_1'][i+1] != 'ENOAH' and df['Col_1'][i+2] != 'United States':
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+df['Col_1'][i+2]
    if Funder == 'GRP Funding':#17/06
        for i in range(0,len(df)):
            if 'Business Address:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH':
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+df['Col_1'][i+2]
    if Funder == 'ABF':
        for i in range(0,len(df)): 
            if i + 2 < len(df):
                if 'Legal Business Name:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH' and 'Street Address' in str(df['Col_1'][i+2]):
                    df['Col_1'][i] = str(df['Col_1'][i])+' '+str(df['Col_1'][i+1])
    if Funder == 'Virtue Capital':#17/06
        df.to_csv('virtue.csv')
        for i in range(0,len(df)):
            if 'LLC' in str(df['Col_1'][i]) and df['Col_2'][i] == 'ENOAH':
                df['Col_2'][i] = str(df['Col_1'][i]).split('LLC')[1]
                df['Col_1'][i] = str(df['Col_1'][i]).split('LLC')[0] +' '+'LLC'
            if 'INC' in str(df['Col_1'][i]) and df['Col_2'][i] == 'ENOAH':
                df['Col_2'][i] = str(df['Col_1'][i]).split('INC')[1]
                df['Col_1'][i] = str(df['Col_1'][i]).split('INC')[0] +' '+'INC'
            if 'SOLUTIONS' in str(df['Col_1'][i]) and df['Col_2'][i] == 'ENOAH':
                df['Col_2'][i] = str(df['Col_1'][i]).split('SOLUTIONS')[1]
                df['Col_1'][i] = str(df['Col_1'][i]).split('SOLUTIONS')[0] +' '+'SOLUTIONS'
    if Funder == 'Quickcapital_Funding_1':#17/06
        for i in range(0,len(df)):
            if df['Col_3'][i] == 'State':
                df['Col_3'][i] = 'ENOAH'
    if Funder == 'White_Shore_Funding':
        for i in range(0,len(df)):
            if ', United States (US)' in df['Col_1'][i]:
                df['Col_1'][i] = str(df['Col_1'][i]).replace(', United States (US)','')
    if Funder == 'Pro_Source_Lending_Group_1':
        for i in range(0,len(df)):
            if ',' in str(df['Col_1'][i])[-1]:
                df['Col_1'][i] = str(df['Col_1'][i])[0:-2]
            if ',' in str(df['Col_2'][i])[-1]:
                df['Col_2'][i] = str(df['Col_2'][i])[0:-2]
                print('prosource',df['Col_2'][i])
    if Funder == 'Dweck Financial':
        for i in range(0,len(df)):
            if 'State' in str(df['Col_4'][i]) and df['Col_4'][i] != 'State':
                df['Col_4'][i+1] = df['Col_4'][i].replace('State','')
                df['Col_4'][i] = 'ENOAH'
            if 'Zipcode' in str(df['Col_5'][i]) and 'Entity Type' in str(df['Col_5'][i+1]):
                df['Col_5'][i+1] = df['Col_5'][i+1].replace('Entity Type','')
            if df['Col_1'][i] == 'Industry':
                df['Col_1'][i] = 'ENOAH'                
    
    
    df.to_csv("Business.csv", escapechar='\\', quoting=csv.QUOTE_MINIMAL)
    Company_Name = 'Company Name: ' + str(Business_Information_Normal(df,data['Company_Keyword'][0],data['Company_KeyLocation'][0],data['Company_ValueColumn'][0],data['Company_Value'][0],data['Company_reqval'][0]))
    Doing_Business_As = 'Doing Business As: ' + str(Business_Information_Normal(df,data['DBA_Keyword'][0],data['DBA_KeyLocation'][0],data['DBA_ValueColumn'][0],data['DBA_Value'][0],data['DBA_reqval'][0]))
    Business_Tax_ID = 'Business Tax ID: ' + str(Business_Information_Normal(df,data['TaxID_Keyword'][0],data['TaxID_KeyLocation'][0],data['TaxID_ValueColumn'][0],data['TaxID_Value'][0],data['TaxID_reqval'][0]))
    Date_of_Incorporation = 'Date of Incorporation: ' + str(Business_Information_Normal(df,data['Incorporation_Keyword'][0],data['Incorporation_KeyLocation'][0],data['Incorporation_ValueColumn'][0],data['Incorporation_Value'][0],data['Incorporation_reqval'][0]))
    Street_Address = 'Street Address(Company): ' + str(Business_Information_Normal(df,data['Address(Company)_Keyword'][0],data['Address(Company)_KeyLocation'][0],data['Address(Company)_ValueColumn'][0],data['Address(Company)_Value'][0],data['Address(Company)_reqval'][0]))
    City = 'City(Company): ' + str(Business_Information_Address(df,data['City(Company)_Keyword'][0],data['City(Company)_KeyLocation'][0],data['City(Company)_ValueColumn'][0],data['City(Company)_Value'][0],data['City(Company)_reqval'][0],data['City(Company)_specialchar'][0],data['City(Company)_position'][0]))
    State = 'State(Company): ' + str(Business_Information_Address(df,data['State(Company)_Keyword'][0],data['State(Company)_KeyLocation'][0],data['State(Company)_ValueColumn'][0],data['State(Company)_Value'][0],data['State(Company)_reqval'][0],data['State(Company)_specialchar'][0],data['State(Company)_position'][0]))
    Zip_Code = 'Zip Code(Company): ' + str(Business_Information_Address(df,data['Zip(Company)_Keyword'][0],data['Zip(Company)_KeyLocation'][0],data['Zip(Company)_ValueColumn'][0],data['Zip(Company)_Value'][0],data['Zip(Company)_reqval'][0],data['Zip(Company)_specialchar'][0],data['Zip(Company)_position'][0]))
    Business_Phone = 'Business Phone: ' + str(Business_Information_Normal(df,data['Phone_Keyword'][0],data['Phone_KeyLocation'][0],data['Phone_ValueColumn'][0],data['Phone_Value'][0],data['Phone_reqval'][0]))
    return Company_Name, Doing_Business_As, Business_Tax_ID, Date_of_Incorporation, Street_Address, City, State, Zip_Code, Business_Phone

def extract_owner_info_template(Funder,table,table_name,type_column):
    df = extract_table_content(table,table_name,type_column)
    requirements = pd.read_csv(dependencydir + 'Template_Owner.csv',on_bad_lines='skip',encoding='cp1252')
    state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv',on_bad_lines='skip',encoding='cp1252')
    data = requirements[requirements.Funders.str.contains(Funder,na=False)]
    data.reset_index(drop=True, inplace=True)
    if Funder == 'Crestmont Capital':#13/06
        for i in range(0,len(df)):
            if df['Col_1'][i] == 'Home address' and df['Col_1'][i+1] == 'ENOAH' and df['Col_2'][i+1] != 'ENOAH':
                df['Col_2'][i] = str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])
            if df['Col_1'][i] == 'Home address' and '-' in str(df['Col_2'][i]):
                df['Col_2'][i] = str(df['Col_2'][i].split('-')[0])
    if Funder == '_Cap_Front':#17/06
        for i in range(0,len(df)):
            if df['Col_1'][i] == 'Address:' and df['Col_1'][i+1] != 'ENOAH' and df['Col_1'][i+2] != 'United States':
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+df['Col_1'][i+2]
    if Funder == 'Fund_Merica':
        for i in range(0,len(df)):
            if df['Col_1'][i] == 'Date Of Birth' and df['Col_1'][i+1] == 'ENOAH' and df['Col_1'][i+2] == 'ENOAH' and df['Col_1'][i+3] == 'Last Name':
                df['Col_2'][i] = str(df['Col_2'][i])+' '+df['Col_2'][i+1]+','+df['Col_2'][i+2]
            if df['Col_1'][i] == 'Date Of Birth' and df['Col_1'][i+1] == 'ENOAH' and df['Col_1'][i+2] == 'Last Name':
                df['Col_2'][i] = str(df['Col_2'][i])+','+df['Col_2'][i+1]
            if df['Col_1'][i] == 'Co-App Date Of Birth' and df['Col_1'][i+1] == 'ENOAH' and df['Col_1'][i+2] == 'ENOAH' and df['Col_1'][i+3] == 'Co-App Last Name':
                df['Col_2'][i] = str(df['Col_2'][i])+' '+df['Col_2'][i+1]+','+df['Col_2'][i+2]
            if df['Col_1'][i] == 'Co-App Date Of Birth' and df['Col_1'][i+1] == 'ENOAH' and df['Col_1'][i+2] == 'Co-App Last Name':
                df['Col_2'][i] = str(df['Col_2'][i])+','+df['Col_2'][i+1]
            df['Col_2'][i] = str(df['Col_2'][i]).replace('--Select--','')
    if Funder == 'ABF':
        for i in range(0,len(df)): 
            if i + 2 < len(df):
                if 'Home Address:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH' and 'Cell Phone:' in str(df['Col_1'][i+2]):
                    df['Col_1'][i] = str(df['Col_1'][i])+' '+str(df['Col_1'][i+1])
    if Funder == 'White_Shore_Funding':
        for i in range(0,len(df)):
            if ', United States (US)' in df['Col_1'][i]:
                df['Col_1'][i] = str(df['Col_1'][i]).replace(', United States (US)','')
    if Funder == 'Spin Capital':
        for i in range(0,len(df)):
            if 'Nar' in df['Col_1'][i]:
                df['Col_1'][i] = str(df['Col_1'][i]).replace('Nar','Name')
    if Funder == 'Big_Think_Capital_2':#17/06
        for i in range(0,len(df)):
            if '..' in str(df['Col_2'][i]):   
                df['Col_2'][i] = str(df['Col_2'][i]).replace('..','')
    if Funder == 'Can I Have Money LLC':
        for i in range(0,len(df)):
            if 'Home Address:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH' and df['Col_1'][i+2] != 'ENOAH' and df['Col_1'][i+3] != 'ENOAH' and df['Col_1'][i+4] != 'ENOAH':   
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+str(df['Col_1'][i+2])+' '+str(df['Col_1'][i+3])+' '+str(df['Col_1'][i+4])
            if 'Home Address:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH' and df['Col_1'][i+2] != 'ENOAH' and df['Col_1'][i+3] != 'ENOAH' and df['Col_1'][i+4] == 'ENOAH':   
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+str(df['Col_1'][i+2])+' '+str(df['Col_1'][i+3])
            if 'Home Address:' in str(df['Col_1'][i]) and df['Col_1'][i+1] != 'ENOAH' and df['Col_1'][i+2] != 'ENOAH' and df['Col_1'][i+3] == 'ENOAH':   
                df['Col_1'][i+1] = str(df['Col_1'][i+1])+' '+str(df['Col_1'][i+2])
            if 'Home Address:' in str(df['Col_3'][i]) and df['Col_3'][i+1] != 'ENOAH' and df['Col_3'][i+2] != 'ENOAH' and df['Col_3'][i+3] != 'ENOAH':   
                df['Col_3'][i+2] = str(df['Col_3'][i+2])+' '+str(df['Col_3'][i+3])
    if Funder == 'Kingdom Kapital':
        for i in range(0,len(df)):
            if 'Ownership %:' in str(df['Col_4'][i]) and df['Col_5'][i] != 'ENOAH':
                df['Col_4'][i] = str(df['Col_4'][i])+' '+str(df['Col_5'][i])
    if Funder == 'Liquify Funding LLC':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_5'][i]) or 'Zip:' in str(df['Col_4'][i]):
                df['Col_4'][i] = str(df['Col_4'][i]).replace('Zip:','zip:')
                df['Col_5'][i] = str(df['Col_5'][i]).replace('Zip:','zip:')
            if 'Full Legal Name Owner1' in str(df['Col_1'][i]):
                df['Col_1'][i] = str(df['Col_1'][i]).replace('Full Legal Name Owner1','Full Legal Name Owner 1')
    if Funder == 'Dweck Financial':
        for i in range(0,len(df)):
            if 'State' in str(df['Col_4'][i]) and df['Col_4'][i] != 'State':
                df['Col_4'][i+1] = df['Col_4'][i].replace('State','')
                df['Col_4'][i] = 'ENOAH'
            if '-' in str(df['Col_4'][i]):
                df['Col_4'][i] = str(df['Col_4'][i]).split('-')[0]
            if 'Zipcode' in str(df['Col_5'][i]) and 'Email Address' in str(df['Col_5'][i+1]):
                df['Col_5'][i+1] = df['Col_5'][i+1].replace('Email Address','')
    if Funder == 'Ultra Capital Funding LLC':
        for i in range(0,len(df)):
            if 'FullName' in str(df['Col_1'][i]):
                df['Col_1'][i+1] = df['Col_1'][i].replace('FullName','Full Name')
    df.to_csv("Owner.csv", escapechar='\\', quoting=csv.QUOTE_MINIMAL)
    First_Name1 = 'First Name1: ' + str(Owner_Information_Normal(df,data['FN1_Keyword'][0],data['FN1_KeyLocation'][0],data['FN1_ValueColumn'][0],data['FN1_Value'][0],data['FN1_reqval'][0],'Owner #1'))
    Last_Name1 = 'Last Name1: ' + str(Owner_Information_Normal(df,data['LN1_Keyword'][0],data['LN1_KeyLocation'][0],data['LN1_ValueColumn'][0],data['LN1_Value'][0],data['LN1_reqval'][0],'Owner #1'))
    Date_of_Birth1 = 'Date of Birth1: ' + str(Owner_Information_Normal(df,data['DOB1_Keyword'][0],data['DOB1_KeyLocation'][0],data['DOB1_ValueColumn'][0],data['DOB1_Value'][0],data['DOB1_reqval'][0],'Owner #1'))
    Social_Security1 = 'Social Security1: ' + str(Owner_Information_Normal(df,data['SSN1_Keyword'][0],data['SSN1_KeyLocation'][0],data['SSN1_ValueColumn'][0],data['SSN1_Value'][0],data['SSN1_reqval'][0],'Owner #1'))
    Mobile_Number1 = 'Mobile Number1: ' + str(Owner_Information_Normal(df,data['MN1_Keyword'][0],data['MN1_KeyLocation'][0],data['MN1_ValueColumn'][0],data['MN1_Value'][0],data['MN1_reqval'][0],'Owner #1'))
    taken_number = Mobile_Number1.replace('Mobile Number1:','')
    if re.search('[a-zA-Z]',taken_number):
        Mobile_Number1 = 'Mobile Number1: ' + 'ENOAH'
    Email1 = 'Email1: ' + str(Owner_Information_Normal(df,data['Email1_Keyword'][0],data['Email1_KeyLocation'][0],data['Email1_ValueColumn'][0],data['Email1_Value'][0],data['Email1_reqval'][0],'Owner #1'))
    Ownership1 = 'Ownership Percentage of Business1: ' + str(Owner_Information_Normal(df,data['Ownership1_Keyword'][0],data['Ownership1_KeyLocation'][0],data['Ownership1_ValueColumn'][0],data['Ownership1_Value'][0],data['Ownership1_reqval'][0],'Owner #1'))
    for i in range(len(df)):#05/06
        if len(df.columns) == 2:
            if df['Col_1'][i] == 'Home address' and df['Col_2'][i] != 'ENOAH' and df['Col_1'][i+1] == 'ENOAH' and df['Col_2'][i+1] != 'ENOAH':
                df['Col_2'][i] = str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])
    Street_Address1 = 'Street Address(Owner)1: ' + str(Owner_Information_Normal(df,data['Address1_Keyword'][0],data['Address1_KeyLocation'][0],data['Address1_ValueColumn'][0],data['Address1_Value'][0],data['Address1_reqval'][0],'Owner #1')).replace('[','')
    print('columnstest',len(df.columns),df.columns)
    City1 = 'City(Owner)1: ' + str(Owner_Information_Address(df,data['City1_Keyword'][0],data['City1_KeyLocation'][0],data['City1_ValueColumn'][0],data['City1_Value'][0],data['City1_reqval'][0],data['City1_specialchar'][0],data['City1_position'][0],'Owner #1')).replace('[','').replace(']','')
    State1 = 'State(Owner)1: ' + str(Owner_Information_Address(df,data['State1_Keyword'][0],data['State1_KeyLocation'][0],data['State1_ValueColumn'][0],data['State1_Value'][0],data['State1_reqval'][0],data['State1_specialchar'][0],data['State1_position'][0],'Owner #1'))
    Zip_Code1 = 'Zip Code(Owner)1: ' + str(Owner_Information_Address(df,data['Zip1_Keyword'][0],data['Zip1_KeyLocation'][0],data['Zip1_ValueColumn'][0],data['Zip1_Value'][0],data['Zip1_reqval'][0],data['Zip1_specialchar'][0],data['Zip1_position'][0],'Owner #1'))
    # print('carrom',Zip_Code1, City1)
    # if Zip_Code1 not in Street_Address1:
    #     Street_Address1 = str(Street_Address1)+' '+str(Zip_Code1.split(' ')[-1])
    # if City1 not in Street_Address1:
    #     Street_Address1 = str(Street_Address1)+' '+str(City1).replace('City(Owner)1: ','')
    First_Name2 = 'First Name2: ' + str(Owner_Information_Normal(df,data['FN2_Keyword'][0],data['FN2_KeyLocation'][0],data['FN2_ValueColumn'][0],data['FN2_Value'][0],data['FN2_reqval'][0],'Owner #2'))
    Last_Name2 = 'Last Name2: ' + str(Owner_Information_Normal(df,data['LN2_Keyword'][0],data['LN2_KeyLocation'][0],data['LN2_ValueColumn'][0],data['LN2_Value'][0],data['LN2_reqval'][0],'Owner #2'))
    Date_of_Birth2 = 'Date of Birth2: ' + str(Owner_Information_Normal(df,data['DOB2_Keyword'][0],data['DOB2_KeyLocation'][0],data['DOB2_ValueColumn'][0],data['DOB2_Value'][0],data['DOB2_reqval'][0],'Owner #2'))
    Social_Security2 = 'Social Security2: ' + str(Owner_Information_Normal(df,data['SSN2_Keyword'][0],data['SSN2_KeyLocation'][0],data['SSN2_ValueColumn'][0],data['SSN2_Value'][0],data['SSN2_reqval'][0],'Owner #2'))
    Mobile_Number2 = 'Mobile Number2: ' + str(Owner_Information_Normal(df,data['MN2_Keyword'][0],data['MN2_KeyLocation'][0],data['MN2_ValueColumn'][0],data['MN2_Value'][0],data['MN2_reqval'][0],'Owner #2'))
    taken_number = Mobile_Number2.replace('Mobile Number2:','')
    if re.search('[a-zA-Z]',taken_number):
        Mobile_Number2 = 'Mobile Number2: ' + 'ENOAH'
    Email2 = 'Email2: ' + str(Owner_Information_Normal(df,data['Email2_Keyword'][0],data['Email2_KeyLocation'][0],data['Email2_ValueColumn'][0],data['Email2_Value'][0],data['Email2_reqval'][0],'Owner #2'))
    Ownership2 = 'Ownership Percentage of Business2: ' + str(Owner_Information_Normal(df,data['Ownership2_Keyword'][0],data['Ownership2_KeyLocation'][0],data['Ownership2_ValueColumn'][0],data['Ownership2_Value'][0],data['Ownership2_reqval'][0],'Owner #2'))
    Street_Address2 = 'Street Address(Owner)2: ' + str(Owner_Information_Normal(df,data['Address2_Keyword'][0],data['Address2_KeyLocation'][0],data['Address2_ValueColumn'][0],data['Address2_Value'][0],data['Address2_reqval'][0],'Owner #2'))
    City2 = 'City(Owner)2: ' + str(Owner_Information_Address(df,data['City2_Keyword'][0],data['City2_KeyLocation'][0],data['City2_ValueColumn'][0],data['City2_Value'][0],data['City2_reqval'][0],data['City2_specialchar'][0],data['City2_position'][0],'Owner #2'))
    State2 = 'State(Owner)2: ' + str(Owner_Information_Address(df,data['State2_Keyword'][0],data['State2_KeyLocation'][0],data['State2_ValueColumn'][0],data['State2_Value'][0],data['State2_reqval'][0],data['State2_specialchar'][0],data['State2_position'][0],'Owner #2'))
    Zip_Code2 = 'Zip Code(Owner)2: ' + str(Owner_Information_Address(df,data['Zip2_Keyword'][0],data['Zip2_KeyLocation'][0],data['Zip2_ValueColumn'][0],data['Zip2_Value'][0],data['Zip2_reqval'][0],data['Zip2_specialchar'][0],data['Zip2_position'][0],'Owner #2'))
    print('..........................................',Zip_Code2)
    mystring1 = Social_Security1
    keyword1 = 'Social Security1:'
    before_keyword1, keyword1, after_keyword1 = mystring1.partition(keyword1)
    mystring2 = Social_Security2
    keyword2 = 'Social Security2:'
    before_keyword2, keyword2, after_keyword2 = mystring2.partition(keyword2)
    if ':' in str(after_keyword2) or ':' in str(after_keyword1):
        after_keyword2 = str(after_keyword2).replace(':','')
        after_keyword1 = str(after_keyword1).replace(':','')
    if after_keyword1 == after_keyword2:
        after_keyword2 = ''
    print(after_keyword2,after_keyword1)
    if after_keyword2.strip()!='ENOAH' and after_keyword2.strip()!='-' and after_keyword2.strip()!='' and after_keyword2.strip()!='n/a' and after_keyword2.strip()!='N/A' and after_keyword2.strip() != after_keyword1.strip() and not re.search('[a-zA-Z]',str(after_keyword2)):
        print('Black_tie_funding',Zip_Code1)
        return First_Name1, Last_Name1, Date_of_Birth1, Social_Security1, Mobile_Number1, Email1, Ownership1, Street_Address1, City1,State1,Zip_Code1,First_Name2,Last_Name2,Date_of_Birth2,Social_Security2,Mobile_Number2,Email2,Ownership2,Street_Address2,City2,State2,Zip_Code2
    else:
        print('Black_tie_funding11',Zip_Code1)
        return First_Name1, Last_Name1, Date_of_Birth1, Social_Security1, Mobile_Number1, Email1, Ownership1, Street_Address1, City1,State1,Zip_Code1

# to extract Industry info template:
def extract_industry_info_template(Funder,table,table_name,type_column):
    type_column.to_csv('type_column.csv')
    df = extract_table_content(table,table_name,type_column)
    requirements = pd.read_csv(dependencydir + 'industry_info.csv',on_bad_lines='skip',encoding='cp1252')
    data = requirements[requirements.Funder.str.contains(Funder,na=False)]
    data.reset_index(drop=True, inplace=True)
    df.to_csv("Industry_Email.csv",escapechar='\\', quoting=csv.QUOTE_MINIMAL)
    Business_Industry = 'Business Industry: ' + str(Business_Information_Normal(df,data['Industry_Keyword'][0],data['Industry_KeyLocation'][0],data['Industry_ValueColumn'][0],data['Industry_Value'][0],data['Industry_reqval'][0]))
    Business_Industry = str(Business_Industry).replace('Business Industry: ','')
    
    Business_Email = 'Business Email: ' + str(Business_Information_Normal(df,data['Email_Keyword'][0],data['Email_KeyLocation'][0],data['Email_ValueColumn'][0],data['Email_Value'][0],data['Email_reqval'][0]))
    Business_Email = str(Business_Email).replace('Business Email: ','')
    
    Business_Proceeds = 'Purpose: ' + str(Business_Information_Normal(df,data['Proceeds_Keyword'][0],data['Proceeds_KeyLocation'][0],data['Proceeds_ValueColumn'][0],data['Proceeds_Value'][0],data['Proceeds_reqval'][0]))
    Business_Proceeds = str(Business_Proceeds).replace('Purpose: ','')
    
    Loan_Amount = 'Amount Requested: ' + str(Business_Information_Normal(df,data['Amount_Keyword'][0],data['Amount_KeyLocation'][0],data['Amount_ValueColumn'][0],data['Amount_Value'][0],data['Amount_reqval'][0]))
    Loan_Amount= str(Loan_Amount).replace('Amount Requested: ','')        
    
    return Business_Industry, Business_Email, Business_Proceeds, Loan_Amount
    