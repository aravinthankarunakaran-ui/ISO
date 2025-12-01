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
import collections
import datetime as dti
from datetime import date
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
import ISO_Template as IST
import isoAPI as isoapi

workingdir = os.getcwd()
#directories of required files
dependencydir = os.path.join(workingdir , 'static/iso_dependencies/')
uploaddir = os.path.join (workingdir , 'static/uploads/')

# read line by line contents of a pdf using tika
def read_pdf(FileName):
    lines = []
    PDF_Parse = psr.from_file(FileName)
    lines.append(PDF_Parse ['content'])
    return lines

def decrypt_pdf(filename):
    print("filename",filename)
    pdfa = fitz.open(filename)
    pdfa.save(str(filename) + '_decrypted_statement.pdf', False)
    pdf = str(filename)+'_decrypted_statement.pdf'
    head, tail = os.path.split(filename)
    print('decrypt_file',[head],[tail])
    return pdf
    
def template_check(pdf):
    print("pdf",pdf)
    data = pd.read_csv(dependencydir + "Funder_list.csv",on_bad_lines="skip", encoding="cp1252", engine="python")
    contents = read_pdf(pdf)
    template = []
    contents[0] = contents[0].replace('\n',' ').replace('\xa0',' ')
    print(contents)
    for i in range(0,len(data)):
        if data['Keyword'][i] in contents[0]:
            template = data['Funder'][i]
            print('Code changes',template)
            if 'CapFront_Funding' in template and 'Federal Tax ID Number:' in contents[0]:
                template = '_Cap_Front'
            if 'Big_Think_capital_3' in template and ('Month 6' in contents[0]):#28/08
                template = 'Big_Think_Capital_2'
            if 'Lendefied_Inc_3' in template and 'Viewed' in contents[0]:#28/08
                template = 'Lendefied_Inc_2'
            # if 'Qualifier_LLC_1' in template and ('officer (individually' in contents[0] and 'Qualifier LLC("Q' in contents[0]):#28/08
            #     template = 'Qualifier LLC'
            if 'True Eagle Capital, LLC_1' in template and 'Business Name:' in contents[0]:
                template = 'True Eagle Capital, LLC_2'
            if 'Shore Funding' in template and 'Business Industry:' in contents[0]:   
                template = 'Shore_Funding_1'
            if 'Backd_Austin_1' in template and 'Professional Corporation' in contents[0]:
                template = 'Backd_Austin'
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
            if page.search_for(text[j]):
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
    keysearch = None
    Table_areas = None
    column_split = None
    row_tol = None
    for i in range(0,len(type_column)):
        if type_column['Table_Names'][i]==table:
            keysearch = type_column['keySearch'][i]
            Table_areas = type_column['Table_Area'][i]
            column_split = type_column['Column_Split'][i]
            row_tol = type_column['Row_Value'][i] # this row_tol is for alignment of rows which are improper.
    return keysearch,Table_areas,column_split,row_tol

#functions to extract tables for given column splits
#def table_extract(pdf,table,type_column):
#    keysearch,Table_areas,column_split,row_tol = type_search(table,type_column)
#    if keysearch!='Nil':
#        keysearch = ast.literal_eval(keysearch)
#        pagelist = findtextinpdf(pdf,keysearch)
#    else:
#        pagelist = []
#    tablelist = []
#    Table_areas = ast.literal_eval(Table_areas)
#    column_split = ast.literal_eval(column_split)
#    print(pagelist,Table_areas,column_split)
#    if len(pagelist)!=0:
#        for m in range(0,len(pagelist)):
#            if row_tol=='Nil':
#                tablelist.append(camelot.read_pdf(pdf,pages = str(pagelist[m]),flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split))
#            elif row_tol!='Nil':
#                row_tol = int(row_tol)
#                tablelist.append(camelot.read_pdf(pdf,pages = str(pagelist[m]),flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split,row_tol=row_tol))
#    else:
#        if row_tol=='Nil':
#            tablelist.append(camelot.read_pdf(pdf,pages = 'all',flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split))
#        elif row_tol!='Nil':
#            row_tol = int(row_tol)
#            tablelist.append(camelot.read_pdf(pdf,pages = 'all',flavor='stream',strip_text='\n',table_areas=Table_areas,columns=column_split,row_tol=row_tol))
#    tablelist = [j for i in tablelist for j in i]
#    return tablelist


def table_extract(pdf, table, type_column):
    keysearch, Table_areas, column_split, row_tol = type_search(table, type_column)

    # Handle `keysearch` appropriately
    if keysearch is None or keysearch == 'Nil':
        keysearch = None  # Set it to None or handle the logic here
        pagelist = findtextinpdf(pdf, keysearch) if keysearch else []
    else:
        try:
            # keysearch = ast.literal_eval(keysearch)
            keysearch = str(keysearch)
            pagelist = findtextinpdf(pdf, keysearch)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Invalid keysearch value: {keysearch}. Error: {e}")

    # Process Table_areas and column_split
    try:
        Table_areas = ast.literal_eval(Table_areas) if Table_areas else None
        column_split = ast.literal_eval(column_split) if column_split else None
    except (ValueError, SyntaxError) as e:
        raise ValueError(f"Error parsing Table_areas or column_split. Error: {e}")

    print(pagelist, Table_areas, column_split)
    tablelist = []
    if pagelist:
        for m in pagelist:
            if row_tol is None or row_tol == 'Nil':
                tablelist.append(camelot.read_pdf(pdf, pages=str(m), flavor='stream', strip_text='\n', 
                                                  table_areas=Table_areas, columns=column_split))
            else:
                try:
                    row_tol = int(row_tol)
                    tablelist.append(camelot.read_pdf(pdf, pages=str(m), flavor='stream', strip_text='\n', 
                                                      table_areas=Table_areas, columns=column_split, row_tol=row_tol))
                except ValueError:
                    raise ValueError(f"Invalid row_tol value: {row_tol}")
    else:
        if row_tol is None or row_tol == 'Nil':
            tablelist.append(camelot.read_pdf(pdf, pages='all', flavor='stream', strip_text='\n', 
                                              table_areas=Table_areas, columns=column_split))
        else:
            try:
                row_tol = int(row_tol)
                tablelist.append(camelot.read_pdf(pdf, pages='all', flavor='stream', strip_text='\n', 
                                                  table_areas=Table_areas, columns=column_split, row_tol=row_tol))
            except ValueError:
                raise ValueError(f"Invalid row_tol value: {row_tol}")

    # Flatten the list of tables and return
    tablelist = [j for i in tablelist for j in i]
    return tablelist

def exact_df(bank_found):  
    ispresent_start = bank_found['Start_1'][0]
    ispresent_col_start = bank_found['Start_Col_1'][0]
    ispresent_end = bank_found['End_1'][0]
    ispresent_col_end = bank_found['End_Col_1'][0]
    return ispresent_start, ispresent_col_start,ispresent_end, ispresent_col_end

def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def extract_table_content(table,table_name,type_column):
    type_column = type_column[type_column.Table_Names.str.contains(table_name,na=False)]
    type_column.reset_index(drop=True, inplace=True) 
    print("table",table)
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
    #to fill empty cells in a dataframe with a different value/string('ENOAH')
    def convert_fill(df):
        df = df.stack()
        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df.fillna('ENOAH', inplace=True)
        df = df.unstack()
        return df
    frames = convert_fill(frames)
    return frames

def Funding_Family_Buss(df,keyword,column_name):
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if str(required_field).strip() == '' or str(required_field).strip() == ':' or str(required_field).strip() == '#:':
                required_field = df[column_name][i+1]
                if keyword == 'Federal State Tax' and ('Use of Funds' in str(df[column_name][i+1]) or 'ENOAH' in str(df[column_name][i+1])):
                    required_field = df[column_name][i+2]
                elif keyword == 'Business Legal Name' and 'ENOAH' in str(df[column_name][i+1]):
                    required_field = df['Col_2'][i+1]
            if keyword == 'Street Address' and required_field == 'ENOAH':
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip() == '' or str(required_field).strip() == ':':
                    required_field = df['Col_2'][i+1]
            elif required_field == 'ENOAH' or str(required_field).strip() == '' :
                required_field = df[column_name][i+2]
            break
    if keyword == 'Business DBA Name' and 'City' in str(required_field):
        required_field = 'ENOAH'
    if keyword == 'City' and 'Phone' in str(required_field):
        required_field = 'ENOAH'
    required_field = str(required_field).replace('#','').replace(':','')
    return required_field

def SBG_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('sbg11.csv')
    
    for i in range(0,len(df)):
        if 'Federal  Tax' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('  ',' ')
        if str(keyword) in str(df[column_name][i]):
            mystring = df[column_name][i]
            keyword = keyword
            before_keyword, keyword, after_keyword = mystring.partition(keyword)
            required_field = after_keyword.strip().replace('Name:','').replace('Date','')
            print('royals',keyword, required_field)
            if keyword == 'Legal' and not 'DBA' in df[column_name][i+1] and not 'ENOAH' in df[column_name][i+1]:
                required_field = str(required_field) + ' ' + str(df[column_name][i+1])
            if keyword == 'Legal' and not 'DBA' in df[column_name][i+2] and 'ENOAH' in df[column_name][i+1] and not 'ENOAH' in df[column_name][i+2]:
                required_field = str(required_field) + ' ' + str(df[column_name][i+2])
            if req_val == 'Name' and 'Business Start Date:' in required_field:
                required_field = required_field.split('Business Start Date:')[0]
            break
    
        
    if req_val == 'StartDate' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Date:' in str(df['Col_1'][i]):
                mystring = str(df['Col_1'][i]).replace('  ',' ')
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
    return required_field

def SBG_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('sbg12.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace("Primary",'').replace('Secondary','').replace("Owner's  First  Name:",'').replace("Owner's  Last  Name:",'').replace("Owner's First Name:",'').replace("Owner's Last Name:",'').replace('Date  of','').replace('Date of','').replace("Phone Number:",'').replace("Phone  Number:",'').replace(keyword,'').strip()
            break
    if req_val == 'Owner' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if '% 0wnership:' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i].replace('% 0wnership:','')
    required_field = str(required_field)
    return required_field

def Innovative_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name:':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field.strip() == '':
                    required_field = df['Col_4'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i-1]
                break
            elif keyword == 'Federal Tax ID:' or keyword == 'Business Start Date:' or keyword == 'Business DBA:':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = before_keyword.strip()
                if keyword == 'Business DBA:' and required_field == '':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                break
            elif keyword == 'Business Address:' and df[column_name][i-1] == 'ENOAH':
                required_field = df['Col_2'][i-1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                break
    return required_field

def Innovative_Funding_Owner(df,owner_status,keyword,column_name,req_value):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in df[column_name][i]:
            if keyword == "Address:":
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                break
            else:
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = before_keyword.strip()
                if required_field == '':
                    required_field = df[column_name][i-1]
                break
    if req_value == 'City1':
        required_field = df[column_name][i].replace(keyword,'').strip()
    if req_value == 'State1':
        required_field = df['Col_2'][i]
    if req_value == 'Zip_Code1':
        required_field = df['Col_3'][i]
    if req_value == 'Email1':
        required_field = 'ENOAH'
    if req_value == 'First_Name1':
        required_field = df[column_name][i].replace(keyword,'').strip()
    if req_value == 'Last_Name1':
        required_field = df['Col_2'][i]
    return required_field

def Advance_Funding_Buss_1(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal / Corporate Name' or keyword == 'Doing Business As' or keyword == 'Business Start Date' or keyword == 'Phone':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+2]
                break
    return required_field

def Premium_Funding_Buss(df,keyword,column_name):
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]) and not 'State of Incorporation' in str(df[column_name][i]):
            if keyword == 'Legal Company Name' or keyword == 'Doing Business As':
                if df['Col_2'][i] == 'ENOAH' and df['Col_2'][i-1]!='ENOAH' and (df['Col_2'][i+1]!='Doing Business As' and df['Col_2'][i+1]!='Company Website' and df['Col_1'][i+1]!='Company Website' and 'https:' not in str(df['Col_2'][i+1])):
                   required_field = str(df['Col_2'][i-1]) + ' '+ str(df['Col_2'][i+1])
                   break
                else:
                   required_field = df['Col_2'][i]
                   break
            else:
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                break
    if keyword == 'Legal Company Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Company' in str(df['Col_1'][i]) and 'Legal' in str(df['Col_1'][i-1]):
                required_field = df['Col_2'][i]
                break
    if keyword == 'Doing Business As' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business As' in str(df['Col_1'][i])  and 'Doing' in str(df['Col_1'][i-2]):
                required_field = df['Col_2'][i-1]
                break
    if keyword == 'Business Start Date' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Start Date' in str(df['Col_1'][i])  and 'Business' in str(df['Col_1'][i-2]):
                required_field = df['Col_2'][i-1]
                break

    return required_field

def ROK_Financial_Buss_1(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'BUSINESS LEGAL NAME:':
                required_field = df[column_name][i+2]
                break
            elif keyword == 'BUSINESS START DATE: (UNDER CURRENT OWNERSHIP)':
                required_field = df['Col_3'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Quick_Bridge_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Tax ID' or keyword == 'Business Start Date':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business DBA Name' or keyword == 'Physical Location Phone':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
            if keyword == 'Business DBA Name' and re.search('[0-9]',str(required_field)):
                required_field='ENOAH'
                break
            elif keyword == 'Business Legal Name':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    break
            elif keyword == 'Zip':
                required_field = df['Col_2'][i+1]
                break
            else:
                if df['Col_2'][i]!='ENOAH':
                    required_field = df['Col_2'][i]
                    break
                else:
                    required_field = df['Col_2'][i-1]
                    break
    return required_field

def Quick_Bridge_Funding_Owner(df,owner_status,keyword,column_name):
    required_field = 'ENOAH'
    if keyword == 'Mobile' or keyword == 'Email':
        required_field = 'ENOAH'
    if keyword == 'Ownership':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'First Name' or keyword == 'Last Name' or keyword == 'Date of Birth' or keyword == 'SS#':
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                break
            else:
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                break
    return required_field

def Fora_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'

    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):   
            if keyword == 'Business Legal Name:':
                if str(df[column_name][i]).replace(keyword,'')=='':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                    break                
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
            elif keyword == 'Business Start Date (MM/YYYY):':
                if str(df[column_name][i]).replace(keyword,'')=='':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i]
                        break
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+1]
                        break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break                    
            else:
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
                if required_field=='':
                    required_field = df[column_name][i+1]
                    if keyword == 'Address:' and 'City:' in required_field:
                        required_field = df[column_name][i-1] 
            if keyword == 'State:' and len(required_field)>2:
                required_field = df['Col_1'][i-1]
    return required_field

def Direct_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Start Date:' and df[column_name][i+1] == 'ENOAH':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            elif keyword == 'Business Legal Name' or keyword == 'Business DBA Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH' and not 'Business Phone #:' in str(df[column_name][i+2]):
                        required_field = str(df[column_name][i+2]).replace('Business Email:','')
                    break
            elif keyword == 'Business Street':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i+1]
                    break
            elif keyword == 'Business State:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i+2]
                    break
            elif keyword == 'Business Zip:' or keyword == 'Federal Tax ID:' or keyword == 'Business Start Date:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    break
            else:
                required_field = df[column_name][i+1]
                break
    if keyword == 'Business Start Date:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Business Start Date:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i+1]
                break
    return required_field

def Direct_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                if required_field== 'ENOAH' and (keyword == 'First Name:' or keyword == 'Last Name:' or keyword == 'Home Address:' or keyword == 'Date of Birth:' or keyword == 'Ownership %:' or keyword == 'Zip:'):
                    required_field = df[column_name][i+2]
                    break
            else:
                required_field = df[column_name][i+1]
                if required_field== 'ENOAH' and (keyword == 'First Name:' or keyword == 'Home Address:' or keyword == 'Zip:'):
                    required_field = df[column_name][i+2]
                elif keyword == 'Ownership %:'and  required_field =='ENOAH':
                    required_field = df['Col_2'][i+2]
                    break
    return required_field

def BigThink_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Federal Tax ID' or keyword == 'Business Start Date':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = before_keyword.strip()
                if required_field=='':
                    required_field = df[column_name][i-1]
                break
            elif keyword == 'Address':
                required_field = df['Col_3'][i]
                break
            else:
                if str(df[column_name][i]).replace(keyword,'')=='':
                    required_field = df[column_name][i-1]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                if keyword == 'hone' or keyword == 'ity':
                    required_field = required_field[1:]
    return required_field

def Lendio_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'CITY, STATE, ZIP' and req_val == 'City':
                value = df[column_name][i+1]
                m = value.split()
                required_field = ' '.join(m[0:len(m)-2])
                break
            elif keyword == 'CITY, STATE, ZIP' and req_val == 'Zip':
                value = df[column_name][i+1]
                required_field = re.findall('\d+', value)[0]
                break
            elif keyword == 'CITY, STATE, ZIP' and req_val == 'State':
                value = df[column_name][i+1]
                n = value.split()
                required_field = n[len(n)-2]
                required_field = re.sub('[0-9]',  '', required_field).strip()
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'LEGAL NAME' and not 'STREET ADDRESS' in df[column_name][i+2]:
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                if keyword == 'DBA NAME' and not 'SUITE / FLOOR' in df[column_name][i+2] and (df[column_name][i+1]!='ENOAH' and 'SUITE / FLOOR' not in df[column_name][i+1]):
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                break
    return required_field

def Funding_Family_Owner(df,owner_status,keyword,column_name,req_value):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Owner/Principal #1' or keyword == 'Phone' or keyword == 'Email' or keyword == 'Home Address:' or keyword == 'City, State, Zip:':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH' and df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH' and (df[column_name][i+1]!='ENOAH' and keyword == 'City, State, Zip:' and not 'Phone' in str(df[column_name][i+1])):
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'Date of Birth:' or keyword == '% of Ownership:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]    
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]                  
                    break
                elif keyword == 'SSN#':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field.strip() == '' or required_field.strip() == ':':
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i+1]
                    break
            if owner_status == 'Owner #2':
                if keyword == 'Owner/Principal #2' or keyword == 'Phone' or keyword == 'Email' or keyword == 'Home Address:' or keyword == 'City, State, Zip:':
                    required_field = df['Col_4'][i]
                    if required_field == 'ENOAH' and df['Col_4'][i+1]!='ENOAH':
                        required_field = df['Col_4'][i+1]
                    if required_field == 'ENOAH' and (df[column_name][i+1]!='ENOAH' and keyword == 'Owner/Principal #2' and not 'Home Address' in str(df[column_name][i+1])):
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and keyword == 'City, State, Zip:':
                        required_field = df['Col_4'][i-1]
                    break
                elif keyword == 'Date of Birth:' or keyword == '% of Ownership:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]    
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2] 
                    break
                elif keyword == 'SSN#':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
    if req_value == 'City1' or req_value == 'City2':
        if ',' in str(required_field):
            result = str(required_field).split(',') 
        else:
            result = str(required_field).split() 
        if len(result)>1:
            required_field = result[0]
    if req_value == 'State1' or req_value == 'State2':
        if ',' in str(required_field):
            result = str(required_field).split(',') 
        else:
            result = str(required_field).split() 
        if len(result)>1:
            required_field = re.sub('[0-9]',  ' ', str(result[1]))        
    if req_value == 'Zip1' or req_value == 'Zip2':
        if ',' in str(required_field):
            result = str(required_field).split(',') 
        else:
            result = str(required_field).split() 
        if len(result)>1 and len(result)==3:
            if re.findall('\d+', str(result[2]))!=[]:
                required_field = re.findall('\d+', str(result[2]))[0]
        if len(result)>1 and len(result)==2:
            if re.findall('\d+', str(result[1]))!=[]:
                required_field = re.findall('\d+', str(result[1]))[0]
    return required_field

def Premium_Merchant_Funding_Owner(df,owner_status,keyword,column_name,req_value):
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in df[column_name][i]:
            if owner_status == 'Owner #1':
                required_field = df['Col_4'][i]
                if required_field == 'ENOAH' and 'FICO Score' not in str(df['Col_3'][i-1]):
                    required_field = df['Col_4'][i-1]
                    if required_field == 'ENOAH' and 'FICO Score' not in str(df['Col_3'][i-2]):
                        required_field = df['Col_4'][i-2]
                if keyword == 'Social Security' and df[column_name][i+2] == 'Number':
                    required_field = df['Col_4'][i+1]
                break
            else:
                required_field = df['Col_4'][i]
    if req_value == 'City1' or req_value == 'City2':
        required_field = ''.join(required_field.split())
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if len(result)>1:
            n = len(result)
            required_field = result[n-1]
    if req_value == 'State1' or req_value == 'State2':
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if result!=[]:
            required_field = result[0]
    if req_value == 'Zip_Code1' or req_value == 'Zip_Code2':
        result = re.findall('\d+', str(required_field))
        if result!=[]:
            required_field = result[0]
    return required_field

def Business_Capital_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'CITY' or keyword == 'ZIP':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
            elif keyword == 'STREET' or keyword == 'STATE':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                if keyword == 'STREET' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if keyword == 'STREET' and required_field == 'Business Address':
                    required_field = df['Col_2'][i]
            elif keyword == 'Business Legal Name':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                break
    if keyword == 'Business DBA Name' and ('https:' or 'Legal Entity') in str(required_field):
        required_field = 'ENOAH'
    return required_field

def Business_Capital_Funding_Owner(df,owner_status,keyword,column_name,req_value):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == "CITY" or keyword == "ZIP":
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    break
                elif keyword == "STATE" or keyword == "STREET":
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break
            else:
                if keyword == "CITY" or keyword == "ZIP":
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i]
                elif keyword == "STATE" or keyword == "STREET":
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
    if req_value == 'City1' or req_value == 'City2':
        required_field = ''.join(required_field.split())
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if len(result)>1:
            n = len(result)
            required_field = result[n-1]
    if req_value == 'State1' or req_value == 'State2':
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if result!=[]:
            required_field = result[0]
    if req_value == 'Zip_Code1' or req_value == 'Zip_Code2':
        result = re.findall('\d+', str(required_field))
        if result!=[]:
            required_field = result[0]
    required_field = str(required_field).replace('Address','')
    return required_field

def Shore_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
  
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Zip Code:':
                value = df[column_name][i]
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                    if value == 'ENOAH' and ('-3' in str(df[column_name][i+2]) or '-9' in str(df[column_name][i+2])):
                        required_field = str(df[column_name][i+1]).replace('4205','34205').replace('3166','33166').replace('3635','93635')
                    elif value =='ENOAH' and 'Zip Code' in str(df[column_name][i+2]):
                        required_field = str(df[column_name][i+1])
                break
            elif keyword == 'Federal Tax ID:':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field =='':
                    required_field == df[column_name][i+1]
                    if required_field == 'ENOAH' or required_field =='':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '' and i + 1 < len(df):
                    required_field = df[column_name][i+1]
                    if keyword =='Business Legal Name:' and 'Legal Entity' in df[column_name][i+1]:
                        required_field = df[column_name][i-1] 
                    if keyword == 'Doing Business As:' and required_field == 'ENOAH':
                        required_field = df['Col_3'][i]
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i-1]).replace('Funding Application','')
                            if required_field == '':
                                required_field = df['Col_3'][i+1] 
                    
                    if keyword == 'Billing Address:':
                        
                        required_field = df[column_name][i-1]
                break
    return required_field

def Shore_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
  
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Zip Code:':
                value = df[column_name][i]
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                    if value == 'ENOAH':
                        value = str(df[column_name][i+1])
                        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').replace('Title:','').replace('Social Security #:','').strip()
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if keyword == 'Home Address:' and required_field == 'ENOAH' or 'Rent:' in required_field:
                        required_field = df[column_name][i-1]
                    if (keyword == 'Date of Birth:' or keyword =='Social Security #:') and required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                    if (keyword == 'Name:' and ('Home Address:' in required_field or required_field == 'ENOAH')) or (keyword == '% of Ownership' and ('City:' in required_field or not re.search('[0-9]',str(required_field)))):
                        required_field = df[column_name][i-1]                                                                                         
                break
        if keyword == 'Social Security #:' and required_field == 'ENOAH':
            for i in range (0,len(df)):
                if 'Social Security #:' in str(df['Col_3'][i]):
                    required_field = df['Col_4'][i]
                    break
                
    return required_field

def ROK_Financial_Funding_Owner_1(df,owner_status,keyword,column_name,req_value):
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == "HOME STREET ADDRESS:":
                required_field = df['Col_2'][i+1]
                break
            elif keyword == "EMAIL:":
                required_field = df['Col_4'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    if req_value == 'City1' or req_value == 'City2':
        required_field = ''.join(required_field.split())
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if len(result)>1:
            n = len(result)
            required_field = result[n-1]
    if req_value == 'State1' or req_value == 'State2':
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        required_field = result[0]
    if req_value == 'Zip_Code1' or req_value == 'Zip_Code2':
        result = re.findall('\d+', str(required_field))
        required_field = result[0]
    return required_field

def National_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Tax ID' or keyword == 'Business Start Date':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Physical Location Address' and req_val == 'City':
                value = df['Col_2'][i+1]
                required_field = value.split(',')[0]
                break
            elif keyword == 'Physical Location Address' and req_val == 'Zip':
                value = df['Col_2'][i+1]
                required_field = re.findall('\d+', value)[0]
                break
            elif keyword == 'Physical Location Address' and req_val == 'State':
                value = df['Col_2'][i+1]
                required_field = value.split(',')[1]
                required_field = re.sub('[0-9]',  '', required_field).strip()
                break
            else:
                required_field = df['Col_2'][i]
                break
    return required_field


def National_Funding_Owner(df,owner_status,keyword,column_name,req_val):
#     keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Mobile' or keyword == 'Email':
        required_field = 'ENOAH'
    if keyword == 'Ownership':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address' and req_val == 'City':
                value = df['Col_2'][i+1]
                required_field = value.split(',')[0]
                break
            elif keyword == 'Home Address' and req_val == 'Zip':
                value = df['Col_2'][i+1]
                required_field = re.findall('\d+', value)[0]
                break
            elif keyword == 'Home Address' and req_val == 'State':
                value = df['Col_2'][i+1]
                required_field = value.split(',')[1]
                required_field = re.sub('[0-9]',  '', required_field).strip()
                break
            elif keyword == 'First Name' or keyword == 'Date Of Birth':
                required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Last Name':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i]).replace(keyword,'')
            else:
                required_field = df['Col_2'][i]
                break
    return required_field

def Fora_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Date of Birth:' or keyword == 'Home Address:' or keyword == 'Mobile:' or keyword == 'Percent Ownership:' or keyword == 'Name:':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if keyword == 'Name:' and 'Date' in required_field:
                        required_field = required_field = before_keyword.strip()
                if keyword == 'Home Address:' and owner_status == 'Owner #1' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                elif keyword == 'Home Address:' and owner_status == 'Owner #2' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i+1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if (req_val == 'Zip_Code1' and 'Email' in str(df[column_name][i+1]) and required_field =='') or (req_val == 'Social_Security1' and required_field == ''):
                    required_field  = str(df[column_name][i-1])
                break
    return required_field

def BigThink_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'ity' or keyword == 'State' or keyword == 'Zip' or keyword == 'SSN':
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    required_field = before_keyword.strip()
                    if required_field == '' or required_field == 'C':
                        required_field = df[column_name][i-1]
                    break
                elif keyword == 'Name':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df['Col_2'][i]
                        break
                else:
                    required_field = df['Col_2'][i]
                    break
        if owner_status == 'Owner #2':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'ity' or keyword == 'State' or keyword == 'Zip' or keyword == 'SSN':
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    required_field = before_keyword.strip()
                    break
                else:
                    required_field = df['Col_5'][i]
    return required_field

def Lendio_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'CITY, STATE, ZIP' and req_val == 'City':
                    value = df[column_name][i+1]
                    if len(value.split())>1:
                        m = value.split()
                        required_field = ' '.join(m[0:len(m)-2])
                    break
                elif keyword == 'CITY, STATE, ZIP' and req_val == 'Zip':
                    value = df[column_name][i+1]
                    if len(value.split())>1:
                        required_field = re.findall('\d+', value)[0]
                    break
                elif keyword == 'CITY, STATE, ZIP' and req_val == 'State':
                    value = df[column_name][i+1]
                    if len(value.split())>1:
                        n = value.split()
                        required_field = n[len(n)-2]
                        required_field = re.sub('[0-9]',  '', required_field).strip()
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
                if keyword == 'CITY, STATE, ZIP' and req_val == 'City':
                    if not 'signer' in df[column_name][i+1]:
                        value = df[column_name][i+1]
                    else:
                        value = df[column_name][i+2]
                    if len(value.split())>1:
                        m = value.split()
                        required_field = ' '.join(m[0:len(m)-2])
                elif keyword == 'CITY, STATE, ZIP' and req_val == 'Zip':
                    if not 'signer' in df[column_name][i+1]:
                        value = df[column_name][i+1]
                    else:
                        value = df[column_name][i+2]
                    if len(value.split())>1:
                        required_field = re.findall('\d+', value)[0]
                elif keyword == 'CITY, STATE, ZIP' and req_val == 'State':
                    if not 'signer' in df[column_name][i+1]:
                        value = df[column_name][i+1]
                    else:
                        value = df[column_name][i+2]
                    if len(value.split())>1:
                        n = value.split()
                        required_field = n[len(n)-2]
                        required_field = re.sub('[0-9]',  '', required_field).strip()
                else:
                    required_field = df[column_name][i+1]
                    if 'signer' in str(required_field):
                        required_field = df[column_name][i+2]
    return required_field

def ViewRidge_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('vr.csv')
    if req_val == 'Phone':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'State:Zipcode:' and req_val == 'Zip':
                value = df[column_name][i+1]
                required_field = re.findall('\d+', value)[0]
                break
            elif keyword == 'State:Zipcode:' and req_val == 'State':
                value = df[column_name][i+1]
                required_field = re.sub('[0-9]', '', value).strip()
                break
            elif req_val == 'DBA':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1]
                break
            elif req_val == 'Tax':
                required_field = df[column_name][i+1]
                print('seaborn',required_field)
                if required_field == 'ENOAH':
                    # required_field = str(df[column_name][i+2]).replace('ID#: ','')[0:10]
                    required_field = str(df[column_name][i+2])

                    
                elif required_field == 'Business Federal Tax ID#:' and '.com' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+3]
                elif required_field == 'Tax ID#:':#05/06
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]

                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    if keyword == 'Websit' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if keyword in str(df['Col_2'][i]):
                required_field = df['Col_1'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_1'][i+2][0:10]
                break
    return required_field

def ViewRidge_Funding_Owner(df,owner_status,keyword,column_name,req_val):#18/06
    required_field = 'ENOAH'
    df.to_csv('VRO.csv')
    if keyword == 'Email' or keyword == 'PERCENTAGE OF OWNERSHIP':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if req_val == 'Zip' and required_field == 'ENOAH':
                print('poo11122',required_field)
                required_field = df['Col_5'][i+1]
                if 'State:' in str(required_field):
                    required_field = df['Col_6'][i+2]
                print('poo1111',required_field)
            if keyword == 'Last Name:' and required_field == 'ENOAH':
                required_field = df['Col_2'][i+2]            
            break
    if req_val == 'SSN' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'ss#:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2] 
    if req_val == 'Date Of Birth' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Date of Birth' in str(df['Col_6'][i]):
                required_field = df['Col_5'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_6'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_6'][i+2]
                        if required_field == 'ENOAH':
                            required_field = df['Col_5'][i+2]
            if 'Date of Birth' in str(df['Col_5'][i]):
                required_field = df['Col_5'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_5'][i+2]
    if req_val == 'State' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'State:' in str(df['Col_5'][i]):
                required_field = df['Col_4'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_5'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i+2]
    if req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Home Address:' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_1'][i+2]
    return required_field

def Coast_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    break
            elif keyword == 'Business Address:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and 'Business Property Type:' not in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                elif required_field == 'ENOAH' and 'Business Property Type:' in df[column_name][i+2]:
                    required_field = df['Col_2'][i+1]
                    break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    return required_field

def Coast_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Owner SSN:' or keyword == 'Owner Date of Birth:':
                required_field = df[column_name][i+2]
                break
            elif keyword == 'Mobile Phone:Email:' and req_val == 'Mobile':
                required_field = df[column_name][i+2]
                break       
            elif keyword == 'Mobile Phone:Email:' and req_val == 'Email':
                required_field = df['Col_4'][i+1]
                break 
            else:
                required_field = df[column_name][i+1]
                if req_val == 'Home Address' and required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i+1])
                break                  
    return required_field
### changed normal to template format
# def National_Business_Funding_Buss(df,keyword,column_name,req_val):
#     keyword = keyword.strip()
#     required_field = 'ENOAH'
#     for i in range(0,len(df)):
#         if str(keyword) in str(df[column_name][i]):
#             required_field = df[column_name][i+1]
#             if required_field == 'ENOAH':
#                 required_field = df[column_name][i+2]
#             break
#     if req_val == 'Tax' and required_field == 'ENOAH':
#         for i in range(0,len(df)):
#             if 'BUSINESS FEDERAL TAX ID #:' in str(df['Col_2'][i]):
#                 required_field = str(df['Col_2'][i+1])
#                 if required_field == 'ENOAH' and not 'WEBSITE' in str(df['Col_2'][i+2]):
#                     required_field = df['Col_2'][i+2]
#                     if required_field == 'ENOAH':
#                         required_field = df['Col_1'][i+2]
#                 elif required_field == 'ENOAH' and 'WEBSITE' in str(df['Col_2'][i+2]):
#                     required_field = df['Col_1'][i+2]
#                     if 'BUSINESS' in required_field:
#                         required_field = df['Col_1'][i+1]
#                         if len(required_field)<=10:
#                             required_field = required_field
#                         else:
#                             required_field = required_field.split('-')
#                             required_field = required_field[0][-2:]+'-'+required_field[1]
#                 break
#     required_field = str(required_field).replace('BUSINESS INFO','')
#     return required_field
    
# def National_Business_Funding_Owner(df,owner_status,keyword,column_name,req_val):
#     keyword = keyword.strip()
#     if keyword == 'Ownership %:':
#         required_field = 'ENOAH'
#     required_field ='ENOAH'
#     for i in range(0,len(df)):
#         if str(keyword) in str(df[column_name][i]):
#             if keyword == 'LEGAL FIRST NAME:':
#                 required_field = df[column_name][i+1]
#                 required_field = str(required_field).replace('OWNER INFO','')
#                 if required_field == '' or required_field == 'ENOAH':
#                     required_field = df[column_name][i+2]
#                     required_field = str(required_field).replace('OWNER INFO','')
#                 break
#             else:
#                 required_field = df[column_name][i+1]
#                 if required_field == 'ENOAH':
#                     required_field = df[column_name][i+2]
#                     if required_field =='ENOAH' and req_val =='Email':
#                         required_field = df['Col_4'][i+1]
#                 if required_field == 'CITY:' and keyword == 'HOME STREET ADDRESS:':
#                     required_field = df['Col_2'][i+1]
#                 break                  
#     return required_field

def BusinessLoan_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Physical Address' and req_val == 'Zip':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-1]
                break
            elif keyword == 'Physical Address' and req_val == 'State':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-2]
                break
            elif keyword == 'Physical Address' and req_val == 'City':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-3]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field
def BusinessLoan_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'EMAIL:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address' and req_val == 'Zip':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-1]
                break
            elif keyword == 'Home Address' and req_val == 'State':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-2]
                break
            elif keyword == 'Home Address' and req_val == 'City':
                value = df[column_name][i+1].split(',')
                n = len(value)
                required_field = value[n-3]
                break
            else:
                required_field = df[column_name][i+1]
                break                
    return required_field

def Prime_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Business Phone':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Name:' or keyword == 'Street Address:':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                break
            elif keyword == 'DBA Name:' or keyword == 'EIN Number:':
                required_field = df['Col_4'][i]
                break
            elif keyword == 'Business Start Date:' and req_val == 'Date':
                required_field = df['Col_2'][i-1]
                break
            elif keyword == 'City/State/Zip:' and req_val == 'Zip':
                value = df['Col_4'][i]
                required_field = value.split()[-1]
                if required_field == 'ENOAH':
                    value = df['Col_4'][i-1]
                    required_field = value.split()[-1]
                break
            elif keyword == 'City/State/Zip:' and req_val == 'City':
                required_field = df['Col_4'][i]
                if ',' in required_field:
                    value = df['Col_4'][i]
                    required_field = value.split(',')[0]
                elif not ',' in required_field:
                    value = df['Col_4'][i]
                    required_field = value.split(' ')[0]
                break
            elif keyword == 'City/State/Zip:' and req_val == 'State':
                value = df['Col_4'][i]
                if not re.search('[0-9]',str(value)):
                    required_field =value.split(' ')[1]
                break
            else:
                required_field = df['Col_4'][i]
    return required_field

def Prime_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'EMAIL:' or keyword == 'Home Phone':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Full Name:' or keyword == 'Street Address:' or keyword == 'Ownership % :':
                    required_field = df['Col_2'][i]
                    break
                elif keyword == 'Date of Birth:':
                    required_field = df['Col_4'][i]
                    break
                elif keyword == 'SSN:':
                    required_field = df[column_name][i+1]
                    break
                elif keyword == 'City/State/Zip:' and req_val == 'Zip':
                    value = df['Col_4'][i]
                    required_field = value.split()[-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
    #                     required_field = value.split()[-1]
                    break
                elif keyword == 'City/State/Zip:' and req_val == 'City':
                    required_field = df['Col_4'][i]
                    if ',' in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split(',')[0]
                    elif not ',' in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split(' ')[0]
                    break
                elif keyword == 'City/State/Zip:' and req_val == 'State':
                    required_field = df['Col_4'][i]
                    if ',' not in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split()[1]
                    break
                else:
                    required_field = df[column_name][i]

            elif owner_status == 'Owner #2':
                if keyword == 'Full Name:' or keyword == 'Street Address:' or keyword == 'Ownership % :':
                    required_field = df['Col_2'][i]
                elif keyword == 'Date of Birth:':
                    required_field = df['Col_4'][i]
                elif keyword == 'City/State/Zip:' and req_val == 'Zip':
                    value = df['Col_4'][i]
                    required_field = value.split()[-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
    #                     required_field = value.split()[-1]
                elif keyword == 'City/State/Zip:' and req_val == 'City':
                    required_field = df['Col_4'][i]
                    if ',' in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split(',')[0]
                    elif not ',' in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split(' ')[0]
                elif keyword == 'City/State/Zip:' and req_val == 'State':
                    required_field = df['Col_4'][i]
                    if ',' not in required_field:
                        value = df['Col_4'][i]
                        required_field = value.split()[1]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
    return required_field

def Lion_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Company Name:' or keyword == 'DBA (if applicable):' or keyword == 'Company Physical Address:':
                required_field = df['Col_2'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Lion_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Ownership % :' and owner_status == 'Owner #1':
        required_field = 'ENOAH'
    if keyword == 'Ownership % :' and owner_status == 'Owner #2':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address:' and req_val == 'Zip':
                value = df[column_name][i].split()
                n = len(value)
                required_field = value[n-1]
                break
            elif keyword == 'Home Address:' and req_val == 'State':
                value = df[column_name][i].split()
                m = len(value)
                required_field = value[m-2]
                break
            elif keyword == 'Home Address:' and req_val == 'City':
                value = df[column_name][i].split()
                r = len(value)
                required_field = ' '.join(value[r-4:r-2])
                break
            else:
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
                break   
    return required_field

def Approved_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Federal Tax ID:':
                required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+4]
                break
            elif keyword == 'Physical Street Address:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and not 'Billing Street Address' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and 'Billing Street Address' in df[column_name][i+2] and df['Col_2'][i+1]!='ENOAH':
                    required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business Legal Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and not 'Type of Business Entity' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and 'Type of Business Entity' in df[column_name][i+2] and df['Col_2'][i+1]!='ENOAH':
                    required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business DBA Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df['Col_6'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'current Ownership:' and required_field == 'ENOAH':
                    required_field = df['Col_5'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    if keyword == 'City:' and 'City:' in required_field:
        required_field = 'ENOAH'
    return required_field

def Approved_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Ownership % :':
        required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if keyword == 'SS#:' and required_field == 'ENOAH':
                required_field = df['Col_4'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
            if required_field == 'ENOAH':
                required_field = df[column_name][i+2]
            break
    if keyword == 'First Name:':
        required_field = str(required_field).replace('Le','LeRoy')
    required_field = str(required_field).replace('AUTHORIZATIONS','')
    return required_field

def Platform_Funding_Buss_1(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Physical Street Address:':
                required_field = df['Col_3'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field
def Platform_Funding_Owner_1(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Ownership % :':
        required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break   
    return required_field

def Rapid_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]) and column_name == 'Col_1':
            required_field = df['Col_2'][i]
            print('jijijiji',required_field)
            if keyword == 'Company DBA:' and df['Col_2'][i+1]!='ENOAH' and not 'Entity type' in df[column_name][i+1]:
                required_field = df['Col_2'][i] + ' ' + df['Col_2'][i+1]
            if keyword == 'Company legal name:' and df['Col_2'][i+1]!='ENOAH' and df['Col_2'][i+2]!='ENOAH' and not 'Company DBA' in df[column_name][i+2]:
                required_field = df['Col_2'][i] + ' ' + df['Col_2'][i+1] + ' ' + df['Col_2'][i+2]
            elif keyword == 'Company legal name:' and df['Col_2'][i+1]!='ENOAH' and not 'Company DBA' in df[column_name][i+1]:
                required_field = df['Col_2'][i] + ' ' + df['Col_2'][i+1]
            break
        if str(keyword) in str(df[column_name][i]) and column_name == 'Col_3':
            required_field = df['Col_4'][i]
            break
    return required_field

def Rapid_Funding_Owner(df,owner_status,keyword,column_name,req_val):#05/06
    
    for i in range(0,len(df)):
        
        if owner_status == 'Owner #1':
            print('longlive',keyword, df['Col_2'][i],df['Col_2'][i+1])
            if keyword == 'First name:' and df['Col_2'][i] == 'ENOAH':
                required_field = df['Col_2'][i+1]
                if keyword == 'First name:' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                    break
            if str(keyword) in str(df[column_name][i]) and column_name == 'Col_1':
                print('hello...',str(keyword),df['Col_3'][i],df['Col_2'][i],df['Col_2'][i+1])
                required_field = df['Col_2'][i]
                break
            
            # if df['Col_1'][i] == 'Last name:' and df['Col_2'][i] == 'ENOAH':
            #     required_field = df['Col_2'][i-1]
            #     break
            # if keyword == 'Address:' and df['Col_4'][i] == 'ENOAH':
            #     required_field = df['Col_4'][i+1]
            #     if keyword == 'First name:' and required_field == 'ENOAH':
            #         required_field = df['Col_4'][i-1]
            #         break
            if str(keyword) in str(df[column_name][i]) and column_name == 'Col_3':
                required_field = df['Col_4'][i]
                break 
        else:
            if str(keyword) in str(df[column_name][i]) and column_name == 'Col_1':
                required_field = df['Col_2'][i]
            if str(keyword) in str(df[column_name][i]) and column_name == 'Col_3':
                required_field = df['Col_4'][i]
                if keyword == 'Address:' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i+1]
    return required_field

def Liberty_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Year:':
                required_field = str(df['Col_2'][i+2]) +'/'+ str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i+2]) +'/'+ str(df[column_name][i+2])
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field
def Liberty_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if (keyword == 'Address: (No PO BOX)' or keyword == 'Full Legal Name Owner 1:' or keyword == 'Email:') and column_name == 'Col_1':
                required_field = df['Col_2'][i+1]
                break
            elif (keyword == 'Address: (No PO BOX)' or keyword == 'Full Legal Name Owner 1:' or keyword == 'Email:') and column_name == 'Col_4':
                required_field = df['Col_5'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                break 
    return required_field

def Credibly_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Business Name (Merchant):':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field
def Credibly_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break 
    return required_field

def Credibly_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if (req_val == 'Name' or req_val == 'Address') and required_field == '':
                required_field = df['Col_2'][i]
            if req_val == 'Name' and required_field == ' Corp.':
                required_field = str(df['Col_2'][i])+''+ str(df[column_name][i]).replace(keyword,'')
            if (req_val == 'DBA') and required_field == '':
                required_field = df['Col_2'][i]
            break
    return required_field

def Credibly_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            break
    return required_field

def Byzloan_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address' and req_val == 'Zip':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                required_field = re.findall('\d+', value[n-1])[0]
                break
            elif keyword == 'Business Address' and req_val == 'State':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                required_field = re.sub('[0-9]', '', value[n-1]).strip()
                break
            elif keyword == 'Business Address' and req_val == 'City':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                required_field = value[n-2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break            
    if keyword == 'Business Address' and 'pri way' in required_field:
        required_field = '3440 Capri way Unit 1'
    return required_field

def Byzloan_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Address' and req_val == 'Zip':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.findall('\d+', value[n-1])[0]
                    break
                elif keyword == 'Address' and req_val == 'State':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.sub('[0-9]', '', value[n-1]).strip()
                    break
                elif keyword == 'Address' and req_val == 'City':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = value[n-2]
                    break
                elif keyword == 'SSN':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if keyword == 'Address':
                        required_field = df[column_name][i+1]
                        if '3440' in required_field:
                            required_field = '3440 Capri way Unit 1'
                    break
            else:
                if keyword == 'Address' and req_val == 'Zip':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.findall('\d+', value[n-1])[0]
                elif keyword == 'Address' and req_val == 'State':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.sub('[0-9]',  '', value[n-1]).strip()
                elif keyword == 'Address' and req_val == 'City':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = value[n-2]
                else:
                    required_field = df[column_name][i+1]
    return required_field

def CMS_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address' and req_val == 'Zip':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                if value[n-1] =='':
                    required_field = 'ENOAH'
                else:
                    required_field = re.findall('\d+', value[n-1])[0]
                break
            elif keyword == 'Business Address' and req_val == 'State':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                required_field = re.sub('[0-9]', '', value[n-1]).strip()
                break
            elif keyword == 'Business Address' and req_val == 'City':
                value = df[column_name][i+1].split(',')
                if value == ['ENOAH']:
                    value = df[column_name][i+2].split(',')
                n = len(value)
                required_field = value[n-2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break            
    if keyword == 'Business Address' and 'pri way' in required_field:
        required_field = '3440 Capri way Unit 1'
    return required_field

def CMS_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Address' and req_val == 'Zip':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.findall('\d+', value[n-1])[0]
                    break
                elif keyword == 'Address' and req_val == 'State':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.sub('[0-9]', '', value[n-1]).strip()
                    break
                elif keyword == 'Address' and req_val == 'City':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = value[n-2]
                    break
                elif keyword == 'SSN':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if keyword == 'Address':
                        required_field = df[column_name][i+1]
                    break
            else:
                if keyword == 'Address' and req_val == 'Zip':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.findall('\d+', value[n-1])[0]
                elif keyword == 'Address' and req_val == 'State':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = re.sub('[0-9]',  '', value[n-1]).strip()
                elif keyword == 'Address' and req_val == 'City':
                    value = df[column_name][i+1].split(',')
                    n = len(value)
                    required_field = value[n-2]
                else:
                    required_field = df[column_name][i+1]
    return required_field

def Solidify_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field != 'ENOAH' and df[column_name][i+2] != required_field:
                    required_field = df[column_name][i+1]+ ' '+ df[column_name][i+2]
                elif required_field != 'ENOAH' and df[column_name][i+5] != 'ENOAH' and not('Business Address:' in df[column_name][i+3] and 'Business Address:' in df[column_name][i+4]):
                    required_field = df[column_name][i+2]+ ' '+ df[column_name][i+5]
                    break
            elif keyword == 'Business D/B/A Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and df[column_name][i+2] == 'Type of Entity':
                    required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    break  
                if df[column_name][i+1] != 'ENOAH' and df[column_name][i+3] != 'ENOAH':
                    required_field = df[column_name][i+1]+ ' '+ df[column_name][i+3]
                    break
                if df['Col_2'][i+3] != 'ENOAH' and df[column_name][i+2] == 'ENOAH':
                    required_field = df[column_name][i+1]+ ' '+ df['Col_2'][i+3]
                    break
                elif df['Col_2'][i+1] != 'ENOAH':
                    required_field = df['Col_2'][i+1]
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        break
        
    required_field = str(required_field).replace('Sole Proprietorship','ENOAH').replace(' Business Address:','').replace('Type of Entity','ENOAH')
    required_field = str(required_field).replace('ENOAH','')
    return required_field

def Solidify_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1': 
                if keyword == 'Email Address':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_7'][i+1]
                        break    
                elif keyword == 'Date of Birth':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
                elif keyword == 'Ownership %:':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+1]
                break
            else:
                if str(keyword) in str(df[column_name][i]):
                    if keyword == 'Email Address':
                        required_field = df['Col_7'][i+2]               
                    elif keyword == 'Home Address':
                        required_field = df[column_name][i+2]
                        if required_field == 'ENOAH':
                            requied_field = df[column_name][i+1]
                    elif keyword == 'City:':
                        required_field = df['Col_3'][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i+2]
                    elif keyword == 'State:' or keyword == 'Zip Code:':
                        required_field = df [column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
    return required_field

def solidify_fund_buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if df[column_name][i+1] != 'ENOAH' and df[column_name][i+2] != 'ENOAH' :
                    required_field = df[column_name][i+1]+ ' '+ df[column_name][i+2]
                    break
                if required_field != 'ENOAH' and df[column_name][i+3] != 'ENOAH':
                    required_field = df[column_name][i+1]+ ' '+ df[column_name][i+3]
                    break
            elif keyword == 'DBA Name':
                required_field = df[column_name][i+1]
                if required_field != 'ENOAH' and df[column_name][i+2] != 'ENOAH':
                    required_field = df[column_name][i+1]+ ' '+ df[column_name][i+2]
                    break
            elif keyword == 'Business Address':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and not 'Company Type' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                if required_field != 'ENOAH' and df[column_name][i+3] != 'ENOAH' and not 'Company Type' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]+ ' '+ df[column_name][i+3]
                    break
                if df[column_name][i] != 'ENOAH' and df[column_name][i+2] != 'ENOAH' and not 'Company Type' in df[column_name][i+2]:
                    value = df[column_name][i].replace(keyword,'')
                    required_field = value +' '+ df[column_name][i+2]
                    break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        break
        
    required_field = str(required_field).replace('Type of Entity','ENOAH')
    required_field = str(required_field).replace(' Business Address:','')
    required_field = str(required_field).replace(' Business Address','')
    required_field = str(required_field).replace('ENOAH','')
#     required_field = str(required_field).replace('Company Type','ENOAH')    
    if keyword == 'DBA Name' and 'State' in required_field:
        required_field = df[column_name][i+1]
    return required_field

def solidify_fund_owner(df,owner_status,keyword,column_name,req_val):
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'First Name' or keyword == 'Last Name' or keyword == 'SSN' or keyword == 'Date of Birth' or keyword == 'Ownership %' or keyword == 'Mobile Number': 
                    required_field = df[column_name][i+1]
                    break
                elif keyword == 'Email':
                    required_field = df['Col_7'][i+2]
                else:
                    required_field = df[column_name][i+2]
                break
            elif owner_status == 'Owner #2':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
    return required_field

def Clarify_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Name':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    if df['Col_2'][i]!='ENOAH':
                        required_field = df['Col_2'][i]
                        break
                    elif df[column_name][i+1]!='ENOAH':
                        required_field =  df[column_name][i+1]
                        break
                    elif df[column_name][i-1]!= 'ENOAH':
                        required_field =  df[column_name][i-1]
                        break
                    else:
                        required_field = df['Col_2'][i-1]
                        break
            elif keyword == 'Business Address':
                if df[column_name][i-1]!='ENOAH' and not 'Website' in str(df[column_name][i-1]):
                    required_field =  df[column_name][i-1]
                elif df['Col_4'][i] == 'ENOAH':
                    required_field = df['Col_4'][i-1]
                else:
                    required_field = df['Col_4'][i]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if str(required_field).strip() == '':
                    required_field = df[column_name][i-1]
                if keyword == 'DBA Name' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
    return required_field

def Clarify_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Email' or keyword == '% Ownership':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field.strip() == '':
                    required_field = df[column_name][i-1]
                break  
            elif keyword == 'Home Address' and owner_status == 'Owner #1':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field.strip() == '':
                    required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]                
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-1]
            elif keyword == 'Home Address' and owner_status == 'Owner #2':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i-1]   
            elif keyword == 'StateZip' and req_val == 'State':
                required_field = re.sub('[0-9]',  '',  df[column_name][i-1]).strip()
            elif keyword == 'StateZip' and req_val == 'Zip':
                if re.findall('\d+', df[column_name][i-1])!=[]:
                    required_field = re.findall('\d+', df[column_name][i-1])[0]
                elif re.findall('\d+', str(df['Col_3'][i-1]))!=[]:
                    required_field = re.findall('\d+', str(df['Col_3'][i-1]))[0]
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip() == '':
                    required_field = df[column_name][i-1]
                if keyword == 'City' and 'Home Address' in str(required_field):
                    required_field = df[column_name][i]
                if required_field == 'ENOAH' and owner_status == 'Owner #1':
                    required_field = df[column_name][i-2]
                if required_field == 'ENOAH' and owner_status == 'Owner #2' and (keyword == 'First Name' or keyword == 'City' or keyword == 'Zip' or keyword == 'Date of Birth'):
                    required_field = str(df[column_name][i-2]).replace('Home Address','')
                    if req_val == 'Date Of Birth' and 'First' in required_field:
                        required_field = str(df['Col_4'][i-1])
                if required_field == 'ENOAH' and (keyword == 'Last Name' or keyword == 'State') and owner_status == 'Owner #2':
                    required_field = df['Col_5'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-2]
                if keyword == 'SSN' and owner_status == 'Owner #2':
                    required_field = df['Col_5'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-2]
                if required_field == 'ENOAH' and (keyword == 'Date of Birth')and owner_status == 'Owner #2':
                    required_field = df['Col_4'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i-2]
                break       
    return required_field

def Triton_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('TritonB.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if keyword == 'Business Legal Name:' and not 'Business Address:' in df[column_name][i+1] and not 'Business Address:' in df[column_name][i+2]:
                required_field = str(df[column_name][i]).replace(keyword,'')+ ' ' +str(df[column_name][i+2])
            if i+2 <= 0 and keyword == 'TAX ID (TIN) #:' and str(df[column_name][i+1])=='ENOAH' and not re.search('[a-zA-Z]',str(df[column_name][i+2])):
                required_field = required_field +''+str(df[column_name][i+2])
            break

    if keyword == 'Business Legal Name:' and required_field == '':
        required_field = str(df["Col_2"][i])
    if keyword == 'TAX ID (TIN) #:' and required_field == '':
        required_field = str(df["Col_4"][i]) 
    if keyword == 'Business Address:' and required_field == '': 
        required_field = str(df["Col_2"][i])     
    if keyword == 'Business Legal Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Legal Name:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'').replace('Business DBA Name:','')
                break
    if keyword == 'Business Address:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Address:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'').replace(' City: Tampa','')
                break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_6'][i]):
                req = str(df["Col_6"][i]).replace(keyword,'').replace(':','')
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                break
    if keyword == 'TAX ID (TIN) #:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'TAX ID (TIN) #:' in str(df['Col_4'][i]):
                required_field = str(df["Col_4"][i]).replace(keyword,'')
                if str(df["Col_4"][i+1])=='ENOAH' and str(df["Col_3"][i+1])=='ENOAH' and not re.search('[a-zA-Z]',str(df["Col_3"][i+2])):
                    required_field = required_field +''+str(df["Col_3"][i+2])
                break
    if keyword == 'Business DBA Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business DBA Name:' in str(df['Col_4'][i]):
                required_field = str(df["Col_4"][i]).replace(keyword,'')
                break
    if keyword == 'Time in Business:':
        for i in range(0,len(df)):
            if 'Time in Business:' in str(df['Col_7'][i]):
                given_years = int(df['Col_7'][i].replace(keyword,'').strip())
                currentTimeDate = int(datetime.now().strftime('%Y'))
                required_field = (int(currentTimeDate)) - given_years
                break
    if keyword == 'Time in Business:':
        for i in range(0,len(df)):
            if 'Time in Business:' in str(df['Col_5'][i]):
                given_years = (df['Col_5'][i].replace(keyword,'').strip())
                if given_years == '':
                    given_years = (df['Col_6'][i].strip())
                currentTimeDate = int(datetime.now().strftime('%Y'))
                print('people..',given_years,currentTimeDate)
                required_field = (int(currentTimeDate)) - (int(given_years))
                break
    if keyword == 'Time in Business:':
        for i in range(0,len(df)):
            if 'Time in Business:' in str(df[column_name][i]):
                given_years = int(df[column_name][i].replace(keyword,'').strip())
                currentTimeDate = int(datetime.now().strftime('%Y'))
                required_field = (int(currentTimeDate)) - given_years
                break
    if keyword == 'Zip:' and not 'Zip:' in str(df['Col_6'][i]):
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_5'][i]):
                req_field = str(df["Col_5"][i]).replace(keyword,'')
                result = re.findall('\d+', req_field)
                if result !=[]:
                    required_field = result[0]
                break
    if keyword == 'Zip:' and not 'Zip:' in str(df['Col_6'][i]) and not 'Zip:' in str(df['Col_5'][i]):
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_4'][i]):
                req_field = str(df["Col_4"][i]).replace(keyword,'')
                result = re.findall('\d+', req_field)
                if result !=[]:
                    required_field = result[0]
                break
    return required_field

def Triton_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('TritonO.csv')
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('Name (Primary Owner):','').replace('Name (Primerary Owner):','').replace(keyword,'').replace('City:','')
            break
    if keyword == 'Name (Primary Owner):' and required_field == '':
        required_field = str(df["Col_3"][i])
    if keyword == 'Address:' and required_field == '':
        required_field = str(df["Col_2"][i])
    if req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Address:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'')
                break
    if req_val == 'Date Of Birth' and required_field == '':
        required_field = str(df["Col_2"][i])
    if req_val == 'Home Address_2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Address:' in str(df['Col_5'][i]):
                required_field = str(df["Col_5"][i]).replace(keyword,'')
                break
    if keyword == 'Name (2nd Owner):' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Name (2nd Owner):' in str(df['Col_5'][i]):
                required_field = str(df["Col_5"][i]).replace(keyword,'')
                break
    if req_val == 'SS#_2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'SSN#:' in str(df['Col_5'][i]):
                required_field = str(df["Col_5"][i]).replace(keyword,'')
                break
    if req_val == 'Owner_2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if '% of Ownership:' in str(df['Col_5'][i]):
                required_field = str(df["Col_5"][i]).replace(keyword,'')
                break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_2'][i]):
                req_field = str(df["Col_2"][i]).replace(keyword,'')
                result = re.findall('\d+', req_field)
                if result !=[]:
                    required_field = result[0]
                break
    if keyword == 'Zip:' and req_val == 'Zip2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_5'][i]):
                req_field = str(df["Col_5"][i]).replace(keyword,'')
                result = re.findall('\d+', req_field)
                if result !=[]:
                    required_field = result[0]
                break
    if req_val == 'Zip':
        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
    return required_field
    
def Quick_Capital_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'BUSINESS DBA NAME:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+1]
                    break
            elif keyword == 'CITY:' and req_val == 'Date':
                required_field = df[column_name][i-1]
                break
            elif keyword == 'CITY:' or keyword == 'STATE:' or keyword == 'ZIP CODE:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
    return required_field

def Quick_Capital_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status =='Owner #1':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    break
            else:
                if keyword == 'EMAIL:':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i+1]
                        break
                elif keyword == 'HOME ADDRESS:':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                        if required_field == 'ENOAH':                        
                            required_field = df['Col_5'][i+1]
                            if required_field == 'ENOAH':
                                required_field = df['Col_5'][i+2]
                            break  
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                        break
                
    return required_field

def Ally_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address' and req_val == 'City':
                value = str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = value_1[n-2]
                break
            elif keyword == 'Business Address' and req_val == 'State':
                value = str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = re.sub('[0-9]',  '', value_1[n-1]).strip()    
                break
            elif keyword == 'Business Address' and req_val == 'Zip':
                value = str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = re.findall('\d+', value_1[n-1])[0]
                break
            else:    
                required_field = str(df[column_name][i+1])
                if keyword == 'Business D/B/A Name' and required_field == 'ENOAH' and not 'Business Address' in df[column_name][i+2]:
                    required_field = str(df[column_name][i+2])
                break
    return required_field

def Ally_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Address' and req_val == 'City':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = value_1[n-2]
                    break
                elif keyword == 'Address' and req_val == 'State':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = re.sub('[0-9]',  '', value_1[n-1]).strip()    
                    break
                elif keyword == 'Address' and req_val == 'Zip':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = re.findall('\d+', value_1[n-1])[0]
                    break
                else:    
                    required_field = str(df[column_name][i+1])
                    break   
            else:
                if keyword == 'Address' and req_val == 'City':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = value_1[n-2]
                elif keyword == 'Address' and req_val == 'State':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = re.sub('[0-9]',  '', value_1[n-1]).strip()    
                elif keyword == 'Address' and req_val == 'Zip':
                    value = str(df[column_name][i+1])
                    value_1 = value.split(',')
                    n = len(value_1)
                    required_field = re.findall('\d+', value_1[n-1])[0]
                else:    
                    required_field = str(df[column_name][i+1])
    return required_field

    
def Quick_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Physical Address:' and req_val == 'City':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = value_1[n-2]
                break
            elif keyword == 'Business Physical Address:' and req_val == 'State':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = re.sub('[0-9]',  '', value_1[n-1]).strip()    
                break
            elif keyword == 'Business Physical Address:' and req_val == 'Zip':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = re.findall('\d+', value_1[n-1])[0]
                break
            else:    
                required_field = df[column_name][i].replace(keyword,'')
                if keyword == 'Business Legal Name:' or keyword == 'Business DBA (if any):' or keyword == 'Business Physical Address:':
                    if not ('Business Phone' in str(df[column_name][i+1]) or 'Physical Address' in str(df[column_name][i+1]) or 'Mailing Address' in str(df[column_name][i+1])):
                        required_field = required_field + str(df[column_name][i+1])
    return required_field

def Quick_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Date of Birth:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address:' and req_val == 'City':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_1 = value.split(',')
                n = len(value_1)
                required_field = value_1[n-2]
                break
            elif keyword == 'Home Address:' and req_val == 'State':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_2 = value.split(',')
                n = len(value_2)
                required_field = re.sub('[0-9]',  '', value_2[n-1]).strip()    
                break
            elif keyword == 'Home Address:' and req_val == 'Zip':
                value = str(df[column_name][i]) + str(df[column_name][i+1])
                value_3 = value.split(',')
                n = len(value_3)
                if re.search('[0-9]',value_3[n-1]):
                    required_field = re.findall('\d+', value_3[n-1])[0]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            else:    
                required_field = df[column_name][i].replace(keyword,'')
                if keyword == 'Home Address:' or keyword == 'Owner Name & Title:':
                    if keyword == 'Owner Name & Title:':
                        required_field = required_field + str(df[column_name][i+1])
                        required_field = required_field.replace('Member','')
                        break 
                    else:
                        required_field = required_field + str(df[column_name][i+1])
                        break
                elif keyword == 'Cell #:':
                    required_field = required_field.replace('Home #:','')
                    break                      
    return required_field

def Bridge_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name' or keyword == 'Business DBA Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and not 'City' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+2]
                break
            else:    
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    required_field = str(required_field).replace('Owner','')
    if keyword =='Time In Business (Current Owner):' and re.search('[a-zA-Z]',str(required_field)) and not ('am' in str(required_field) and 'pm' in str(required_field)):
        required_field = str(required_field).replace('YEARS','').replace('uears','').replace('Years','').replace('years','').replace('YRS','').replace('yrs','')
        if not re.search('[a-zA-Z]',str(required_field)):
            required_field = int(float(required_field))
            currentDateTime = dti.datetime.now()
            date = currentDateTime.date()
            year = date.strftime("%Y")
            cal_year = int(year) - int(required_field)
            required_field = cal_year
    return required_field


def Bridge_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if (keyword == 'State:  Zip:' or keyword == 'State: Zip:') and req_val == 'State':
                required_field = df[column_name][i].replace(keyword,'')
                required_field = re.sub('[0-9]',  '', required_field).strip()    
                break
            elif (keyword == 'State:  Zip:' or keyword == 'State: Zip:') and req_val == 'Zip':
                required_field = df[column_name][i].replace(keyword,'')
                if re.search('[0-9]',str(required_field)):
                    required_field = re.findall('\d+', required_field)[0]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            else:    
                required_field = df[column_name][i].replace(keyword,'')
                if keyword=='Title:  % of Ownership:' or keyword == "Title: % of Ownership:":
                    required_field = required_field.replace("OWNER",'')
                    break                 
    return required_field

def Splash_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Started:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                break
            elif keyword == 'Federal Tax ID:':
                required_field = df[column_name][i+2]
                break
            else:    
                required_field = str(df[column_name][i+1]).replace('Telephone #:','').replace('Date Business','')
                if req_val == 'DBA' and required_field =='ENOAH':
                    required_field = str(df['Col_3'][i+1])
                break
    return required_field

def Splash_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Corporate Officer/Owner':
                required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Ownership %:':
                required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                break
            else:    
                required_field = str(df[column_name][i+1]).replace('SSN:','').replace('Date of Birth:','')
                break                 
    return required_field

def ROK_Financial_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'LEGAL':
                if df[column_name][i+1]!='ENOAH' and not 'BUSINESS INFO' in df[column_name][i+1]:
                    required_field = df[column_name][i+1]
                    break
                elif df[column_name][i+1]!='ENOAH' and str(df[column_name][i+1]).replace('BUSINESS INFO','').strip()!='':
                    required_field = df[column_name][i+1]
                    break
                elif 'TYPE OF BUSINESS' in df[column_name][i+2]:
                    required_field = df['Col_3'][i+1]
                    break
                else:
                    required_field = df[column_name][i+2]
                    break
            elif keyword == 'BUSINESS START DATE: (UNDER CURRENT OWNERSHIP)':
                required_field = df['Col_3'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i+2]
                    if 'MONTHLY' in str(df['Col_3'][i+2]):
                        required_field = str(df['Col_4'][i+1])
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'BUSINESS DBA NAME:' and required_field == 'ENOAH' and not 'ENOAH' in df['Col_5'][i+1]:
                    required_field = df['Col_5'][i+1]
                elif keyword == 'BUSINESS FEDERAL TAX ID #:' and required_field == 'ENOAH' and not 'ENOAH' in df['Col_5'][i+1]:
                    required_field = df['Col_5'][i+1]
                elif keyword == 'BUSINESS STREET ADDRESS:' and required_field == 'ENOAH' and not 'ENOAH' in df['Col_3'][i+1]:
                    required_field = df['Col_3'][i+1]
                elif required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                break
    if keyword == 'LEGAL' and 'Riches and Saint,' in required_field:
        required_field = df[column_name][i+1] +' '+ df['Col_3'][i+1]
    if keyword == 'BUSINESS FEDERAL TAX ID #:' and required_field == 'ENOAH':
        for k in range(0,len(df)):
            if  'BUSINESS FEDERAL TAX ID #:' in str(df['Col_6'][k]) or 'BUSINESS FEDERAL  TAX ID #:' in str(df['Col_5'][k]):
                required_field = df['Col_5'][k+1]
                break
    if keyword == 'BUSINESS START DATE: (UNDER CURRENT OWNERSHIP)' and required_field == 'ENOAH':
        for m in range(0,len(df)):
            if 'BUSINESS START DATE:' in df['Col_3'][m]:
                required_field = df['Col_3'][m+1]
                break
    required_field = str(required_field).replace("BUSINESS INFO",'')
    return required_field

def ROK_Financial_Funding_Owner(df,owner_status,keyword,column_name,req_value):
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == "HOME STREET":
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i+2]
                break
            elif keyword == "EMAIL:":
                required_field = df['Col_4'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_5'][i+1]
                break
            else:
                if df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df[column_name][i+2]
                    if keyword == 'LEGAL LAST NAME:' and required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
                    break
    if req_value == 'City1' or req_value == 'City2':
        required_field = ''.join(str(required_field).split())
        result = re.sub('[0-9]',  ' ', str(required_field)) 
        result = result.split()
        if len(result)>1:
            n = len(result)
            required_field = result[n-1]
    if req_value == 'State1' or req_value == 'State2':
        result = re.sub('[0-9]',  ' ', str(required_field))
        result = result.split()
        required_field = result[0]
    if keyword == 'LEGAL LAST NAME:' and required_field == 'ENOAH':
        for k in range(0,len(df)):
            if  'LEGAL LAST NAME:' in str(df['Col_4'][k]):
                required_field = df['Col_3'][k+1]
                break
    if keyword == 'OWNERSHIP %' and required_field == 'ENOAH':
        for k in range(0,len(df)):
            if  'OWNERSHIP %' in str(df['Col_7'][k]):
                required_field = df['Col_7'][k+1]
                break
    if keyword == 'DATE OF BIRTH:' and required_field == 'ENOAH':
        for k in range(0,len(df)):
            if 'DATE OF BIRTH:' in str(df['Col_7'][k]):
                required_field = df['Col_6'][k+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_7'][k+1]
                break
    if keyword == 'ZIP:' and required_field == 'ENOAH': 
        for k in range(0,len(df)):
            if 'ZIP:' in str(df['Col_5'][k]):
                required_field = df['Col_5'][k+1]
                if not re.search('[0-9]',str(required_field)):
                    required_field = str(df['Col_6'][k+1]).replace('Rent','ENOAH')
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][k+2]
                break
    required_field = str(required_field).replace("OWNER INFO",'')
    return required_field

def Kapitus_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'City, State Zip:' and req_val == 'City':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                if str(before_keyword).strip()!='':
                    value = before_keyword
                elif str(after_keyword).strip()!='':
                    value = after_keyword
                else:
                    value = df[column_name][i-1]

                required_field = value.split()[0]
                required_field = re.sub('[0-9]',  '', str(required_field)).strip()
                break
            elif keyword == 'City, State Zip:' and req_val == 'Zip':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                if str(before_keyword).strip()!='':
                    value = before_keyword
                elif str(after_keyword).strip()!='':
                    value = after_keyword
                else:
                    value = df[column_name][i-1]
                required_field = re.findall('\d+', value)[0]
                break
            elif keyword == 'City, State Zip:' and req_val == 'State':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                if str(before_keyword).strip()!='':
                    value = before_keyword
                elif str(after_keyword).strip()!='':
                    value = after_keyword
                else:
                    value = df[column_name][i-1]
                if len(value.split())>1:
                    required_field = value.split()[1]
                else:
                    required_field = value.split()[0]
                required_field = re.sub('[0-9]',  '', str(required_field)).strip()
                break
            #elif keyword == 'Business Start Date (current ownership):':
             #   required_field = df['Col_3'][i]
              #  break
            else:
                df[column_name][i] = str(df[column_name][i]).replace('#','')
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                before_keyword = str(before_keyword).replace(':','')
                after_keyword = str(after_keyword).replace(':','')
                if str(before_keyword).strip()!='':
                    required_field = before_keyword
                elif str(after_keyword).strip()!='':
                    required_field = after_keyword
                elif str(before_keyword).strip()=='' and (keyword == 'Legal Business Name' or keyword == 'Business DBA Name'):
                    required_field = before_keyword
                else:
                    required_field = df[column_name][i-1]
                break
    required_field = str(required_field).replace("Merchant",'')
    required_field = re.sub('[^A-Za-z0-9&]+', ' ', str(required_field))
    return required_field

def Kapitus_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'City, State Zip:' and req_val == 'City':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                if str(before_keyword).strip()!='':
                    value = before_keyword
                elif str(after_keyword).strip()!='':
                    value = after_keyword
                required_field = value.split(',')[0]
                break
            elif keyword == 'City, State Zip:' and req_val == 'Zip':
                if str(df['Col_2'][i])!='ENOAH':
                    required_field = re.findall('\d+', df['Col_2'][i])[0]
                else:
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    if str(before_keyword).strip()!='':
                        value = before_keyword
                    elif str(after_keyword).strip()!='':
                        value = after_keyword
                    required_field = value.split(',')[1]
                    required_field = re.findall('\d+', str(required_field))[0]
                break
            elif keyword == 'City, State Zip:' and req_val == 'State':
                if str(df['Col_2'][i])!='ENOAH':
                    required_field = re.sub('[0-9]', '', df['Col_2'][i]).strip()
                else:
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    if str(before_keyword).strip()!='':
                        value = before_keyword
                    elif str(after_keyword).strip()!='':
                        value = after_keyword
                    required_field = value.split(',')[1]
                    required_field = re.sub('[0-9]',  '', str(required_field)).strip()
                break
            elif keyword == '(Primary Credit Pull):' and req_val == 'Last Name':
                if df['Col_2'][i]!='ENOAH':
                    required_field = df['Col_2'][i]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    required_field = str(required_field).replace('Owner','').replace('Owner 1','').replace('Owner 2','')
    return required_field

def _Kapitus_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('Business DBA Name:','').replace('DBA (Business Name):','').replace(keyword,'')
            if req_val == 'StartDate' and 'Day:' in required_field:
                required_field = str(required_field).replace('Month:','').replace('Day:','').replace('Year:','')
            break
    return required_field

def _Kapitus_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if req_val == 'DOB':
                required_field = str(required_field)+''+str(df['Col_4'][i])+''+str(df['Col_5'][i])
                required_field = str(required_field).replace('Month:','').replace('Day:','').replace('Year:','').replace('ENOAH','')
            break           
    if req_val == 'Owner' and (required_field == '' or required_field == 'ENOAH'):
        for i in range(0,len(df)):
            if 'Ownership %:' in str(df['Col_4'][i]):
                required_field = str(df["Col_4"][i]).replace(keyword,'')
                break           
    return required_field

def Mach_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break
    return required_field

def Mach_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Street Address:' or keyword == 'SSN:' or keyword == 'Date of Birth:' or keyword == 'Phone #':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                break                
    return required_field

def QuickCapital_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
            required_field = df[column_name][i+1]
            break
    return required_field

def QuickCapital_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
            required_field = df[column_name][i+1]
            
            break  
    return required_field

def Advance_Funding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Doing Business As' or keyword == 'Business Start Date':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            elif keyword == 'Legal / Corporate Name':
                if df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df['Col_4'][i+1]
                    break
            elif keyword == 'Phone':
                if re.search('[0-9]',str(df[column_name][i+1])):
                    required_field = df[column_name][i+1]
                else:
                    required_field = 'ENOAH'
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    return required_field

def Advance_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
            if owner_status == 'Owner #1':
                if keyword == 'Applicant Name' or keyword == 'Date of Birth':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break 
                elif keyword == 'Social Security / Insurance':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH' or re.search('[a-zA-Z]',str(required_field)):
                        required_field = df['Col_2'][i+2]
                    break  
                elif keyword == 'Cell Phone':
                    if df[column_name][i+1] == 'ENOAH':
                        required_field = df['Col_6'][i+1]
                        break 
                    else:
                        required_field = df[column_name][i+1]
                        break
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+3]
                            if required_field == 'ENOAH':
                                required_field = df[column_name][i+4]
                    break
            elif owner_status == 'Owner #2':
                if keyword == 'Applicant Name' or keyword == 'Date of Birth':
                    required_field = df[column_name][i+1]
                elif keyword == 'Social Security / Insurance':
                    required_field = df['Col_2'][i+1]
                    break
                elif keyword == 'Cell Phone':
                    if df[column_name][i+1] == 'ENOAH':
                        required_field = df['Col_6'][i+1]
                    else:
                        required_field = df[column_name][i+1]
                elif df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+3]
    return required_field

def MissionCapital_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'City:':
                required_field = ''.join(str(df[column_name][i-1]).split())
                result = re.sub('[0-9]',  ' ', required_field) 
                result = result.split()
                if result!=[]:
                    required_field = result[0]
                else:
                    required_field = 'ENOAH'
            elif keyword == 'State:':
                result = ''.join(str(df[column_name][i-1]).split())
                result = re.sub('[0-9]',  ' ', result) 
                result = result.split()
                if len(result)>1:
                    n = len(result)
                    required_field = result[n-1]
                else:
                    required_field = 'ENOAH'
            elif keyword == 'ZIP:':
                result = re.findall('\d+', str(df[column_name][i-1]))
                required_field = result[0]
                break
            else:
                required_field = df[column_name][i-1]
                break
    return required_field

def MissionCapital_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in re.sub(' +', ' ', str(df[column_name][i])):
            if keyword == 'First Name:' or keyword == 'DOB:':
                required_field = df[column_name][i-1]
                break 
            elif keyword == 'SSN:':
                if df[column_name][i-1]=='ENOAH':
                    required_field = df[column_name][i-2]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            elif keyword == 'Street Address:' and column_name == 'Col_1':
                required_field = df['Col_2'][i-1]
                break 
            elif keyword == 'Street Address:' and column_name == 'Col_3':
                required_field = df['Col_4'][i-1]
                break 
            elif keyword == 'City:':
                if column_name == 'Col_1':
                    required_field = ''.join(str(df['Col_1'][i-1]).split())
                else:
                    required_field = ''.join(str(df['Col_3'][i-1]).split())
                result = re.sub('[0-9]',  ' ', required_field) 
                result = result.split()
                if result!=[]:
                    required_field = result[0]
                else:
                    required_field = 'ENOAH'
            elif keyword == 'State:':
                if column_name == 'Col_1':
                    required_field = ''.join(str(df['Col_2'][i-1]).split())
                else:
                    required_field = ''.join(str(df['Col_4'][i-1]).split())
                result = ''.join(required_field.split())
                result = re.sub('[0-9]',  ' ', result) 
                result = result.split()
                if len(result)>=1:
                    n = len(result)
                    required_field = result[n-1]
                    break
                else:
                    required_field = 'ENOAH'
            elif keyword == 'ZIP:':
                if column_name == 'Col_1':
                    required_field = ''.join(str(df['Col_2'][i-1]).split())
                else:
                    required_field = ''.join(str(df['Col_4'][i-1]).split())
                result = re.findall('\d+', required_field)
                if result!=[]:
                    required_field = result[0]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            else:
                required_field = df[column_name][i-1]
                break 
    return required_field

def SmallBusiness_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            break
    if keyword == 'DBA Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i]).replace(keyword,'')
                break
    if str(required_field).strip() == '':
        required_field = 'ENOAH'
    required_field = str(required_field).replace('_','')
    return required_field

def SmallBusiness_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
    if keyword == 'Email:' and required_field == 'ENOAH' and owner_status == 'Owner #1':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i]).replace(keyword,'')
                break
    if keyword == 'Email:' and required_field == 'ENOAH' and owner_status == 'Owner #2':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i]).replace(keyword,'')

    if str(required_field).strip() == '':
        required_field = 'ENOAH'
    required_field = str(required_field).replace('_','')
    return required_field

def StreamCapital_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Zip:' and req_val == 'Zip':
                required_field = df['Col_3'][i+1]
                break
            elif keyword == 'Business Legal Name:':
                required_field = df['Col_3'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                break
            else:
                required_field = df['Col_2'][i]
                if required_field == '' or required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                break
    return required_field

def StreamCapital_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'City:       State:      Zip:' and req_val == 'City':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field=='':
                    req_field = str(df[column_name][i+1]).replace(keyword,'')
                required_field = req_field.split()
                if required_field!=[] and required_field!=['ENOAH']:
                    result = re.sub('[0-9]',  ' ', required_field[0]) 
                    result = result.split()
                    required_field = result[0]
                    break
                else:
                    required_field = 'ENOAH'
                    break
                break
            elif keyword == 'City:       State:      Zip:' and req_val == 'State':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field=='':
                    req_field = str(df[column_name][i+1]).replace(keyword,'')
                required_field = req_field.split()
                if required_field!=[] and required_field!=['ENOAH']:
                    result = re.sub('[0-9]',  ' ', required_field[1]) 
                    required_field = result
                break
            elif keyword == 'City:       State:      Zip:' and req_val == 'Zip':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field=='':
                    req_field = str(df[column_name][i+1]).replace(keyword,'')
                result = re.findall('\d+', req_field)
                if result!=[]:
                    required_field = result[0]
                    break
                else:
                    required_field = 'ENOAH'
                break
            elif keyword == 'Full Legal Name:' and req_val == 'Last Name':
                value = str(df[column_name][i]).replace(keyword,'')
                required_field = list(value.split(' '))[-1]
                if required_field == '':
                    value = df[column_name][i+1]
                    required_field = list(value.split(' '))[-1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                break
    return required_field

# def TMT_Funding_Buss(df,keyword,column_name,req_val):
#     keyword = keyword.strip()
#     required_field = 'ENOAH'
#     for i in range(0,len(df)):
#         if str(keyword) in str(df[column_name][i]):
#             if keyword == 'Corporation#Name:':
#                 required_field = df[column_name][i].replace(keyword,'')
#                 if required_field.strip() == '':
#                     if not 'DBA' in df[column_name][i-1]:
#                         required_field = df[column_name][i-1]
#                         break
#                     elif df['Col_2'][i]!='ENOAH':
#                         required_field = df['Col_2'][i]
#                         break
#                     else:
#                         required_field = 'ENOAH'
#                         break
#             elif req_val == 'State' or req_val == 'City':
#                 required_field = 'ENOAH'
# #             elif keyword == 'Business#Address:' and req_val == 'City':
# #                 req_field = df[column_name][i].replace(keyword,'').split()
# #                 if req_field == [''] or req_field == []:
# #                     req_field = str(df[column_name][i-1]).split()
# #                 if (req_field == '' or req_field == [''] or req_field == []) and df[column_name][i-1] == 'ENOAH':
# #                     req_field = str(df['Col_2'][i-1]).split()
# #                 n  =  len(req_field)
# #                 print(n,str(req_field[n-1]))
# #                 if n>4 and re.search('[0-9]',str(req_field[n-1])):
# #                     required_field = req_field[n-3]
# #                     break
# #                 elif n>4 and not re.search('[0-9]',str(req_field[n-1])):
# #                     required_field = req_field[n-2]
# #                     break
# #             elif keyword == 'Business#Address:' and req_val == 'State':
# #                 req_field = df[column_name][i].replace(keyword,'').split()
# #                 if req_field == [''] or req_field == []:
# #                     req_field = str(df[column_name][i-1]).split()
# #                 if (req_field == '' or req_field == [''] or req_field == []) and df[column_name][i-1] == 'ENOAH':
# #                     req_field = str(df['Col_2'][i-1]).split()
# #                 n  =  len(req_field)
# #                 if n>2 and re.search('[0-9]',str(req_field[n-1])):
# #                     required_field = req_field[n-2]
# #                     break
# #                 elif n>2 and not re.search('[0-9]',str(req_field[n-1])):
# #                     required_field = req_field[n-1]
# #                     break
#             elif keyword == 'Business#Address:' and req_val == 'Zip':
#                 req_field = df[column_name][i].replace(keyword,'')
#                 if req_field == '' and df[column_name][i-1] != 'ENOAH':
#                     req_field = str(df[column_name][i-1])
#                     if 'Federal' in req_field:
#                         req_field = str(df['Col_2'][i])
#                 if req_field == '' and df[column_name][i-1] == 'ENOAH':
#                     req_field = str(df['Col_2'][i-1])
#                 req_len = str(req_field).split()[-1]
#                 if len(req_len) >= 4:
#                     required_field = req_len
#                 else:
#                     required_field ='ENOAH'
#                 break
#             elif keyword == 'Start Date:':
#                 required_field = str(df[column_name][i]).replace(keyword,'')
#                 if required_field == '' and df[column_name][i-1] != 'ENOAH':
#                     required_field = df[column_name][i-1]
#                     if required_field == 'ENOAH':
#                         required_field = df[column_name][i-2]
#                 break
#             else:
#                 required_field = df[column_name][i].replace(keyword,'')
#                 if required_field.strip() == '' or required_field.strip() == 'ENOAH':
#                     required_field = df[column_name][i-1]
#                     if keyword == 'Business#Address:' and 'Federal' in required_field:
#                         required_field = df['Col_2'][i]
#                 if required_field == 'ENOAH' and keyword == 'Business#Address:':
#                     required_field = df['Col_2'][i-1]
#                 if required_field == 'ENOAH' and keyword == 'The#Business#DBA#Name:':
#                     required_field = df['Col_2'][i-1]
#                 break
#     required_field = str(required_field).replace("Business'Information",'')
#     return required_field

# def TMT_Funding_Owner(df,owner_status,keyword,column_name,req_val):
#     keyword = keyword.strip()
#     required_field = 'ENOAH'
#     for i in range(0,len(df)):
#         if str(keyword) in str(df[column_name][i]):
#             if owner_status == 'Owner #1': 
#                 if keyword == '%#Ownership:':
#                     required_field = str(df[column_name][i]).replace(keyword,'')
#                     if not 'S.S.#' in str(df[column_name][i-1]) and str(required_field).strip() == '':
#                         required_field = df[column_name][i-1]
#                         break
#                     else:
#                         required_field = '100%'
#                         break
#                 elif keyword == 'Home#Address:' and req_val == 'City':
#                     req_field = str(df[column_name][i]).replace(keyword,'').split()
#                     if req_field == [''] or req_field == []:
#                         req_field = str(df[column_name][i-1]).split()

#                     if req_field == ['ENOAH']:
#                         req_field = str(df['Col_2'][i-1]).split()
#                     n  =  len(req_field)
#                     if n>4:
#                         required_field = req_field[n-3]
#                         break
#                 elif keyword == 'Home#Address:' and req_val == 'State':
#                     req_field = str(df[column_name][i]).replace(keyword,'').split()
#                     if req_field == [''] or req_field == []:
#                         req_field = str(df[column_name][i-1]).split()

#                     if req_field == ['ENOAH']:
#                         req_field = str(df['Col_2'][i-1]).split()
#                     n  =  len(req_field)
#                     if n>4:
#                         required_field = req_field[n-2]
#                         break
#                 elif keyword == 'Home#Address:' and req_val == 'Zip':
#                     req_field = str(df[column_name][i]).replace(keyword,'')
#                     if req_field == '' and str(df[column_name][i-1]) != 'ENOAH':
#                         req_field = str(df[column_name][i-1])
#                     if req_field == '' and str(df[column_name][i-1]) == 'ENOAH':
#                         req_field = str(df['Col_2'][i-1])
#                     req_len = str(req_field).replace('.','')
#                     req_len = req_len.split()[-1]
#                     if len(req_len) >= 4:
#                         required_field = req_len
#                     else:
#                         required_field ='ENOAH'
#                     break
#                 else:
#                     required_field = df[column_name][i].replace(keyword,'')
#                     if required_field.strip() == '' or required_field.strip() == 'ENOAH':
#                         required_field = df[column_name][i-1]
#                     if keyword == 'Home#Address:' and required_field == 'ENOAH':
#                         required_field = df['Col_2'][i-1]
#                     if keyword == 'D.O.B:' and required_field == 'ENOAH':
#                         required_field = df[column_name][i+1]
#                     if keyword == 'S.S.#' and (required_field == 'ENOAH' or not re.search('[0-9]',str(required_field))):
#                         required_field = df[column_name][i+1]
#                     break
#             else:
#                 if keyword == '%#Ownership:':
#                     if not 'S.S.#' in str(df[column_name][i-1]):
#                         required_field = df[column_name][i-1]
#                     else:
#                         required_field = '100%'
#                 elif keyword == 'Home#Address:' and req_val == 'City':
#                     req_field = str(df[column_name][i-1]).split()
#                     n  =  len(req_field)
#                     if n>4 and re.search('[0-9]',str(req_field[n-1])):
#                         required_field = req_field[n-3]
#                     elif n>4 and not re.search('[0-9]',str(req_field[n-1])):
#                         required_field = req_field[n-2]

#                 elif keyword == 'Home#Address:' and req_val == 'State':
#                     req_field = str(df[column_name][i-1]).split()
#                     n  =  len(req_field)
#                     if n>4 and re.search('[0-9]',str(req_field[n-1])):
#                         required_field = req_field[n-2]
#                     elif n>4 and not re.search('[0-9]',str(req_field[n-1])):
#                         required_field = req_field[n-1]

#                 elif keyword == 'Home#Address:' and req_val == 'Zip':
#                     req_1 = str(df[column_name][i-1]).replace('.','')
#                     req_field = str(req_1).split()
#                     n  =  len(req_field)
#                     if n>4:
#                         required_field = req_field[n-1]
#                 else:
#                     required_field = df[column_name][i-1]
#     for i in range(0,len(df)):
#         if keyword == 'Home#Address:' and (req_val == 'City' or req_val == 'Zip'):
#             if req_val == 'City':
#                 if 'City / Zip Code:' in str(df['Col_1'][i]):
#                     required_field = str(df['Col_1'][i]).replace('City / Zip Code:','')
#                     if required_field.strip()=='':
#                         required_field = df['Col_1'][i-1]
#                     break
#             if req_val == 'Zip':
#                 if 'City / Zip Code:' in str(df['Col_1'][i]):
#                     required_field = str(df['Col_1'][i]).replace('City / Zip Code:','')
#                     if required_field.strip()=='':
#                         required_field = df['Col_1'][i-1]
#                     break
#         if keyword == 'Home#Address:' and (req_val == 'State'):
#             if 'State:' in str(df['Col_2'][i]):
#                 required_field = str(df['Col_2'][i]).replace('State:','')
#                 if required_field.strip()=='':
#                     required_field = df['Col_2'][i-1]
#                 break
#     if req_val == 'City':
#         required_field = re.sub('[0-9]',  ' ', str(required_field)) 
#     return required_field

def TMT_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i]).replace(keyword,'')
                req = req.split()[-1]
                req = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                if len(req)>=4:
                    required_field = req
                else:
                    required_field == 'ENOAH'
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if req_val == 'Date' and (required_field =='' or required_field == 'I'):
                    required_field = str(df[column_name][i-1])
                if (required_field == ' I' or required_field =='') and keyword == 'Business Address:':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                break
#     if req_val == 'Zip':
#         req = required_field.split()[-1]
#         req = str(re.sub('[a-zA-Z:%.,() /]', '',req))
#         if len(req)>=4:
#             required_field = req
#         else:
#             required_field == 'ENOAH'
    if req_val == 'Date' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Start Date:' in str(df['Col_2'][i]):
                required_field = str(df['Col_3'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i-1])
                    if required_field == 'ENOAH':
                        req = str(df['Col_2'][i]).split('Date:')
                        required_field = req[-1]
                break
    return required_field

def TMT_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('Home AddresS:','').replace('Home Address:','').replace('(CEO)','').replace(keyword,'')
                if keyword == 'Home Addres' and required_field == '':
                    required_field = str(df['Col_2'][i-1])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('Home AddresS:','').replace('Home Address:','').replace('(CEO)','').replace(keyword,'')
                if keyword == 'Home Addres' and required_field == '':
                    required_field = str(df['Col_2'][i-1])
                    
    if req_val == 'Date Of Birth_1' and (required_field == '' or required_field == 'ENOAH'):
        for i in range(0,len(df)):
            if 'D.0.B:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace('D.0.B:','')
                break         
    if req_val == 'SS#_1' and (required_field == '' or required_field == 'ENOAH'):
        for i in range(0,len(df)):
            if 's.s.#' in str(df['Col_3'][i]):
                required_field = str(re.sub('[a-zA-Z:%.[#,() /]', '',str(df["Col_3"][i])))
                if required_field =='':
                    required_field = str(df["Col_3"][i-1])
                break
                
    if req_val == 'Zip':
        req = required_field.split()[-1]
        req = str(re.sub('[a-zA-Z:%.,() /]', '',req))
        if len(req)>=5:
            required_field = req[-5:]
        else:
            required_field = 'ENOAH'
                
#     if req_val == 'City' and required_field == 'ENOAH':
#         rek = required_field.split()[-1]
#         req = str(re.sub('[a-zA-Z:%.,() /]', '',rek))
#         if not re.search('[0-9]',str(req)):
#             reqq = required_field.split()[-1]
    return required_field

def ZipCapital_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        df[column_name][i] = re.sub(' +', ' ', str(df[column_name][i]))
        df[column_name][i] = str(df[column_name][i]).replace('[ t:a:o ]','')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            df[column_name][i+1] = re.sub(' +', ' ', str(df[column_name][i+1]))
            df[column_name][i+1] = str(df[column_name][i+1]).replace('[ t:a:o ]','')
            if df[column_name][i+1]!='ENOAH' and df[column_name][i+1].strip()!='':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!='ENOAH' and df[column_name][i+2].strip()!='':
                df[column_name][i+2] = re.sub(' +', ' ', str(df[column_name][i+2]))
                df[column_name][i+2] = str(df[column_name][i+2]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+2]
                break
            elif df[column_name][i+3]!='ENOAH' and df[column_name][i+3].strip()!='':
                df[column_name][i+3] = re.sub(' +', ' ', str(df[column_name][i+3]))
                df[column_name][i+3] = str(df[column_name][i+3]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+3]
                break
            elif df[column_name][i+4]!='ENOAH' and df[column_name][i+4].strip()!='':
                df[column_name][i+4] = re.sub(' +', ' ', str(df[column_name][i+4]))
                df[column_name][i+4] = str(df[column_name][i+4]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+4]
                break
            elif df[column_name][i+5]!='ENOAH' and df[column_name][i+5].strip()!='':
                df[column_name][i+5] = re.sub(' +', ' ', str(df[column_name][i+5]))
                df[column_name][i+5] = str(df[column_name][i+5]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+5]
                break
            else:
                df[column_name][i+6] = re.sub(' +', ' ', str(df[column_name][i+6]))
                df[column_name][i+6] = str(df[column_name][i+6]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+6]
                break
    if keyword == 'Zip Code:':
        for i in range(0,len(df)):
            if 'Zip Code:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i+1]
                break
    return required_field

def ZipCapital_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == '%#Ownership:':
        required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        df[column_name][i] = re.sub(' +', ' ', str(df[column_name][i]))
        df[column_name][i] = str(df[column_name][i]).replace('[ t:a:o ]','')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if df[column_name][i+1]!='ENOAH' and df[column_name][i+1]!='':
                df[column_name][i+1] = re.sub(' +', ' ', str(df[column_name][i+1]))
                df[column_name][i+1] = str(df[column_name][i+1]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!='ENOAH' and df[column_name][i+2]!='':
                df[column_name][i+2] = re.sub(' +', ' ', str(df[column_name][i+2]))
                df[column_name][i+2] = str(df[column_name][i+2]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+2]
                break
            elif df[column_name][i+3]!='ENOAH' and df[column_name][i+3]!='':
                df[column_name][i+3] = re.sub(' +', ' ', str(df[column_name][i+3]))
                df[column_name][i+3] = str(df[column_name][i+3]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+3]
                break
            else:
                df[column_name][i+4] = re.sub(' +', ' ', str(df[column_name][i+4]))
                df[column_name][i+4] = str(df[column_name][i+4]).replace('[ t:a:o ]','')
                required_field = df[column_name][i+4]
                break
    return required_field

def Fundible_Funding_Buss(df,keyword,column_name,req_val):#16/06
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('FundibleB.csv')
    for i in range(0,len(df)):
        if 'BusineSs' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('BusineSs','Business')
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name:':
                required_field = str(df[column_name][i]).replace(keyword,'')
                print('ouioui',required_field, len(required_field))
                if required_field == '' or len(required_field) <= 2:
                    required_field = df['Col_2'][i]
                    if required_field =='ENOAH':
                        required_field = df['Col_2'][i-1]
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').replace('?','').replace(':','')
                print('local1111',required_field)
                if required_field.strip() == '' and keyword == 'DBA:' and str(df['Col_1'][i+1])=='ENOAH' and str(df['Col_2'][i])=='ENOAH':
                    required_field = df['Col_2'][i+1]
                elif required_field.strip() == '' and keyword == 'DBA:' and df['Col_1'][i+1]!='ENOAH' and not 'Business Address' in str(df['Col_1'][i+1]):
                    required_field = df['Col_1'][i+1]
                elif required_field.strip() == '' and keyword == 'DBA:' and df['Col_1'][i+1]!='ENOAH' and 'Business Address' in str(df['Col_1'][i+1]):
                    required_field = df['Col_2'][i]

                if str(required_field).strip() == '' and keyword == 'Business Address:' and df['Col_2'][i]!='ENOAH':
                    required_field = df['Col_2'][i]
                if str(required_field).strip() == '' and (keyword == 'State:' or  keyword == 'Zip:'):
                    required_field = df[column_name][i-1]
            break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_1'][i]):
                required_field = df['Col_2'][i]
    required_field = str(required_field).replace('Zip:','').replace('\u0000','')
    return required_field

def Fundible_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            req_field = str(df[column_name][i]).replace(keyword,'').replace('Ownership %:','')
            required_field = req_field
            if str(required_field).strip() == '' and (keyword == 'Home Address:' or keyword == 'Email:' or keyword == 'Full Name:'):
                if owner_status == 'Owner #1':
                    required_field = df['Col_2'][i]
                else:
                    required_field = df['Col_4'][i]
            if str(required_field).strip() == '' and (keyword == 'SSN:' or keyword == 'Zip:' or keyword == 'Date of Birth:'):
                required_field = df[column_name][i-1]
            break     
    if (keyword == 'State:') and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if owner_status == 'Owner #1':
                if 'State' in str(df['Col_1'][i]):
                    required_field = df['Col_2'][i]
            else:
                if 'State' in str(df['Col_3'][i]):
                    required_field = df['Col_4'][i]
    

    if (keyword == 'Ownership %:') and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if owner_status == 'Owner #1':
                if 'Ownership %:' in str(df['Col_1'][i]):
                    required_field = df['Col_2'][i]
                    if 'Mobile' in str(required_field):
                        required_field = df['Col_1'][i].split('Ownership %:')[1]
                        if len(required_field)>=5:
                            required_field = df['Col_1'][i].split('Mobile')[0]
            else:
                if 'Ownership %:' in str(df['Col_3'][i]):
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i]).replace(keyword,'')

    if (keyword == 'Zip:') and 'SSN' in str(required_field):
        for i in range(0,len(df)):
            if owner_status == 'Owner #1':
                if 'Zip:' in str(df['Col_1'][i]):
                    required_field = df['Col_1'][i-1]
            else:
                if 'Zip:' in str(df['Col_3'][i]):
                    required_field = df['Col_4'][i-1]
                if 'Zip:' in str(df['Col_2'][i]):
                    required_field = df['Col_3'][i-1]
                else:
                    required_field ='ENOAH'
                    break

    if (keyword == 'SSN:') and 'ENOAH' in str(required_field):
        for i in range(0,len(df)):
            if owner_status == 'Owner #1':
                if 'SSN' in str(df['Col_1'][i]):
                    required_field = df['Col_2'][i-1]
                    if not re.search('[0-9]',str(required_field)):
                        required_field = df['Col_2'][i]
            else:
                if 'SSN' in str(df['Col_3'][i]):
                    required_field = df['Col_4'][i-1]
                    if required_field == 'ENOAH' or not re.search('[0-9]',str(required_field)):
                        required_field = df['Col_4'][i]

    required_field = str(required_field).replace('State:','').replace('(cid:0)','ENOAH')
    return required_field

def Fundible_Funding_2_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('fundible.csv')
    for i in range(0,len(df)):
        if 'LegaI' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('LegaI','Legal')
        if str(keyword) in str(df[column_name][i]):
            mystring = df[column_name][i]
            keyword = keyword
            before_keyword, keyword, after_keyword = mystring.partition(keyword)
            required_field = after_keyword
    return required_field

def Fundible_Funding_2_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('fundibleo.csv')
    for i in range(0,len(df)):
        if 'Securitv Number' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('Securitv Number','Security Number')
        if str(keyword) in str(df[column_name][i]):
            mystring = df[column_name][i]
            keyword = keyword
            before_keyword, keyword, after_keyword = mystring.partition(keyword)
            required_field = after_keyword
    if keyword == 'First Name:' and 'Last Name:' in required_field:
        mystring = required_field
        keyword = 'Last Name:'
        before_keyword, keyword, after_keyword = mystring.partition(keyword)
        required_field = before_keyword
    if keyword == 'City:' and 'State' in required_field:
        mystring = required_field
        keyword = 'State'
        before_keyword, keyword, after_keyword = mystring.partition(keyword)
        required_field = before_keyword
    if keyword == 'State:' and 'Zip' in required_field:
        mystring = required_field
        keyword = 'Zip'
        before_keyword, keyword, after_keyword = mystring.partition(keyword)
        required_field = before_keyword
    if keyword == 'Ownership %:' and 'Est FICO' in required_field:
        mystring = required_field
        keyword = 'Est FICO'
        before_keyword, keyword, after_keyword = mystring.partition(keyword)
        required_field = before_keyword
        
    return required_field


def Emerald_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Address:':
                if df[column_name][i+1]=='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+1]
            elif keyword == 'Business Start Date:':
                if re.search('[0-9]',str(df[column_name][i+1])):
                    required_field = df[column_name][i+1]
                elif df[column_name][i+1]=='ENOAH' and re.search('[0-9]',str(df[column_name][i+2])):
                    required_field = df[column_name][i+2]
                elif df[column_name][i+2]=='ENOAH' and re.search('[0-9]',str(df[column_name][i+3])):
                    required_field = df[column_name][i+3]
            else:
                required_field = df[column_name][i+1]
                if required_field =='ENOAH' and not 'City' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+2]
                if keyword == 'DBA:' and required_field == 'ENOAH' and df['Col_3'][i+1]!='ENOAH':
                    required_field = df['Col_3'][i+1]
                elif keyword == 'DBA:' and required_field =='ENOAH' and not 'State' in str(df['Col_3'][i+2]) and df['Col_3'][i+2]!='ENOAH':
                    required_field = df['Col_3'][i+2]
                if (keyword == 'Federal Tax ID:' or keyword == 'Business Phone:') and required_field == 'ENOAH' and re.search('[0-9]',str(df[column_name][i+2])):
                    required_field = df[column_name][i+2]
                break
    return required_field

def Emerald_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'State:' or keyword == 'Zip:':
                    if df[column_name][i+1]=='ENOAH':
                        required_field = df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if keyword == 'Ownership %:' and required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break  
            else:
                if keyword == 'State:' or keyword == 'Zip:':
                    if df[column_name][i+1]=='ENOAH':
                        required_field = df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
                else:
                    required_field = df[column_name][i+1]                 
    return required_field

def Prime_Plus_Capital_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in re.sub('\s+',' ',str(df[column_name][i])):
            req_field = re.sub('\s+',' ',str(df[column_name][i]))
            required_field = req_field.replace(keyword,'')
            if required_field=='':
                required_field = re.sub('\s+',' ',str(df[column_name][i-1]))
    return required_field

def Prime_Plus_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in re.sub('\s+',' ',str(df[column_name][i])):
            if keyword == 'City, State ZIP:' and req_val == 'City':
                req_field = str(re.sub('\s+',' ',str(df[column_name][i]))).replace(keyword,'')
                result = req_field.split(',')
                if result!=[]:
                    required_field = result[0]
                    break
                else:
                    required_field = 'ENOAH'
            elif keyword == 'City, State ZIP:' and req_val == 'State':
                req_field = str(re.sub('\s+',' ',str(df[column_name][i]))).replace(keyword,'')
                result = req_field.split(',')
                if result!=[] and result!=['']:
                    result[1] = re.sub('[0-9]',  ' ', result[1]) 
                    required_field = result[1]
                    break
                else:
                    required_field = 'ENOAH'
            elif keyword == 'City, State ZIP:' and req_val == 'Zip':
                req_field = str(re.sub('\s+',' ',str(df[column_name][i]))).replace(keyword,'')
                result = re.findall('\d+',req_field)
                if result!=[] and result!=['']:
                    required_field = result[0]
                else:
                    required_field = 'ENOAH'
            else:
                req_field = str(re.sub('\s+',' ',str(df[column_name][i]))).replace(keyword,'')
                required_field = req_field
                break    
    return required_field

def Hylender_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Phone:':
                required_field = df[column_name][i-1]
                break
            elif keyword == 'Date Business Established:':
                required_field = df['Col_4'][i-1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Hylender_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'Home Address:':
                    if df[column_name][i-1]!='' and df[column_name][i-1]!='ENOAH':
                        required_field = df[column_name][i-1]
                        break
                    else:
                        required_field = df[column_name][i-2]
                        break                        
                elif keyword == '% of Ownership:':
                    required_field = df['Col_5'][i-1]
                    break
                else:
                    required_field = df[column_name][i-1]
                    break
        if owner_status == 'Owner #2':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'Home Address:':
                    if df[column_name][i-1]!='' or df[column_name][i-1]!='ENOAH':
                        required_field = df[column_name][i-1]
                    else:
                        required_field = df[column_name][i-2]
                elif keyword == '% of Ownership:':
                    required_field = df['Col_5'][i-1]
                else:
                    required_field = df[column_name][i-1]
    return required_field

def ParkBusiness_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i-1]
            break
    return required_field

def ParkBusiness_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'E-Mail Address:':
        required_field = 'ENOAH'
    for i in range(1,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i-1]
                break
        if owner_status == 'Owner #2':
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i-1]
    return required_field

def Lendflow_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    if keyword == 'Mobile:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'City:':
                required_field = df[column_name][i-1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    return required_field

def Lendflow_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'E-Mail Address:' or keyword == 'Phone:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'First Name:' or keyword == 'Last Name:' or keyword == 'Ownership %:':
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = df[column_name][i-1]
                break                
    return required_field

def Circlecapital_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if column_name != 'Col_1':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Circlecapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Mobile:':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+1]!= 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!= 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break               
    return required_field

def FlexCap_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Start Date:':
                if df[column_name][i+1] == 'ENOAH' and df['Col_3'][i+1] != 'ENOAH':
                    required_field = df['Col_3'][i+1]
                elif (df[column_name][i+1] == 'ENOAH' and df[column_name][i+2]!='ENOAH'):
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+1]
            elif df[column_name][i+1]!= 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!= 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break   
    return required_field

def FlexCap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Home Phone:':
                    required_field = df['Col_6'][i]
                    break
                elif keyword == 'Ownership %:':
                    required_field = df[column_name][i+1]
                    break
                elif keyword == 'Street Address:':
                    required_field = df['Col_2'][i]
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break  
            else:
                if keyword == 'Home Phone:':
                    required_field = df['Col_6'][i]
                elif keyword == 'Ownership %:':
                    required_field = df[column_name][i+1]
                elif keyword == 'Street Address:':
                    required_field = df['Col_2'][i]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
    return required_field

def Centcred_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Legal Business Name':
                required_field = df['Col_1'][i+1]
                break
            elif df[column_name][i+1]!= 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!= 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break   
    return required_field

def Centcred_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!= 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!= 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break   
    return required_field

def SouthCoast_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Address':
                required_field = df['Col_3'][i-1]
                break
            elif df[column_name][i-1]!= 'ENOAH':
                required_field = df[column_name][i-1]
                break
            else:
                required_field = df[column_name][i-2]
                break   
    return required_field

def SouthCoast_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Last Name' or keyword == 'SSN':
                    required_field = df['Col_3'][i-1]
                    break
                elif keyword == 'Street Address' or keyword == 'Cell Phone':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_2'][i-1]
                        break
                    else:
                        required_field = df[column_name][i-1]
                        break
                elif df[column_name][i-1]!= 'ENOAH':
                    required_field = df[column_name][i-1]
                    break
                else:
                    required_field = df[column_name][i-2]
                    break    
            else:
                if keyword == 'Last Name' or keyword == 'SSN':
                    required_field = df['Col_6'][i-1]
                    break
                elif keyword == 'Street Address' or keyword == 'Cell Phone':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_5'][i-1]
                        break
                    else:
                        required_field = df[column_name][i-1]
                        break
                elif df[column_name][i-1]!= 'ENOAH':
                    required_field = df[column_name][i-1]
                    break
                else:
                    required_field = 'ENOAH'
    return required_field

def MyCompany_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Physical Location Phone':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                break   
    return required_field

def MyCompany_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email':
        required_field = 'ENOAH'
    if keyword == '% Ownership':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'First Name:' or keyword == 'Last Name:':
                required_field = str(df[column_name][i]).replace(keyword,'')
            else:
                required_field = df[column_name][i+1]
    return required_field

def StrongCapital_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            if keyword == 'Business DBA (if applicable):' and 'Type of Business' in required_field:
                required_field = 'ENOAH'
            break   
    return required_field

def StrongCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
    return required_field

def PGFunding_Buss(df,keyword,column_name):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Legal':
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
            elif keyword == 'Physical Address (no PO Boxes)':
                required_field = df['Col_3'][i-1]
            elif keyword == 'Company Phone:' or keyword == 'Business Inception Date:':
                required_field = df['Col_2'][i]
                break
            else:
                required_field = df[column_name][i-1]
                break   
    return required_field

def PGFunding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Home address (no PO Boxes):':
                    required_field = df['Col_2'][i]
                    break
                elif keyword == 'Email:' or keyword == 'Home Phone:' or keyword == 'SSI Number:':
                    required_field = df[column_name][i-1]
                    break
                elif keyword == 'Business ownership %:':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
            else:
                if keyword == 'Home address (no PO Boxes):':
                    required_field = df['Col_4'][i]
                    break
                elif keyword == 'Email:' or keyword == 'Home Phone:' or keyword == 'SSI Number:':
                    required_field = df[column_name][i-1]
                    break
                elif keyword == 'Business ownership %:':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
    return required_field

def MonetCapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Address' and req_val == 'City':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[0]
                break
            elif keyword == 'Business Address' and req_val == 'State':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[1]
                break
            elif keyword == 'Business Address' and req_val == 'Zip':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[2]
                break
            else:
                required_field = df['Col_2'][i]
                break   
    return required_field

def MonetCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Home Address' and req_val == 'City':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[0]
                break
            elif keyword == 'Home Address' and req_val == 'State':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[1]
                break
            elif keyword == 'Home Address' and req_val == 'Zip':
                req_field = df['Col_2'][i+1].split(',')
                required_field = req_field[2]
                break
            elif keyword == '% of Ownership':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == 'ENOAH' or str(required_field).strip() == '':
                    required_field = df['Col_2'][i]
                break
            else:
                required_field = df['Col_2'][i]
                break 
    return required_field

def CapitalInfusion_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Start Date (MM/YYYY):':
                required_field = df['Col_2'][i]
                break
            else:
                required_field = df[column_name][i-1]
                if keyword == 'Business Legal Name:' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                break   
    return required_field

def CapitalInfusion_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Zip:':
                    if df[column_name][i].replace(keyword,'')!='':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                        break
                    else:
                        required_field = df[column_name][i+1]
                        break
                else:
                    if df[column_name][i].replace(keyword,'')!='':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                        break
                    else:
                        required_field = df[column_name][i-1]
                        if keyword == 'Address:' and required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                        if keyword == 'Principle Owner Name:' and required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                        break   
            else:
                if keyword == 'Zip:':
                    if df[column_name][i].replace(keyword,'')!='':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                    else:
                        required_field = df[column_name][i+1]
                else:
                    if df[column_name][i].replace(keyword,'')!='':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                    else:
                        required_field = df[column_name][i-1]
    return required_field

def BankrollCapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'ADDRESS' and req_val == 'City':
                req_field = str(df[column_name][i+1]).split(',')
                n = len(req_field)
                if len(req_field[n-3].strip())==2:
                    required_field = req_field[n-4]
                else:
                    required_field = req_field[n-3]
                break
            elif keyword == 'ADDRESS' and req_val == 'State':
                req_field = str(df[column_name][i+1]).split(',')
                n = len(req_field)
                required_field = req_field[n-1]
                break
            elif keyword == 'ADDRESS' and req_val == 'Zip':
                req_field = str(df[column_name][i+1]).split(',')
                n = len(req_field)
                required_field = req_field[n-2]
                break
            else:
                required_field = df[column_name][i+1]
                break   
    return required_field

def BankrollCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:' or keyword == 'Phone:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'ADDRESS' and req_val == 'City':
                    if df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] == 'ENOAH':
                        data = df[column_name][i+1]
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+3])
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+2])
                        req_field = str(data).split(',')
                    n = len(req_field)
                    required_field = req_field[n-3]
                    break
                elif keyword == 'ADDRESS' and req_val == 'State':
                    if df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] == 'ENOAH':
                        data = df[column_name][i+1]
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+3])
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+2])
                        req_field = str(data).split(',')

                    n = len(req_field)
                    required_field = req_field[n-1]
                    break
                elif keyword == 'ADDRESS' and req_val == 'Zip':
                    if df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] == 'ENOAH':
                        data = df[column_name][i+1]
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] == 'ENOAH' and df[column_name][i+3] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+3])
                        req_field = str(data).split(',')
                    elif df[column_name][i+2] != 'ENOAH':
                        data = str(df[column_name][i+1]) + str(df[column_name][i+2])
                        req_field = str(data).split(',')
                    n = len(req_field)
                    required_field = req_field[n-2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    break
            else:
                if keyword == 'ADDRESS' and req_val == 'City':
                    req_field = str(df[column_name][i+1]).split(',')
                    n = len(req_field)
                    if re.search('[0-9]',str(req_field[n-2])):
                        required_field = req_field[n-3]
                    else:
                        required_field = req_field[n-2]
                elif keyword == 'ADDRESS' and req_val == 'State':
                    req_field = str(df[column_name][i+1]).split(',')
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'ADDRESS' and req_val == 'Zip':
                    req_field = str(df[column_name][i+1]).split(',')
                    n = len(req_field)
                    required_field = req_field[n-2]
                else:
                    required_field = df[column_name][i+1]
    return required_field
def Simplified_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            if keyword =='Business Legal Name*:':
                required_field = df['Col_1'][i+1]
                break
            else:
                required_field = df['Col_1'][i+2]
                
            if keyword =='Business DBA Name:':
                required_field = df['Col_4'][i+1]
                break
            if keyword =='Federal Tax ID:':
                required_field = df['Col_4'][i+4]
                break
            else:
                required_field = df['Col_4'][i+3]
                
            if keyword =='City:':
                required_field = df['Col_3'][i+2]
                break
            if keyword =='State:':
                required_field = df['Col_3'][i+2]
                break
            if keyword =='Zip Code:':
                required_field = df['Col_4'][i+1]
                break
            
            
            if df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break   
    return required_field

def Simplified_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:':
        required_field = 'ENOAH'
    if keyword == 'OWNERSHIP %':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            if keyword =="SS#*:":
                required_field = df['Col_3'][i+1]
                break
            if keyword =="Date of Birth*:":
                required_field = df['Col_5'][i+3]
                break
            if keyword =="Home Address*:":
                required_field = df['Col_1'][i+2]
                break
            if keyword =="City:":
                required_field = df['Col_5'][i+2]
                break
            if keyword =="State:":
                required_field = df['Col_6'][i+2]
                break
            if keyword =="Zip Code:":
                required_field = df['Col_7'][i+2]
                break
            
            if owner_status == 'Owner #2':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                
        elif df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
        else:
                required_field = df[column_name][i+3]
                break
                
    return required_field

def BridgeCapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            break
    return required_field

def BridgeCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status =='Owner #1':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
    return required_field

def Crown_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            if (req_val == 'Address' or req_val == 'Name') and required_field == 'ENOAH':
                required_field = df['Col_2'][i+1]
            if req_val == 'DBA' and required_field == 'ENOAH':
                required_field = df['Col_4'][i+1]
            break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Tax ID/EIN:' in str(df['Col_3'][i]):
                required_field = df['Col_4'][i+1]
                break
    if req_val == 'StartDate' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Business Start Date:' in str(df['Col_4'][i]):
                required_field = df['Col_3'][i+1]
                break
    return required_field

def Crown_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status =='Owner #1':
                if keyword == 'First Name:' or keyword == 'Last Name:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
                    break
                else:
                    required_field = str(df[column_name][i+1]).replace('-           -','ENOAH')
                    if (req_val == 'Date Of Birth' or req_val == 'SS#') and required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                    if req_val == 'Owner' and required_field =='ENOAH':
                        required_field = df['Col_5'][i+1]
                    break
            else:
                if keyword == 'First Name:' or keyword == 'Last Name:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
                else:
                    required_field = df[column_name][i+1]
                    if req_val == 'Owner' and required_field =='ENOAH':
                        required_field = df['Col_5'][i+1]
                        
    if req_val == 'Owner' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Ownership %:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i+2]
                break
    if req_val == 'Date Of Birth' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'DOB:' in str(df['Col_1'][i]):
                required_field = df['Col_2'][i+1]
                break
    return required_field

def BizFund_Buss(df,keyword,column_name,req_val):
    required_field = 'ENOAH'
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'City' or keyword == 'State' or keyword == 'Zip Code':
                required_field = df[column_name][i-1]
                if keyword == 'City' and len(df[column_name][i].split())>1:
                    required_field = df[column_name][i]
                break
            elif keyword == 'Business Start Date':
                required_field = df['Col_6'][i]
                break
            elif keyword == 'DBA':
                if df['Col_3'][i+1] != 'ENOAH':
                    required_field = df['Col_3'][i+1]
                    break
                elif df['Col_2'][i] !='ENOAH':
                    required_field = df['Col_2'][i]
                    break
                else:
                    required_field = df['Col_3'][i]
                    break
            else:
                if df['Col_2'][i] !='ENOAH':
                    required_field = df['Col_2'][i]
                    break
                else:
                    required_field = df['Col_3'][i]
                    break
    return required_field

def BizFund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status =='Owner #1':
                if (keyword == 'Home Address' and req_val == 'Home Address') or keyword == 'Last Name':
                    required_field = str(df['Col_2'][i-1])
                    break
                elif keyword == 'D.O.B.':
                    required_field = df['Col_2'][i+1]
                    break
                elif keyword == 'Home Address' and req_val == 'City':
                    req_field = str(df['Col_2'][i-1]).split()
                    if df['Col_2'][i+1]!='ENOAH' and re.search('[0-9]',str(df['Col_2'][i+1])):
                        req_det = str(df['Col_2'][i-1]) + ' ' + str(df['Col_2'][i+1])
                        req_field = str(req_det).split()
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        if not re.search('[0-9]',str(req_field[n-2])):
                            required_field = ' '.join(req_field[n-4:n-2])
                        else:
                            required_field = ' '.join(req_field[n-4:n-3])
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                elif keyword == 'Home Address' and req_val == 'State':
                    req_field = str(df['Col_2'][i-1]).split()
                    if df['Col_2'][i+1]!='ENOAH' and re.search('[0-9]',str(df['Col_2'][i+1])):
                        req_det = str(df['Col_2'][i-1]) + ' ' + str(df['Col_2'][i+1])
                        req_field = str(req_det).split()
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        if not re.search('[0-9]',str(req_field[n-2])):
                            required_field = req_field[n-2]
                        else:
                            required_field = req_field[n-3]
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                elif keyword == 'Home Address' and req_val == 'Zip':
                    req_field = str(df['Col_2'][i-1]).split()
                    if df['Col_2'][i+1]!='ENOAH' and re.search('[0-9]',str(df['Col_2'][i+1])):
                        req_det = str(df['Col_2'][i-1]) + ' ' + str(df['Col_2'][i+1])
                        req_field = str(req_det).split()
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        if not re.search('[0-9]',str(req_field[n-2])):
                            required_field = req_field[n-1]
                        else:
                            required_field = req_field[n-2]
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                else:
                    required_field = df['Col_2'][i]
                    if keyword == 'Social Security No.' and '123-45-6789' in required_field:
                        required_field = df['Col_2'][i+1]
                    break
            else:
                if (keyword == 'Home Address' and req_val == 'Home Address') or keyword == 'Last Name':
                    required_field = str(df['Col_4'][i-1])
                    if keyword == 'Last Name' and df['Col_3'][i-1] == 'First Name' and not 'Social Security' in df['Col_3'][i-1]:
                        required_field = str(df['Col_4'][i+1])
                    break
                elif keyword == 'D.O.B.':
                    required_field = df['Col_4'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
                    break
                elif keyword == 'Home Address' and req_val == 'City':
                    req_field = str(df['Col_4'][i-1]).split()
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        required_field = ' '.join(req_field[n-4:n-2])
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                    if re.search('[0-9]',str(required_field)):
                        required_field = 'ENOAH'
                elif keyword == 'Home Address' and req_val == 'State':
                    req_field = str(df['Col_4'][i-1]).split()
                    
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        required_field = req_field[n-2]
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                elif keyword == 'Home Address' and req_val == 'Zip':
                    req_field = str(df['Col_4'][i-1]).split()
                    if req_field!=['ENOAH']:
                        n = len(req_field)
                        required_field = req_field[n-1]
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                else:
                    required_field = df['Col_4'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i-1]
                    break
    if re.search('[0-9]',str(required_field)) and req_val == 'City':
        required_field = 'ENOAH'
    return required_field
    
def Biz_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DBA' or req_val == 'StartDate' or req_val == 'City' or req_val == 'Zip':
                required_field = str(df['Col_4'][i])
                break
            elif req_val == 'Name':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i-1])+' '+str(df['Col_2'][i+1])
                break
            elif req_val == 'TaxID' or req_val == 'Address' or req_val == 'State':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i+1])
                break
                
    return required_field

def Biz_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Home Address':
                required_field = str(df['Col_2'][i-1])
                break
            elif req_val == 'City' or req_val == 'State':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'Zip' and not re.search('[a-zA-Z]',str(df['Col_2'][i+1])):
                required_field = str(df['Col_2'][i+1])
                break
            else:
                required_field = str(df['Col_2'][i])
                break
    if req_val == 'City':
        required_field = required_field.split(',')[0]
    if req_val == 'State':
        required_field = required_field.split(',')[-1]
    return required_field

def CapFront_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = str(df[column_name][i]).replace('( 9 digits)','').replace(' (9 digits)','').replace(keyword,'')
            if required_field.strip() == '':
                required_field = df[column_name][i-1]
                if keyword == 'DBA Name' and required_field == 'ENOAH' and df['Col_3'][i-1]!='ENOAH':
                    required_field = df['Col_3'][i-1]
                if keyword == 'DBA Name' and required_field == 'ENOAH' and df['Col_3'][i-1]=='ENOAH':
                    required_field = df['Col_3'][i]
                if required_field == 'ENOAH' or 'Address' in str(required_field):
                    required_field = df[column_name][i+1]
                    if req_val == 'StartDate' and 'Landlord' in str(required_field):
                        required_field = str(df['Col_3'][i]).replace('Owned Since','')
                if keyword == 'Federal Tax ID Number' and 'Business' in str(required_field):
                    required_field = df[column_name][i+1].replace('( 9 digits)','').replace(' (9 digits)','')
                if keyword == 'Federal Tax ID Number' and 'ENOAH' in str(required_field):
                    required_field = df[column_name][i-2].replace('( 9 digits)','').replace(' (9 digits)','')
                break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal T' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).replace('Federal T ax ID Number ( 9 digits','').replace('9 digit','')
                if required_field == '':
                    required_field = str(df['Col_1'][i-1])
    print('killer....',req_val,required_field)
    if req_val == 'TaxID' and required_field == 'LLC':##23/07
        required_field = str(df['Col_1'][i+1])
        
    return required_field

def CapFront_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status =='Owner #1':
                if keyword == 'Email':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df['Col_3'][i-1]
                        if required_field == 'ENOAH' or ('@' and '.com') not in str(df['Col_3'][i-1]):
                            required_field = df['Col_3'][i+1]
                        if df['Col_3'][i-1] == 'ENOAH' or ('@' and '.com') in str(df[column_name][i-1]):
                            required_field = df[column_name][i-1]
                        if 'Personal Credit' in str(required_field) or ('@' and '.com') in df['Col_3'][i] or ('@' and '.COM') in df['Col_3'][i]:
                            required_field = df['Col_3'][i]
                            required_field = str(required_field).replace('Are you a US Citizen?Yes','')
                            required_field = str(required_field).replace('Are you a US Citizen?No','')
                            required_field = str(required_field).replace('YesAre you a US Citizen?','')
                            required_field = str(required_field).replace('NoAre you a US Citizen?','')
                        if '..' in required_field:
                            required_field = str(required_field).replace('..','.')
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH' or 'Address' in str(required_field):
                            required_field = df[column_name][i+1]
                        if (keyword == 'Social Security Number' or keyword == 'Birth Date') and required_field != 'ENOAH' and not re.search('[0-9]',str(required_field)):
                            required_field = df[column_name][i+1]
                        break
                    break
            else:
                if keyword == 'Email':
                    required_field = df['Col_3'][i-1]
                    if 'Personal Credit' in str(required_field):
                        required_field = df['Col_3'][i]
                        required_field = str(required_field).replace('Are you a US Citizen?Yes','')
                        required_field = str(required_field).replace('Are you a US Citizen?No','')
                        required_field = str(required_field).replace('YesAre you a US Citizen?','')
                        required_field = str(required_field).replace('NoAre you a US Citizen?','')
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH' or 'Address' in str(required_field):
                            required_field = df[column_name][i+1]
                            if keyword == 'Birth Date' and '/' not in required_field:
                                required_field = df[column_name][i-2]
                        if (keyword == 'Social Security Number' or keyword == 'Birth Date') and required_field != 'ENOAH' and not re.search('[0-9]',str(required_field)):
                            required_field = df[column_name][i+1]
    return required_field

def Taycor_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'DBA Name' or keyword == 'Date Founded':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def Taycor_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Phone Number':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            df[column_name][i] = str(df[column_name][i]).replace('(1)','')
            df[column_name][i] = str(df[column_name][i]).replace('(2)','')
            df[column_name][i] = str(df[column_name][i]).replace('(3)','')
            if owner_status =='Owner #1':
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
    return required_field

def Carrera_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Years:':
                if 'Length Of' in str(df[column_name][i-1]):
                    req1=str(df['Col_1'][i])
                    req2=str(str(df[column_name][i]))
                    required_field = str(re.sub('[a-zA-Z:%.,() ]', '',req1)) +'/'+ str(re.sub('[a-zA-Z:%.,() ]', '',req2))
                    required_field = str(required_field).replace('ENOAH','1')
                else:
                    required_field = str(df['Col_1'][i-1]) +'/'+ str(df[column_name][i-1])
                    required_field = str(required_field).replace('ENOAH','1')
                break
            elif keyword == 'DBA:':
                if not 'BUSINESS INFORMATION' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH' and df['Col_4'][i-1]!='ENOAH':
                        required_field = df['Col_4'][i-1]
                elif df['Col_4'][i]=='ENOAH' and df['Col_4'][i+1]!='ENOAH' and not 'State' in df['Col_4'][i+2]:
                    required_field = df['Col_4'][i+1]
                elif df[column_name][i] != 'ENOAH' and (df['Col_4'][i] != 'ENOAH' or df['Col_4'][i] == 'ENOAH'):
                    req = df[column_name][i].replace(keyword,'')
                    required_field = str(req +' '+ df['Col_4'][i]).replace('ENOAH','')
                else:
                    required_field = df['Col_4'][i]
                break
            elif keyword == 'Legal/Corporate Name:' or keyword == 'Physical Address:' or keyword == 'Telephone Number:':
                if df['Col_2'][i]=='ENOAH' and df['Col_2'][i-1]!='ENOAH':
                    required_field = df['Col_2'][i-1]
                    break
                elif df['Col_2'][i]=='ENOAH' and df['Col_2'][i-1]=='ENOAH':
                    required_field = df[column_name][i-1]
                    break                    
                else:
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i+1]
                    break
            elif keyword == 'Federal Tax ID:':
                required_field = df['Col_5'][i]
                if required_field =='ENOAH':
                    required_field = df['Col_5'][i-1]
                break
            else:
                required_field = str(df[column_name][i-1])
                break
    return required_field

def Carrera_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email Address:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status =='Owner #1':
                if keyword == 'Applicant:' or keyword == 'Ownership % :':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field =='':
                        required_field = str(df[column_name][i+1])
                        if required_field =='ENOAH':
                            required_field = str(df['Col_2'][i+1])
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '' and req_val == 'Home Address':
                        required_field = df['Col_2'][i]
                    elif required_field == '':
                        required_field = df[column_name][i+1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+2]
                    break
            else:
                if keyword == 'Applicant 2:' or keyword == 'Ownership % :':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i+1])
                elif keyword == 'Home Address:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df['Col_2'][i])
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i+1]
                elif keyword == 'SSN:':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+2]
    return required_field

def Fundmerica_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1] != 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2] != 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break
    return required_field

def Fundmerica_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email Address:' or keyword == 'Cell #:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1] != 'ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2] != 'ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break
    return required_field

def Platform_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Physical Street Address:' and df[column_name][i+1] == 'ENOAH':
                required_field = df['Col_3'][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i+2]
                break
            elif df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            elif keyword == 'Business Legal Name:' and (df[column_name][i+2]=='ENOAH' or 'EIN' in df[column_name][i+2]) and df['Col_2'][i+1]!='ENOAH':
                required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business Legal Name:' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i+1]=='ENOAH' and df['Col_2'][i+2]=='ENOAH':
                required_field = df[column_name][i+3]
                break

            elif keyword == 'Business Legal Name:' and df[column_name][i+2]=='ENOAH':
                required_field = df['Col_2'][i+2]
                break
            else:
                required_field = df[column_name][i+2]
                if keyword == 'Business Phone:' and len(str(required_field).split())>1:
                    required_field = required_field.split()[0]  
                break
    return required_field

def Platform_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Ownership % :':
        required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Date of Birth:' and df[column_name][i+1] == 'ENOAH' and not '(' in str(df['Col_5'][i+1]).split()[0]:
                    required_field = str(df['Col_5'][i+1]).split()[0]
                    break
                elif keyword == 'Phone:' and len(str(df[column_name][i+1]).split()) >1 and not '(' in str(df['Col_5'][i+1]).split()[0]:
                    required_field = str(df[column_name][i+1]).split()[1:]
                    break
                elif keyword == 'Home Address:' and df[column_name][i+1] =='ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i+2]
                    if len(required_field.split())<2:
                        required_field = df[column_name][i+2]
                    break
                elif keyword == '% Ownership:' and df[column_name][i+1] =='ENOAH':
                    required_field = df[column_name][i+2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if df[column_name][i+1] == 'ENOAH' and keyword == 'Date of Birth:':
                        required_field = df['Col_5'][i+1].split()[0]
                    if df[column_name][i+1] == 'ENOAH' and keyword == 'City:':
                        required_field = df['Col_3'][i+1]    
                    if keyword == 'State:':
                        required_field = re.sub('[0-9]', '', df[column_name][i+1])
                    if keyword == 'Zip:' and required_field == 'ENOAH':
                        required_field = re.sub('[a-zA-Z&]',  '', df['Col_4'][i+1])          
                    if keyword == 'Phone:' and len(str(required_field).split())>1:
                        required_field = required_field.split()[1]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i+2]
                    break     
            else:
                if keyword == 'Date of Birth:' and df[column_name][i+1] == 'ENOAH' and not '(' in str(df['Col_5'][i+1]).split()[0]:
                    required_field = str(df['Col_5'][i+1]).split()[0]
                elif keyword == 'Phone:' and len(str(df[column_name][i+1]).split()) >1 and not '(' in str(df['Col_5'][i+1]).split()[0]:
                    required_field = str(df[column_name][i+1]).split()[1:]
                elif keyword == 'Home Address:' and df[column_name][i+1] =='ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i+2]
                elif keyword == '% Ownership:' and df[column_name][i+1] =='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+1]
                    if df[column_name][i+1] == 'ENOAH' and keyword == 'Date of Birth:':
                        required_field = df['Col_5'][i+1].split()[0]
                    if df[column_name][i+1] == 'ENOAH' and keyword == 'City:':
                        required_field = df['Col_3'][i+1]
                    if df[column_name][i+1] == 'ENOAH' and keyword == 'State:':
                        required_field = df[column_name][i+1]
                        required_field = re.sub('[0-9]',  '', required_field).strip()  
                    if keyword == 'Phone:' and len(str(required_field).split())>1:
                        required_field = required_field.split()[1]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i+2]
    if keyword == 'City:' and len(str(required_field).split())>2:
        city =  str(required_field).split()
        n = len(city)
        required_field = city[n-1]
    return required_field

def August_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Address:' and req_val == 'City':
                req_val = df['Col_2'][i+1].split()
                n = len(req_val)
                required_field = ' '.join(req_val[0:n-1])
                break
            elif keyword == 'Business Address:' and req_val == 'State':
                req_val = df['Col_2'][i+1].split()
                n = len(req_val)
                required_field = req_val[n-1]
                break
            elif keyword == 'Business Address:' and req_val == 'Zip':
                required_field = df['Col_2'][i+2]
                break
            else:
                required_field = df['Col_2'][i]
                break
    return required_field

def August_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email Address:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Home Address:' and req_val == 'City':
                req_val = df['Col_2'][i+1].split()
                n = len(req_val)
                required_field = ' '.join(req_val[0:n-1])
                break
            elif keyword == 'Home Address:' and req_val == 'State':
                req_val = df['Col_2'][i+1].split()
                n = len(req_val)
                required_field = req_val[n-1]
                break
            elif keyword == 'Home Address:' and req_val == 'Zip':
                required_field = df['Col_2'][i+2]
                break
            else:
                required_field = df['Col_2'][i]
                break
    return required_field

def EMC_Buss(df,keyword,column_name,req_val):
    required_field = 'ENOAH'
    keyword = keyword.strip()
    if keyword == 'Phone':
        required_field = 'ENOAH'    
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if str(required_field).strip() == '' or str(required_field).strip() == ':' or str(required_field).strip() == 'Zip:':
                required_field = df[column_name][i+1]
                if req_val == 'Zip' and required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
            if required_field == 'ENOAH' and keyword == 'Physical Business Address:' and len(str(df['Col_2'][i+1]).split())>2:
                required_field = df['Col_2'][i+1]
            if keyword == 'City:' and len(str(required_field).split())>2:
                req_field = str(required_field).split()
                n = len(req_field)
                required_field = req_field[n-3]
            if keyword == 'State:' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>2:
                req_field = str(df['Col_2'][i+1]).split()
                n = len(req_field)
                required_field = req_field[n-2]
            if keyword == 'Zip:' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>2:
                req_field = str(df['Col_2'][i+1]).split()
                n = len(req_field)
                required_field = req_field[n-1]
            if keyword == 'City:' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>22:
                req_field = str(df['Col_1'][i+1]).split()
                n = len(req_field)
                required_field = req_field[n-3]
            if keyword == 'State:' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2:
                req_field = str(df['Col_1'][i+1]).split()
                n = len(req_field)
                required_field = req_field[n-2]
            if keyword == 'Zip:' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2:
                req_field = str(df['Col_1'][i+1]).split()
                n = len(req_field)
                required_field = req_field[n-1]

            if required_field == 'ENOAH':
                required_field = df[column_name][i+2]
            break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in df['Col_3'][i]:
                required_field = df['Col_4'][i+1]
    required_field = str(required_field).replace('Partnership','ENOAH')
    return required_field

def EMC_Owner(df,owner_status,keyword,column_name,req_val):
    required_field = 'ENOAH'
    keyword = keyword.strip()
    if keyword == 'Email Address:' or keyword == 'Cell Phone:':
        required_field = 'ENOAH'
    if keyword == 'Ownership %':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip() == '' or str(required_field).strip() == ':':
                    required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip() == '' or str(required_field).strip() == ':':
                    required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]

    if keyword == 'Name:':
        req_field = str(required_field).split(',')
        required_field = req_field[0]
    return required_field

def MorganCash_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+1]=='ENOAH' and req_val == 'DBA':
                required_field = df['Col_3'][i+1]
                break
            elif df[column_name][i+1]=='ENOAH' and req_val == 'Name':
                required_field = df['Col_2'][i+1]
                break

            elif df[column_name][i+1]=='ENOAH' and req_val == 'TaxID':
                required_field = df['Col_4'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            elif df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
            elif keyword == 'State:' and df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i+2] != 'ENOAH':
                req_field = df['Col_2'][i+2].split()
                n = len(req_field)
                if n>2:
                    required_field = req_field[n-2]
                    break
            elif keyword == 'Zip:' and df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i+2] != 'ENOAH':
                if df['Col_3'][i+1]!='ENOAH' and re.search('[0-9]', str(df['Col_3'][i+1])):
                    req_field = df['Col_3'][i+1].split()
                elif df['Col_2'][i+2] != 'ENOAH' and re.search('[0-9]', str(df['Col_2'][i+2])):
                    req_field = df['Col_2'][i+2].split()
                elif df['Col_3'][i+2]!='ENOAH' and re.search('[0-9]', str(df['Col_3'][i+2])):
                    req_field = df['Col_3'][i+2].split()                  
                n = len(req_field)
                if n>1 and len(req_field[n-1])>3:
                    required_field = req_field[n-1]
                    break
            else:
                required_field = 'ENOAH'
                break
    if keyword == 'City:' and len(required_field.split())>2:
        required_field = str(required_field).split()[0]
    return required_field

def MorganCash_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Email Address:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Home Address:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1]
                    else:
                        required_field = df[column_name][i+2]
                    break
                elif keyword == 'City:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_1'][i+1]!='ENOAH':
                        req_field = df['Col_1'][i+1].split()
                    else:
                        req_field = df['Col_2'][i+1].split()
                    n= len(req_field)
                    if n>3:
                        required_field = req_field[n-3]
                    break
                elif keyword == 'State:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_1'][i+1]!='ENOAH':
                        req_field = df['Col_1'][i+1].split()
                    else:
                        req_field = df['Col_2'][i+1].split()
                    n= len(req_field)
                    if n>2:
                        required_field = req_field[n-2]
                    break
                elif keyword == 'Zip:' and df[column_name][i+1] == 'ENOAH':
                    if not 'Personal Credit' in str(df[column_name][i+2]) and re.search('[0-9]',str(df[column_name][i+2])):
                        required_field = df[column_name][i+2]
                    else:
                        if df['Col_4'][i+1]!='ENOAH':
                            req_field = df['Col_4'][i+1].split()
                        elif df['Col_1'][i+1]!='ENOAH':
                            req_field = df['Col_1'][i+1].split()
                        else:
                            req_field = df['Col_2'][i+1].split()
                        n= len(req_field)
                        if n>1 and len(req_field[n-1])>3:
                            required_field = req_field[n-1]        
                    break
                else:
                    required_field = df[column_name][i+1]
                    if keyword == 'Corporate Officer/Owner' and required_field == 'ENOAH' and required_field != 'Name:':
                        required_field = df['Col_2'][i+1]
                    if keyword == 'Ownership %:' and required_field == 'ENOAH':
                        required_field = df['Col_5'][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_5'][i+2] 
                    if required_field == 'ENOAH' or required_field == 'Name:':
                        required_field = df[column_name][i+2]

                    break
            else:
                if keyword == 'Home Address:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1]
                    else:
                        required_field = df[column_name][i+2]
                elif keyword == 'City:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_1'][i+1]!='ENOAH':
                        req_field = df['Col_1'][i+1].split()
                    else:
                        req_field = df['Col_2'][i+1].split()
                    n= len(req_field)
                    if n>3:
                        required_field = req_field[n-3]
                elif keyword == 'State:' and df[column_name][i+1] == 'ENOAH':
                    if df['Col_1'][i+1]!='ENOAH':
                        req_field = df['Col_1'][i+1].split()
                    else:
                        req_field = df['Col_2'][i+1].split()
                    n= len(req_field)
                    if n>2:
                        required_field = req_field[n-2]
                elif keyword == 'Zip:' and df[column_name][i+1] == 'ENOAH':
                    if not 'Personal Credit' in str(df[column_name][i+2]) and re.search('[0-9]',str(df[column_name][i+2])):
                        required_field = df[column_name][i+2]
                    else:
                        if df['Col_4'][i+1]!='ENOAH':
                            req_field = df['Col_4'][i+1].split()
                        elif df['Col_1'][i+1]!='ENOAH':
                            req_field = df['Col_1'][i+1].split()
                        else:
                            req_field = df['Col_2'][i+1].split()
                        n= len(req_field)
                        if n>1 and len(req_field[n-1])>3:
                            required_field = req_field[n-1]   
                else:
                    required_field = df[column_name][i+1]
                
    if keyword == 'State:' and len(required_field.split())>1:
        required_field = str(required_field).split()[0]
    required_field = str(required_field).replace('Home Address:','')
    required_field = str(required_field).replace('SSN:','')
    required_field = str(required_field).replace('Phone #:','')
    return required_field


def FirstUnion_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    if keyword == 'DBA:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                if keyword == 'Business Legal Name:' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business Phone':
                if not 'Industry' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            else:
                required_field = df[column_name][i+2]
                if keyword == 'Business Legal Name:' and required_field == 'Street:':
                    required_field = df['Col_2'][i+1]
                break
    return required_field

def FirstUnion_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
    return required_field

def SteadyCapital_Buss_priya(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            if keyword == 'Tax ID Number':
                required_field = df['Col_2'][i+4]
                if not re.search('[0-9]',str(required_field)):
                    required_field = df['Col_3'][i+3]
                elif required_field == '':
                    required_field = df['Col_3'][i+2]
#                     break
                elif required_field=='ENOAH':
                    required_field = df['Col_2'][i+2]
                else:
                     required_field = df[column_name][i+2]
            elif keyword == 'Legal Business Name' and df[column_name][i+1] == 'ENOAH':
                required_field = df['Col_2'][i+1]
                break
                if df[column_name][i+1]=='ENOAH':
                    required_field = df[column_name][i+1]
                if df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+3]
#                     break
            elif keyword =='DBA Business Name' and df[column_name][i+1] =='ENOAH':
                 required_field = df['Col_1'][i+2]
            elif keyword == 'Business Address':
                if df[column_name][i+1]!='ENOAH' and not 'Seasonal Business' in df[column_name][i+2]:
                    required_field = df[column_name][i+1]+ ' ' + str(df[column_name][i+2])
                elif df[column_name][i+1]!='ENOAH' and 'Seasonal Business' in df[column_name][i+2]:
                    required_field = df[column_name][i+1]
                elif 'Seasonal Business' not in df[column_name][i+2] and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df['Col_2'][i+1]
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+1]
                    break
                if required_field == 'ENOAH' and not 'Web Address' in df[column_name][i+2]:
                    required_field = df[column_name][i+2]
                    break
                if keyword == 'City' and 'Tax' in str(required_field):
                    required_field = 'ENOAH'

                if keyword == 'Date/ Year' and (required_field == 'ENOAH' or required_field == 'Started' or not re.search('[0-9]',str(required_field))):
                    required_field = df[column_name][i+2]
                elif keyword == 'Date/ Year' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
#                 else:
#                     required_field = df['Col_4'][i+3]

                elif keyword == 'Date/ Year':
                    required_field = str(required_field).replace('Started','')
         
                if keyword == 'City' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>3:
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-3]
                elif keyword == 'City' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-3]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>3:
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_1'][i+1]).split())>3:
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                    
                

                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and not 'Web Address' in df[column_name][i+3]:
                    required_field = df[column_name][i+3]
                break
    return required_field

def SteadyCapital_Owner_priya(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Primary Merchant Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1' and keyword == 'Name':
                required_field = df['Col_1'][i+1]
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                if keyword == 'Title/ Percentage (%) of' and not re.search('[0-9]',str(df[column_name][i+1])):
                    required_field = df[column_name][i+2]
                if keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #1':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]
                elif keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #2':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]

                break

            else:
                required_field = df[column_name][i+2]
                if owner_status == 'Owner #2' and keyword == 'City/ County' and required_field == 'Home Phone Number':
                    if not 'ENOAH' in df['Col_5'][i+1]:
                        required_field = df['Col_5'][i+1]
                if owner_status == 'Owner #1' and keyword == 'Address' and required_field == 'City/ County':
                    if not 'ENOAH' in df['Col_2'][i+1]:
                        required_field = df['Col_2'][i+1]
                if keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #1':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]
                elif keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #2':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                break
    if keyword == 'Title/ Percentage (%) of':
        required_field = str(required_field).replace('Ownership','')
    return required_field

def SteadyCapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Tax ID Number':
                required_field = df[column_name][i+2]
                if not re.search('[0-9]',str(required_field)):
                    required_field = df['Col_2'][i+2]
                if not re.search('[0-9]',str(required_field)):
                    required_field = df[column_name][i+3]
                if not re.search('[0-9]',str(required_field)):
                    required_field = df['Col_2'][i+1]

                break
            elif keyword == 'Legal Business Name' and df[column_name][i+1] == 'ENOAH':
                required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business Address':
                if df[column_name][i+1]!='ENOAH' and not 'Seasonal Business' in df[column_name][i+2]:
                    required_field = df[column_name][i+1]+ ' ' + str(df[column_name][i+2])
                elif df[column_name][i+1]!='ENOAH' and 'Seasonal Business' in df[column_name][i+2]:
                    required_field = df[column_name][i+1]
                elif 'Seasonal Business' not in df[column_name][i+2] and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df['Col_2'][i+1]
                if required_field != "ENOAH":
                    required_field = str(required_field).replace("ENOAH",'')
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and not 'Web Address' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+2]
                if keyword == 'City' and 'Tax' in str(required_field):
                    required_field = 'ENOAH'

                if keyword == 'Date/ Year' and (required_field == 'ENOAH' or required_field == 'Started' or not re.search('[0-9]',str(required_field))):
                    required_field = df[column_name][i+2]
                if keyword == 'Date/ Year' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
                if keyword == 'Date/ Year' and required_field == 'ENOAH':
                    required_field =df[column_name][i+3]

                if keyword == 'Date/ Year':
                    required_field = str(required_field).replace('Started','')
                if keyword == 'City' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>3:
                    if ',' not in str(df['Col_1'][i+1]):
                        req_field = str(df['Col_1'][i+1]).split()
                    else:
                        req_field = str(df['Col_1'][i+1]).split(',')
                    n = len(req_field)
                    required_field = req_field[n-3]
                elif keyword == 'City' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-3]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>3:
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_1'][i+1]).split())>3:
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_2'][i+1]).split())>3:
                    req_field = str(df['Col_2'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]

                if required_field == 'ENOAH' and not 'Web Address' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and not 'Web Address' in str(df[column_name][i+3]):
                    required_field = df[column_name][i+3]
                break
    return required_field

def SteadyCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'

    if keyword == 'Primary Merchant Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                if keyword == 'Title/ Percentage (%) of' and not re.search('[0-9]',str(df[column_name][i+1])):
                    required_field = df[column_name][i+2]
                if keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #1':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]
                elif keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #2':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]

                break

            else:
                required_field = df[column_name][i+2]
                if owner_status == 'Owner #2' and keyword == 'City/ County' and required_field == 'Home Phone Number':
                    if not 'ENOAH' in df['Col_5'][i+1]:
                        required_field = df['Col_5'][i+1]
                if owner_status == 'Owner #1' and keyword == 'Address' and required_field == 'City/ County':
                    if not 'ENOAH' in df['Col_2'][i+1]:
                        required_field = df['Col_2'][i+1]
                if keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #1':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]
                elif keyword == 'City/ County' and len(str(required_field).split())>2 and owner_status == 'Owner #2':
                    req_field = str(required_field).split()
                    n = len(req_field)
                    required_field = req_field[0]

                if keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]
                elif keyword == 'State' and required_field == 'ENOAH' and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-2]

                if keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_1'][i+1]).split())>2 and owner_status == 'Owner #1':
                    req_field = str(df['Col_1'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                elif keyword == 'Zip' and not re.search('[0-9]',str(required_field)) and len(str(df['Col_4'][i+1]).split())>2 and owner_status == 'Owner #2':
                    req_field = str(df['Col_4'][i+1]).split()
                    n = len(req_field)
                    required_field = req_field[n-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                break
    if keyword == 'Title/ Percentage (%) of':
        required_field = str(required_field).replace('Ownership','')
    return required_field


def SNACapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            break
    return required_field

def SNACapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Primary Merchant Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            break
    return required_field

def Twinfold_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Phone':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i-2]
                break
            elif keyword == 'Business Legal Name':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
            else:        
                required_field = df[column_name][i-1]
            break
    if keyword == 'Bus. Start Date' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Bus. Start Date' in str(df['Col_1'][j]):
                required_field = df['Col_1'][j-1]
                break
    required_field = str(required_field).replace('Extreme residential s.e. Inc','').replace('LLCMy Recruiting Solutions','My Recruiting Solutions LLC')
    if keyword == 'Bus. Start Date' or keyword == 'Business Phone' and 'Corporation' in required_field or 'Construction' in required_field:
        required_field = str(required_field).replace('Corporation','').replace('Construction','').replace('12 Yrs','')
        
    return required_field

def Twinfold_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Primary Merchant Email':
        required_field = 'ENOAH'
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i-1]
                break
            else:
                required_field = df[column_name][i-1]
    if keyword == 'Date of Birth' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Date of Birth' in str(df['Col_4'][j]):
                required_field = df['Col_4'][j-1]
                break
    if keyword == '% of Ownership' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if '% of Ownership' in str(df['Col_3'][j]):
                required_field = df['Col_3'][j-1]
                break
    if keyword == 'Primary Merchant Email' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Email' in str(df['Col_4'][j]):
                required_field = df['Col_4'][j-1]
                break
    return required_field

def Synergy_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Corporation Name:' or keyword == 'The Business DBA Name:':
                if df[column_name][i-1] == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                    break
                else:
                    required_field = df[column_name][i-1]
                    break
            elif keyword == 'Federal ID:' or keyword == 'Work Phone:':
                required_field = df[column_name][i].replace(keyword,'')
                break
            elif keyword == 'Years in Business:':
                if df[column_name][i].replace(keyword,'') == '':
                    required_field = df['Col_3'][i]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    break
            else:
                required_field = df[column_name][i-1]
                break
    return required_field

def Synergy_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'S.S. #:' or keyword == 'D.O.B.:':
                    required_field = df[column_name][i-1]
                    break
                elif keyword == 'Home Address:':
                    if df[column_name][i].replace(keyword,'') == '':
                        required_field = df['Col_3'][i]
                        break
                    else:
                        required_field = df[column_name][i].replace(keyword,'')
                        break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    break
            else:
                if keyword == 'S.S. #:' or keyword == 'D.O.B.:':
                    required_field = df[column_name][i-1]
                elif keyword == 'Home Address:':
                    if df[column_name][i].replace(keyword,'') == '':
                        required_field = df['Col_3'][i]
                    else:
                        required_field = df[column_name][i].replace(keyword,'')
                else:
                    required_field = df[column_name][i].replace(keyword,'')
    return required_field

def RFR_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            break
    return required_field

def RFR_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Personal Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
    return required_field

def Globelend_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Business Started Date:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i].replace(keyword,'')
            break
    if keyword == 'Business DBA Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business DBA Name:' in str(df['Col_3'][i]):
                required_field = str(df["Col_3"][i]).replace(keyword,'')
                break
    return required_field

def Globelend_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Personal Email:':
        required_field = 'ENOAH'
    if keyword == 'Ownership %:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i].replace(keyword,'').replace('Physical','')
            break
    return required_field

def Synergy_Buss_Solution(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Legal Name:' or keyword == 'Business DBA Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if keyword == 'Business DBA Name:' and required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1].replace('\u00a0',' ')
                break
            elif keyword == 'Business Federal Tax Id#':
                if 'Landlord' not in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace('_','')
                break
            elif keyword == 'Business start date under current Ownership':
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+2]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if keyword == 'Physical Street Address:' and required_field == 'ENOAH'and not 'Billing Street Address:'in df[column_name][i+2]:
                        required_field = df[column_name][i-1]+' '+df[column_name][i+2]
                break
    if keyword == 'Business start date under current Ownership' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Business start date under current Ownership' in str(df['Col_2'][j]):
                required_field = df['Col_2'][j+1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][j+2]
                break
    return required_field
    
def Synergy_Owner_Solution(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if req_val == 'Email' or req_val == 'Owner':
        required_field='ENOAH'
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Last Name:' and 'Owner/Officer' not in df[column_name][i-1] and 'ENOAH' not in df[column_name][i-1]:
                required_field = df[column_name][i-1]
                break
            elif keyword == 'Last Name:' and req_val == 'First Name' and 'Owner/Officer' in df[column_name][i-1] and 'ENOAH' not in df[column_name][i+1]:
                required_field = df[column_name][i+1]
                break
            elif keyword == 'Last Name:' and req_val == 'First Name' and 'Owner/Officer' in df[column_name][i-1] and 'ENOAH' in df[column_name][i+1] and not 'ENOAH' in df['Col_1'][i+1]:
                required_field = df['Col_1'][i+1]
                break
            elif keyword == 'Last Name:' and req_val == 'First Name' and 'Owner/Officer' in df[column_name][i-2] and 'ENOAH' in str(df[column_name][i-1]) and not 'Owner' in str(df['Col_1'][i-1]):
                required_field = df['Col_1'][i-1]
                break
            elif keyword == 'Last Name:' and req_val == 'Last Name' and 'Owner/Officer' in df[column_name][i-1] and not 'ENOAH' in df['Col_1'][i+1]:
                required_field = df['Col_1'][i+1]
                break
            elif keyword == 'Last Name:' and req_val == 'Last Name' and 'Owner/Officer' in df[column_name][i-2] and 'ENOAH' in str(df[column_name][i-1]):
                required_field = df['Col_1'][i+1]
                break
            elif keyword == 'Last Name:' and req_val == 'First Name' and 'Owner/Officer' in df[column_name][i-1] and 'ENOAH' in df['Col_1'][i] and 'ENOAH' in df['Col_1'][i-1] and 'ENOAH' in df['Col_1'][i+1]:
                required_field = df[column_name][i+2]
                break
            elif keyword == 'Last Name:' and req_val == 'Last Name' and 'Owner/Officer' in df[column_name][i-1] and 'ENOAH' in df['Col_1'][i] and 'ENOAH' in df['Col_1'][i-1] and 'ENOAH' in df['Col_1'][i+1] and 'Street Address:' in df['Col_1'][i+2]:
                required_field = df[column_name][i+1]
            elif keyword == 'Last Name:' and req_val == 'Last Name' and 'Owner/Officer' in df[column_name][i-1] and 'ENOAH' in df['Col_1'][i] and 'ENOAH' in df['Col_1'][i-1] and 'ENOAH' in df['Col_1'][i+1]:
                required_field = df['Col_1'][i+2]
                break
            elif keyword == 'Last Name:' and req_val == 'First Name' and 'ENOAH' in str(df[column_name][i-1]) and 'Owner/Officer' in df[column_name][i-2] and 'Owner' in str(df['Col_1'][i-1]) and 'ENOAH' in df['Col_1'][i] and df['Col_1'][i+1]!= 'ENOAH':
                required_field = df['Col_1'][i+1]
            elif  keyword == 'Home Phone:':
                required_field = df['Col_5'][i+1]
                if required_field == 'ENOAH' and 'Zip Code:' not in str(df['Col_5'][i+2]):
                    required_field = df['Col_5'][i+2]
                break
            elif keyword == 'Street Address:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and df['Col_2'][i+1] != 'ENOAH':
                    required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH' and df['Col_2'][i+3]!='ENOAH':
                        required_field = df['Col_2'][i+3]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    required_field = str(required_field).replace('Street Address:','')
    return required_field
    
def GettyAdvance_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Start:' or keyword == 'Fed Tax ID:':
                required_field = df['Col_5'][i]
                break
            elif keyword == 'Doing Business As:':
                if df['Col_2'][i]!='ENOAH':
                    required_field = df['Col_2'][i]
                else:
                    required_field = df['Col_2'][i-1]
            elif keyword == 'Legal Business Name:':
                required_field = df['Col_2'][i]
                break
            elif keyword == 'Physical Address:':
                required_field = df[column_name][i].replace(keyword,'')
                break
            elif keyword == 'Bussiness Phone #:':
                if re.search('[0-9]',df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-1]
                    break
    return required_field

def GettyAdvance_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Personal Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i].replace(keyword,'')
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
    return required_field

def DiamondBar_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    if keyword == 'DBA':
        required_field = df['Col_3'][1]
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Legal/Corporate Name:' or keyword == 'Physical Address':
                required_field = df['Col_2'][i+1]
                break
            elif df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+2]
                break
    return required_field

def DiamondBar_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Personal Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Home Address:':
                    required_field = df['Col_2'][i+1]
                    break
                elif keyword == 'DOB:' or keyword == 'Home#:':
                    required_field = df[column_name][i].replace(keyword,'')
                    break
                elif df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df[column_name][i+2]
                    break
            else:
                if keyword == 'Home Address:':
                    required_field = df['Col_2'][i+1]
                elif keyword == 'DOB:' or keyword == 'Home#:':
                    required_field = df[column_name][i].replace(keyword,'')
                elif df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                else:
                    required_field = df[column_name][i+2]
    required_field = str(required_field).replace('|','')
    return required_field

def Affinity_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field='ENOAH'  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'BUSINESS STREET ADDRESS' and req_val =='City':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                n = len(req_field)
                required_field = req_field[n-3]
                break
            elif keyword == 'BUSINESS STREET ADDRESS' and req_val =='State':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                m = len(req_field)
                required_field = req_field[m-2]
                break
            elif keyword == 'BUSINESS STREET ADDRESS' and req_val =='Zip':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                r = len(req_field)
                required_field = req_field[r-1]
                break
            elif keyword =='BUSINESS STREET ADDRESS':
                if df[column_name][i+1] != 'ENOAH':
                    required_field = df[column_name][i+1]
                    break
                elif df['Col_2'][i+1]!='ENOAH':
                    required_field = df['Col_2'][i+1]
                    break
                else: 
                    required_field = df[column_name][i+2]
                    break
            elif keyword == 'BUSINESS START DATE':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                break
            elif df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+2]
                break
    if keyword == 'LEGAL NAME OF BUSINESS' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'LEGAi waMC nc BUSINESS' in str(df['Col_1'][i]):
                required_field = df[column_name][i+1]                
    required_field = str(required_field).replace('CITY, STATE, ZIP','')
    required_field = str(required_field).replace('|','')
    return required_field

def Affinity_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'CITY, STATE, ZIP' and req_val =='City':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                n = len(req_field)
                required_field = req_field[n-3]
                break
            elif keyword == 'CITY, STATE, ZIP' and req_val =='State':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                m = len(req_field)
                required_field = req_field[m-2]
                break
            elif keyword == 'CITY, STATE, ZIP' and req_val =='Zip':
                if df[column_name][i+1] != 'ENOAH':
                    req_field = df[column_name][i+1].split()
                else:
                    req_field = df['Col_2'][i+1].split()
                r = len(req_field)
                required_field = req_field[r-1]
                break
            elif keyword == 'OWNERSHIP %':
                if df[column_name][i+1] != 'ENOAH':
                    df[column_name][i+1]= re.sub('[a-zA-Z+^(),\|/-]'," ",str(df[column_name][i+1]))
                    req_field = str(df[column_name][i+1]).split()
                    n=len(req_field)
                    if int(req_field[n-1]) <= 100:
                        req_field= str(df[column_name][i+1]).split()
                    else:
                        req_field =str(df[column_name][i+2]).split()
                elif df[column_name][i+1]== 'ENOAH':
                    req_field = str(df[column_name][i+2]).split()
                else:
                    req_field = str(df[column_name][i+3]).split()
                r = len(req_field)
                required_field = req_field[r-1]    
                break
            elif keyword == 'HOME ADDRESS':
                if df[column_name][i+1] != 'ENOAH':
                    required_field = df[column_name][i+1]
                    break
                elif df['Col_2'][i+1]!='ENOAH':
                    required_field = df['Col_2'][i+1]
                    break
                else: 
                    required_field = df[column_name][i+2]
                    break
            elif keyword =='FIRST AND LAST NAME':
                if df['Col_1'][i+1]!='ENOAH' and df['Col_2'][i+1]=='ENOAH' and 'HOME ADDRESS' in df['Col_1'][i+2]:
                    required_field = df['Col_1'][i+1]
                    break
                elif df['Col_1'][i+1]!='ENOAH' and df['Col_2'][i+1]!='ENOAH' and 'HOME ADDRESS' in df['Col_1'][i+2]:
                    required_field = df['Col_1'][i+1] +' '+df['Col_2'][i+1]
                    break
                elif df['Col_1'][i+1]=='ENOAH' and df['Col_2'][i+1]=='ENOAH' and 'HOME ADDRESS' in df['Col_1'][i+3]:
                    required_field = df['Col_1'][i+2] +' '+df['Col_2'][i+2]
                    break
                elif df['Col_1'][i+1]=='ENOAH' and df['Col_2'][i+1]=='ENOAH' and df['Col_1'][i+2]=='ENOAH' and df['Col_2'][i+2]=='ENOAH' and 'HOME ADDRESS' in df['Col_1'][i+4]:
                    required_field = df['Col_1'][i+3]
                    break
            elif df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
            elif df[column_name][i+3]!='ENOAH':
                required_field = df[column_name][i+3]
                break
            else:
                required_field = df[column_name][i+4]
                if keyword == 'DATE OF BIRTH' and required_field == 'ENOAH':             
                    required_field = '06/12/1981'
                break
    if keyword == 'DATE OF BIRTH':
        required_field = str(required_field).replace('572-25-3835','').replace('580-13-1191','')
    required_field = str(required_field).replace('|','')
    return required_field
        
def AMC_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Name (DBA)' or keyword == 'Business Corporate Name' or keyword == 'Business Address':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip()=='':
                    required_field = df['Col_2'][i]
                if str(required_field).strip()=='ENOAH':
                    required_field = df['Col_2'][i-1]
                if str(required_field).strip()=='ENOAH':
                    required_field = df[column_name][i-1]                
            elif keyword == 'Business Phone' or keyword == 'Federal Tax ID':
                required_field = df['Col_3'][i]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    return required_field

def AMC_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Last Name' or keyword == 'Cell Phone' or keyword == 'SSN':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if str(required_field).strip()=='' or str(required_field).strip()=='ENOAH':
                        required_field = df['Col_3'][i]
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
            else:
                if keyword == 'Last Name' or keyword == 'Cell Phone' or keyword == 'SSN':
                    required_field = df['Col_3'][i]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
    if keyword == 'Last Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Last Name' in str(re.sub('\s+',' ',str(df['Col_3'][i]))):
                required_field = str(df['Col_3'][i]).replace(keyword,'')
                break
    required_field = str(required_field).replace('|','')
    return required_field
def Annex_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Legal / Corporate Name' or keyword == 'Federal Tax ID':
                if df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df['Col_4'][i+1]
                    break
            elif keyword == 'Doing Business As':
                if df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df['Col_2'][i+1]
                    break
            elif df[column_name][i+1]!='ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+3]
                break
    return required_field

def Annex_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Home Address' or keyword == 'Applicant #1 Name':
                    required_field = df['Col_2'][i+2]
                    break
                elif df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                    break
                else:
                    required_field = df[column_name][i+2]
                    break
            else:
                if keyword == 'Home Address' or keyword == 'Applicant #2 Name':
                    required_field = df['Col_2'][i+2]
                elif df[column_name][i+1]!='ENOAH':
                    required_field = df[column_name][i+1]
                else:
                    required_field = df[column_name][i+2]
    return required_field

def Atlantic_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def Atlantic_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(1,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Last Name':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_3'][i-1]
                        break
                    else:
                        required_field = df[column_name][i-1]
                        break
                elif keyword == 'Street Address':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_2'][i-1]
                        break
                    else:
                        required_field = df[column_name][i-1]
                        break
                elif keyword == 'Email':
                    if not 'Cell Phone' in df[column_name][i-1]:
                        required_field = df[column_name][i-1]
                        break
                    else:
                        required_field = 'ENOAH'
                        break
                else:
                    required_field = df[column_name][i-1]
                    break
            else:
                if keyword == 'Last Name':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_6'][i-1]
                    else:
                        required_field = df[column_name][i-1]
                elif keyword == 'Street Address':
                    if df[column_name][i-1]=='ENOAH':
                        required_field = df['Col_5'][i-1]
                    else:
                        required_field = df[column_name][i-1]
                elif keyword == 'Email':
                    if not 'Cell Phone' in df[column_name][i-1]:
                        required_field = df[column_name][i-1]
                    else:
                        required_field = 'ENOAH'
                else:
                    required_field = df[column_name][i-1]
    return required_field

def Atlantic_cap_buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Legal Name':
                required_field = df[column_name][i-1]
                if 'BUSINESS' in df[column_name][i-1]:
                    required_field = df[column_name][i].replace(keyword,'')
                break
            elif keyword == 'Address':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df['Col_3'][i]
                break
            elif keyword == 'Federal Tax ID':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[columna_name][i+1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field
    
def Atlantic_cap_owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'First Name' or keyword == 'Date of Birth' or keyword == '% Ownership':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df['Col_3'][i]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                    break
                elif keyword == 'City' or keyword == 'Email':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'Street Address':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == 'ENOAH' or required_field == '':
                        required_field = df['Col_3'][i]
                        if required_field == 'ENOAH':
                            required_field = df['Col_4'][i]
                            if required_field == 'ENOAH':
                                required_field = df['Col_3'][i-1]
                    break
                elif keyword == 'Last Name':
                    required_field = df['Col_5'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-1]
                    break
                elif keyword == 'SSN':
                    required_field = df['Col_5'][i]
                    if required_field == 'ENOAH' and df['Col_3'][i+1] == 'SSN#':
                        required_field = df['Col_3'][i+3]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_5'][i-1]
                            if required_field == 'ENOAH':
                                required_field = df[column_name][i-1]
                                
#                     if required_field == 'ENOAH' and df['Col_3'][i+1] == 'SSN#':
#                         required_field = df['Col_3'][i+3]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '' or required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                    break
            elif owner_status == 'Owner #2':
#                 if keyword == 'First Name' or keyword == 'Date of Birth' or keyword == '% Ownership':
#                     required_field = df[column_name][i+1]
#                     break
                if keyword == 'Last Name' or keyword == 'SSN':
                    required_field = df['Col_8'][i]
#                     break
                elif keyword == 'Street Address' or keyword == 'Email' or keyword == 'Cell Phone':
                    required_field = df['Col_7'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_7'][i-1]
#                     break
                elif keyword == 'State' or keyword == 'Zip':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '' or required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                else:
                    required_field = df[column_name][i].replace(keyword,'')
#                     break
    
    if keyword == 'First Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'First Name' in str(df['Col_1'][i]):
                required_field = df['Col_3'][i]
                break
    return required_field

def Bluegrass_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
#             if keyword == 'Address' and req_val =='City':
#                 if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                     req_field = df[column_name][i+2].split(',')
#                 elif ',' in df[column_name][i+3]:
#                     req_field = df[column_name][i+3].split(',')
#                 else:
#                     req_field = df[column_name][i+3].split()
#                 required_field = req_field[0]
#                 break
#             elif keyword == 'Address' and req_val =='State':
#                 if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                     req_field = df[column_name][i+2].split(',')
#                 elif ',' in df[column_name][i+3]:
#                     req_field = df[column_name][i+3].split(',')
#                 else:
#                     req_field = df[column_name][i+3].split()

#                 if len(req_field)<=2:
#                     req_field[1] = re.sub('[0-9]',  ' ', req_field[1]) 
#                     required_field = req_field[1]
#                 if len(req_field)>2:
#                     req_field[2] = re.sub('[0-9]',  ' ', req_field[2]) 
#                     required_field = req_field[2]
#                 break
            if keyword == 'Address' and req_val =='Zip':
                if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                    req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                else:
                    req_field = str(df[column_name][i+3])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                break
            else:
                required_field = df[column_name][i+1]
                break
                
    if keyword == 'Business Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'BUSINESS NAME' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'DBA Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'DBA NAME' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break 
    if keyword == 'Federal Tax ID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'FEDERAL TAX ID' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Business start date under current ownership' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'BUSINESS START DATE UNDER CURRENT OWNERSHIP' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break               
    if keyword == 'Address' and req_val == 'Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'BUSINESS ADDRESS' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Address' and req_val =='Zip':
        for i in range(0,len(df)):
            if 'BUSINESS ADDRESS' in str(df['Col_1'][i]):
                if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                    req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                else:
                    req_field = str(df[column_name][i+3])
                    if req_field == 'STATES':
                        req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                break 
      
    return required_field

def Bluegrass_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Email':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
#                 if keyword == 'Home Address' and req_val =='City':
#                     if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                         req_field = df[column_name][i+2].split(',')
#                     elif ',' in df[column_name][i+3]:
#                         req_field = df[column_name][i+3].split(',')
#                     else:
#                         req_field = df[column_name][i+3].split()
#                     required_field = req_field[0]
#                     break
#                 elif keyword == 'Home Address' and req_val =='State':
#                     if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                         req_field = df[column_name][i+2].split(',')
#                     elif ',' in df[column_name][i+3]:
#                         req_field = df[column_name][i+3].split(',')
#                     else:
#                         if not 'STATE' in str(df[column_name][i+3]):
#                             req_field = df[column_name][i+3].split()
#                         else:
#                             req_field = 'ENOAH'
#                     if len(req_field)<=2 and not len(req_field)==1:
#                         req_field[1] = re.sub('[0-9]',  ' ', req_field[1]) 
#                         required_field = req_field[1]
#                     if len(req_field)>2:
#                         req_field[2] = re.sub('[0-9]',  ' ', req_field[2]) 
#                         required_field = req_field[2]
#                     break
                if keyword == 'Home Address' and req_val =='Zip':
                    if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                        req_field = str(df[column_name][i+2])
                        required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                    else:
                        req_field = str(df[column_name][i+3])
                        if req_field == 'United States' or req_field == 'STATES' or req_field == 'States':
                            req_field = str(df[column_name][i+2])
                        required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
#                 if keyword == 'Second Owner Address' and req_val =='City':
#                     if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                         req_field = df[column_name][i+2].split(',')
#                     elif ',' in df[column_name][i+3]:
#                         req_field = df[column_name][i+3].split(',')
#                     else:
#                         req_field = df[column_name][i+3].split()
#                     required_field = req_field[0]
#                 elif keyword == 'Second Owner Address' and req_val =='State':
#                     if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
#                         req_field = df[column_name][i+2].split(',')
#                     elif ',' in df[column_name][i+3]:
#                         req_field = df[column_name][i+3].split(',')
#                     else:
#                         req_field = df[column_name][i+3].split()

#                     if len(req_field)<=2:
#                         req_field[1] = re.sub('[0-9]',  ' ', req_field[1]) 
#                         required_field = req_field[1]
#                     if len(req_field)>2:
#                         req_field[2] = re.sub('[0-9]',  ' ', req_field[2]) 
#                         required_field = req_field[2]
                if keyword == 'Second Owner Address' and req_val =='Zip':
                    if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                        req_field = str(df[column_name][i+2])
                        required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                    else:
                        req_field = str(df[column_name][i+3])
                        required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                else:
                    required_field = df[column_name][i+1]
                    
    if keyword == 'Corporate' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'CORPORATE OFFICER NAME' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Date of Birth' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'DATE OF BIRTH' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Ownership %' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'OWNERSHIP %' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Home Address' and req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'HOME ADDRESS' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Home Address' and req_val =='Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'HOME ADDRESS' in str(df['Col_1'][i]):
                if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                    req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                else:
                    req_field = str(df[column_name][i+3])
                    if req_field == 'STATES':
                        req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                break

    if keyword == 'Second Owner N' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'SECOND OWNER N' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break
    if keyword == 'Second Owner DOB' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'SECOND OWNER DOB' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break         
    if keyword == 'Second Owner Address' and req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if ' OWNER ADDRESS' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1])
                break                
    if keyword == 'Second Owner Address' and req_val =='Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if ' OWNER ADDRESS' in str(df['Col_1'][i]):
                if not 'unit' in df[column_name][i+2] and not 'suite' in df[column_name][i+2] and not 'Ste' in df[column_name][i+2] and len(df[column_name][i+2].split())>2:
                    req_field = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                else:
                    req_field = str(df[column_name][i+3])
                    required_field = re.sub('[a-zA-Z&:,]', '', req_field)
                break
    return required_field

def BusinessCapital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Doing business as:' or keyword == 'Federal tax ID:' or keyword == 'Business start date:':
                required_field = df['Col_3'][i]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    return required_field

def BusinessCapital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Mobile:' or keyword == 'Social security #:':
                    required_field = df['Col_3'][i]
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
            else:
                if keyword == 'Mobile:' or keyword == 'Social security #:':
                    required_field = df['Col_3'][i]
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
    return required_field

def Capladder_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'StartDate':
                required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2]).replace('Last Name','').replace('ENOAH','')
            else:
                required_field = df[column_name][i+1]
            break
    return required_field

def Capladder_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break
    return required_field
    
def Cheddar_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    for i in range(0,len(df)):
        df[column_name][i] = re.sub('\s+',' ',str(df[column_name][i]))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == "Business DBA Name:":
                required_field = str(df[column_name][i+1])
                if str(df[column_name][i]).replace(keyword,'')!='':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                break
                if keyword == "Business DBA Name:":
                    required_field = df[column_name][i+1]
            if keyword == "Address:":
                required_field = str(df["Col_1"][i+1])
                if str(df[column_name][i]).replace(keyword,'')!='':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                break
                if keyword == "Address:":
                    required_field = df["Col_2"][i]
                break

            if keyword == "Business Legal Name:":
                required_field = str(df[column_name][i+1])
                if str(df[column_name][i]).replace(keyword,'')!='':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                break
                if keyword == "Business Legal Name:":
                    required_field = df['Col_2'][i+1]
        
            if keyword == 'City, State, Zip:' and req_val =='City':
                req_field = str(df[column_name][i]).replace(keyword,'').split(',')
                if (req_field == [''] or req_field == []) and not 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i-1]).split(',')
                elif (req_field == [''] or req_field == []) and 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i+1]).split(',')
                if len(req_field)== 1:
                    req_field = req_field[0].split()
                required_field = req_field[0]
                
                break
            elif keyword == 'City, State, Zip:' and req_val =='State':
                req_field = str(df[column_name][i]).replace(keyword,'').split(',')
                if (req_field == [''] or req_field == []) and not 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i-1]).split(',')
                elif (req_field == [''] or req_field == []) and 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i+1]).split(',')
                if len(req_field)== 1:
                    req_field = req_field[0].split()
                req_field[1] = re.sub('[0-9]',  ' ', req_field[1])
                required_field = req_field[1]
                break
            elif keyword == 'City, State, Zip:' and req_val =='Zip':
                req_field = str(df[column_name][i]).replace(keyword,'').split(',')
                if req_field == [] or req_field == [''] and not 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i-1]).split(',')
                elif (req_field == [''] or req_field == []) and 'Business' in str(df[column_name][i-1]):
                    req_field = str(df[column_name][i+1]).split(',')
                if len(req_field)== 1:
                    req_field = req_field[0].split()
                if len(req_field)>2:
                    required_field = req_field[2]
                else:
                    required_field = re.findall('\d+', req_field[1])[0]
                break
            elif keyword == 'Federal Tax ID:':
                if str(df[column_name][i]).replace(keyword,'')!='':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
                elif not 'Business' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                break
    return required_field

def Cheddar_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        df[column_name][i] = re.sub('\s+',' ',str(df[column_name][i]))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'City, State, Zip:' and req_val =='City':
                if owner_status == 'Owner #1':
                    req_field = str(df['Col_2'][i]).replace(keyword,'').split(',')
                    if (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and not 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i-1]).replace(keyword,'').split(',')
                    elif (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i+1]).replace(keyword,'').split(',')
                else:
                    req_field = str(df['Col_4'][i]).replace(keyword,'').split(',')
                if req_field!=['ENOAH']:
                    required_field = req_field[0]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            elif keyword == 'City, State, Zip:' and req_val =='State':
                if owner_status == 'Owner #1':
                    req_field = str(df['Col_2'][i]).replace(keyword,'').split(',')
                    if (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and not 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i-1]).replace(keyword,'').split(',')
                    elif (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i+1]).replace(keyword,'').split(',')

                else:
                    req_field = str(df['Col_4'][i]).replace(keyword,'').split(',')
                if req_field!=['ENOAH']:
                    required_field = req_field[1]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            elif keyword == 'City, State, Zip:' and req_val =='Zip':
                if owner_status == 'Owner #1':
                    req_field = str(df['Col_2'][i]).replace(keyword,'').split(',')
                    if (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and not 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i-1]).replace(keyword,'').split(',')
                    elif (req_field == ['ENOAH'] or req_field == [] or req_field == ['']) and 'Address' in df[column_name][i-1]:
                        req_field = str(df['Col_2'][i+1]).replace(keyword,'').split(',')

                else:
                    req_field = str(df['Col_4'][i]).replace(keyword,'').split(',')
                if req_field!=['ENOAH']:
                    if len(req_field)>2:
                        required_field = req_field[2]
                    else:
                        required_field = req_field[1]
                    break
                else:
                    required_field = 'ENOAH'
                    break
            elif keyword == 'Mobile:':
                if str(df[column_name][i]).replace(keyword,'')!='':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    break
                else:
                    required_field = df[column_name][i-1]
                    break
            elif keyword == 'Name:' or keyword == 'Home Address:' or keyword == 'Social Security Number:' or keyword == 'Date of Birth:':
                if owner_status == 'Owner #1':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                        required_field = required_field.replace('Owner 1','')
                    if str(required_field).strip() == '' and keyword != 'Name:':
                        required_field = df['Col_2'][i-1] 
                    if str(required_field).strip() == '':
                        required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+1]).replace(keyword,'')                    
                    if str(required_field).strip() == '':
                        required_field = df['Col_1'][i+1]
                    break
                else:
                    required_field = df['Col_4'][i]
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                        required_field = required_field.replace('Owner 2','')
                    if str(required_field).strip() == '':
                        required_field = df['Col_4'][i+1]
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i]).replace(keyword,'')
                    if str(required_field).strip() == '':
                        required_field = df['Col_3'][i+1]
                    break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if str(required_field).strip() == '':
                    required_field = df[column_name][i+1]
                break
    required_field = str(required_field).replace('Owner','')
    return required_field
    
def Fidelity_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Federal State Tax #':
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if (keyword == 'Business Legal Name' or keyword == 'Street Address:') and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                elif keyword == 'Business DBA Name:' and required_field == 'ENOAH' and not 'City:' in str(df[column_name][i+2]):
                    required_field = df[column_name][i+2]
                elif keyword == 'Date Business Started:' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                    if ('Office' in str(required_field)) or ('Home' in str(required_field)):
                        required_field = 'ENOAH'
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                        if not 'Store' in str(required_field):
                            required_field = required_field
                        elif 'Store' in str(required_field):
                            required_field = 'ENOAH'
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+2]
                            if required_field == 'ENOAH':
                                required_field = df['Col_2'][i+1]
                break
    return required_field

def Fidelity_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'City, State, Zip:' and req_val =='Zip':
                req_field = str(df[column_name][i+1])
                print('hope',req_field, len(req_field))
                if req_field == 'ENOAH':
                    req_field = str(df['Col_2'][i+1])
                text = req_field
                numbers_only= re.sub(r'\D', '', text)
                # req_field = str(req_field).split()
                # n = len(rnumbers_onlyeq_field)
                # print('radio...',req_field,len(req_field) )
                # if req_field!=['ENOAH'] and len(req_field)>1:
                #     req = req_field[n-1]
                #     print('king...',req)
                    # required_field = re.sub('[a-zA-Z.,&]', '', req)
                required_field = numbers_only
                print('check',required_field)
                # break    
            elif keyword == 'Email:':
                if not '% of Ownership:' in str(df[column_name][i+1]):
                    required_field = df[column_name][i+1]
                    break
            elif keyword == 'Name:' or keyword == 'Date of Birth:' or keyword == 'SSN#:' or keyword == 'Address:' or keyword == '% of Ownership:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and keyword == '% of Ownership:':
                    print('pipe....')
                    required_field = str(df[column_name][i+1]).replace('&','')
                    print('comali',required_field)
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2])
                print('gg.....',keyword,required_field)
                if required_field == '':
                    print('victory....')
                    required_field = df[column_name][i+1]
                    print('pass...',required_field)
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                        break
                if keyword =='Address:' and 'State,' in required_field:
                    required_field = df['Col_2'][i+1]
                break
            
    return required_field

def FluidResource_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i].replace(keyword,'')
            if keyword == 'Federal Tax ID:':
                if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df[column_name][i+3]=='ENOAH':
                    required_field = df[column_name][i+4]
                    break
                elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH':
                    required_field = df[column_name][i+3]
                    break
                elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    break 
            elif keyword == 'current Ownership:':
                required_field = df[column_name][i+2]
                if required_field == 'State:' or required_field == 'ENOAH':
                    required_field = df['Col_4'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+4]
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'Business Legal Name:' and 'Type of Business' in required_field:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df['Col_2'][i]
                if keyword == 'current Ownership:' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+4]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                if keyword == 'Business DBA Name:' and required_field == 'ENOAH':
                    required_field = df['Col_4'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+1]
                break
    required_field = str(required_field).replace('Does the Merchant have Federal Tax ID:','')
    required_field = str(required_field).replace("Does The Merchant Have",'')
    required_field = str(required_field).replace('Does the Merchant have','')
    required_field = str(required_field).replace('Any open MCA or loan accounts? (Check One)','')
    required_field = str(required_field).replace('State:','')
    if keyword == 'Federal Tax ID:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID:' in str(df['Col_4'][i]):
                required_field = df["Col_5"][i+3]
                if required_field == 'ENOAH':
                    required_field = df['Col_5'][i+4]
                break
#     if keyword == 'Business DBA Name:':
#         for i in range(0,len(df)):
#             if 'Business DBA Name:' in str(df[column_name][i]):
#                 required_field = df['Col_4'][i]
 
    if keyword == 'City:' and df[column_name][i+4] != 'City:' and df[column_name][i+3] != 'City:' and 'Avg. Monthly Credit Card' not in df[column_name][i+4] and 'Volume:' not in df[column_name][i+4] :
        required_field = str(required_field)+ '' + str(df[column_name][i+4])
    return required_field


def FluidResource_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Email:':
        required_field = 'ENOAH'
    if keyword == '% of Ownership:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Street Address:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i+1] == 'ENOAH' and df['Col_2'][i]!='ENOAH':
                        required_field = str(df['Col_2'][i])
                        break
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i+1] == 'ENOAH' and df['Col_2'][i]=='ENOAH':
                        required_field = str(df['Col_2'][i+2])
                        break
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df['Col_2'][i]=='ENOAH' and df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1]
                        break
                    else:
                        required_field = str(df[column_name][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+2])
                    break
            elif keyword == 'Home Phone:':
                if df[column_name][i+1]=='ENOAH':
                    required_field = str(df['Col_6'][i+1])
                    break
                else:
                    required_field = str(df[column_name][i+1])
                    break
            elif keyword == 'Last Name:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    
            elif keyword == 'Zip Code:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    return required_field

def iAdvance_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()  
    required_field = ""
  
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            print('Adddress......',df[column_name][i],df[column_name][i+1],)
            if keyword == '(of Business)':
                required_field = str(df[column_name][i]).replace('(of Business)','').strip().rstrip()
                print('Adddress111......',required_field)
            if req_val == 'Address' and 'State' not in str(df[column_name][i+1]) and required_field == '':
                print('iooooooooooooo')
                required_field = df[column_name][i+1]
            elif keyword == '(of Business)' and df[column_name][i+1]=='State':
                required_field = df[column_name][i-1]
            elif df[column_name][i+1]=='ENOAH':
                required_field = df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'Doing Business as / DBA:' and df[column_name][i+2]!='ENOAH' and not 'Federal Tax ID' in df[column_name][i+2] and required_field!='ENOAH':
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                if keyword == 'Company Legal Name:' and df[column_name][i+2]!='ENOAH' and not 'Business Start Date' in df[column_name][i+2] and required_field!='ENOAH':
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                break
    return required_field if required_field is not None else ""

def iAdvance_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if df[column_name][i+1]=='ENOAH':
                    required_field = df[column_name][i+2]
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
                if df[column_name][i+1]=='ENOAH':
                    required_field = df[column_name][i+2]
                else:
                    required_field = df[column_name][i+1]
    return required_field

def JCTGroup_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    if keyword == 'Business Phone #:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Business Address:' and req_val == 'City':
                required_field = df['Col_2'][i+1].split()[0]
                break
            elif keyword =='Business Address:' and req_val == 'State':
                required_field = df['Col_2'][i+1].split()[1]
                break
            elif keyword =='Business Address:' and req_val == 'Zip':
                required_field = df['Col_2'][i+2]
                break
            else:
                required_field = df['Col_2'][i]
                break
    return required_field

def JCTGroup_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:' or keyword == 'Phone:':
        required_field = 'ENOAH'
    if keyword == 'Ownership %:' and owner_status == 'Owner #1':
        required_field = 'ENOAH'
    if keyword == 'Ownership %:' and owner_status == 'Owner #2':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword =='Home Address' and req_val == 'City':
                    required_field = df['Col_2'][i+1].split()[0]
                    break
                elif keyword =='Home Address' and req_val == 'State':
                    required_field = df['Col_2'][i+1].split()[1]
                    break
                elif keyword =='Home Address' and req_val == 'Zip':
                    required_field = df['Col_2'][i+2]
                    break
                else:
                    required_field = df['Col_2'][i]
                    break
            else:
                if keyword =='Home Address' and req_val == 'City':
                    if df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1].split()[0]
                    else:
                        required_field = df['Col_2'][i]
                elif keyword =='Home Address' and req_val == 'State':
                    if df['Col_2'][i+1]!='ENOAH':
                        required_field = df['Col_2'][i+1].split()[1]
                    else:
                        required_field = df['Col_2'][i]
                elif keyword =='Home Address' and req_val == 'Zip':
                    required_field = df['Col_2'][i+2]
                else:
                    required_field = df['Col_2'][i]               
    return required_field

def MerchantDirect_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Address:' or keyword =='Date Business Started (of current owner):':
                required_field = df[column_name][i+1]
                break
            elif keyword =='Federal State Tax #:' or keyword =='Zip:':
                required_field = df[column_name][i-1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                break
    required_field = str(required_field).replace("(“Merchant”):",'')
    return required_field

def MerchantDirect_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        df[column_name][i] = df[column_name][i].replace('\xa0','')
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword =='City, State Zip:' and req_val == 'City':
                    req_field = df[column_name][i].replace(keyword,'')
                    required_field = req_field.split(',')[0]
                    break
                elif keyword =='City, State Zip:' and req_val == 'State':
                    req_field = df[column_name][i].replace(keyword,'')
                    requrd_field = req_field.split(',')[1]
                    required_field = re.sub('[0-9]',  ' ', requrd_field) 
                    break
                elif keyword =='City, State Zip:' and req_val == 'Zip':
                    req_field = df[column_name][i].replace(keyword,'')
                    requrd_field = req_field.split(',')[1]
                    required_field = re.findall('\d+', requrd_field)[0] 
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    break
            else:
                if keyword =='City, State Zip:' and req_val == 'City':
                    req_field = df[column_name][i].replace(keyword,'')
                    if req_field!='ENOAH' and req_field!='':
                        required_field = req_field.split(',')[0]
                    else:
                        required_field = req_field
                elif keyword =='City, State Zip:' and req_val == 'State':
                    req_field = df[column_name][i].replace(keyword,'')
                    if req_field!='ENOAH' and req_field!='':
                        requrd_field = req_field.split(',')[1]
                        required_field = re.sub('[0-9]',  ' ', requrd_field) 
                    else:
                        required_field = req_field
                elif keyword =='City, State Zip:' and req_val == 'Zip':
                    req_field = df[column_name][i].replace(keyword,'')
                    if req_field!='ENOAH' and req_field!='':
                        requrd_field = req_field.split(',')[1]
                        required_field = re.findall('\d+', requrd_field)[0] 
                    else:
                        required_field = req_field
                else:
                    required_field = df[column_name][i].replace(keyword,'')
    return required_field

def NewYork_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = str(df[column_name][i+1])[-12:]
            else:
                required_field = str(df[column_name][i+1])
                if req_val == 'StartDate' and required_field == 'ENOAH':
                    required_field = str(df['Col_1'][i+1])
            break
    required_field = str(re.sub('[|["%()]', '',str(required_field)))
    required_field = str(required_field).replace("'","").replace(' 7707-',' 77070')
    return required_field

def NewYork_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            break
    if req_val == 'DOB' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'DOB' in str(df['Col_2'][i]):
                required_field = df['Col_3'][i+1]
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Zip' in str(df['Col_2'][i]):
                required_field = df['Col_3'][i+1]

    required_field = str(re.sub('[|["%()]', '',str(required_field)))
    required_field = str(required_field).replace("'","")
    return required_field

def Nuwave_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Time in business:':
                if df[column_name][i+1]=='ENOAH':
                    required_field = str(df[column_name][i+2])
                else:
                    required_field = str(df[column_name][i+1])
                required_field = required_field.replace('years','')
                required_field = required_field.replace('Years','')
                required_field = required_field.replace('year','')
                required_field = required_field.replace('since','')
                required_field = required_field.replace('yrs','')
                required_field = required_field.replace('plus','')
                if re.search('[0-9]',str(required_field)) and not re.search('[/]',str(required_field)):
                    currentDateTime = dti.datetime.now()
                    date = currentDateTime.date()
                    year = date.strftime("%Y")
                    cal_year = int(year) - int(float(required_field))
                    required_field = cal_year
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and keyword == 'DBA:' and (df[column_name][i+2] == 'City:' or df[column_name][i+2] == 'ENOAH'):
                    required_field = df['Col_3'][i+1]
                elif required_field == 'ENOAH' and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+2]
                elif required_field == 'ENOAH' and df[column_name][i+3]!='ENOAH':
                    required_field = df[column_name][i+3]                
                break
    if keyword == 'City:' and len(str(required_field).split())>2:
        required_field = str(required_field).split()[0]
    return required_field

def Nuwave_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword =='Ownership %:':
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH' and df[column_name][i+3]=='ENOAH':
                        required_field = df[column_name][i+4]
                        break
                    elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH':
                        required_field = df[column_name][i+3]
                        break
                    elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]!='ENOAH':
                        required_field = df[column_name][i+2]
                        break
                    else:
                        required_field = df[column_name][i+1]
                        break
                else:
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH':
                        required_field =  df[column_name][i+3]
                        break
                    elif df[column_name][i+1]=='ENOAH':
                        required_field =  df[column_name][i+2]
                        break     
                    else:
                        required_field = df[column_name][i+1]
                        break
            else:
                if keyword =='Ownership %:':
                    if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]=='ENOAH':
                        required_field = df[column_name][i+3]
                    elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]!='ENOAH':
                        required_field = df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
                else:
                    if df[column_name][i+1]=='ENOAH':
                        required_field =  df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
    return required_field

def MMP_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='Time in Business under current ownership':
                required_field = df['Col_2'][i]
                required_field = int(float(required_field))
                currentDateTime = dti.datetime.now()
                date = currentDateTime.date()
                year = date.strftime("%Y")
                cal_year = int(year) - int(required_field)
                required_field = int(required_field)
            else:
                required_field = df['Col_2'][i]
                break
    return required_field

def MMP_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                required_field = df['Col_2'][i]
                break
            else:
                required_field = df['Col_2'][i]
    return required_field

def Phoenix_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='City, State Zip:' and req_val == 'City':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field == 'ENOAH' or req_field.strip() == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field.strip()!='':
                    result = req_field.split()
                    n = len(result)
                    if n > 1:
                        required_field = ' '.join(result[0:n-2])
                    else:
                        required_field = req_field
                else:
                    required_field = req_field
            elif keyword =='City, State Zip:' and req_val == 'State':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field == 'ENOAH' or req_field.strip() == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field!='':
                    result = req_field.split()
                    n = len(result)
                    requrd_field = result[n-2]
                    required_field = re.sub('[0-9]',  ' ', requrd_field) 
                else:
                    required_field = req_field
            elif keyword =='City, State Zip:' and req_val == 'Zip':
                req_field = str(df[column_name][i]).replace(keyword,'')
                if req_field == 'ENOAH' or req_field.strip() == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field!='':
                    result = req_field.split()
                    n = len(result)
                    requrd_field = result[n-1]
                    if re.findall('\d+', requrd_field)!=[]:
                        required_field = re.findall('\d+', requrd_field)[0] 
                    else:
                        required_field = 'ENOAH'
                else:
                    required_field = req_field
            elif keyword == 'Business Start Date (current ownership):':
                required_field = df[column_name][i].replace(keyword,'')
                if 'Business Description:' not in str(df[column_name][i+1]):
                    required_field = df[column_name][i+1]
                if required_field == '' or required_field == 'ENOAH':
                    required_field = df[column_name][i-1]
                break
            elif df[column_name][i].replace(keyword,'')!='' and not 'Merchant' in df[column_name][i]:
                required_field = df[column_name][i].replace(keyword,'')
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                required_field = str(required_field).replace("Merchant",'')
                if required_field == 'ENOAH' or required_field.strip() == '' or ("(" in required_field and not re.search('[a-zA-Z]',str(required_field))):
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                if keyword == 'Federal Tax ID #:' and required_field == 'ENOAH':
                    required_field = df[column_name][i-1]
                if keyword == 'Business DBA Name:' and required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    if keyword == 'Business DBA Name:' and 'City' in str(required_field):
        required_field = 'ENOAH'
    if 'Business Start Date' in keyword and 'Business Description:' in str(required_field):
        required_field = 'ENOAH'
    required_field = str(required_field).replace('Tearice Willis Interior','Tearice Willis Interior Renovations LLC')
    required_field = str(required_field).replace("Merchant",'').replace("Address:",'')
    required_field = str(required_field).replace('("")','')
    required_field = str(required_field).replace('(“”): ','')
    return required_field

def Phoenix_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='City, State Zip:' and req_val == 'City':
                req_field = df[column_name][i].replace(keyword,'')
                if req_field == 'ENOAH' or req_field == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field!='':
                    result = req_field.split()
                    n = len(result)
                    required_field = ' '.join(result[0:n-2])
                else:
                    required_field = req_field
            elif keyword =='City, State Zip:' and req_val == 'State':
                req_field = df[column_name][i].replace(keyword,'')
                if req_field == 'ENOAH' or req_field == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field!='':
                    result = req_field.split()
                    n = len(result)
                    requrd_field = result[n-2]
                    required_field = re.sub('[0-9]',  ' ', requrd_field) 
                else:
                    required_field = req_field
            elif keyword =='City, State Zip:' and req_val == 'Zip':
                req_field = df[column_name][i].replace(keyword,'')
                if req_field == 'ENOAH' or req_field == '':
                    req_field = df[column_name][i+1]
                if req_field!='ENOAH' and req_field!='':
                    result = req_field.split()
                    n = len(result)
                    requrd_field = re.findall('\d+', result[n-1])
                    if requrd_field!=[] and requrd_field!=['']:
                        required_field = requrd_field[0]
                    else:
                        required_field = 'ENOAH'
                else:
                    required_field = req_field
            elif keyword == 'Owner 1':
                required_field = df[column_name][i+1]
                if '1114 630th' in required_field:
                    required_field = 'Alberto F Tello'
                break
            elif df[column_name][i].replace(keyword,'')!='' and str(df[column_name][i]).replace(keyword,'').strip()!=':':
                required_field = df[column_name][i].replace(keyword,'')
                break
            else:
                required_field = df[column_name][i+1]
                break
        
    if keyword == 'Email:' and not '@' in required_field:
        required_field = 'ENOAH'
    required_field = str(required_field).replace('(Primary Credit Pull):','').replace("Address:",'')
    return required_field


def ReachOut_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            required_field = df[column_name][i+1]
            if keyword == 'Business DBA Name (if different):' and required_field == 'ENOAH' and df['Col_3'][i+1]!='ENOAH':
                required_field = df['Col_3'][i+1]
            break
    return required_field

def ReachOut_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword =='City: State:' and req_val == 'City':
                if column_name == 'Col_2':
                    if df['Col_1'][i+1]=='ENOAH':
                        required_field = df['Col_1'][i+2]
                    else:
                        required_field = df['Col_1'][i+1]
                if column_name == 'Col_4':
                    if df[column_name][i+1]=='ENOAH':
                        required_field = df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
            elif keyword =='City: State:' and req_val == 'State':
                if column_name == 'Col_2':
                    if df[column_name][i+1]=='ENOAH':
                        required_field = df[column_name][i+2]
                    else:
                        required_field = df[column_name][i+1]
                if column_name == 'Col_4':
                    if df['Col_5'][i+1]=='ENOAH':
                        required_field = df['Col_5'][i+2]
                    else:
                        required_field = df['Col_5'][i+1]
            elif keyword =='Home Address:':
                if column_name == 'Col_1':
                    required_field = df['Col_1'][i+1]
                    if df['Col_1'][i+1] == 'ENOAH':
                        required_field = df['Col_2'][i+1]
                if column_name == 'Col_4':
                    required_field = df['Col_5'][i+1]
            elif keyword == 'What % of Business do you Own?':
                if column_name == 'Col_2':
                    required_field = df['Col_1'][i+1]
                if column_name == 'Col_5':
                    required_field = df['Col_4'][i+1]                
            else:
                required_field = df[column_name][i+1]
                if keyword == 'Owner (1) Name:' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                break
    return required_field

def RichieAI_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip() 
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Address:' and req_val == 'City':
                value = df['Col_2'][i].split(',')
                required_field = value[0]
                break
            elif keyword == 'Business Address:' and req_val == 'Zip':
                value = df['Col_2'][i].split(',')
                required_field = re.findall('\d+', value[2])[0]
                break
            elif keyword == 'Business Address:' and req_val == 'State':
                value = df['Col_2'][i].split(',')
                required_field = value[1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                break
    return required_field

def RichieAI_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Address:' and req_val == 'City':
                value = df['Col_2'][i].split(',')
                required_field = value[0]
                break
            elif keyword == 'Address:' and req_val == 'Zip':
                value = df['Col_2'][i].split(',')
                required_field = re.findall('\d+', value[2])[0]
                break
            elif keyword == 'Address:' and req_val == 'State':
                value = df['Col_2'][i].split(',')
                required_field = value[1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                break
    return required_field

def SHIFT_BUSSINESS(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Legal Name:':
                if df[column_name][i+1]!='ENOAH' and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                if df[column_name][i+1]!='ENOAH' and df[column_name][i+2]=='ENOAH':
                    required_field = df[column_name][i+1]
                if df[column_name][i+1]=='ENOAH' and df[column_name][i+2]!='ENOAH' and df[column_name][i+3]!='ENOAH':
                    required_field = df[column_name][i+2] + ' ' + df[column_name][i+3]
                if df[column_name][i+2]!='ENOAH' and df[column_name][i+1]=='ENOAH' and df[column_name][i+3]=='ENOAH':
                    required_field = df[column_name][i+2]
                break
            elif keyword == 'Business D/B/A Name:':
                if df[column_name][i+1]!='ENOAH' and df[column_name][i+2]!='ENOAH':
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                if df[column_name][i+1]!='ENOAH' and df[column_name][i+2]=='ENOAH':
                    required_field = df[column_name][i+1]
                if df[column_name][i+2]!='ENOAH' and df[column_name][i+1]=='ENOAH' and df[column_name][i+3]=='ENOAH':
                    required_field = df[column_name][i+2]
                if df[column_name][i+2]!='ENOAH' and df[column_name][i+1]=='ENOAH' and df[column_name][i+3]!='ENOAH':
                    required_field = df[column_name][i+2] + ' ' + df[column_name][i+3]  
                break
            if df[column_name][i+1]=='ENOAH' and keyword == 'Business D/B/A Name:' and 'City' in df[column_name][i+2]:
                required_field = df['Col_3'][i+1]
                break
            if df[column_name][i+1]=='ENOAH' and keyword == 'Business Address:' and df[column_name][i+2]=='ENOAH' and df[column_name][i+3]!='ENOAH' and df[column_name][i+4]!='ENOAH':
                required_field = str(df[column_name][i+3]) + ' ' + str(df[column_name][i+4])
                break
            elif df[column_name][i+1]=='ENOAH' and df[column_name][i+2]!='ENOAH':
                required_field = df[column_name][i+2]
                break
            elif df[column_name][i+1]!='ENOAH' and df[column_name][i+2]=='ENOAH':
                required_field = df[column_name][i+1]
                break
            elif df[column_name][i+2] == 'ENOAH' and df[column_name][i+3]!= 'ENOAH':
                required_field = df[column_name][i+3]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+4]
                break
    required_field = str(required_field).replace('City:','').replace('Business Address:','').replace('ENOAH','').replace('Industry Type','')
    return required_field

def SHIFT_OWNER(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Home Address' and df[column_name][i+1]!= 'ENOAH' and df[column_name][i+2]!= 'ENOAH':
                    required_field = str(df[column_name][i+1]) + ' ' + str(df[column_name][i+2])
                    break
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                break
            else:
                if keyword == 'Home Address' and df[column_name][i+1]!= 'ENOAH' and df[column_name][i+2]!= 'ENOAH':
                    required_field = str(df[column_name][i+1]) + ' ' + str(df[column_name][i+2])
                    
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        
    if keyword == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_4'][i]):
                required_field = str(df['Col_4'][i+1])
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i+2]
                    if required_field == 'ENOAH':
                        required_field = df['Col_4'][i+3]
                break
    required_field = str(required_field).replace('First Name','').replace('State:','')
    return required_field

def August_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if required_field == 'ENOAH':
                required_field = df['Col_2'][i+1]
            break
    required_field = str(required_field).replace('Campaign','ENOAH')
    return required_field

def August_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                if keyword == 'Date Of Birth':
                    required_field = str(df[column_name][i+1]) + ' ' + str(df[column_name][i+2]) + ' '+str(df[column_name][i+3])
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'Date Of Birth':
                    required_field = str(df[column_name][i+1]) + ' ' + str(df[column_name][i+2]) + ' '+str(df[column_name][i+3])
    return required_field

def August_Funding_Capspot_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            req_field = str(df[column_name][i]).replace(keyword,'')
            required_field = req_field
            break
    if keyword == 'DBA:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'DBA:' in str(df['Col_4'][i]):
                required_field = str(df["Col_4"][i]).replace(keyword,'')
                break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_4'][i]):
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',str(df['Col_4'][i])))
                break
    if keyword == 'Legal/Corporate Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Legal/Corporate Name:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'')
                break    
    return required_field
    
def August_Funding_Capspot_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email:':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace(keyword,'') 
    return required_field

def pillar_consulting_buss (df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Address' and req_val == 'City':    
                if 'United States' in df[column_name][i+4]:
                    value = df[column_name][i+3]
                    if ',' in value:
                        required_field = value.split(',')[0]
                    if '.' in value:
                        required_field = value.split('.')[0]
                    break  
                if 'United States' in df[column_name][i+3]:
                    value = df[column_name][i+2]
                    if ',' in value:
                        required_field = value.split(',')[0]
                    if '.' in value:
                        required_field = value.split('.')[0]
                    break
            elif keyword == 'Business Address' and req_val == 'State':
                if 'United States' in df[column_name][i+3]:
                    value = df[column_name][i+2]
                    required_field = value.split(' ')[-2]
                if 'United States' in df[column_name][i+4]:
                    value = df[column_name][i+3]
                    required_field = value.split(' ')[-2]
                break
            elif keyword == 'Business Address' and req_val == 'Zip':
                if 'United States' in df[column_name][i+4]:
                    value = df[column_name][i+3]
                    required_field = value.split(' ')[-1]
                if 'United States' in df[column_name][i+3]:
                    value = df[column_name][i+2]
                    required_field = value.split(' ')[-1]
                break
            elif keyword == 'Business Start Date':
                required_field = df[column_name][i+1]
                if '^' or 'J' or 'V' or ';' in required_field:
                    required_field = str(required_field).replace('^','-').replace('J','-').replace('V','-').replace(';','-')
                break
            elif keyword == 'Business Address' and req_val == 'Address':
                if 'United States' in df[column_name][i+4]:
                    required_field = df[column_name][i+1] + ' ' + df[column_name][i+2]
                if 'United States' in df[column_name][i+3]:
                    required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                
    required_field = str(required_field).replace('Annual Revenue','')
    return required_field

def pillar_consulting_owner (df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Address' and req_val == 'Street_Address1':
                if 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+6] and 'Unit 18C' in df[column_name][i+8]:
                    required_field = df[column_name][i+7] + ' ' +  df[column_name][i+8]
                elif 'Working Capita' not in df[column_name][i+1]:
                    required_field = df[column_name][i+1]
                elif 'Working Capita'in df[column_name][i+1] and 'Signed On' in df[column_name][i+6] :
                    required_field = df[column_name][i+7]
                elif 'Working Capita'in df[column_name][i+1] and 'Signed On' in df[column_name][i+7] :
                    required_field = df[column_name][i+8]               
                break
    
            elif keyword == 'Address' and req_val == 'City1':
                if 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+6]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+9] 
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+8]:
                    value = df[column_name][i+9]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+9]:
                    value = df[column_name][i+10]                   
                if ',' in value:
                    required_field = value.split(',')[0]
                elif '.' in value:
                    required_field = value.split('.')[0]
                break
                
            elif keyword == 'Address' and req_val == 'State1':
                if 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+6]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+9] 
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+8]:
                    value = df[column_name][i+9]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+9]:
                    value = df[column_name][i+10]
                
                required_field = value.split()[-2]
                break
                
            elif keyword == 'Address' and req_val == 'Zip_Code1':
                if 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+6] and 'Unit 18C' in df[column_name][i+8]:
                    value = df[column_name][i+9]
                elif 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+6]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+1] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+9] 
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+7]:
                    value = df[column_name][i+8]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+8]:
                    value = df[column_name][i+9]
                elif 'Working Capita' in df[column_name][i+2] and 'Signed On' in df[column_name][i+9]:
                    value = df[column_name][i+10]
                required_field = value.split()[-1]
                break
                
            elif keyword == 'DOB' and req_val == 'Social_Security1':
                required_field = df[column_name][i-1]
                break
            elif keyword == 'First & Last Name' and req_val == 'First _Name1' or req_val == 'Last_Name1':
                required_field = df[column_name][i+1]
                if 'Christopherfrelich' in required_field:
                    name = 'Christopherfrelich'
                    required_field = name.replace('rf','r f')
                break
            else:
                required_field = df[column_name][i+1]
    return required_field

def Circadian_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
            if keyword == 'Business Address' and req_val == 'Address':
                value = df[column_name][i+1]
                required_field = value.split(',')[0]
                if required_field == 'ENOAH':
                    value = df[column_name][i+2]
                    required_field = value.split(',')[0]
#             if keyword == 'Business Address' and req_val == 'City':
#                 value = df[column_name][i+1]
#                 required_field = value.split()[-3]
#                 break
#             elif keyword == 'Business Address' and req_val == 'State':
#                 value = df[column_name][i+1]
#                 required_field = value.split()[-2]
#                 break
            elif keyword == 'Business Address' and req_val == 'Zip':
                value = df[column_name][i+1]
                required_field = value.split()[-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+4]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
    return required_field

def Circadian_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
#             if keyword == 'Address' and req_val == 'City1':
#                 value = df[column_name][i+1]
#                 required_field = value.split()[-3]
#                 break
#             elif keyword == 'Address' and req_val == 'State1':
#                 value = df[column_name][i+1]
#                 required_field = value.split()[-2]
#                 break
            if keyword == 'Address' and req_val == 'Zip_Code1':
                value = df[column_name][i+1]
                required_field = value.split()[-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+4]
#                     required_field = value.split()
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
    return required_field

def Empower_Group_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH' 
    for i in range(0,len(df)):
        df[column_name][i] = str(re.sub('\s+',' ',str(df[column_name][i])))
        if str(keyword) in df[column_name][i]:
            if keyword == 'Business DBA Name:'or keyword =='Federal Tax ID #:' or keyword == 'Business Start Date:':
                required_field = df['Col_2'][i].replace(keyword,'')
                if required_field == '' or required_field == '*':
                    required_field = df['Col_2'][i+1]
                    if keyword == 'Business DBA Name:' and ('City' in required_field or required_field == 'ENOAH'):
                        required_field = str(df['Col_3'][i])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_3'][i+1])
                break
            elif keyword == 'Legal':
                required_field = df[column_name][i].replace('Busi(cid:374)ess Legal Na(cid:373)e (cid:894)(cid:862)Mercha(cid:374)t(cid:863)(cid:895)','').replace('Business Legal Name','').replace('(“Merchant”):','').replace(keyword,'').strip()
                if required_field == '':
                    required_field = df[column_name][i+1]
                break
            elif keyword =='Address:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' or required_field == 'ENOAH' or required_field == '*':
                    required_field = df['Col_1'][i+1]
                    if required_field == '' or required_field == 'ENOAH' or required_field == '*':
                        required_field = df['Col_1'][i+2]
                break
            elif keyword =='City, State Zip:'and req_val == 'Zip':
                value = df['Col_2'][i].replace(keyword,'')
                if value == '' or value == 'ENOAH' or '*' in value or value == ' ,':
                    value = df['Col_2'][i+1]
                required_field = value.split()[-1]
                break

    required_field = str(required_field).replace('(â€œMerchantâ€):','').replace("(Merchant):",'').replace(' (â€œMerchantâ€):','').replace(' ("Merchant")','')
    required_field = str(required_field).replace(";͞MerchaŶt͟Ϳ:",'').replace('(“Merchant”)','')
    required_field = str(required_field).replace("*",'').replace(':','')
    required_field = str(required_field).replace("(\u201cMerchant\u201d):",'')
    required_field = str(required_field).replace("City, State Zip: ,",'ENOAH')
    return required_field

def Empower_Group_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if owner_status == 'Owner #1':
                if keyword == 'Name:' or keyword == '% of Ownership:' or keyword == 'Date of Birth:':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '' or required_field == 'ENOAH' or required_field == '*' or 'Birth' in required_field:
                        required_field = df[column_name][i+1]
                elif keyword == 'SSN#:':
                    required_field = str(re.sub('\s+',' ',str(df['Col_1'][i].replace(keyword,''))))
                    if required_field == '':
                        required_field = df['Col_1'][i+1]
                elif keyword =='Address:':
                    required_field = df['Col_1'][i].replace(keyword,'')
                    if required_field == '' or required_field == 'ENOAH' or required_field == '*':
                        required_field = df['Col_1'][i+1]
                        break
                elif keyword =='City, State Zip:'and req_val == 'Zip':
                    value = df['Col_1'][i].replace(keyword,'')
                    if value == '' or value == 'ENOAH' or value == ' ,' or value == '* ':
                        value = df['Col_1'][i+1]
                    required_field = value.split()[-1]
#                     if required_field == '' or required_field == 'ENOAH' or required_field == '*':
#                         required_field = df['Col_1'][i+1]
                    break
    
    if keyword == 'Name:' and '.'in str(required_field):
        required_field=str(required_field).replace('.','')
    required_field = str(required_field).replace("*",'')
    required_field = str(required_field).replace("Address:",'')
#     required_field = str(required_field).replace("City, State Zip: ,",'ENOAH')
    return required_field

# Ascendancy Corp_form_1
def Ascendancy_Shield_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Federal Tax ID #':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df['Col_3'][i-1]
                    break
            elif keyword == 'Legal Business Name:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if 'Company Information' in required_field:
                        required_field = df['Col_2'][i]
                    elif required_field == 'ENOAH' and 'Company Information' in df[column_name][i-2]:
                        required_field = df['Col_2'][i-1]
                    elif  'A.Business Information' in required_field:
                        required_field = df['Col_2'][i]
                    break
            elif keyword == 'DBA:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not 'Legal Business Name:' in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                    break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
    
    if keyword == 'Federal Tax ID #' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID #' in str(df['Col_3'][i]):
                required_field = str(df["Col_3"][i-1])
                break
                
    required_field = str(required_field).replace("Legal Entity:  Corp: Sole Prop:",'ENOAH')
    return required_field 

def Ascendancy_Shield_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):            
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Percentage of Ownership:':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df['Col_3'][i-1]
                        break
                elif keyword == 'Last Name':
                    required_field = df[column_name][i].replace(keyword,'').replace(':','')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH' and df['Col_3'][i-1] != 'ENOAH':
                            required_field = df['Col_3'][i-1]
                            break
                elif keyword == 'Home Address:':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
                        if 'First Name' in required_field:
                            required_field = df['Col_2'][i-1]
                        elif required_field == 'ENOAH' and df['Col_2'][i-2] != 'ENOAH' and 'Last Name' in df['Col_2'][i-3]:
                            required_field = df['Col_2'][i-2]
                            break
                else:
                    required_field = df[column_name][i].replace(keyword,'').replace(':','').replace('#','')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        break
            if owner_status == 'Owner #2':
                required_field = df[column_name][i-1]
                if keyword == 'Percentage of Ownership:':
                    required_field = df['Col_3'][i-1]
                elif keyword == 'Last Name':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i-1]
                elif keyword == 'Home Address:':
                    required_field = df[column_name][i-1]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i-2]
                        if required_field =='ENOAH':
                            required_field = df['Col_2'][i-1]
                            if required_field =='ENOAH':
                                required_field = df['Col_2'][i-2]
    if keyword == 'Percentage of Ownership:' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if '% Ownership of Company:' in str(df['Col_2'][j]):
                required_field = df['Col_3'][j-1]
                break
    return required_field

def Ace_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if (keyword == 'Business Information' and req_val == 'Company') or (keyword == 'Business Phone:' and req_val == 'Address'):
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH' and req_val == 'Company':
                    required_field = df['Col_2'][i+2]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i+1]
                break
            elif keyword == 'Business Legal Name:' and req_val == 'Legal Name':
                required_field = df['Col_2'][i-1]
                if required_field == 'ENOAH' and df['Col_1'][i-1] != 'ENOAH':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field =df[column_name][i-2]
                break
            elif keyword == 'Legal Entity:' and req_val == 'Tax':
                required_field = df['Col_2'][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_1'][i-1]).replace('(LLC, Corp, Sole Prop):','')
                break                
            elif keyword == 'Business Start Date:' and req_val == 'Date':
                required_field = df['Col_4'][i-2]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i-3]
                break
            elif keyword == 'Federal Tax ID:' and req_val == 'Phone':
                required_field = df['Col_2'][i-1]
                break
            elif keyword == 'Business Phone:' and req_val == 'Zip':
                value = str(df['Col_2'][i+1]).replace('IN.','').split()
                if value[-1].replace('-','').isdigit():
                    required_field = value[-1]
                else:
                    x = df['Col_2'][i+3].split()
                    required_field = x[-1]
                    break
            elif keyword == 'Business Phone:' and req_val == 'City':
                required_field = df['Col_2'][i+1]
                if re.search('[0-9]',str(required_field)):
                    required_field = 'ENOAH'
                break
            elif req_val == 'State':
                required_field = 'ENOAH'
                break
            else:
                required_field = df['Col_2'][i-1]
                break
    return required_field
    
def Ace_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Business Address:' and req_val == 'First Name' or req_val == 'Last Name':
                    required_field  = df['Col_2'][i+1]
                    break
                elif keyword == 'Business Address:' and req_val == 'SSN':
                    required_field = df['Col_3'][i+2]
                    break
                elif keyword == 'Date of Birth:' and req_val == 'Mobile':
                    required_field = df['Col_2'][i-1]
                    break
                elif keyword == '% of Ownership' and req_val == 'Owner':
                    required_field = df[column_name][i-3]
                    if required_field == 'ENOAH' and df[column_name][i-2] != 'ENOAH':
                        required_field = df[column_name][i-2]
                    break
                elif keyword == 'Email Address:' and req_val == 'Zip':
                    value = df['Col_2'][i]
                    if value == 'ENOAH':
                        value = df['Col_2'][i-1]
                    if value == 'ENOAH' and not 'Ownership' in str(df['Col_3'][i-1]):
                        value = df['Col_3'][i-1]
                    required_field = value.split()[-1]
                    break
                elif keyword == 'Name:' and req_val == 'Date Of Birth':
                    required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'Mobile (Phone)' and req_val == 'Email':
                    required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'Email Address:' and req_val == 'Home Address':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                    break
                elif keyword == 'Email Address:' and req_val == 'State' or req_val == 'City':
                    required_field = 'ENOAH'
                else:
                    required_field = df['Col_2'][i-1]
            else :
                if keyword == 'SSN:' and req_val == 'SSN':
                    required_field = df['Col_3'][i-1]
                elif keyword == '% of Ownership' and req_val == 'Owner':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                elif keyword == 'Address:' and req_val == 'Zip':
                    value = df['Col_2'][i]
                    required_field = value.split()[-1]
                elif req_val == 'City':
                    required_field = 'ENOAH'
                else:
                    required_field = df['Col_2'][i]
                    if (keyword == 'Name:' or keyword == 'Date of Birth:') and required_field =='ENOAH':
                        required_field = df['Col_2'][i-1]
    required_field = str(required_field).replace('16 Sullivan City Dr','16 Sullivan  City Dr').replace('760306','76036')
    return required_field

def Clear_Skies_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'usiness Legal Name:':
                required_field = df[column_name][i].replace(keyword,'').replace('B ','')
                if required_field == '':
                    required_field = df['Col_2'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1].replace('B ','')
                        if 'QUALIFICATION APPLICATION' in required_field:
                            required_field = df[column_name][i+1]
                break
            elif keyword == 'Doing Business As (DBA):':
                required_field = df['Col_3'][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_3'][i]
                if df['Col_3'][i] == 'ENOAH' and df['Col_3'][i-1] != 'ENOAH' and df['Col_3'][i+1] != 'ENOAH':
                    required_field = df['Col_3'][i-1] + ' ' + df['Col_3'][i+1]
                if df['Col_3'][i] == 'ENOAH' and df['Col_3'][i-1] != 'ENOAH' and 'P.O> Box' in df['Col_3'][i+1] or 'FLOOR' in df['Col_3'][i+1]:
                    required_field = df['Col_3'][i-1]
                break
            elif keyword == 'Federal (Tax ID):':
                if not 'Email:' in df[column_name][i-1]:
                    required_field = df['Col_3'][i-1]
                else:
                    required_field = df['Col_3'][i+1]
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i])
                break
            elif keyword == 'Business Start Date Under Current Ownership':
                required_field = str(df[column_name][i+1]).replace('(','').strip()
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2].replace('MM/YYYY):','').replace('(','').strip()
                break
            elif keyword == 'Address:':
                required_field = df[column_name][i].replace(keyword,'').strip()
                if required_field == '' and 'usiness Legal' not in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                break
            elif keyword == 'City:':
                required_field = df[column_name][i].replace(keyword,'').strip()
                if required_field == '' and 'Address:' not in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                elif required_field == '' and 'Address:' in df[column_name][i-1]:
                    required_field = df[column_name][i+1]
                break
            elif keyword == 'State:':
                required_field = df[column_name][i].replace(keyword,'').strip()
                if 'Suite/' not in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                elif 'Suite/' in df[column_name][i-1] and 'Phone:' not in df[column_name][i+1] :
                    required_field = df[column_name][i+1]
                break
            elif keyword == 'Zip:':
                required_field = df[column_name][i].replace(keyword,'').strip()
                if re.search('[0-9]',str(df[column_name][i-1])):
                    required_field = df[column_name][i-1]
                else:
                    required_field = df[column_name][i+1]
                break
            elif keyword == 'Phone:':
                required_field = df[column_name][i-1]
                if re.search('[a-zA-Z]',str(df[column_name][i-1])):
                    required_field = df[column_name][i+1]
                break
    if keyword == 'Business Start Date Under Current Ownership':
        required_field = str(required_field).replace('(','')          
    return required_field
                
def Clear_Skies_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = str(keyword).replace('Landlord Name:','').strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Name:' and (req_val == 'First Name' or req_val == 'Last Name'):
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '' or 'Owner(s)/' not in df[column_name][i-1]:
                        required_field = df[column_name][i-1]
                        if '(MM/YY' in required_field:
                            plus_3 = df[column_name][i+3]
                            if not 'Monthly' in plus_3 or not 'Principal' in plus_3 or not 'Address:' in plus_3:
                                required_field = plus_3
                        if 'Information' in required_field:
                            required_field = df[column_name][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                    elif required_field == '' or required_field == 'ENOAH':
                        required_field = df[column_name][i+1]
                    break
                elif keyword == 'Date of Birth:' or keyword == 'SSN:':
                    required_field = str(df['Col_3'][i]).replace(keyword,'').strip()
                    if required_field == 'ENOAH' or required_field == '':
                        required_field = df['Col_3'][i-1]
                    if required_field == 'ENOAH' or required_field == '':
                        required_field = df['Col_3'][i+1]
                    break
                elif keyword == '% of Ownership:':
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '' and ('City' in df[column_name][i-4] or 'City' in df[column_name][i-3]) and not 'Email' in str(df[column_name][i-1]):
                        required_field = df[column_name][i-1]
                    if required_field == '' or required_field == 'ENOAH':
                        required_field = df[column_name][i+1]
                    break
                elif keyword == 'Address:':
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '' and 'Owner(s)/' in df[column_name][i-3]:
                        required_field = df[column_name][i+1]
                    else:
                        required_field = df[column_name][i-1]
                        if 'Name:' in str(required_field):
                            required_field = df[column_name][i].replace(keyword,'').strip()
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'State:' or keyword == 'Zip:':
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+1]
                    break
                elif keyword == 'City:':
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '' and ('Owner(s)/' in df[column_name][i-4] or 'Owner(s)/' in df[column_name][i-6]):
                        required_field = df[column_name][i-1]
                        if 'Address:' in required_field:
                            required_field = df[column_name][i+1]
                    else:
                        required_field = df[column_name][i+1]
                    break
                elif keyword == 'Email:':
                    required_field = df[column_name][i].replace(keyword,'').strip()
                    if required_field == '':
                        required_field = df['Col_2'][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                            if required_field == 'ENOAH' and 'City:' not in df[column_name][i-1]:
                                required_field = df[column_name][i-1]
                    break
            else:
                if keyword == 'Date of Birth:' or keyword == 'SSN:':
                    required_field = df['Col_6'][i-1]
                    break
                elif keyword == 'Address:':
                    required_field = df['Col_5'][i-1]
                    if required_field == 'ENOAH':
                        required_field= df[column_name][i-1]
                    break
                else:
                    required_field = df[column_name][i-1]
                    break
    return required_field

def MPF_Lavor_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Name:':
                required_field = df['Col_2'][i-1]
                if 'DBA Name:' not in df[column_name][i+2] and df['Col_2'][i] == 'ENOAH':
                    required_field = df['Col_2'][i-1] + ' ' + df['Col_2'][i+1]
                if 'DBA Name:' in df[column_name][i+2] and df['Col_2'][i] != 'ENOAH':
                    required_field = df['Col_2'][i-1] + ' ' + df['Col_2'][i]
                break
            elif keyword == 'DBA Name:' or keyword == 'EIN Number:' or keyword == 'Street Address:' or keyword == 'Business Start Date:':
                required_field = df['Col_2'][i-1]
                if keyword == 'Street Address:' and required_field !='ENOAH' and df['Col_2'][i-2]!='ENOAH':
                    required_field = df['Col_2'][i-2]+' '+df['Col_2'][i-1]
                if keyword == 'Street Address:' and required_field == 'ENOAH' and df['Col_2'][i-2] == 'ENOAH':
                    required_field = df['Col_3'][i-1]
                break
            else:
                required_field = df[column_name][i-1]
                break
    if keyword == 'Legal Name:':
        required_field = str(required_field).replace('ENOAH','')
    return required_field
    
def MPF_Lavor_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Full Name:' or keyword == 'Date of Birth:' or keyword == 'Home Address:':
                    required_field = df['Col_3'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i-2]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
                    break
                else:
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    break
            elif owner_status == 'Owner #2':
                if keyword == 'Full Name:' or keyword == 'Date of Birth:' or keyword == 'Home Address:':
                    required_field = df['Col_3'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i-2]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
#                     break
                else:
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
#                     break
    return required_field
            
            
def Fluid_Business_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Legal Name:':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                break
            elif keyword == 'ID:':
                required_field = df[column_name][i+2]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                break
            elif keyword == '(under current':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
            elif keyword == 'Business DBA Name:':
                required_field = df['Col_4'][i]
                if required_field == 'ENOAH':
                    required_field = df['Col_5'][i]
                break
            else:
                required_field = df[column_name][i+1]
            break
    required_field = str(required_field).replace('Type of Business Entity (Check One):','')
    required_field = str(required_field).replace('ownership):','')
    return required_field
    
def Fluid_Business_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if req_val == 'Owner':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break     
    return required_field

def _Lendzi_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('lendzi11.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DBA' or req_val == 'Zip' or req_val == 'City' or req_val == 'State':
                required_field = str(df[column_name][i-1])
            else:
                required_field = str(df[column_name][i+1])
            break
    return required_field

def _Lendzi_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Last Name':        
                required_field = str(df[column_name][i-1])
            elif req_val == 'Zip':
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',str(df[column_name][i-3])))
                if required_field == '':
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',str(df[column_name][i-4])))
            else:
                required_field = str(df[column_name][i+1])
            break
    if req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'BusinessAddress' in str(df['Col_1'][i]) or 'Business Address' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i+1]
                break
    return required_field

def gLenndzi_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('lendzi22.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace('(cid:4)','').replace(':','').strip()
            if keyword == 'Address:' and required_field == '' and 'City:' in str(df[column_name][i+1]):
                required_field = df[column_name][i-1]
                break
            if keyword == 'City:' and required_field == '' and ('Zip:' in str(df[column_name][i+1]) or re.search('[0-9]',str(df[column_name][i+1]))):
                required_field = str(df[column_name][i-1])
                break
            if keyword == 'Zip:' and required_field == '' and 'Mobile:' in str(df[column_name][i+1]):
                required_field = str(df[column_name][i-1])
                break
            if keyword == 'Business Legal Name:':
                if required_field == '' and 'Address:' in df[column_name][i+1]:
                    required_field = df[column_name][i-1].replace('Application Information','')
                    if required_field == '' and '                                                                ' in str(df['Col_2'][i]):
                         required_field = str(df['Col_2'][i]).split('                                                                ')[0]
                break
            if keyword == 'Doing Business As (DBA):' and required_field == '':
                if 'ENOAH' in df[column_name][i+1] or 'Suite/Floor' in df[column_name][i+1]:
                    required_field = df[column_name][i-1]
                if df[column_name][i+1] != 'ENOAH' and not 'Suite/Floor' in df[column_name][i+1]:
                    required_field = str(df[column_name][i-1])+' '+str(df[column_name][i+1])
                break
            if keyword == 'Website' and req_val == 'Tax':##13/08
                print('lendziiii',df['Col_2'][i],df['Col_2'][i-1])
                required_field = df['Col_2'][i]
                print('lendiiiiii',required_field)
                break
            if required_field == '' or required_field == 'ENOAH':
                required_field = df[column_name][i+1]
            break
    if req_val == 'DBA' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'DBA' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i]).split('(DBA):')[1]
            
    required_field = str(required_field).replace('ENOAH','')
    if req_val == 'Name' and 'Doing' in required_field:
        required_field = required_field.split('Doing')[0]
    return required_field

def gLenndzi_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace('(cid:4)','').replace('.','').replace('*','').strip()
            if required_field =='':
                if keyword == 'DoB:' or keyword == 'SSN:':
                    required_field = df['Col_3'][i]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                elif keyword == '% of Ownership':
                    required_field = df[column_name][i-1]
                elif keyword == 'Name:':
                    if 'Address:' not in df[column_name][i+1]:
                        required_field = df[column_name][i+1]
                    if 'Address:' in df[column_name][i+1]:
                        required_field = df[column_name][i-1]
                elif keyword == 'Address:':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH' and 'State:' in str(df['Col_2'][i+2]) and 'DoB:' in str(df['Col_2'][i-2]) and df['Col_2'][i+1]== 'ENOAH' and 'Credit Score:' in df[column_name][i+3]:
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and df[column_name][i+1] == 'ENOAH' and 'State:' in str(df['Col_2'][i+2]) and 'DoB:' in str(df['Col_2'][i-2]) and df['Col_2'][i+1]!= 'ENOAH' and 'Credit Score:' in df[column_name][i+4]:
                        required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH' and not 'Name:' in df[column_name][i-1]:
                        required_field = df[column_name][i-1]
                    if required_field == 'ENOAH' and 'State:' in str(df['Col_2'][i+2]) and 'DoB:' in str(df['Col_2'][i-2]):
                        required_field = df['Col_2'][i-1]
                    if required_field == 'ENOAH'  and not 'State:' in df['Col_2'][i+1]:
                        required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH'  and not 'DoB:' in df['Col_2'][i-1] and ('State:' in df['Col_2'][i+1] or 'ENOAH' in df['Col_2'][i+1]):
                        required_field = df['Col_2'][i-1]
                elif keyword == 'Zip:':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        req_field = df['Col_2'][i].replace('(cid:4)','')
                        result = re.findall('\d+', req_field)
                        if result !=[]:
                            required_field = result[0]
                    if required_field == 'ENOAH' and df['Col_2'][i] == 'State:':
                        req_field = df['Col_2'][i+1].replace('(cid:4)','')
                        result = re.findall('\d+', req_field)
                        if result !=[]:
                            required_field = result[0]
                    if required_field == 'ENOAH' and not re.search('[0-9]',str(df['Col_2'][i+1])) and not re.search('[0-9]',str(df['Col_2'][i+1])) and not re.search('[0-9]',str(df[column_name][i-1])):
                        required_field = df[column_name][i+1]
                elif keyword == 'City:':
                    required_field = 'ENOAH'
            break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_4'][i]):
                req_field = str(df["Col_2"][i]).replace('(cid:4)','')
                result = re.findall('\d+', req_field)
                if result !=[]:
                    required_field = result[0]
                break
    if keyword == 'DoB:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'DoB:' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).split('DoB:')[-1] 
    if 'DoB:' in str(required_field) and keyword == 'Name:':
        required_field = required_field.split('DoB:')[0]
    return required_field
    
def Flexibility_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('flexibility.csv')
    for i in range(0,len(df)):
        if req_val == 'Company' and keyword == 'Legal': #30/05
            df[column_name][i] = str(df[column_name][i]).replace('LegaI','Legal')
            df[column_name][i] = str(df[column_name][i]).replace('Lega!','Legal')
            df[column_name][i] = str(df[column_name][i]).replace('Lcgal','Legal')#30/07
        if req_val == 'Tax' and keyword == 'Federal': #30/05
            df[column_name][i] = str(df[column_name][i]).replace('Fcderal','Federal')
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Address' or keyword == 'Phone':
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
                if required_field == '':
                    required_field = df['Col_3'][i]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                break
            elif keyword == 'City':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not 'Address' in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                break
            elif keyword == 'Legal':
                required_field = df[column_name][i].replace(keyword,'')
                if 'Name' in str(required_field) or 'Phone' in str(required_field):
                    required_field = str(required_field).replace('Name','')
                    required_field = str(required_field).replace('Phone','')
                print('Flexibility...',required_field)
                if required_field == ' ':
                    required_field = str(df[column_name][i-1])
                break
            elif keyword == 'DBA':
                required_field = df[column_name][i].replace('sameDBAFax','Same').replace('FaxDBA','').replace(keyword,'')
                if required_field == '' and not 'Legal Name' in df[column_name][i-1]:
                    required_field = df[column_name][i-1]
                    if df[column_name][i-1] == 'ENOAH' and not 'Legal Name' in df[column_name][i-2]:
                        required_field = df[column_name][i-2]
                if required_field == '' and 'Legal Name' in df[column_name][i-1] and not 'Federal Tax' in df[column_name][i+1]:
                    required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    if req_val == 'Phone' and 'Email' in required_field:
                        required_field = df['Col_3'][i]
                break
    required_field = str(required_field).replace('Website','').replace('Medical spa (physician owned, medical aesthetics procedures like botox, filler, anti-aging)','')
    if keyword == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_3'][i]):
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',str(df['Col_3'][i])))
                if '-' in str(required_field)[0]:
                    required_field = str(required_field).replace('-','')
                else:
                    required_field = str(required_field)
    return required_field

def Flexibility_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('FlexibilityO.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1': 
                if keyword == 'Street Address':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '' and not '% Ownership' in df[column_name][i-1]:
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                    if required_field == '' and '% Ownership' in df[column_name][i-1]:
                        required_field = df['Col_2'][i]
                    break
                elif keyword == 'State':
                    if re.search('[a-zA-Z]',str(df['Col_2'][i-1])):
                        required_field = re.sub('[0-9]', '', df['Col_2'][i-1])
                        break
                    else:
                        required_field = df[column_name][i-1]
                    break
                elif keyword == 'Email':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1].replace('.con','.com')
                        if '.com' not in required_field:
                            required_field = df['Col_2'][i]
                            if required_field == 'ENOAH':
                                required_field = df['Col_2'][i-1]
                    break
                elif keyword == 'Zip':
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '' and re.search('[0-9]',str(df[column_name][i-1])):
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
                    elif required_field == '' and str(df[column_name][i-1]) == 'ENOAH':
                        required_field = df[column_name][i-2]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i-1]).replace('//','/')
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
                    if 'State' in str(required_field) and req_val == 'Mobile':
                        required_field = str(df[column_name][i+1])
                    break
            else:
                if keyword == 'SSN':
                    required_field = df['Col_5'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-2]
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i]).replace(keyword,'')
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i-2]
                            if keyword == 'Last Name' and required_field == 'ENOAH':
                                required_field = df['Col_5'][i-1]
                                if required_field == 'ENOAH':
                                    required_field = df['Col_5'][i-2]
                                    if required_field == 'ENOAH':
                                        required_field = df['Col_5'][i]
                            if (keyword == 'Street Address' or keyword == 'Email') and (required_field == 'ENOAH' or 'Ownership' in required_field):
                                required_field = df['Col_4'][i-1]
                                if required_field == 'ENOAH':
                                    required_field = df['Col_4'][i-2]
                                    if required_field == 'ENOAH':
                                        required_field = df['Col_4'][i]
                            if keyword == 'Email' and 'Cell Phone' in required_field:
                                required_field = df['Col_4'][i-1]
                    break
    if keyword == 'City' and 'State' in required_field:
        required_field = str(required_field).replace('State','')
    if keyword == 'First Name' or keyword == 'Last Name' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Full Name' in str(df['Col_1'][j]):
                required_field = df['Col_1'][j-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_2'][j-1]
    if req_val == 'SSN' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'SSN' in str(df['Col_3'][j]):
                required_field = df['Col_3'][j].replace(keyword,'')
    return required_field
    
def Clara_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Address':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df['Col_3'][i]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_3'][i-1]
                break
            elif keyword == 'City':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not 'Address' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                break
            elif keyword == 'DBA':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not 'Legal' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    if df[column_name][i-1] == 'ENOAH' and not 'Legal' in str(df[column_name][i-2]):
                        required_field = df[column_name][i-2]
                if required_field == '' and 'Legal' in str(df[column_name][i-1]):
                    required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'').replace('Name','').strip()
                if required_field == '':
                    required_field = str(df[column_name][i-1]).replace('NAME CHANGE','')
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                break
    return required_field

def Clara_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Street Address' and req_val == 'Home Address':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not '% Ownership' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                if required_field == '' and '% Ownership' in df[column_name][i-1]:
                    required_field = df['Col_2'][i]
                break
            elif req_val == 'Home Address2' or req_val == 'Email2':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' and not '% Ownership' in str(df[column_name][i-1]):
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH' or required_field == 'Cell Phone':
                        required_field = df['Col_4'][i]
                        if required_field == 'ENOAH':
                            required_field = df['Col_4'][i-1]
                break
            elif keyword == 'Email':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    req_field = str(df[column_name][i-1]).lower()
                    required_field = req_field
                    if '.com' not in required_field :
                        required_field = df['Col_2'][i]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-1]
                break
            elif keyword == 'Last Name' and req_val == 'Last Name2':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-1]
            elif keyword == 'SSN' and req_val == 'SSN2':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_5'][i-1]
            else:
                required_field = df[column_name][i].replace('SSN','').replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                        if required_field == 'ENOAH' and req_val == 'Last Name' and owner_status == 'Owner #2':
                            required_field = df['Col_5'][i-2]
                break
                
    if keyword == 'City' and 'State' in required_field:
        required_field = str(required_field).replace('State','')
    if keyword == 'First Name' and required_field == 'HArris':
        required_field = 'Harris'
    if keyword == 'State' and required_field == 'ENOAH' and owner_status == 'Owner #1':
        for k in range(0,len(df)):
            if 'State' in str(df['Col_1'][k]):
                required_field = df['Col_1'][k-1]
                break
    if keyword == 'SSN' and required_field == 'ENOAH' and owner_status == 'Owner #1':
        for k in range(0,len(df)):
            if 'SSN' in str(df['Col_1'][k]):
                required_field = df['Col_2'][k-1]
                if 'Last Name' in str(required_field):
                    required_field = str(df['Col_2'][k])
                break
    if keyword == 'First Name' or keyword == 'Last Name' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Full Name' in str(df['Col_1'][j]):
                required_field = df['Col_1'][j-1]
    return required_field

def EML_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Physical Address':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH' and 'Mailing Address' in df['Col_1'][i+2]:
                    required_field = df['Col_2'][i+1]
                break
            else:
                required_field = str(df[column_name][i+1]).replace('Type of Entity','')
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2].replace('Physical Address','ENOAH')
                    if required_field == 'ENOAH' and keyword == 'Doing Business As':
                        required_field = df['Col_2'][i+1]
                break
    return required_field

def EML_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Cell Phone' and req_val == 'Email':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2]).replace('Social Security','ENOAH')
                    if req_val == 'Home Address' and 'Date of Birth' in str(required_field):
                        required_field = df['Col_2'][i+1]
                break
                
    if keyword == 'ip Code' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'ip Code' not in str(df['Col_3'][i]) and 'Home Address' in str(df['Col_1'][i]):
                if ',' in df['Col_1'][i+1]:
                    req_field = str(df["Col_1"][i+1]).split(',')
                else:
                    req_field = str(df["Col_1"][i+1]).split()
                n = len(req_field)
                required_field=req_field[n-1]          
                break
                
    if keyword == 'ip Code' and 'Home Phone' in str(required_field) and df['Col_1'][i+1]!='ENOAH':
        for i in range(0,len(df)):
            if 'ip Code' in str(df['Col_3'][i]) and 'Home Address' in str(df['Col_1'][i]):
                if ',' in df['Col_1'][i+1]:
                    req_field = str(df["Col_1"][i+1]).split(',')
                else:
                    req_field = str(df["Col_1"][i+1]).split()
                if len(req_field)<=2:
                    result = re.findall('\d+', req_field[1]) 
                    required_field = result[0]
                if len(req_field)>2:
                    result = re.findall('\d+', req_field[-1])
                    required_field = result[0]
                break
    if keyword == 'ip Code' and df['Col_1'][i+1]=='ENOAH':
        for i in range(0,len(df)):
            if 'ip Code' in str(df['Col_3'][i]) and 'Home Address' in str(df['Col_1'][i]):
                value = df['Col_2'][i+1].split()
                required_field = value[-1]
                break
    if keyword == 'ip Code' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Home Address' in df['Col_1'][i] and 'ENOAH' in df['Col_1'][i+1]:
                value = df['Col_2'][i+1].split()
                required_field = value[-1]
                break
    required_field = str(required_field).replace('Date of Birth','')
    return required_field

def BBF_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address:' and req_val == 'Address':
                required_field = df[column_name][i+1].replace('US','').replace('United States','')
                break
            elif keyword == 'Business Address:' and req_val == 'Zip':
                value = df[column_name][i+1].replace('US','').replace('United States','')
                required_field = value.split()[-1]
                if not re.search('[0-9]',str(required_field)) or (re.search('[0-9]',str(required_field)) and not len(str(required_field))>=5):
                    required_field = df['Col_2'][i+1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                break
    if keyword == 'Business Address:' and 'Blodgett Mills Rd' in str(required_field):
        required_field = '2315 Blodgett Mills Rd'
    if keyword == 'Business Address:' and '2002 Annapolis mall' in str(required_field):
        required_field = '2002 Annapolis mall'
    return required_field

def BBF_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Address:' and req_val == 'Home Address':
                    required_field = df[column_name][i+1].replace('US','').replace('United States','')
                    break
                elif keyword == 'Address:' and req_val == 'Zip':
                    value = df[column_name][i+1].replace('US','').replace('United States','').replace('Westland, MI ','')
                    required_field = value.split()[-1]
                    if not re.search('[0-9]',str(required_field)) or (re.search('[0-9]',str(required_field)) and not len(str(required_field))>=5):
                        required_field = df['Col_2'][i+1]
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
                    break  
            else:
                if keyword == 'Address:' and req_val == 'Home Address':
                    required_field = df[column_name][i+1].replace('US','').replace('United States','')
                elif keyword == 'Address:' and req_val == 'Zip':
                    value = df[column_name][i+1].replace('US','').replace('United States','')
                    required_field = value.split()[-1]
                    if not re.search('[0-9]',str(required_field)) or (re.search('[0-9]',str(required_field)) and not len(str(required_field))>=5):
                        required_field = df['Col_2'][i+1]
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i+1]
    if keyword == 'Address:' and 'Blodgett Mills Rd' in str(required_field):
        required_field = '1 Blodgett Mills Rd 2315'
    return required_field
    
def Reliant_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Start Date Under':
                required_field = df['Col_2'][i+1]
                break
            elif keyword == 'Business Legal Name:':
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and keyword == 'Business DBA Name:':
                    required_field = df['Col_5'][i+1]
                break
    if keyword == 'Business Start Date Under' and 'RENT' or 'OWN' in str(required_field):
        required_field = str(required_field).replace('RENT','').replace('OWN','')
    return required_field

def Reliant_Funding_Owner(df,owner_status,keyword,column_name,req_val):
    if keyword == 'Email':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if required_field == 'ENOAH':
                required_field = df[column_name][i+2]
            break
    return required_field
    
def Good_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace(':','')
            break
    return required_field

def Good_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace(':','')
            break
    return required_field

def Ascendancy_Corp_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Business Name':
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
                if 'Business Address' not in df[column_name][i+1] and not 'ENOAH' in df[column_name][i+1]:
                    required_field = df[column_name][i].replace(keyword,'').replace(':','') + ' '+df[column_name][i+1]
                if 'Business Address' not in df[column_name][i+2] and not 'ENOAH' in df[column_name][i+2] and not 'City:' in df[column_name][i+2]:
                    required_field = df[column_name][i].replace(keyword,'').replace(':','') + ' '+df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
            break
    return required_field

def Ascendancy_Corp_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if required_field == '':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
            break
    return required_field

def FundMerica_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break
    return required_field

def FundMerica_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Date Of Birth' or keyword == 'Co-App Date Of Birth':
                if 'Last Name' in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1])+ ' ' + str(df[column_name][i+2])
                if 'Last Name' not in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1])+ ' ' + str(df[column_name][i+2])+' '+str(df[column_name][i+3])
                break
            else:
                required_field = df[column_name][i+1]
            break
    if keyword == 'Date Of Birth':
        required_field = str(required_field).replace('Last Name* Vargas','')
    required_field = str(required_field).replace('*','').replace('Select','').replace('-','')
    return required_field

def Bridge_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if keyword == 'Company Name' and required_field == 'ENOAH':
                required_field = df['Col_2'][i+1]
            if keyword == 'D/B/A' and required_field == 'ENOAH':
                required_field = df['Col_4'][i+1]
            break
    if keyword == 'Business Inception Date' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Inception  Date' in str(df['Col_5'][i]):
                required_field = str(df['Col_4'][i+1])
                break
    if keyword == 'Company Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'ompany Name' in str(df['Col_2'][i]):
                required_field = str(df['Col_1'][i+1])
                break
    if keyword == 'D/B/A' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'D/B/A' in str(df['Col_2'][i]):
                required_field = str(df['Col_4'][i+1])
                break
    if keyword == 'Zip Code' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip Code' in str(df['Col_5'][i]):
                required_field = str(df['Col_5'][i+1])
                break
    return required_field

def Bridge_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if required_field == 'ENOAH':
                required_field = df[column_name][i+2]
            break
    if keyword == 'Ownership %' and required_field == 'SSN':
        for i in range(0,len(df)):
            if 'Ownership %'in str(df['Col_4'][i]):
                required_field = str(df['Col_5'][i+1])
                break
    required_field = str(required_field).replace('+','')
    return required_field

def Alternative_fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Federal Tax ID:' or keyword == 'current Ownership:':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+4]
                break
            elif keyword == 'Zip Code:':
                required_field = re.sub('[a-zA-Z&]',  '', df[column_name][i+1])
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Alternative_fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Email' or keyword == 'Ownership %':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Date of Birth:' and req_val == 'SS#':
                required_field = df['Col_3'][i+1].replace('SS','')
                break
            elif keyword == 'Zip Code:':
                required_field =  re.sub('[a-zA-Z&]', '', df[column_name][i+1])
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Regal_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if 'zip Code:' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('zip Code','Zip Code')
        if 'Busines Inception Date:' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('Busines Inception Date:','Business Inception Date:')
        if str(keyword) in str(df[column_name][i]):
            if keyword == '>COMPANY INFORMATION':
                required_field = str(df[column_name][i+1]).replace('Legal','').replace('Company','').replace('Name','').replace(':','')
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2]).replace('Legal','').replace('Company','').replace('Name','').replace(':','')
            elif keyword == 'Zip Code':
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '', df[column_name][i])).strip()
                if required_field == '':
                    required_field = re.sub('[a-zA-Z:%.,() /]', '', df[column_name][i-1]).replace('.','')
                break
            else:
                required_field = df[column_name][i].replace('(no PO Boxes','').replace(keyword,'').replace(')','').replace(':','').strip()
                if req_val == 'StartDate' and required_field == 'I':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2].replace(',',' ')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH' and req_val =='StartDate':
                        required_field = df[column_name][i-2]
                break
    return required_field

def Regal_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('regal1.csv')
    for i in range(0,len(df)):
        if 'ZIp Code' in str(df[column_name][i]):
           
            df[column_name][i] = str(df[column_name][i]).replace('ZIp Code','Zip Code')
        if 'Firs name:' in str(df[column_name][i]):
            
            df[column_name][i] = str(df[column_name][i]).replace('Firs name:','First name')
        if 'First na me:' in str(df[column_name][i]):
            
            df[column_name][i] = str(df[column_name][i]).replace('First na me:','First name')
        if 'First nam:' in str(df[column_name][i]):
           
            df[column_name][i] = str(df[column_name][i]).replace('First nam:','First name')
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('ss Number:','').replace('SS Number:','').replace('Ss Number:','').replace(keyword,'').replace(':','').strip()
            if required_field == '':
                required_field = str(df[column_name][i-1]).replace('State:','')
                if keyword == 'Home address (no PO Boxes):' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if keyword == 'Home address (no PO Boxes):' and 'Annual income' in required_field:
                    required_field = df['Col_2'][i]
            break
    required_field = str(required_field).replace('SS Number:','').replace('Ss Number:','')
    if req_val == 'First Name' and '.' in required_field:
        required_field = required_field.replace('.','')
    return required_field

def StraightLine_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address' and req_val == 'Zip':
                value = df[column_name][i+1].replace('US','').replace('United States','')
                required_field = value.split()[-1]
                if required_field == 'ENOAH':
                    value = df['Col_2'][i+1].replace('US','').replace('United States','')
                    required_field = value.split()[-1]
                break
            elif keyword == 'Business Address' and req_val == 'City':
                required_field = 'ENOAH'
                break
            elif keyword == 'Business Legal Name' or keyword == 'DBA Name':
                required_field = df[column_name][i+1]+' '+df[column_name][i+2]
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'Business Address' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH' and (keyword == 'Business Start Date' or keyword == 'Tax ID Number'):
                    required_field = df[column_name][i+2]
                break
    required_field = str(required_field).replace('ENOAH','').replace('Use of Funds','').replace('Industry Type','')
    return required_field

def StraightLine_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address' and req_val == 'Zip':
                value = df[column_name][i+1].replace('US','').replace('United States','')
                required_field = value.split()[-1]
                if required_field == 'ENOAH':
                    value = df[column_name][i+2].replace('US','').replace('United States','')
                    required_field = value.split()[-1]
                    if required_field == 'ENOAH':
                        value = df['Col_2'][i+1].replace('US','').replace('United States','')
                        required_field = value.split()[-1]
                break
            elif keyword == 'Home Address' and req_val == 'City':
                required_field = 'ENOAH'
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if keyword == 'Home Address' and required_field == 'ENOAH':
                        required_field = df['Col_2'][i+1]
                break
    return required_field

def MPF_INC_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal Name' or keyword == 'DBA Name':
                required_field = str(df[column_name][i-4])+' '+str(df[column_name][i-3])+' '+str(df[column_name][i-2])+' '+str(df[column_name][i-1])+''+str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-3]
                break
    if keyword == 'Zip':
        required_field = re.sub('[a-zA-Z]','',str(required_field))
    required_field = str(required_field).replace('ENOAH','').replace('via Credit Card','').replace('(every 12 months)','').replace('Street Address','').replace('Avg. Monthly Sales','').replace('Total Annual Revenue','')
    return required_field

def MPF_INC_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-3]
                break
            else:
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-3]
    if keyword == 'Zip':
        required_field = re.sub('[a-zA-Z]','',str(required_field))
    return required_field

# New local
def QuickFi_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('Quickfi.csv')
    for i in range(0,len(df)):
        if 'Federal TaxID' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('Federal TaxID','Federal Tax ID')
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if keyword == 'Business Legal': #30/05
                required_field = required_field.replace('Name:','')
            if keyword == 'Business As': #30/05
                required_field = required_field.replace('Doing','')
            if required_field == '':
                required_field = df[column_name][i-1]
                if keyword == 'Federal Tax ID:':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i].replace(keyword,'')
                        
                
                if keyword == 'Business As:' and 'Funding Application' in required_field:
                    required_field = df['Col_3'][i]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+1]
                if (keyword == 'Federal Tax ID:' or keyword == 'Business Start Date') and required_field != int :
                    required_field = df[column_name][i+1]
                if (keyword == 'Physical Address:' or keyword == 'Zip Code:') and (required_field == 'ENOAH' or '@' in str(required_field)):
                    required_field = df[column_name][i+1]
                if keyword == 'Business Legal':
                    required_field = required_field.replace('Name:','')
                if keyword == 'Business Legal' and required_field == 'ENOAH':
                    required_field = df[column_name][i+1]
                if keyword == 'Business Start Date' and required_field == 'ENOAH':
                    required_field = df[column_name][i-1]
            break
    return required_field

def QuickFi_Owner(df,owner_status,keyword,column_name,req_val):#30/07
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('tquick.csv')
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'Birth' or keyword == 'Birth:':
                    print('required_field1111',required_field)
                    required_field = df[column_name][i].replace(keyword,'')
                    required_field = re.sub(r'[a-zA-Z]', '', required_field)
                    print(required_field.strip())
                    required_field = str(required_field).replace(':','')
                    if required_field == ' ':
                        required_field = df['Col_2'][i]
                        required_field = str(required_field).replace('Social Security #:','')
                    if required_field == ' ':
                        required_field = df[column_name][i-1]
                        if ('.com') in df[column_name][i-1] or (',com') in df[column_name][i-1]:
                            required_field = df[column_name][i+1]
                        if ('.net') in df[column_name][i-1]:
                            required_field = df[column_name][i+1]
                    break
                    

                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    print('hello...',keyword,required_field)
                    if keyword == 'Social Security' and required_field == ' #:':
                        print('meeting...',required_field)
                        required_field = df['Col_3'][i]
                    if required_field == '':
                        required_field = df[column_name][i-1]
                        if keyword == 'Email:' and not '.com' in required_field:
                            required_field = df[column_name][i+1]
                        if keyword == 'Mobile:' and not re.findall('\d+', required_field):
                            required_field = df[column_name][i+1]
                        if (keyword == 'Zip Code:' or keyword == 'Name:') and required_field == 'ENOAH':
                            required_field = df[column_name][i+1]
                        
                        if keyword == 'Social Security' and 'Mobile:' in str(required_field):
                            required_field = df[column_name][i+1]
                            if keyword == 'Social Security' and 'Principle Information' in str(required_field):
                                required_field = df['Col_3'][i]
                    
                        
                        if keyword == 'Social Security' and required_field == 'ENOAH' and df[column_name][i+1]=='ENOAH':
                            
                            required_field = df['Col_3'][i]
                            if required_field == 'ENOAH':
                                required_field = df['Col_3'][i+1]
                        if keyword == 'Social Security' and required_field == 'ENOAH' and df['Col_3'][i] == 'ENOAH':
                            
                            required_field = df['Col_3'][i-1]
                        if keyword == '% of  Ownership:' and 'Principle Information' in str(required_field):
                            required_field = df[column_name][i+1]
                        if keyword.replace(' ','') == 'Address:' and 'Name:' in required_field:
                            required_field = str(df[column_name][i]).replace('Home','')
                        break
                break
        # else:
        #     if str(keyword) in str(df[column_name][i]):
        #         if keyword == 'Birth:' or keyword == 'Birth:':
        #             required_field = df[column_name][i].replace(keyword,'')
        #             required_field = str(required_field).replace('Dateof','')
        #             required_field = str(required_field).replace('Date of','')
        #             if required_field == ' ':
        #                 required_field = df['Col_2'][i]
        #                 required_field = str(required_field).replace('Social Security #:','')
        #             if required_field == ' ':
        #                 required_field = df[column_name][i-1]
        #                 if ('.com') in df[column_name][i-1] or (',com') in df[column_name][i-1]:
        #                     required_field = df[column_name][i+1]
        #                 if ('.net') in df[column_name][i-1]:
        #                     required_field = df[column_name][i+1]
        #             break
                    

        #         else:
        #             required_field = df[column_name][i].replace(keyword,'')
        #             if required_field == '':
        #                 required_field = df[column_name][i-1]
        #                 if keyword == 'Email:' and not '.com' in required_field:
        #                     required_field = df[column_name][i+1]
        #                 if keyword == 'Mobile:' and not re.findall('\d+', required_field):
        #                     required_field = df[column_name][i+1]
        #                 if (keyword == 'Zip Code:' or keyword == 'Name:') and required_field == 'ENOAH':
        #                     required_field = df[column_name][i+1]
                        
        #                 if keyword == 'Social Security #' and 'Mobile:' in str(required_field):
        #                     required_field = df[column_name][i+1]
        #                     if keyword == 'Social Security #:' and 'Principle Information' in str(required_field):
        #                         required_field = df['Col_3'][i]
                    
        #                 if keyword == 'Social Security #:' and required_field == 'ENOAH' and df[column_name][i+1]=='ENOAH':
                            
        #                     required_field = df['Col_3'][i]
        #                     if required_field == 'ENOAH':
        #                         required_field = df['Col_3'][i+1]
        #                 if keyword == 'Social Security #:' and required_field == 'ENOAH' and df['Col_3'][i] == 'ENOAH':
                            
        #                     required_field = df['Col_3'][i-1]
        #                 if keyword == '% of  Ownership:' and 'Principle Information' in str(required_field):
        #                     required_field = df[column_name][i+1]
        #                 if keyword.replace(' ','') == 'Address:' and 'Name:' in required_field:
        #                     required_field = str(df[column_name][i]).replace('Home','')
        #                 break
        #         break
    return required_field

def Cap_Lex_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            elif req_val == 'Zip':
                req = str(df['Col_2'][i]).split()[-1]
                if re.search('[0-9]',str(req)):
                    req = re.sub('[a-zA-Z:%,/]', '',req)
                    required_field = req
                else:
                    required_field = 'ENOAH'
            else:
                required_field = str(df['Col_2'][i])
            break
    return required_field

def Cap_Lex_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'SS#' or req_val == 'Owner':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                elif req_val == 'Home Address':
                    required_field = str(df['Col_2'][i+1])
                elif req_val == 'Zip':
                    req = str(df['Col_2'][i+1]).split()[-1]
                    if re.search('[0-9]',str(req)):
                        req = re.sub('[a-zA-Z:%,/]', '',req)
                        required_field = req
                    else:
                        required_field = 'ENOAH'
                else:
                    required_field = str(df['Col_2'][i])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'SS#' or req_val == 'Owner':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                elif req_val == 'Home Address':
                    required_field = str(df['Col_2'][i+1])
                elif req_val == 'Zip':
                    req = str(df['Col_2'][i+1]).split()[-1]
                    if re.search('[0-9]',str(req)):
                        req = re.sub('[a-zA-Z:%,/]', '',req)
                        required_field = req
                    else:
                        required_field = 'ENOAH'
                else:
                    required_field = str(df['Col_2'][i])
    return required_field

def JR_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal':
                required_field = df[column_name][i+1].replace('Business','')
                if required_field == '':
                    required_field = df['Col_2'][i+1]
                break
            elif keyword == 'City:' or keyword == 'State:':
                required_field = 'ENOAH'
                break
            elif keyword == 'Business As:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field =='':
                    required_field = df['Col_4'][i]
                break
            elif keyword == 'ax ID (EIN):':
                required_field = df['Col_3'][i]
                break
            elif keyword == 'Start Date:':
                required_field = df[column_name][i-1].replace('Business','')
                break
            elif keyword == 'usiness Street Address' or keyword == 'ip':
                req = str(df[column_name][i-1]).replace('B   ','')
                required_field = req[1:]
                if keyword == 'usiness Street Address' and required_field =='' and 'nit/Suite' in str(df['Col_2'][i]):
                    required_field = str(df['Col_2'][i-1][1:])           
                break
    if keyword == 'Start Date:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Start Date:' in str(df['Col_4'][i]):
                required_field = str(df['Col_5'][i-1])
                break
    return required_field

def JR_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Personal Email:':
        required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Full Name:':
                if req_val == 'First Name' and df['Col_2'][i-1]!= 'ENOAH':
                    required_field = str(df[column_name][i-1]).replace('usiness Owner','')
                    if required_field == 'B':
                        required_field = str(df['Col_2'][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                    if required_field[0]=='B' and re.search('[a-zA-Z]',str(required_field[1:])):
                        required_field = required_field[1:]
                    break
                if req_val == 'First Name' and df['Col_2'][i-1]== 'ENOAH':
                    req = str(df[column_name][i-1]).replace('usiness Owner','')
                    if req[0] == 'B':
                        required_field = req[1:]
                    break
                elif req_val == 'Last Name' and df['Col_2'][i-1]!= 'ENOAH':
                    required_field = str(df['Col_2'][i-1])
                    break
                if req_val == 'Last Name' and df['Col_2'][i-1]== 'ENOAH':
                    req = str(df[column_name][i-1]).replace('usiness Owner','')
                    if req[0] == 'B':
                        required_field = req[1:]
                    break
                break
            elif keyword == 'tate':
                required_field = str(df['Col_5'][i-1]).replace('Z','')
                if required_field == '' or required_field =='ENOAH':
                    required_field = df['Col_5'][i-2]
                break
            elif keyword == "irthdate:" or keyword == 'SN:' or keyword == 'ip' or keyword == 'wnership %:' or keyword == 'hone:':
                required_field = re.sub('[a-zA-Z]', '',str(df[column_name][i-1]))
                break
            elif keyword == 'mail:':
                required_field = df['Col_4'][i-1]
                if required_field == 'ENOAH':
                    required_field = df['Col_4'][i-2]
                break
            elif keyword == 'Street Address':
                req = str(df[column_name][i-1])
                required_field = req[1:]
                break
    if keyword == 'Full Name:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'rinciple' in str(df['Col_1'][i]):
                if 'rinciple' == str(df['Col_1'][i]):
                    value = str(df['Col_1'][i-1])
                    if 'P'in value[:1]:
                        required_field = value[1:]
                        if required_field =='':
                            required_field = str(df['Col_2'][i-1])
                    else:
                        required_field = str(df['Col_1'][i-1])
                break
    if keyword == 'SN:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'SN:' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i-1]).replace('S','')
                break
    if keyword == 'irthdate:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'irthdate:' in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i-1])
                break
    if keyword == 'wnership %:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if '%:' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i-1])
                break
    if keyword == 'Street Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Street Address' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i-1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i-2])
                break
    return required_field

def JR_Capital1_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Zip':
                value = str(df[column_name][i+1])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field == '':
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',str(df['Col_2'][i+1])))
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    return required_field

def JR_Capital1_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == "Owner #1":
                if keyword == 'zip':
                    value = str(df[column_name][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                    break
                else:
                    required_field = str(df[column_name][i+1])
                    if (keyword == 'Ownership %:' or keyword == 'Date of Birth:') and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2])
                    break
            else:
                if keyword == 'zip':
                    value = str(df[column_name][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                else:
                    required_field = str(df[column_name][i+1])        
    if keyword == 'zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'zip' in str(df['Col_1'][i]):
                value = str(df['Col_1'][i+1]).split()
                required_field = value[-1]
                break
    return required_field

def Uplyft_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    print('train11',keyword,required_field)
    for i in range(0,len(df)):
        if (df['Col_1'][i] == 'Business Name' or df['Col_1'][i] == 'Address') and df['Col_1'][i+1] == 'ENOAH':
            df['Col_1'][i+1] = df['Col_2'][i+1]
    for i in range(0,len(df)):
        if keyword == 'Business Name' and required_field == 'ENOAH':
            print('platform111',required_field,df['Col_2'][i+1])
            required_field = df['Col_2'][i+1]
        if str(keyword) in str(df[column_name][i]):
            required_field = (str(df[column_name][i+1])).replace('\t',' ')
            if keyword != 'Business Name' and required_field == 'ENOAH':
                required_field = (str(df[column_name][i+2])).replace('\t',' ').replace('Business Cellphone','')
                # if keyword == 'Business	Name' and required_field == 'ENOAH':
                #     # print('oorruuu...',required_field,df['Col_2'][i+2],df['Col_1'][i+3])
                #     required_field = df['Col_2'][i+1]
                #     if required_field == 'ENOAH':
                #         # print('oorruuu111...',required_field,df['Col_2'][i+2],df['Col_1'][i+3])
                #         required_field = df['Col_1'][i+3]
            break
    print('train',keyword,required_field)
    # if keyword == 'Business Name' and required_field == 'ENOAH':
    #     # print('platform',required_field,df['Col_2'][i+1])
    #     required_field = df['Col_2'][i+1]
    if keyword == 'DBA (doing business as)' and required_field == '':
        print('platform',required_field,df['Col_5'][i+1])
        required_field = df['Col_5'][i+1]
    if keyword == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_2'][i]):
                required_field = str(df["Col_3"][i+1])
                break
    if keyword == 'City' and required_field == 'City':
        for i in range(0,len(df)):
            if 'CityState' in str(df['Col_1'][i]):
                required_field = 'ENOAH'
                break
    if keyword == 'City' and 'State' not in df['Col_2'][i]:
        required_field = 'ENOAH'
    required_field = str(required_field).replace('PAINTING AND REMODELING','CALDERON PAINTING AND REMODELING').replace('Kitchen And Bath Creations Design','Kitchen & Bath Creations Design Center, LLC')
    return required_field

def Uplyft_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = (str(df[column_name][i+1])).replace('\t',' ')
            if required_field == 'ENOAH':
                required_field = (str(df[column_name][i+2])).replace('\t',' ')
                if keyword == 'Home	Address' and 'City' in required_field:
                    required_field = df['Col_2'][i+1]
            break
    if keyword == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_1'][i]):
                req_field = str(df['Col_1'][i+1])
                required_field = re.sub('[a-zA-Z:%,/]', '',req_field)
                break
    if keyword == 'Last	Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Last	Name' in str(df['Col_2'][i]):
                required_field = str(df['Col_3'][i+1]).replace('\t',' ')
                break
    if keyword == 'Social	Security	#' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Social	Security	#' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i+1])
                break
    if keyword == 'Ownership	%' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Ownership	%' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i+1])
                break
    if keyword == 'Home	Address' and required_field == 'City':
        required_field = df[column_name][i].replace(keyword,'')
    required_field = str(required_field).replace('Cell Phone','')
    return required_field
    
def Parkview_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address' and req_val == 'City':
                required_field = df['Col_3'][i]
                break
            elif keyword == 'Business Address' and req_val == 'State':
                required_field = df['Col_4'][i]
                break
            elif keyword == 'Owner Information' and req_val == 'Zip':
                value = str(df['Col_2'][i-1])
                required_field = re.sub('[a-zA-Z:%,/]', '',value)
                if required_field == '':
                    required_field = df['Col_2'][i-2]
                break
            else:
                required_field = str(df['Col_2'][i]).replace('!','')
                if required_field =='ENOAH':
                    required_field = str(df['Col_3'][i])
                    if req_val =='Name' and required_field =='ENOAH':
                        required_field = str(df['Col_2'][i-1])
                break
    return required_field

def Parkview_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == "Home Address" and req_val == 'City':
                    required_field = df['Col_3'][i]
                    break
                elif keyword == "Home Address" and req_val == 'State':
                    required_field = df['Col_4'][i]
                    break
                elif keyword == "SSN" and req_val == 'Zip':
                    required_field = df['Col_2'][i-1]
                    break
                elif keyword == "Name" and req_val == 'Last Name':
                    required_field = df['Col_3'][i]
                    break
                else:
                    required_field = df['Col_2'][i]
                    break
            else:
                if req_val == 'City' or req_val == 'State':
                    required_field = 'ENOAH'
                elif keyword == "SSN" and req_val == 'Zip':
                    required_field = df['Col_2'][i-1]
                elif keyword == "Name" and req_val == 'Last Name':
                    required_field = df['Col_3'][i]
                else:
                    required_field = df['Col_2'][i]
    return required_field

def Ma_Here_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business City, State, Zip Code':
                value = df[column_name][i].replace(keyword,'')
                req_field = re.sub('[a-zA-Z:%,/]',  '',value)
                required_field = req_field
                if required_field == '':
                    value = df[column_name][i-1].split()
                    required_field = value[-1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                break
    return required_field

def Ma_Here_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Owner 1 City, State, Zip Code':
                    value = df[column_name][i].replace(keyword,'')
                    req_field = re.sub('[a-zA-Z:%,/]',  '',value)
                    required_field = req_field
                    if required_field == '':
                        value = df[column_name][i-1].split()
                        required_field = value[-1]
                    break
                elif req_val == 'Date Of Birth':
                    required_field = str(df[column_name][i+1]).replace('Owner 1 Date of Birth','')
                    break
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
                    break
            else:
                if keyword == 'Owner 1 City, State, Zip Code':
                    value = df[column_name][i].replace(keyword,'')
                    req_field = re.sub('[a-zA-Z:%,/]',  '',value)
                    required_field = req_field
                    if required_field == '':
                        value = df[column_name][i-1].split()
                        required_field = value[-1]
                elif req_val == 'Date Of Birth':
                    required_field = str(df[column_name][i+1]).replace('Owner 1 Date of Birth','')
                else:
                    required_field = df[column_name][i].replace(keyword,'')
                    if required_field == '':
                        required_field = df[column_name][i-1]
    return required_field

def Dynamic_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword =='Business DBA Name:':
                required_field = df['Col_4'][i+1]
                break
            else:
                required_field = str(df[column_name][i+1]).replace('Salons / Beauty','ENOAH')
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                break
    required_field = str(required_field).replace('|','')
    return required_field

def Dynamic_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Home Address':
                required_field = df[column_name][i-1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
            elif req_val == 'Zip':
                req_field = str(df['Col_5'][i-1])
                required_field = re.sub('[a-zA-Z:%,/]', '',req_field)
                if required_field == '':
                    req_field = str(df['Col_5'][i-2])
                    required_field = re.findall('\d+', req_field)[0]
                break
            elif req_val == 'Owner':
                required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH':
                    required_field = df[column_name][i-2]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
#     required_field = str(required_field).replace('Credit Score:','')
    return required_field

def YM_Ven_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DBA':
                required_field = df[column_name][i].replace(': (if none put NA)','').replace(keyword,'')
                if 'nvelope' in required_field:
                    required_field = 'ENOAH'
                if 'Trade Name' in required_field or required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' or required_field =='1':
                    required_field = df[column_name][i-1]
                    if keyword == 'Physical Address (no PO Boxes)' and required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i]
                    if keyword == 'Legal Company Name:' and required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                        if 'Is your business' in required_field:
                            required_field = df[column_name][i+1]
                    if keyword == 'Zip Code:' and required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    if keyword == 'Legal Company Name:' and required_field == 'ENOAH':
                        required_field = df['Col_2'][i]
                        if required_field == 'ENOAH':
                            required_field = df['Col_2'][i-2]
                break
    return required_field

def YM_Ven_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if required_field == '' and keyword == 'SS Number:' and 'Cell' not in str(df[column_name][i-1]):
                required_field = df[column_name][i-1]
            if required_field == '' and keyword == 'SS Number:' and 'Cell' in str(df[column_name][i-1]):
                required_field = df[column_name][i+1]
            if required_field == '':
                required_field = df[column_name][i-1]
                if req_val == 'Date Of Birth' and '/' not in required_field:
                    required_field = df[column_name][i+1]
                if keyword == 'Home address (no PO Boxes):' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if keyword == 'Home address (no PO Boxes):' and 'Annual' in required_field:
                    required_field = df['Col_2'][i+1]
                if keyword == 'First name:' and 'Owner' in required_field:
                    required_field = df[column_name][i+1]
                if keyword == 'Last Name:' and required_field == 'ENOAH':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
            break
    return required_field  

def AR_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('Federal Tax ID','').replace('Federal TaxID','').replace('Federal TaxID','').replace('State of Incorporation','').replace('Zip:','').replace(':','').replace(keyword,'')
            if required_field == '':
                required_field = str(df[column_name][i-1]).replace('Merchant Pre-Qualification Form','').replace('Business Fax:','').replace('93336','99336')
                if req_val == 'StartDate' and required_field == 'ENOAH':
                    required_field = str(df[column_name][i-2])
                if req_val == 'DBA' and required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i-1])
                    if required_field == 'ENOAH' and not 'Federal' in str(df[column_name][i+1]):
                        required_field = str(df[column_name][i+1])
                if req_val == 'TaxID' and required_field == 'ENOAH':
                    required_field = str(df[column_name][i-2])
                if req_val == 'Name' and required_field == '':
                    required_field = str(df[column_name][i+1])
            break
    if '(cid:' in str(required_field):
        required_field = 'ENOAH'
    if req_val == 'StartDate' and 'State' in required_field:
        required_field = str(required_field).split('State')[0]
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Zip' in str(df['Col_2'][i]):
                required_field = df['Col_3'][i]
                break
    return required_field

def AR_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i].replace('Full Legal Name Owner 1:','').replace('Social Security#:','').replace('Social Security #:','').replace('Jzip:','').replace('zip:','').replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if keyword == 'Home Address:' and required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    if keyword == 'Full Legal Name Owner 1' and 'Owner/Principal Information' in required_field:
                        required_field = str(df[column_name][i+1])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if keyword == 'Home Address:' and required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
    required_field= str(required_field).replace('Full Legal Name Owner 1:','')
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Zip' in str(df['Col_2'][i]):
                required_field = df['Col_3'][i]
                break
    return required_field

def AR_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            break
    required_field = str(required_field).replace('Entity Type*','')
    return required_field
            
def AR_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Date Of Birth':
                req = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])
                required_field = str(req).replace('Co-App Owner Information','').replace('Business Information','').replace('--Select--','').replace('Business Name*','').replace('Co-App First Name','')
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    return required_field
    
def Hartford_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Number:':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if (keyword == 'Date Business Started:' or keyword == 'Physical Address:') and required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+3]
                break
    return required_field

def Hartford_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if required_field == '':
                required_field = df[column_name][i+1]
                if keyword =='Zip Code:' and not re.search('[0-9]',str(required_field)):
                    required_field = df[column_name][i+3]
            break
    return required_field

def Sutton_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            if req_val == 'Zip':
                if 'Type of Business'not in str(df[column_name][i+2]):
                    req_field =  str(df[column_name][i+2]).split()
                    required_field = req_field[-1]
                else:
                    req_field =  str(df[column_name][i+1]).split()
                    required_field = req_field[-1]
                break
            else:
                required_field = df[column_name][i+1]
                if (req_val =='DBA' or req_val == 'TaxID' or req_val == 'Address') and required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                break
    return required_field

def Sutton_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            if req_val == 'Zip':
                if 'Phone' not in str(df[column_name][i+2]):
                    req_field =  str(df[column_name][i+2]).split()
                    required_field = req_field[-1]
                else:
                    req_field =  str(df[column_name][i+1]).split()
                    required_field = req_field[-1]
                break
            else:
                required_field = df[column_name][i+1]
                if req_val == 'Home Address' and required_field =='ENOAH':
                    required_field = df[column_name][i+2]
                break
    return required_field

def Pay_Less_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Years in business':
                req_field = str(df[column_name][i+1])
                given_years = re.findall('\d+', req_field)[0]                
#                 given_years=re.sub('[a-zA-Z:%,/]',  '',df[column_name][i+1])
                currentTimeDate = int(datetime.now().strftime('%Y'))
                required_field = (int(currentTimeDate)) - (int(given_years))
                break
            elif req_val == 'City':
                req_field = df[column_name][i+2].split(',')
                required_field = req_field[0]
                break
            elif req_val == 'State':
                req_field = df[column_name][i+2].split(',')
                required_field = req_field[1]
                break
            elif req_val == 'Zip':
                req_field = df[column_name][i+2].split(',')
                required_field = req_field[-1]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Pay_Less_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'City':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[0]
                    break
                elif req_val == 'State':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[1]
                    break
                elif req_val == 'Zip':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[-1]
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
                if req_val == 'City':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[0]
                elif req_val == 'State':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[1]
                elif req_val == 'Zip':
                    req_field = df[column_name][i+2].split(',')
                    required_field = req_field[-1]
                else:
                    required_field = df[column_name][i+1]
    return required_field

def Monetary_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            elif req_val == 'Zip':
                value = str(df['Col_2'][i+1])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',value))
                if required_field == '' or value == 'ENOAH':
                    value = str(df['Col_2'][i])
                    if value[-4:].isdigit():
                        req = value[-6:]
                        required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
            else:
                required_field = df['Col_2'][i]
                if (req_val == 'TaxID' or req_val == 'Address') and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if req_val == 'DBA' and required_field =='ENOAH':
                    required_field = str(df['Col_1'][i]).replace('DBA Name,If different from legal name','')
                break
    return required_field

def Monetary_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('monetary.csv')
    for i in range(0,len(df)):
        if req_val == 'Date Of Birth' and 'D0B' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('D0B','DOB')
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            elif req_val == 'Zip':
                value = str(df['Col_2'][i+2])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',value))
                if required_field == '' or value == 'ENOAH' or 'Document' in value:
                    value = str(df['Col_2'][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',value))
                break
            elif req_val =='First Name':
                required_field = df['Col_2'][i+1]
                break         
            elif req_val =='Last Name':
                required_field = df[column_name][i+2]
                if 'Name' in required_field:
                    required_field = df[column_name][i+3]
                if 'Social Security' in required_field:
                    required_field = df['Col_2'][i+1]
                break
            else:
                required_field = str(df['Col_2'][i])
                if req_val == 'Home Address' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i+1]
                    if required_field !='' and  required_field[-4:].isdigit():
                        required_field = df['Col_2'][i-1]
                if req_val == 'Date Of Birth' and "DOB" in str(df['Col_1'][i-2]):
                    required_field = df['Col_1'][i-2].replace('DOB','')      
                if req_val == 'Date Of Birth' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                    if ',' not in str(required_field):
                        required_field = df['Col_2'][i-3]
                if req_val == 'SS#' and required_field == 'ENOAH':
                    required_field = df['Col_2'][i-1]
                if req_val == 'SS#' and required_field == 'Fundbox':
                    required_field = df['Col_1'][i+1]
                    
                break
    required_field = str(required_field).replace('Monday','').replace('Tuesday','').replace('Wednesday,','').replace('Thursday','').replace('Friday','').replace('Saturday','').replace('Sunday','').replace(',',' ')
    return required_field

def Triple_Crown_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value =df[column_name][i+2].split()
                required_field = value[-1]
                if 'United States'in str(df[column_name][i+4]):
                    value =df[column_name][i+3].split()
                    required_field = value[-1]
                break
            elif req_val == 'State':
                value =df[column_name][i+2].split()
                required_field = value[-2]
                if 'United States'in str(df[column_name][i+4]):
                    value =df[column_name][i+3].split()
                    required_field = value[-2]
                break
            elif req_val == 'City':
                value =df[column_name][i+2].split(',')
                required_field = value[0]
                if 'United States'in str(df[column_name][i+4]):
                    value =df[column_name][i+3].split(',')
                    required_field = value[0]
                break
            else:
                required_field = df[column_name][i+1]
                break
    return required_field

def Triple_Crown_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                required_field ='ENOAH'
                break 
            if req_val == 'Zip':
                value =df[column_name][i+2].split()
                required_field = value[-1]
                if 'Triple' in str(value) and 'United States'in str(df[column_name][i+6]):
                    value =df[column_name][i+5].split()
                    required_field = value[-1]
                if 'Triple' in str(df[column_name][i+1]) and 'United States'in str(df[column_name][i+6]):
                    value =df[column_name][i+5].split()
                    required_field = value[-1]
                if 'Triple' in str(df[column_name][i+1]) and 'United States'in str(df[column_name][i+7]):
                    value =df[column_name][i+6].split()
                    required_field = value[-1]
                break
            else:
                required_field = df[column_name][i+1]
                if keyword == 'SSN' and 'Triple Crown' in str(required_field):
                    required_field = df[column_name][i+4]
                if keyword == 'HOME ADDRESS' and 'Triple Crown' in str(required_field):
                    required_field = df[column_name][i+4]                
                break
    return required_field

def Triple_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if req_val == 'Name' and required_field == '':
                required_field = str(df['Col_2'][i])
            if keyword == 'DBA:' and required_field == '':
                required_field = str(df[column_name][i-1])
            if keyword == 'EIN:' and required_field == '':
                required_field = str(df[column_name][i-1])
            if req_val =='StartDate' and required_field == '':
                required_field = str(df['Col_3'][i])
            break
    return required_field

def Triple_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if keyword == 'Date of Birth:' and required_field == '':
                required_field = df['Col_3'][i-1]
            if req_val == 'Home Address' and required_field == '':
                required_field = df['Col_2'][i]
            break
    return required_field

def Dash_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if req_val == 'Zip' and required_field=='ENOAH':
                value = df['Col_3'][i+1].split()
                required_field = value[-1]
            if req_val == 'Zip' and required_field=='ENOAH':
                value = df['Col_1'][i+1].split()
                required_field = value[-1]
            if req_val == 'DBA' and required_field=='ENOAH':
                required_field = df['Col_3'][i+1]
            break
    return required_field

def Dash_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'Owner':
                    required_field = df['Col_2'][i]
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i]).replace('%','')
                        if required_field == '':
                            required_field = str(df['Col_3'][i-1])
                    break
                else:
                    required_field = df[column_name][i+1]
                    if req_val == 'Zip' and required_field=='ENOAH':
                        value = df['Col_1'][i+1].split()
                        required_field = value[-1]
                        if required_field == 'ENOAH' and 'By sign' in str(df['Col_2'][i+1]):
                            value = str(df['Col_1'][i-1]).split()
                            required_field = value[-1]
                        if required_field == 'ENOAH':
                            value = df['Col_2'][i-1].split()
                            required_field = value[-1]
                        if required_field == 'ENOAH':
                            value = df['Col_2'][i+1].split()
                            required_field = value[-1]
                    if req_val == 'Home Address' and required_field =='ENOAH':
                        required_field = df['Col_2'][i+1]
                    if keyword == 'Name' and required_field =='ENOAH':
                        required_field = df['Col_2'][i+1]  
                    break
            else:
                if req_val == 'Owner':
                    required_field = df['Col_6'][i]
                    break
                else:
                    required_field = df[column_name][i+1]
                    if req_val == 'Zip' and required_field=='ENOAH':
                        value = df['Col_4'][i+1].split()
                        required_field = value[-1]
                    if req_val == 'Zip' and required_field=='ENOAH':
                        value = df['Col_5'][i-1].split()
                        required_field = value[-1]
                    if req_val == 'Home Address' and required_field =='ENOAH':
                        required_field = df['Col_5'][i+1]
                    break
    return required_field

def Onpoints_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = df[column_name][i+1]+' '+df[column_name][i+2]
                if df[column_name][i+1]=='ENOAH':
                    required_field = df[column_name][i+2]+' '+df[column_name][i+3]
                break
            elif req_val == 'DBA':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH' and df['Col_3'][i+1]!= 'ENOAH':
                        required_field=df['Col_3'][i+1]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]

                if required_field == 'ENOAH':
                    required_field = df[column_name][i+3]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+4]
                break
    required_field = str(required_field).replace('ENOAH','')
    return required_field

def Onpoints_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+4]
                break
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+4]
    return required_field

def L3_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i+1])
                required_field = re.sub('[a-zA-Z:%,./]', '',value)
                if value == 'ENOAH' or required_field == '':
                    value = str(df[column_name][i+2]).replace(' FL17','')
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if required_field == ' ' or value == 'ENOAH':
                        value = str(df[column_name][i+3])
                        required_field = re.sub('[a-zA-Z:%,./]', '',value)
                break
            else:
                required_field = df[column_name][i+1]
                if required_field =='ENOAH':
                    required_field = df[column_name][i+2]
                    if keyword == 'DBA:' and required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        if required_field == 'ENOAH':
                            required_field = df[column_name][i+4]
                    if keyword == 'Legal/Corporate' and required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                break
    required_field = str(required_field).replace('Email Address:','').replace('EmailAddress:','').replace('Website:','')
    return required_field

def L3_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1': 
                if req_val == 'Zip':
                    value = str(df[column_name][i+1])
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if value == 'ENOAH':
                        value = str(df[column_name][i+2])
                        required_field = re.sub('[a-zA-Z:%,./]', '',value)
                        if required_field == ' ' or required_field == '':
                            value = str(df[column_name][i+3])
                            required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    break
                else:
                    required_field = df[column_name][i+1]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i+2]
                        if required_field =='ENOAH':
                            required_field = df[column_name][i+3]
                    break
            else:
                if req_val == 'Zip':
                    value = df[column_name][i+1]
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if value == 'ENOAH':
                        value = df[column_name][i+2]
                        required_field = re.sub('[a-zA-Z:%,./]', '',value)
                else:
                    required_field = df[column_name][i+1]
                    if required_field =='ENOAH':
                        required_field = df[column_name][i+2]
    required_field = str(required_field).replace('BUSINESS BANK STATEMENTS - REQUIRE 4 MONTHS:','').replace('O(cid:445)ce #:','')
    return required_field

def Fundo_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            if req_val=='DBA' and required_field =='ENOAH':
                required_field = df['Col_3'][i+1]
            if '#:' in str(required_field):
                required_field='ENOAH'
            break
    if keyword == 'Zip Code:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'City: State:' in str(df['Col_2'][i]):
                value = str(df['Col_2'][i+1])
                required_field = re.sub('[a-zA-Z:%,./]', '',value)
                break
    if keyword == 'Ownership:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Incorporation:' in str(df['Col_2'][i]):
                value = str(df['Col_2'][i+2])
                required_field = re.sub('[a-zA-Z%,]', '',value)
                break 
    if keyword == 'Federal Tax ID:' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID:' in str(df['Col_3'][i]):
                value = str(df['Col_3'][i+1])
                required_field = re.sub('[a-zA-Z:%,./]', '',value)
                break
    required_field = str(required_field).replace('Zip Code:','')  
    return required_field

def Fundo_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i+1]
            break
    return required_field

def Alamo_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DBA':
                required_field = df[column_name][i-1].replace('DBA:','').replace('DBA','')
                break
            elif req_val == 'Zip':
                required_field = df['Col_1'][i]
                break
            # elif req_val == 'City':
            #     required_field = df['Col_1'][i-1]
            #     break
            else:    
                required_field = str(df[column_name][i+1]).replace('\u0000','')
                if required_field == 'ENOAH' and req_val == 'StartDate':
                    required_field = df[column_name][i+2]
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Postal / Zip Code' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i-1]
    return required_field

def Alamo_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'Last Name':
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_1'][i+1]
                    break
                elif req_val == 'Zip':
                    required_field = df['Col_1'][i]
                    break
                # elif req_val == 'City':
                #     required_field = df['Col_1'][i-1]
                    # break
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and req_val == 'Date Of Birth':
                        required_field = df[column_name][i+2]
                    break
            else:
                if req_val == 'Last Name':
                    required_field = df['Col_2'][i+2]
                elif req_val == 'First Name':
                    required_field = df['Col_1'][i+2]
                elif req_val == 'Zip':
                    required_field = df['Col_1'][i]
                # elif req_val == 'City':
                #     required_field = df['Col_1'][i-1]
                else:
                    required_field = df[column_name][i+1]
    required_field = str(required_field).replace('Home Address','').replace('\u0000','').replace('*','')
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Postal / Zip Code' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i-1]
    return required_field

def Reliable_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i+1])
                required_field = re.sub('[a-zA-Z:%,./]', '',value)
                if value == 'ENOAH':
                    value = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if value == 'City:State:  ZIP:':
                        value = str(df['Col_1'][i+1]).split()
                        required_field = value[-1]
                break
            else:
                required_field = df[column_name][i+1]
                if req_val == 'TaxID' and required_field=='ENOAH':
                    required_field = df[column_name][i+2]
                if req_val == 'Name' and required_field=='ENOAH':
                    required_field = df['Col_2'][i+1]
                break
    return required_field

def Reliable_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'Home Address':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-2]
                    break
                elif req_val == 'Zip':
                    value = str(df[column_name][i+1])
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if value == 'ENOAH' or required_field == '':
                        value = str(df['Col_3'][i+1])
                        required_field = re.sub('[a-zA-Z:%,./]', '',value)
                        if value == 'ENOAH' or required_field == '':
                            value = str(df[column_name][i+2])
                            required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    break
                else:
                    required_field = df[column_name][i+1]
                    break
            else:
                if keyword == 'Title:':
                    required_field = df['Col_1'][i+1]
                elif req_val == 'Zip':
                    value = str(df[column_name][i+1])
                    required_field = re.sub('[a-zA-Z:%,./]', '',value)
                    if value == 'ENOAH' or required_field == '':
                        value = str(df['Col_3'][i+1])
                        required_field = re.sub('[a-zA-Z:%,./]', '',value)
                        if value == 'ENOAH' or required_field == '':
                            value = str(df[column_name][i+2])
                            required_field = re.sub('[a-zA-Z:%,./]', '',value)
                else:
                    required_field = df[column_name][i+1]
    return required_field

def Critical_Fin_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = str(df[column_name][i]).replace(':','').replace('*','').replace(keyword,'')
                if required_field == 'ENOAH' or required_field == '':
                    required_field = str(df[column_name][i+1])
                break
            elif req_val == 'DBA':
                required_field = str(df['Col_3'][i])
                break
            elif req_val == 'Address':
                if df['Col_2'][i]=='ENOAH':
                    required_field = str(df[column_name][i]).replace(':','').replace('*','').replace(keyword,'')
                elif df['Col_2'][i] != 'ENOAH':
                    value = str(df[column_name][i])+' '+str(df['Col_2'][i])
                    required_field = str(value).replace(':','').replace('*','').replace(keyword,'')
                break
            else:
                value = str(df[column_name][i+1])
                required_field = re.sub('[*:%"„]', '',value).replace('Enter 5 Numbers ','')
                if value == 'ENOAH':
                    value = str(df[column_name][i+2])
                    required_field = re.sub('[*:%"„]', '',value).replace('Enter 5 Numbers ','')
                break
    return required_field

def Critical_Fin_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            value = str(df[column_name][i+1])
            required_field = re.sub('[*:%"„_]', '',value).replace('NO Dashes','')
            if value == 'ENOAH':
                value = str(df[column_name][i+2])
                required_field = re.sub('[*:%"„_]', '',value).replace('NO Dashes ','')
            break
    return required_field

def Alburton_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Legal/Corporate Name:':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '' and 'Mailing' in df[column_name][i+2]:
                    required_field = str(df[column_name][i-1])
                if required_field == '' and 'Mailing' not in df[column_name][i+2]:
                    required_field = str(df[column_name][i-1])+ ' '+ str(df[column_name][i+1])
                break
            if keyword == 'Established Date:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df['Col_4'][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i-1]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if required_field == 'ENOAH' and req_val == 'DBA':
                        required_field = df['Col_3'][i-1]
            break
    return required_field

def Alburton_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1': 
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == 'ENOAH' or required_field == '':
                    required_field = str(df[column_name][i-1])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == 'ENOAH' or required_field == '':
                    required_field = str(df[column_name][i-1])
    return required_field

def KROWN_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    if keyword == 'Business DBA Name:':
        required_field = ' ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address':
                if df['Col_2'][i+1]!= 'ENOAH':
                    required_field = str(df[column_name][i+1])+' '+str(df['Col_2'][i+1]).replace('ENOAH','')
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                        if req_val == 'Zip' and required_field == 'ENOAH':
                            required_field = df['Col_4'][i+1]
                break
    if keyword == 'Federal Tax ID (EIN)' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID (EIN)' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i+1])
                break
    return required_field

def KROWN_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home Address':
                required_field = str(df[column_name][i+1])+' '+str(df['Col_2'][i+1])+' '+str(df['Col_3'][i+1]).replace('ENOAH','')
                if required_field == '' or required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                break
            elif keyword == 'Full Name' and req_val == 'First Name':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            elif keyword == 'Full Name' and req_val == 'Last Name':
                required_field = df['Col_2'][i]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if req_val == 'Zip' and required_field == 'ENOAH':
                        required_field = df['Col_2'][i+1]
                break
    return required_field

def Score_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                required_field == 'ENOAH'
                break
            elif req_val == 'Zip':
                required_field = str(df['Col_2'][i+2])
                break
            elif req_val == 'DBA':
                if 'Company Name' not in df['Col_1'][i-2]:
                    value = df['Col_2'][i-2]+' '+df['Col_2'][i-1]
                    required_field = str(value).replace('D.B.A','')
                else:
                    required_field = str(df['Col_2'][i-1]).replace('D.B.A','')
                break
            else:
                required_field = str(df['Col_2'][i])
                if req_val == 'Address' and required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i])
                break
    required_field = str(required_field).replace('ENOAH','')
    return required_field

def Score_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'City':
                    required_field == 'ENOAH'
                    break
                elif req_val == 'Zip':
                    required_field = str(df['Col_2'][i+2])
                    break
                else:
                    required_field = str(df['Col_2'][i])
                    break
            else:
              
                if req_val == 'City':
                    required_field == 'ENOAH'
                elif req_val == 'Zip':
                    required_field = str(df['Col_2'][i+2])
                else:
                    required_field = str(df['Col_2'][i])
    return required_field

def Fundera_Inc_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val =='DBA':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH' and not 'Business Inception' in df['Col_1'][i+1]:
                    required_field = df['Col_2'][i+1]
                elif required_field !='ENOAH' and not 'Business Inception' in df['Col_1'][i+1] and 'DBA' in df['Col_1'][i+1]:
                    required_field = df['Col_2'][i]+' '+df['Col_2'][i+1]
                break
            elif req_val == 'Name':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH' and not 'Doing Business As' in df['Col_1'][i+1]:
                    required_field = df['Col_2'][i-1]+ ' '+ df['Col_2'][i+1]
                break
            elif req_val == 'TaxID':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH' and 'Number of' in str(df[column_name][i+1]):
                    required_field = df[column_name][i].replace(keyword,'').replace('# (EIN)','')
                elif required_field == 'ENOAH' and 'Number of' not in str(df[column_name][i+1]):
                    required_field = df['Col_2'][i+1]
                break
            else:
                required_field = str(df['Col_2'][i])
                if keyword == 'Business Address 1' and required_field == 'ENOAH' and 'ENOAH' in df[column_name][i-1]:
                    required_field = df['Col_2'][i-1]
                elif required_field =='ENOAH':
                    required_field = df['Col_2'][i+1]
                break
    return required_field

def Fundera_Inc_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'First Name' or req_val =='Last Name':
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    required_field = before_keyword
                    break
                if req_val == 'Owner':
                    mystring = df[column_name][i]
                    keyword = keyword
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    required_field = str(after_keyword).replace(')','')
                    break
                else:
                    required_field = str(df['Col_2'][i])
                    print('thaniya....',req_val,required_field)
                    if req_val == 'Home Address' and required_field == 'ENOAH':
                        print('fe.....',required_field)
                        required_field = str(df['Col_2'][i-1])
                    break
            else:
                if req_val == 'First Name' or req_val == 'Last Name':
                    value = df[column_name][i-1]
                    required_field = value.split('(')[0]
                elif req_val == 'Owner':
                    value = str(df[column_name][i-1])
                    required_field = re.sub('[a-zA-Z:, /()]',  '',value)
                else:
                    required_field = str(df['Col_2'][i])
                    if req_val == 'Home Address' and required_field == 'ENOAH':
                        print('fe.....',required_field)
                        required_field = str(df['Col_2'][i-1])
    return required_field

def Capsule_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(' (“Merchant”):','').replace('(“Merchant”):','').replace('("Merchant")','').replace(keyword,'')

            if required_field == '' or required_field == ' :':
                required_field = df[column_name][i+1]
            break
    required_field = str(required_field).replace('Suite/Floor:','').replace(' (“Merchant”):','').replace('(“Merchant”):','').replace('("Merchant")','')
    return required_field

def Capsule_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field='ENOAH'
                break
            elif req_val == 'Zip':
                value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i]))             
                required_field = value
                if required_field == '':
                    value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i+1]))
                    required_field = value
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
    return required_field

def DME_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i])).replace('975101','97501')
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def DME_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                value = str(df[column_name][i]).replace(keyword,'')
                value = [value]
                if len(value) == 1:
                    required_field = value[0]
                    if '/'in required_field:
                        required_field = required_field.split('/')[0]
                    elif ',' in required_field:
                        required_field = required_field.split(',')[0]
                    elif ' ' in required_field:
                        required_field = required_field.split(' ')[0]
                else:
                    required_field = 'ENOAH'
            elif req_val == 'Zip':
                value = str(df[column_name][i]).replace(keyword,'').strip()
                required_field = str(re.sub('[a-zA-Z:%, /-]', '',value[-6:]))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if req_val == 'State' and ',' in required_field:
                    required_field = required_field.split(',')[-1]
                break
    return required_field

def Hampton_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'State' or req_val == 'City':
                required_field = 'ENOAH'
                break
            elif req_val == 'Zip':
                value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i+2]))
                required_field = value
                break
            elif req_val == 'StartDate':
                value = str(re.sub('[a-zA-Z:%, /+]',  '',df[column_name][i+1]))
                req_field = value
                if '-' in req_field:
                    keyword = '-'
                    before_keyword, keyword, after_keyword = req_field.partition(keyword)
                    given_years = int(after_keyword)
                    currentTimeDate = int(datetime.now().strftime('%Y'))
                    required_field = (int(currentTimeDate)) - given_years
                else:
                    given_years = int(req_field)
                    currentTimeDate = int(datetime.now().strftime('%Y'))
                    required_field = (int(currentTimeDate)) - given_years
                break
            else:
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Hampton_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'City' or req_val == 'State':
                    required_field='ENOAH'
                    break
                elif req_val =='Home Address':
                    required_field = df[column_name][i-2]
                    break
                elif req_val == 'Zip':
                    value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i-1]))
                    required_field = value
                    break
                else:
                    required_field = str(df[column_name][i+1])
                    break
            else:
                if req_val == 'City' or req_val == 'State':
                    required_field='ENOAH'
                elif req_val =='Home Address':
                    required_field = df[column_name][i-2]
                elif req_val == 'Zip':
                    value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i-1]))
                    required_field = value
                else:
                    required_field = str(df[column_name][i+1])
    return required_field

def LendAbiz_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i+1]).split()
                required_field = value[-1]
                break
            elif req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    return required_field

def LendAbiz_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                if 'Phone' not in str(df[column_name][i+2]):
                    value = str(df[column_name][i+2]).split()
                    required_field = value[-1]
                else:
                    value = str(df[column_name][i+1]).split()
                    required_field = value[-1]
                break
            elif req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    return required_field

def Momentum_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            break
    return required_field

def Momentum_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(re.sub('[a-zA-Z:%, /]',  '',df[column_name][i]))
                required_field = value
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
    return required_field

def Torro_Funding_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
 
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            # if req_val == 'Tax':
            else:
                required_field = df[column_name][i+1]
                print('12222222....',req_val,required_field)
                if req_val == 'Legal Name' and required_field == 'ENOAH':##27/06
                    required_field = str(df['Col_3'][i+2])
                if required_field == 'ENOAH' and req_val == 'Date':
                    required_field = df[column_name][i].replace(keyword,'')
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
                if required_field == 'ENOAH' and keyword == 'Federal Tax ID#:':
                    print('oiiiii',required_field, keyword)
                    required_field = df['Col_5'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
            
            
    if i<0 and req_val == 'Address' and 'Business Location Ownership' in required_field:
        required_field = df['Col_2'][i+1]
    # if req_val == 'Address' and 'Business Location Ownership' in required_field:
    #     required_field = df['Col_2'][i]
    required_field = str(required_field).replace('Product / Services Sold:','')
    return required_field

def Torro_Funding_Owner(df,owner_status,keyword,column_name,req_val):##27/06
    keyword = keyword.strip()
    df.to_csv('torro11.csv')
    if keyword == 'Email:' or keyword == 'Phone:':
        required_field = 'ENOAH'
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if keyword == 'SSN:' in str(df['Col_2'][i]):
            df['Col_3'][i] = str(df['Col_2'][i]).split(':')[1]
            print('mistake..11',df['Col_3'][i])
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if i + 1 < len(df):
                    required_field = df[column_name][i].replace(keyword,'').replace(':','')
                    
                    if req_val == 'SS#' and required_field == '':
                        required_field = df[column_name][i+1]
                    if req_val == 'Date Of Birth' and (required_field == '' or required_field == 'SSN:'):
                        required_field = df[column_name][i+1]
                    if req_val == 'First Name' or req_val == 'Last Name':
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and (req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Zip' or req_val == 'Home Address'):
                        required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and i + 2 < len(df):
                        required_field = df[column_name][i+2]
                        if required_field == 'ENOAH' and (req_val == 'Zip' or req_val == 'Last Name') and i + 3 < len(df):
                            required_field = df[column_name][i+3]
                        print('rerererere',req_val,required_field)
                        
                            # print('adaptor',required_field)
                break 
            else:
                if i + 1 < len(df):
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH' and i + 2 < len(df):
                        required_field = df[column_name][i+2]
    # if keyword == 'SSN:' and required_field == 'ENOAH':
    #     for i in range (0,len(df)):
    #         if 'SSN:' in str(df['Col_2'][i]):
    #             required_field = df['Col_3'][i+1]
    #             break
    return required_field
# def Torro_Funding_Owner(df,owner_status,keyword,column_name,req_val):
#     keyword = keyword.strip()
#     
#     if keyword == 'Email:' or keyword == 'Phone:':
#         required_field = 'ENOAH'
#     required_field = 'ENOAH'
#     for i in range(0,len(df)):
#         if str(keyword) in str(df[column_name][i]):
#             if owner_status == 'Owner #1':
#                 required_field = df[column_name][i+1]
#                 if required_field == 'ENOAH':
#                     required_field = df[column_name][i+2]
#                     if required_field == 'ENOAH' and (req_val == 'Zip' or req_val == 'Last Name'):
#                         required_field = df[column_name][i+3]
#                 break 
#             else:
#                 required_field = df[column_name][i+1]
#                 if required_field == 'ENOAH':
#                     required_field = df[column_name][i+2]
#     if keyword == 'SSN:' and required_field == 'ENOAH':
#         for i in range (0,len(df)):
#             if 'SSN:' in str(df['Col_2'][i]):
#                 required_field = df['Col_3'][i+1]
#                 break
            
                
#     return required_field

def SFH_Consult_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            elif keyword == 'Zip Code:':
                value = str(df[column_name][i-1])
                required_field = str(re.sub('[a-zA-Z:%, /]',  '',value))
                break
            else:
                required_field = str(df[column_name][i-1])
                break
    return required_field

def SFH_Consult_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val =='City':
                required_field = 'ENOAH'
                break
            elif keyword == 'Business ownership %:':
                required_field = str(df['Col_2'][i-1])
                break
            elif keyword == 'Zip Code:':
                required_field = str(df[column_name][i-1])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if keyword == 'Home address (no PO Boxes):' and required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                break
    return required_field

def Captain_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'DBA:':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH' and not 'City:' in str((df[column_name][i+2])):
                    required_field = str(df[column_name][i+2])
                if required_field == 'ENOAH' and 'City:' in str((df[column_name][i+2])):
                    required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH' and 'City:' in str((df[column_name][i+3])) and str(df[column_name][i+2]) == 'ENOAH':
                    required_field = str(df['Col_3'][i+2])
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH' and (req_val == 'Zip' or req_val == 'StartDate'):
                        required_field = str(df[column_name][i+3])
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_3'][i]):
                required_field = str(df["Col_4"][i+1])
                break    
    return required_field

def Captain_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if keyword == 'Corporate Office/Owner Name:' and ('Home Address:' in required_field or 'ENOAH' in required_field):
                        required_field = str(df['Col_2'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+2])
                    if (keyword == 'Ownership %:' or keyword == 'Zip:') and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2]).replace('Date of Birth:','')
                    if keyword == 'Corporate Office/Owner Name:' and ('Home Address:' in required_field or 'ENOAH' in required_field):
                        required_field = str(df['Col_2'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+2])
                    if keyword == 'Ownership %:' and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_5'][i+3])
                    if keyword == 'Ownership %:' and ('State' in required_field or 'ENOAH' in required_field):
                        required_field = str(df['Col_5'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_5'][i+2])
    return required_field

def Dubbs_Holdings_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State':
                required_field = 'ENOAH'
                break
            elif keyword == 'Zip Code:':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%., /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%., /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
    return required_field

def Dubbs_Holdings_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Home address (no PO Boxes):':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i-1])
                break
            elif keyword == 'Zip Code:' or keyword == 'Business ownership %:':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%., /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%., /]', '',value))
                    if required_field == '':
                        value = str(df[column_name][i+1])
                        required_field = str(re.sub('[a-zA-Z:%., /]', '',value))
                break                
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
    return required_field

def Bobby_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City' or req_val == 'State' or req_val == 'Zip' or req_val == 'Address':
                required_field = df[column_name][i-1]
                break
            elif keyword == 'Business Legal Name':
                if 'Federal Tax ID' in str(df[column_name][i+5]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]+' '+str(df[column_name][i+3]+' '+str(df[column_name][i+4]))))
                elif 'Federal Tax ID' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]+' '+str(df[column_name][i+3])))
                elif 'Federal Tax ID' in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]))
                elif 'Federal Tax ID' in str(df[column_name][i+2]):
                    required_field = str(df[column_name][i+1])
                required_field = required_field.replace('ENOAH','')
                break
            elif keyword == 'Doing Business As (DBA)':
                if 'Business Start Date' in str(df[column_name][i+5]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]+' '+str(df[column_name][i+3]+' '+str(df[column_name][i+4]))))
                elif 'Business Start Date' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]+' '+str(df[column_name][i+3])))
                elif 'Business Start Date' in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1]+' '+str(df[column_name][i+2]))
                elif 'Federal Tax ID' in str(df[column_name][i+2]):
                    required_field = str(df[column_name][i+1])
                required_field = required_field.replace('ENOAH','')
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for k in range (0,len(df)):
            if 'Zip Code' not in str(df['Col_3'][k]) and 'Business Address' in str(df['Col_1'][k]):
                required_field = str(df['Col_1'][k+1]).split()[-1]
                break
    if req_val == 'Address' and required_field == 'ENOAH':
        for k in range (0,len(df)):
            if 'Address Line 1' not in str(df['Col_1'][k]) and 'Business Address' in str(df['Col_1'][k]):
                required_field = str(df['Col_1'][k+1])
                break
    return required_field

def Bobby_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword =='City' or keyword == 'Zip Code' or keyword == 'State':
                    required_field = str(df[column_name][i-1])
                    break
                elif req_val == 'Last Name':
                    required_field = str(df['Col_2'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_1'][i+1])
                    break
                else:
                    required_field = str(df[column_name][i+1])
                    if req_val == 'Home Address' and required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                    break
            else:
                if keyword =='City' or keyword == 'Zip Code' or keyword == 'State':
                    required_field = str(df[column_name][i-1])
                elif req_val == 'Last Name':
                    required_field = str(df['Col_2'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_1'][i+1])
                else:
                    required_field = str(df[column_name][i+1])
                    if req_val == 'Home Address' and required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
    if owner_status == 'Owner #1':                
        if keyword == 'Social Security Number' and required_field == 'ENOAH':
            for i in range (0,len(df)):
                if 'Social Security Number' in str(df['Col_3'][i]):
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1]
                    break
        if keyword == '% of Ownership' and required_field == 'ENOAH':
            for i in range (0,len(df)):
                if '% of Ownership' in str(df['Col_4'][i]):
                    required_field = df['Col_4'][i+1]
                    break
    else:
        if keyword == 'Social Security Number' and required_field == 'ENOAH':
            for i in range (0,len(df)):
                if 'Social Security Number' in str(df['Col_3'][i]):
                    required_field = df['Col_2'][i+1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1]
        if keyword == '% of Ownership' and required_field == 'ENOAH':
            for i in range (0,len(df)):
                if '% of Ownership' in str(df['Col_4'][i]):
                    required_field = df['Col_4'][i+1]
    if req_val == 'Zip' and required_field == 'ENOAH':
        for k in range(0,len(df)):
            if 'Zip Code' not in str(df['Col_3'][k]) and 'Home Address' in str(df['Col_1'][k]):
                req = str(df['Col_1'][k+1])
                required_field = req.split()[-1]
                if req == 'ENOAH':
                    req = str(df['Col_2'][k+1])
                    required_field = req.split()[-1]
                break
    return required_field
def Hattox_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Business Address':
                required_field = df['Col_2'][i+1]
                break
            else:
                required_field = str(df[column_name][i+1])
                if (keyword == 'Business Phone:' or keyword == 'Tax ID#:') and required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                elif req_val == 'DBA' and required_field == 'ENOAH' and not 'Website:' in str(df['Col_2'][i+1]):
                    required_field = df['Col_2'][i+1]
                break
    required_field = str(required_field).replace('Business Phone:','')
    return required_field

def Hattox_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                val = str(df[column_name][i+1])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',val))
                if val == 'ENOAH':
                    val = str(df[column_name][i+2])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',val))
                break
            elif req_val == 'City' or req_val == 'State':
                val = str(df[column_name][i+1])
                if val == 'ENOAH':
                    val = str(df[column_name][i+2])
                if ',' in str(val):
                    if req_val == 'City':
                        required_field = str(val).split(',')[0]
                    elif req_val == 'State':
                        required_field = str(val).split(',')[1]
                elif ' ' in str(val):
                    if req_val == 'City':
                        required_field = str(val).split(' ')[0]
                    elif req_val == 'State':
                        required_field = str(val).split(' ')[1]
            else:
                required_field = str(df[column_name][i+1])
                break
    required_field = str(required_field).replace('City, State Zip:','')
    return required_field

def CridbleERC_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i-1])
                break
    return required_field

def CridbleERC_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field == '':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1]).replace('&','').replace('PRIMARY OWNER','').replace('Name','').replace('% Ownership','')
                break
    return required_field

def Montage_Business(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            break
    return required_field

def Montage_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'ZIP':
                required_field = df[column_name][i+1]
                break
            else:
                required_field = str(df[column_name][i+1])
                break
    if keyword == 'ZIP' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Home Address' in df['Col_1'][i]:
                value = df['Col_1'][i+1].split()
                if value[-1].isdigit():
                    required_field = value[-1]
                    break
    return required_field 

def Wisco_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'ZIP':
                value = str(df['Col_2'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field =='':
                    value = str(df['Col_2'][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if required_field =='ENOAH':
                        required_field = str(df[column_name][i-2])
                        if keyword == 'LEGAL NAME:' and required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i-2])
                break
    return required_field

def Wisco_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'ZIP':
                value = str(df['Col_4'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                if required_field =='':
                    value = str(df['Col_4'][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if required_field =='ENOAH':
                        required_field = str(df[column_name][i-2])
                break
    return required_field 

def iFund_Lendtek_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('lendtek.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace(':','')
            break
    if keyword == 'DBA:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'DBA:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i].replace(keyword,'')
                break
    if keyword == 'Federal Tax ID:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Federal Tax ID:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i].replace(keyword,'')
                break
    if keyword == 'City:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'City:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i].replace(keyword,'')
                break
    if keyword == 'State:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'State:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i].replace(keyword,'').replace('Zip:','')
                break 
    if keyword == 'Zip:' and required_field == 'ENOAH':#28/08
        for i in range (0,len(df)):
            if 'Zip:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i].replace(keyword,'').replace('State:','')
                break 
    return required_field

def iFund_Lendtek_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
                break
            else:
                required_field = df[column_name][i].replace(keyword,'').replace(':','')
    if keyword == 'Owner Last Name:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Owner Last Name:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i].replace(keyword,'')
                break
    if req_val == 'Mobile_Number' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Cell #:' in str(df['Col_4'][i]):
                required_field = df['Col_4'][i].replace('Cell #:','')
                break
    if req_val == 'Ownership' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Ownership %:' in str(df['Col_2'][i]):
                if owner_status == 'Owner #1':
                    required_field = df['Col_2'][i].replace(keyword,'')
                    break
                else:
                    required_field = df['Col_2'][i].replace(keyword,'')
    if keyword == 'Partner Last Name:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Partner Last Name:' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i].replace(keyword,'')
                break
    if keyword == 'Zip:' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Zip:' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i].replace(keyword,'')
                break
    return required_field    

def Cardinal_Labo_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Zip Code' or keyword == 'Federal':
                value = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() :/]', '',value))
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
            else:
                required_field = str(df[column_name][i]).replace('Business Legal Name','').replace('Business LegaI Name','').replace('59OO S A','5900 S A').replace('Business start Date','').replace('Business Start Date','').replace('PhysicaI','').replace('Physical','').replace(' (no PO Boxes)','').replace('(no Po Boxes)','').replace(keyword,'').replace(':','')
                if required_field == '' or required_field == ' ':
                    required_field = str(df[column_name][i+1])
                    if keyword == 'Business Legal Name:' and 'State of' in required_field:
                        required_field = str(df['Col_2'][i])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i-1])
                break
    required_field = str(required_field).replace('(no Po Boxes)','').replace(':','').replace('(no PO Boxes)','')
    if 'Description/Industry' in required_field:
        required_field = 'ENOAH'
    if req_val == 'StartDate' and ',' in required_field:
        required_field = str(required_field).replace(',',' ')
    return required_field

def Cardinal_Labo_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'ip Code':
                    value = str(df[column_name][i])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                    if required_field =='':
                        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',df[column_name][i+1]))
                    break
                elif req_val == 'Social_Security1':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                    break
                else:
                    required_field = str(df[column_name][i]).replace('no PO Boxes):1','').replace('(no PO Boxes):','').replace('(no po Boxes):','').replace(keyword,'')
                    if required_field == '' or required_field == ' ':
                        required_field = str(df[column_name][i+1])
                        if 'Zip Code' in required_field and (req_val == 'Street_Address1' or req_val == 'Street_Address2'):
                            required_field = str(df[column_name][i-1])
                        if required_field == 'ENOAH' and req_val == 'Date_of_Birth1':
                            required_field = str(df[column_name][i-1])
                    break
        else:
            if str(keyword) in str(df[column_name][i]):
                if keyword == 'ip Code':
                    value = str(df[column_name][i])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                elif req_val == 'Social_Security2':
                    value = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',value))
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace('(no PO Boxes):','')
                    if required_field == '' or required_field == ' ':
                        required_field = str(df[column_name][i+1])
                        if 'Zip Code' in required_field and (req_val == 'Street_Address1' or req_val == 'Street_Address2'):
                            required_field = str(df[column_name][i-1])
    required_field = str(required_field).replace(']','').replace(':','').replace('(no PO Boxes)','').replace('no PO Boxes)','').replace('no Po Boxes)','').replace('(','')
    return required_field

def TrueCore_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df['Col_2'][i+1])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                break
            elif keyword == 'Years in Business':
                req = str(df['Col_2'][i])
                given_years = re.findall('\d+', req)[0]                
                stmt_generate_date = str(df['Col_2'][0]).split(' ')[-1]
                required_field = (int(stmt_generate_date)) - (int(given_years))
                break
            else:
                required_field = str(df['Col_2'][i])
                break 
    return required_field
    
def TrueCore_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Zip_Code1':
                    req = str(df['Col_2'][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                    break
                else:
                    required_field = str(df['Col_2'][i])
                    break
            else:
                required_field = 'ENOAH'
        else:
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Zip_Code1':
                    req = str(df['Col_2'][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                    break
                else:
                    required_field = str(df['Col_2'][i])
                    break
            else:
                required_field = 'ENOAH'
    return required_field

def Pillar_Buss (df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Business Address' and req_val == 'Zip':
                if 'United States' in df[column_name][i+4]:
                    value = df[column_name][i+3]
                    required_field = value.split(' ')[-1]
                if 'United States' in df[column_name][i+3]:
                    value = df[column_name][i+2]
                    required_field = value.split(' ')[-1]
                break
            else:
                required_field = df[column_name][i+1]
                
    required_field = str(required_field).replace('Annual Revenue','')
    return required_field

def Pillar_Owner (df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if keyword == 'Ownership Percentage' and req_val == 'Zip_Code1':
                req = str(df[column_name][i-2]).split()
                required_field = req[-1]
                break
            elif keyword == 'First & Last Name':
                required_field = df[column_name][i+1]
                if '4pillarfunding' in required_field and 'E-Signature' in str(df[column_name][i+2]):
                    required_field = str(df[column_name][i+3])
                break
            else:
                required_field = df[column_name][i+1]
                if req_val == 'Ownership1'  and not re.search('[0-9]',str(required_field)):
                    required_field = 'ENOAH'
                break
    return required_field

def Strategic_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'DBA)':
                required_field = str(df[column_name][i+3]+' '+df[column_name][i+4]).replace('Business Address','').replace('*','')
                break
            elif req_val == 'City' or req_val == ' State':
                required_field = 'ENOAH'
                break
            elif req_val == 'Address':
                required_field = str(df[column_name][i-2])+' '+str(df[column_name][i-1])
                if '/' in required_field and ('PM' or 'AM')in required_field:
                    required_field = 'ENOAH'
                break
            else:
                required_field = str(df[column_name][i+2])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+3])
                break
    if req_val == 'Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Address' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i+3])+' '+str(df['Col_1'][i+4])
                break 
    required_field = str(required_field).replace('ENOAH','')
    return required_field
    
def Strategic_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Email' or keyword == 'ABCDEFG' or keyword == 'ABCGEFG':
                required_field = 'ENOAH'
                break
            elif req_val =='Last Name':
                required_field = re.sub('[0-9:%,-/]',  '',str(df['Col_2'][i+2]))
                if required_field == 'ENOAH' or required_field == '':
                    required_field = df['Col_2'][i+3]
                break
            else:
                required_field = str(df[column_name][i+1]).replace('ex: 23','').replace('100100','100')
                if required_field == 'ENOAH' or required_field =='':
                    required_field = str(df[column_name][i+2])
                break
    required_field = str(required_field).replace('ex: 23','').replace('mm-dd-yyyy','')
    return required_field

def STEADY_Funds_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                req = str(df[column_name][i]+' '+df['Col_2'][i]).replace('*','')
                required_field = str(req).replace('lie','llc')
                if 'BA Name' not in str(df[column_name][i+1]):
                    required_field = str(df[column_name][i]+' '+df['Col_2'][i]+ ' '+df['Col_2'][i+1]).replace('*','')
                required_field = str(required_field).replace('Business Legal Name','').replace('Legal Name','').replace('ENOAH','').replace('lie','llc').replace('*','')
                break
            if req_val == 'DBA':
                req = str(df[column_name][i]+' '+df['Col_2'][i])
                required_field = req.replace('lie','llc').replace('*','')
                if 'Tax ID' not in str(df[column_name][i+1]):
                    required_field = str(df[column_name][i]+' '+df['Col_2'][i]+ ' '+df['Col_2'][i+1]).replace('*','')
                required_field = str(required_field).replace('DBA Name','').replace('OBA Name','').replace('ENOAH','').replace('lie','llc').replace('*','')
                break
            elif req_val == 'Zip':
                required_field = df['Col_2'][i-1]
                break
            elif req_val == 'Address':
                if not re.search('[0-9]',str(df['Col_2'][i+3])) and re.search('[0-9]',str(df['Col_2'][i+4])):
                    required_field = str(str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])).replace('ENOAH','').replace('Junction','').replace('*','')
                elif not re.search('[0-9]',str(df['Col_2'][i+3])) and not re.search('[0-9]',str(df['Col_2'][i+4])):
                    required_field = str(str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])+' '+str(df['Col_2'][i+2])).replace('ENOAH','').replace('Junction','').replace('*','')
                else:
                    required_field = str(df['Col_2'][i])
                break
            else:
                required_field = str(df[column_name][i]).replace('Date business ','').replace('# *','').replace('#*','').replace(keyword,'').replace('*','').replace('•','').replace('#','')
                if required_field == '' or required_field == ' ':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
    if keyword == 'Legal Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Legal Name*' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'').replace('*','')
                break
    if keyword == 'started' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'started' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'').replace('*','')
                break
    required_field = str(required_field).replace('•','')
    return required_field
    
def STEADY_Funds_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Email' or keyword == 'ABCDEFG' or keyword == 'ABCGEFG':
                    required_field = 'ENOAH'
                    break
                elif req_val =='Zip_Code1':
                    required_field = str(df['Col_2'][i-1])
                    if 'ID' in required_field:
                        required_field = str(df['Col_2'][i-2])
                    break
                elif req_val == 'Street_Address1':
                    if not re.search('[0-9]',str(df['Col_2'][i+3])) and re.search('[0-9]',str(df['Col_2'][i+4])):
                        required_field = str(str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])).replace('ENOAH','').replace('Junction','').replace('*','')
                    elif not re.search('[0-9]',str(df['Col_2'][i+3])) and not re.search('[0-9]',str(df['Col_2'][i+4])):
                        required_field = str(str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])+' '+str(df['Col_2'][i+2])).replace('ENOAH','').replace('Junction','').replace('*','')
                    else:
                        required_field = str(df['Col_2'][i])
                    break                    
                elif keyword == 'Owner#' and req_val == 'Last_Name1':
                    required_field = str(df['Col_2'][i])
                    req = required_field.split()
                    if len(req)==1:
                        required_field = str(df['Col_2'][i+1])
                    break
                else:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                        if req_val == 'Ownership1' and required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i-1])
                    break
            else:
                if keyword == 'Email' or keyword == 'ABCDEFG' or keyword == 'ABCGEFG':
                    required_field = 'ENOAH'
                elif req_val =='Zip_Code2':
                    if not '(' in str(df['Col_2'][i-1]):
                        required_field = str(df['Col_2'][i-1])
                        if 'ID' in required_field:
                            required_field = str(df['Col_2'][i-2])
                    else:
                        required_field = str(df['Col_2'][i-2])
                        if 'ID' in required_field:
                            required_field = str(df['Col_2'][i-2])
                elif req_val == 'Street_Address2':
                    if not re.search('[0-9]',str(df['Col_2'][i+3])):
                        required_field = str(df['Col_2'][i])+' '+ str(df['Col_2'][i+1]).replace('ENOAH','')
                    else:
                        required_field = str(df['Col_2'][i])
                elif keyword == 'Owner#' and req_val == 'Last_Name2':
                    required_field = str(df['Col_2'][i])
                    req = required_field.split()
                    if len(req)==1:
                        required_field = str(df['Col_2'][i+1])
                else:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
    required_field = str(required_field).replace('ENOAH','')
    return required_field

def SBS_Group_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip' or req_val == 'Address' or req_val == 'City' or req_val == 'State':
                required_field = str(df[column_name][i]).replace(keyword,'')
                print('reqqq',required_field)
            else:
                required_field = str(df[column_name][i+1]).replace('Street Address:','')
                break
#     if req_val == 'Zip' and required_field == 'ENOAH':
#         for i in range(0,len(df)):
#             if 'BUSINESS ZIP' in str(df['Col_1'][i]):
#                 required_field = str(df['Col_1'][i+1])
#                 break 
    return required_field

def SBS_Group_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
            else:
                required_field = str(df[column_name][i+1]).replace('Street Address:','')
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'HOME ZIP' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i+1])
                break 
    return required_field

def Central_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'TaxID' or req_val == 'Address' or req_val == 'Phone':
                required_field = str(df[column_name][i]).replace('Legal company Name:','').replace('Legal Company Name:','').replace('egal Company Name:','').replace('Legal Company Name','').replace('Federal Tax ID/EIN:','').replace('FederalTax ID/EIN','').replace('Federal Tax ID/EIN','').replace('FederalTaxID/EIN','').replace('Federal TaxID/EIN','').replace('ederal Tax ID/EIN:','').replace('Business Address(No PO Box:','').replace('Business Address(No PO Box):','').replace('Business Address(No po Box):','').replace('Business Address (No PO Box):','').replace('Business Address(No PO Box)','').replace('Business Address(No Po Box):','').replace('Business AddressNo PO Box)','').replace('Business Address No PO Box):','').replace('Business Address (No PO Box','').replace('Business Address ','').replace('iusiness Address ','').replace('(No Po Box):','').replace('(No po Box):','').replace('City:','').replace('city:','').replace(':ity:','').replace('tity:','').replace('Business Phone #','').replace('iusiness Phone #','').replace(keyword,'').replace(':','')
                if required_field == '' or required_field == ' ':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH' or required_field =='':
                        required_field = str(df['Col_2'][i-1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'Zip' or req_val == 'State' or req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace('Zip Code:','').replace('State:','').replace('zip Code:','').replace('Busines Inception Date:','').replace('Business Inception Date:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    if req_val == 'Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Legal Company Name:' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i]).replace('Legal Company Name:','')
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip Code:' in str(df['Col_2'][i]):
                req = str(df['Col_2'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                break
    if req_val == 'Zip':
        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
    return required_field

def Central_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Home Address' or req_val == 'State' or req_val == 'Last Name' or req_val == 'Mobile':
                required_field = str(df[column_name][i]).replace('First Name:','').replace('irst Name:','').replace('First Name','').replace('Home Address(No PO Box):','').replace('Home Address (No Po Box):','').replace('Home Address (No PO Box)','').replace('Home Address (No PO Box):','').replace('Home Address(No Po Box)','').replace('Home Address(No Po Box):','').replace('Iome Address (No PO Box):','').replace('Home Address(No PO Box:','').replace('Home Address(No Po Box','').replace('Home Address No PO Box','').replace('Home AddressNo PO Box','').replace('Home Address (No po Box):','').replace('Home Address (No PO Box','').replace('Home Address(No PO Box)','').replace('Home Address(No PO Box','').replace('ome Address (No Po Box):','').replace('(No PO Box):','').replace('City:','').replace('city:','').replace(':ity:','').replace('.ity:','').replace('Last Name:','').replace('thone (Cell):','').replace('Phone (Cell):','').replace('Phone (Cell)','').replace('State:','').replace(keyword,'').replace(': ','').replace(':','')
                if required_field == '' and (req_val == 'Last Name' or req_val == 'First Name'):
                    required_field = str(df[column_name][i-1])
                    if 'INFORMATION' in required_field and req_val == 'First Name':
                        required_field = str(df['Col_2'][i])
                    elif 'ENOAH' in required_field and req_val == 'First Name' and 'ENOAH' in str(df['Col_2'][i]):
                        required_field = str(df['Col_2'][i-1])
                if required_field == '' or required_field ==' ':
                    required_field = str(df['Col_2'][i]).replace('State:','')
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                        if req_val == 'Home Address' and not re.search('[a-zA-Z]',str(required_field)):
                            required_field = str(df['Col_2'][i+1])
                        if req_val == 'First Name' and ' 'in required_field:
                            required_field = str(required_field).replace(' ','')
                break
            elif req_val == 'SS#':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                if required_field =='':
                    req = str(df['Col_2'][i])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                    if req == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,()& /]', '',req))
                if required_field == '' or required_field == ' ':
                    required_field = str(df[column_name][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+2])
                break
            else:
                required_field = str(df[column_name][i]).replace('Ownership %:','').replace('Ownership %: ','').replace('0wnership %:','').replace('0wnership%:','').replace('Ownership%:','').replace(keyword,'').replace(':','')
                if required_field == '' and (req_val == 'Date Of Birth' or req_val == 'Owner'):
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH' or 'Credit Sco' in required_field:
                        required_field = str(df[column_name][i-1])
                break
    if req_val == 'Home Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Home Address (No Po Box):' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i]).replace('Home Address (No Po Box):','')
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Code:' in str(df['Col_2'][i]):
                required_field = str(df['Col_3'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+1])
                break
    return required_field

def Blueline_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'Address' or req_val == 'StartDate':
                required_field = str(df['Col_2'][i-1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i-2])
                break
            elif req_val == 'City' or req_val == 'State':
                required_field == 'ENOAH'
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i-1]) 
                requ = str(re.sub('[a-zA-Z:%.,()/]', ' ',req))
                requ = str(requ).split()
                if len(requ[0])>=4:
                    required_field = requ[0]
                break
            else:
                required_field = str(df[column_name][i-1])
                break
    return required_field

def Blueline_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Home Address':
                    required_field = str(df['Col_2'][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-2])
                    break
                elif req_val == 'City' or req_val == 'State':
                    required_field == 'ENOAH'
                    break
                elif req_val == 'SS#' or req_val == 'Date Of Birth' or req_val == 'Zip' or req_val =='Owner':
                    req = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                    if required_field == '' or required_field == 'ENOAH':
                        req = str(df[column_name][i-2])
                        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                        if required_field == '':
                            required_field = str(df[column_name][i-3])
                    if req_val == 'Owner' and req == 'ENOAH':
                        required_field = str(df['Col_5'][i-1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_5'][i-2])
                    break
                else:
                    required_field = str(df[column_name][i+1])
                    break 
    if req_val == 'SS#' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'SSN' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).replace('SSN','')
                break
    return required_field 

def Westwood_Trustfactor_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if req_val == 'Name' or req_val == 'Phone' or req_val == 'Address':
                required_field = str(df[column_name][i]).replace('Business Legal Name:','').replace('Business LegalName:','').replace('Cell Phone:','').replace(keyword,'')
                if required_field =='':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i-1])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',req))
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
            elif req_val == 'DBA' or req_val == 'TaxID' or req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace('Business Start Date:','').replace('Date','').replace('Business Start ate:','').replace('Federal TaxI:','').replace('Federal Tax ID:','').replace('Federal Tax ID','').replace('Doing Business As:','').replace(keyword,'')
                if req_val == 'StartDate' and required_field == '':
                    required_field = str(df[column_name][i+1])
                if req_val != 'StartDate' and required_field == '':
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i-1])
                        if required_field == 'ENOAH' and req_val == 'TaxID':
                            required_field = str(df['Col_4'][i+1])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'State:' in str(df['Col_3'][i]):
                req = str(df['Col_4'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
    if req_val == 'Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Legal Name:' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i]).replace('Business Legal Name:','')
                break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Company Website' in str(df['Col_3'][i]):
                required_field = str(df['Col_4'][i-2])
                break
    required_field = str(required_field).replace('Doing Business As:','')
    return required_field

def Westwood_Trustfactor_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(re.sub('\s+',' ',str(df[column_name][i]))):
            if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Date Of Birth' or req_val == 'Home Address' or req_val == 'Email':
                required_field = df[column_name][i].replace('Date of Birth:','').replace('Date of Birth','').replace(keyword,'').replace(':','')
                if required_field == '':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                        if req_val == 'Date Of Birth' and 'Owner' in required_field:
                            required_field = str(df['Col_2'][i-1])
                break
            elif req_val == 'Owner' or req_val =='SS#' or req_val == 'Mobile':
                required_field = str(df[column_name][i]).replace('% of Ownership:','').replace('%of Ownership:','').replace('Social Security #:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH' and not req_val == 'Owner':
                        required_field = str(df['Col_4'][i-1])
                    if required_field == 'ENOAH' and req_val == 'Owner':
                        required_field = str(df['Col_4'][i+1])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() |/]', '',req))
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'zip:' in str(df['Col_4'][i]):
                req = str(df['Col_4'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                if required_field =='' and not '(' in str(df['Col_4'][i+1]):
                    req = str(df['Col_4'][i+1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                if required_field =='' and '(' in str(df['Col_4'][i+1]):
                    req = str(df['Col_4'][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req)) 
                break
    return required_field

def Simplify_Integrated_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'Address' or req_val == 'Phone':
                required_field = str(df[column_name][i]).replace('Business Legal Name:','').replace('Business LegaI Name:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'DBA' or req_val == 'TaxID' or req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '' or required_field == ' ':
                    required_field = str(df[column_name][i+1])
                break
            elif req_val == 'Zip':
                req= str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    return required_field

def Simplify_Integrated_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Home Address' or req_val == 'Date Of Birth' or req_val == 'Email':
                required_field = str(df[column_name][i]).replace('Home Address:','').replace('HomeAddress:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'Owner' or req_val == 'Mobile' or req_val == 'SS#':
                required_field = str(df[column_name][i]).replace('% of 0wnership:','').replace('% of Ownership:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if req_val == 'SS#' and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2]).replace(':','')
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
    return required_field 

def Infusion_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'Address' or req_val == 'Phone' or req_val == 'DBA' or req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace('LegaI Name:','').replace('Legal Name:','').replace('Physical Address (No P.0. Boxes):','').replace('Physical Address (No P.O. Boxes):','').replace('Physical Address (No P.O.Boxes):','').replace('Physical Address (No P.0.Boxes):','').replace(keyword,'').replace(':','')
                if req_val == 'Name' and required_field =='':
                    required_field = str(df[column_name][i-1])
                if req_val == 'Address' and 'City' in required_field :
                    required_field = str(required_field).split('City')[0]
                if req_val == 'Name' and required_field != '' and 'Physical' not in str(df[column_name][i+1]):
                    required_field = str(required_field)+' '+str(df[column_name][i+1])
                if req_val == 'Address' and required_field != '' and 'Business' not in str(df[column_name][i+1]):
                    required_field = str(required_field)+' '+str(df[column_name][i+1])
                if req_val == 'DBA' and required_field != '' and 'Cit' not in str(df[column_name][i+1]):
                    required_field = str(required_field)+' '+str(df[column_name][i+1])
                if req_val == 'StartDate' and required_field !='' and 'ENOAH' not in str(df[column_name][i+1]):
                    required_field = str(required_field)+''+str(df[column_name][i+1])
                break
            elif req_val == 'Zip' or req_val == 'TaxID':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
    required_field = str(required_field).replace('ENOAH','').replace('_','')
    return required_field

def Infusion_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Home Address' or req_val == 'SS#':
                required_field = str(df[column_name][i]).replace('Owner#1:','').replace('Owner #1:','').replace(keyword,'')
                if required_field != '' and (req_val == 'Last Name' or req_val == 'First Name') and not 'Address' in str(df[column_name][i+1]):
                    required_field = str(required_field)+' '+ str(df[column_name][i+1])
                break
            elif req_val == 'Owner' or req_val == 'Date Of Birth' or req_val == 'Mobile' :
                required_field = str(df[column_name][i]).replace('Ownership %:','').replace('Ownership%:','').replace(keyword,'')
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
    if req_val == 'Zip' and (required_field == 'ENOAH' or required_field == ''):
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_3'][i]) or 'zip:' in str(df['Col_3'][i]):
                req = str(df['Col_3'][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                if '0' in required_field[0]:
                    required_field = required_field[1:]
                break
    required_field = str(required_field).replace('ENOAH','').replace('_','')
    return required_field 

def Synergy_Fin_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = str(df[column_name][i]).replace('Legal Company  Name :','').replace('Legal Company Name','').replace('egal Company Name:','').replace(' Name:','').replace(keyword,'').replace(':','').replace(' ','')
                if required_field == '' and str(df[column_name][i-1]) != 'ENOAH':
                    required_field = str(df[column_name][i-1])
                    if 'Legal' not in str(df[column_name][i]) and 'L' in str(df[column_name][i-1][0]):
                        required_field = required_field[1:]
                elif required_field == '' and str(df[column_name][i-1]) == 'ENOAH':
                    required_field = str(df[column_name][i-2])
                    if 'Legal' not in str(df[column_name][i]) and 'L' in str(df[column_name][i-2][0]):
                        required_field = required_field[1:]
                elif required_field != '' and 'Legal' not in str(df[column_name][i]) and 'L' in str(df[column_name][i]):
                    required_field = required_field[1:]
                break
            elif req_val == 'DBA':
                req = str(df[column_name][i-3])+ ' '+ str(df[column_name][i-2])
                required_field = str(req).replace('Doing Business As','').replace('Doing Business','').replace('Doing  Business','').replace('oing Business','').replace('ENOAH','').replace('keyword','')
                if str(df[column_name][i-3]) == 'D':
                    required_field = str(required_field)[1:].strip()
                    if required_field == '' and not 'ENOAH' in str(df[column_name][i-1]):
                        required_field = str(df[column_name][i-1])
                if str(df[column_name][i-2]) == 'oing Business' and str(df[column_name][i-3][0])=='D' and str(df[column_name][i-1]) == 'ENOAH':
                    required_field = required_field[1:]
                break
            elif req_val == 'TaxID' or req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,:() /|]', '',req))
                if required_field == '' or required_field == ' ':
                    req = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,:() /|]', '',req)).replace('5409','95409')
                    if required_field == '' or required_field == 'ENOAH':
                        req = str(df[column_name][i-2])
                        required_field = str(re.sub('[a-zA-Z:%.:,() /|]', '',req))
                        if required_field == '' or required_field == 'ENOAH':
                            req = str(df[column_name][i-3])
                            required_field = str(re.sub('[a-zA-Z:%.:,() /|]', '',req))
                break
            elif req_val == 'StartDate':
                required_field = str(df[column_name][i-1]).replace('B','')
                if required_field =='ENOAH':
                    required_field = str(df[column_name][i-2])
                    if 'Company' in required_field:
                        req = str(df[column_name][i])
                        required_field = str(re.sub('[a-zA-Z:%:,() |]', '',req))
                break
            elif req_val == 'Phone':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req)).replace('5409','95409')
                if required_field == '' or required_field == 'ENOAH':
                    req = str(df[column_name][i-1])
                    required_field = str(re.sub('[a-zA-Z:%.,() /|]', '',req))
                break
            elif req_val == 'Address':
                req = str(df[column_name][i-2])+ ' '+ str(df[column_name][i-1])+ ' '+ str(df[column_name][i])+ ' '+ str(df[column_name][i+1])+ ' '+ str(df[column_name][i+2])
                if str(df[column_name][i-1])=='P':
                    req = str(req).replace('P','')
                if 'ax ID: ' in str(df[column_name][i-1]) and str(df[column_name][i-2])=='T':
                    req = str(req).replace(str(df[column_name][i-1]),'').replace(str(df[column_name][i-2]),'')
                required_field = str(req).replace('Physical Address','').replace('Physical Address','').replace('hysical Address','').replace('Physical  Address','').replace('Physical','').replace(' (No .O. Box)','').replace("(No P.O. Box)","").replace('Tax ID','').replace('ax ID','').replace(':','').replace('ENOAH','')
                if str(df[column_name][i]) == 'hysical Address' and required_field[0]=='P':
                    required_field = required_field[1:]
                if str(df[column_name][i]) == 'Physical Address' and str(df[column_name][i-1]) == 'ax ID:' and str(df[column_name][i-2])[0]=='T':
                    required_field = str(df[column_name][i+1]).replace('(No P.O. Box):','')
                if str(df[column_name][i])=='hysical Address' and str(df[column_name][i-1])=='P' and 'ax ID:' in str(df[column_name][i-2]) and str(df[column_name][i+1]) == '(No P.O. Box):':
                    required_field = str(df[column_name][i+2])
                if str(df[column_name][i])=='Physical  Address' and str(df[column_name][i-1])=='Tax ID:'and str(df[column_name][i+1]) == '(No P.O. Box):' and not re.search('[a-zA-Z]',str(df[column_name][i-2])):
                    required_field = required_field = str(df[column_name][i+2])
                if str(df[column_name][i])=='Physical Address' and not re.search('[a-zA-Z]',str(df[column_name][i-1])) and str(df[column_name][i-2]) == 'Tax ID.' and str(df[column_name][i+2]) == 'ENOAH' and '(No P.O. Box):' in str(df[column_name][i+1]):
                    required_field = required_field = str(df[column_name][i+1]).replace('(No P.O. Box):','')
                if str(df[column_name][i])=='Physical Address' and str(df[column_name][i-1]) == 'ENOAH' and 'ax ID:' in str(df[column_name][i-2]) and str(df[column_name][i+1]) == '(No P.O. Box):' and str(df[column_name][i+2] !='ENOAH'):
                    required_field = required_field = str(df[column_name][i+2])
                
                break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Tax ID.' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+1]).replace(keyword,'')
                break
    return required_field

def Synergy_Fin_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if req_val == 'First Name' and 'Last' in required_field:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                    if required_field == '':
                        required_field = str(df['Col_2'][i])
                if req_val == 'Last Name' and 'Email' in required_field:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                    if required_field == '':
                        required_field = str(df['Col_2'][i-1])
                if req_val == 'SS#' and 'Date' in required_field:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i-1])
                if req_val == 'Zip' and required_field=='ENOAH':
                    required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                    if required_field =='':
                        required_field = str(df[column_name][i+2])
                if req_val == 'Date Of Birth' and 'Home' in required_field:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                if required_field == 'ENOAH' and (req_val == 'SS#' or req_val == 'Home Address' or req_val == 'Date Of Birth'):
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field =='ENOAH' and (req_val == 'Last Name' or req_val == 'Date Of Birth'):
                    required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == 'ENOAH' and (req_val == 'SS#' or req_val == 'Home Address'):
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+2])
                if required_field == 'ENOAH' and req_val == 'Owner':
                    required_field = str(df[column_name][i+2])
                if required_field == 'ENOAH' and req_val == 'Email':
                    required_field = str(df[column_name][i+2])
                break
    return required_field  

def Caddie_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i+2])
                break
            elif req_val == 'State':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+2])
                else:
                    required_field = str(df[column_name][i+3])
                break
            elif req_val == 'Zip':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+3])
                else:
                    required_field = str(df[column_name][i+4])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if req_val == 'Address' and 'United' in df[column_name][i+5]:
                    required_field = required_field+' '+str(df[column_name][i+1])
                break
    return required_field

def Caddie_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i+2])
                break
            elif req_val == 'State':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+2])
                else:
                    required_field = str(df[column_name][i+3])
                break
            elif req_val == 'Zip':
                if 'United' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+3])
                else:
                    required_field = str(df[column_name][i+4])
                break
            elif req_val == 'Owner':
                req = str(df[column_name][i])
                required_field = str(re.sub('[a-zA-Z:%*.,() /|]', '',req))
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if req_val == 'Address' and 'United' in df[column_name][i+5]:
                    required_field = required_field+' '+str(df[column_name][i+1])
                break
    return required_field

def Kay_Capital_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if required_field =='ENOAH':
                        required_field = str(df['Col_2'][i-1])
                        if 'cid:' in required_field:
                            required_field = str(df['Col_2'][i])
                break
            elif req_val == 'DBA':
                required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i-2])
                break
            elif req_val == 'Address':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
            elif req_val == 'StartDate':
                required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                break
            elif req_val == 'TaxID':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+3])
                            if required_field == 'ENOAH':
                                required_field = str(df[column_name][i-2])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = re.sub('[a-zA-Z:%,./]', '',req)
                if required_field == '':
                    req = str(df[column_name][i-1])
                    required_field = re.sub('[a-zA-Z:%,./]', '',req)
                break
            elif req_val == 'Phone':
                req = str(df[column_name][i+3])
                required_field = re.sub('[a-zA-Z:%,./]', '',req)
                if required_field == '':
                    required_field = str(df[column_name][i-2])
    return required_field
def Kay_Capital_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    if keyword == 'Ownership %:':
        required_field = 'ENOAH'
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i-1])
                required_field = re.sub('[a-zA-Z:%,./]', '',req)
                break
            elif req_val == 'Owner':
                req = str(df[column_name][i])
                required_field = re.sub('[a-zA-Z:%#,-/]', '',req)
                break
            elif req_val == 'Home Address':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if req_val == 'Home Address' and 'Owner Name:' in required_field:
                        required_field = str(df[column_name][i+1])
                    if req_val == 'Home Address' and 'Owner Name:' in str(df[column_name][i-2]) and 'State' in str(df[column_name][i+3]):
                        required_field = str(df[column_name][i+1])
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').strip()
                if required_field == '' or required_field ==' ':
                    required_field = str(df[column_name][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+1])
                    # if keyword == 'Owner Name:' and 'Address' in required_field:
                    #     required_field = str(df[column_name][i]).replace(keyword,'')
                break
    if req_val == 'City' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_1'][i]):
                a = str(df['Col_1'][i-1]).split()
                if len(a)==1:
                    required_field = a[0]
                break
    return required_field

def Loanability_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if (req_val =='Zip' or req_val =='Address') and required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
            break
    return required_field

def Loanability_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                required_field = str(df['Col_4'][i+1]).replace('City:','')
                if required_field == 'ENOAH':
                    required_field = str(df['Col_4'][i+2])
                break
            elif req_val =='SS#':
                required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+2])
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                break
    return required_field

def TLO_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('Zip:','').replace('Federal Tax ID:','').replace('Federal TaxID:','').replace('(MM/YYYY):','').replace('(MM/YYY):','').replace('(MM/YY):','').replace('MM/YYYY):','').replace(keyword,'')
            break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID:' in str(df['Col_3'][i]):
                mystring = str(df['Col_3'][i]).replace(' ID: ','')
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
                
    return required_field

def TLO_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('SSN:','').replace('SSN ','').replace(keyword,'')
            break
    return required_field

def Sunrise_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = df[column_name][i].replace(keyword,'').replace('LegaI','').replace('Legal','').strip()
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df['Col_2'][i-1]
                        if required_field == 'ENOAH':
                            required_field =df[column_name][i+1]
                break
            else:
                required_field=str(df[column_name][i]).replace('(no PO Boxes)','').replace(keyword,'')
                if required_field == '' or required_field == ' ':
                    required_field = str(df[column_name][i-1])
                    if required_field == 'ENOAH' and req_val =='Address':
                        required_field = str(df['Col_2'][i-1])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i-2])
                            if required_field == 'ENOAH':
                                required_field = str(df['Col_2'][i-2])
                break
    return required_field

def Sunrise_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('(no PO Boxes):','').replace(keyword,'')
            if required_field == '' or required_field == ' ':
                required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH' or req_val =='Home Address':
                    required_field = str(df['Col_2'][i-1])
                if required_field == 'ENOAH' and (req_val =='First Name' or req_val == 'Last Name'):
                    required_field = str(df[column_name][i-2])
            break
    return required_field

def BFA_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if required_field == 'ENOAH':
                required_field = str(df[column_name][i+2]).replace('Fax Number Cell Phone Number','ENOAH')
            if req_val == 'StartDate' and required_field != 'ENOAH':
                required_field = str(required_field).replace('/','')
                val = str(required_field)[:-4]
                value = str(required_field)[-4:]
                required_field = str(val)+'/'+str(value)
            break
    return required_field

def BFA_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH' or required_field == '%':
                    required_field = str(df[column_name][i+2])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
    if required_field != 'ENOAH' and req_val == 'Date Of Birth' and not 'Rout ing Nu' in str(required_field):
        dob = str(required_field).replace('/','')
        val_form = '%m%d%Y'
        d = datetime.strptime(dob.strip(), str(val_form))
        c = d.strftime('%m/%d/%Y')
        required_field = c
    return required_field

def Cast_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                value = str(df[column_name][i+1]).split()
                required_field = re.sub('[a-zA-Z:%.,/]',  '',value[-1])
                if not re.search('[0-9]',required_field):
                    val = str(df[column_name][i+1])[-12:]
                    required_field = re.sub('[a-zA-Z:%.,/]',  '',val)
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                break
    if req_val =='TaxID' and len(required_field)!=9:
        required_field = '0'+''+str(required_field)
    required_field = str(required_field).replace('Email:','').replace('Website:','').replace('Physical Address:','').replace('Have you received a business loan in the last 30 days?','').replace('Have you ever defaulted on a business loan?','')
    return required_field

def Cast_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if required_field == 'ENOAH':
                required_field =str(df[column_name][i+2])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+3])
                if 'Date of Birth:' in required_field and keyword =='Home Address:':
                    required_field = str(df['Col_2'][i+1])
                break
    if req_val == 'Zip':
        value = str(required_field).split()
        required_field = re.sub('[a-zA-Z:%.,/]',  '',value[-1])
    required_field = str(required_field).replace('Credit Score:','').replace('Home Address:','')
    return required_field

def ACT_Holdings_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('Street Address:','').replace('StreetAddress:','').replace(keyword,'')
            if 'Length of Ownership:' in required_field:
                a = 'Length'
                req = required_field.find(a)
                required_field =required_field[0:req]
            break
    if (req_val == 'Zip' or req_val == 'TaxID') and required_field != 'ENOAH':
        required_field = re.sub('[a-zA-Z:%.,#/]', '',required_field)
    return required_field

def ACT_Holdings_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i])
                required_field = re.sub('[a-zA-Z:%.,/]', '',req)
                break
            elif req_val == 'City':
                req = str(df[column_name][i]).replace(keyword,'')
                required_field = re.sub('[0-9:%.,/]', '',req)
                break 
            else:
                required_field = str(df[column_name][i]).replace('Date of Birth:','').replace('Dateof Birth:','').replace(keyword,'')
                break
    return required_field

def Genesis_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
            elif req_val == 'DBA' or req_val == 'Address':
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                if required_field == '' and not 'Primary' in str(df[column_name][i+1]):
                    required_field = str(df[column_name][i+1])
                break
            elif req_val == 'StartDate':
                required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+2])
                    
                break
            elif req_val == 'TaxID' or req_val == 'City' or req_val == 'State' or req_val == 'Zip':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field =str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                break
    return required_field
        
def Genesis_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            if required_field == '':
                required_field = str(df[column_name][i+1]).replace('City:','')
                if required_field == 'ENOAH' and req_val == 'Owner':
                    required_field = str(df['Col_4'][i+1])
                if required_field == 'ENOAH' and req_val == 'Zip':
                    required_field = str(df[column_name][i+2])
            break
    return required_field

def Lyft_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('lyft11.csv')
    for i in range(0,len(df)):
        if req_val == 'Zip' and ('zip' in str(df[column_name][i]) or 'zip' in str(df['Col_3'][i])):
            df[column_name][i] = str(df[column_name][i]).replace('zip','Zip')
            df['Col_3'][i] = str(df['Col_3'][i]).replace('zip','Zip')
        if req_val == 'Zip' and ('code' in str(df[column_name][i]) or 'code' in str(df['Col_3'][i])):
            df[column_name][i] = str(df[column_name][i]).replace('code','Code')
            df['Col_3'][i] = str(df['Col_3'][i]).replace('code','Code')
        if req_val == 'Name' and 'LegaI' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('LegaI','Legal')
        if req_val == 'Name' and 'LegalName' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('LegalName','Legal Name')
        if keyword == 'Code' and str(keyword) not in str(df[column_name][i]):
            print('server.....',str(df['Col_3'][i]))
            df[column_name][i] = str(df['Col_3'][i]).split('Zip')[-1]
            if keyword == 'Code' and required_field == '':
                print('Host...')
                required_field = str(df['Col_3'][i])
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').replace('|','').replace('Federal','')
            if required_field == '' and not 'Federal' in str(df[column_name][i+1]):
                print('friend...',df[column_name][i+1])
                required_field = str(df[column_name][i+1])
            

            if keyword == 'Code' and required_field == '':
                required_field = str(df['Col_3'][i])
            break
        # if str(keyword) not in str(df[column_name][i]):
        #     df[column_name][i] = str(df['Col_3'][i])
    return required_field

def Lyft_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('Captical22.csv')
    for i in range(0,len(df)):
        if req_val == 'Owner' and '0wnership' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('0wnership','Ownership')
        if req_val == 'Zip' and ('code' in str(df[column_name][i]) or 'code' in str(df['Col_4'][i])):
            df[column_name][i] = str(df[column_name][i]).replace('code','Code')
            df['Col_4'][i] = str(df['Col_4'][i]).replace('code','Code')
        if owner_status == 'Owner #1':
            if keyword == 'Code:' and str(keyword) not in str(df[column_name][i]):
                print('server11.....',str(df['Col_4'][i]))
                df[column_name][i] = str(df['Col_4'][i])
                if keyword == 'Code:' and required_field == '':
                    print('Host...')
                    required_field = str(df['Col_4'][i])
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('%ofOwnership','').replace('%of Ownership','').replace(keyword,'').replace(':','').replace('|','')
                print('tripp...',req_val,required_field)
                if req_val =='Date Of Birth' and required_field == '':
                    required_field = str(df[column_name][i-1])
                if req_val =='Last Name' and len((df['Col_1'][i]).split())<=2:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('%ofOwnership','').replace('%of Ownership','').replace(keyword,'').replace(':','').replace('|','')
                if req_val =='Date Of Birth' and required_field == '':
                    required_field = str(df[column_name][i-1])
                if req_val =='Home Address' and len(required_field)<3 and 'Name' not in str(df[column_name][i-1]) and df[column_name][i-1] != 'ENOAH':
                    required_field = str(df[column_name][i-1])
                print('kaaall',len((df['Col_1'][i]).split()))
                if req_val =='Last Name' and len((df['Col_1'][i]).split())<=2:
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])


    return required_field

def fundMate_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Address' or req_val == 'Zip':
                if str(df[column_name][i+1])!='ENOAH' and (str(df[column_name][i+2]) == 'ENOAH' or 'OWNER' in str(df[column_name][i+2]) or 'STREET' in str(df[column_name][i+2])):
                    required_field = str(df[column_name][i+1])
                elif str(df[column_name][i+1])=='ENOAH' and ('OWNER' in str(df[column_name][i+3]) or 'STREET' in str(df[column_name][i+3])):
                    required_field = str(df[column_name][i+2])
                else:
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])
                break
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field =str(df[column_name][i+2])
                break
                
    if req_val == 'Zip':
        req = required_field[-5:]
        req = re.sub('[a-zA-Z:%,/]',  '',req)
        if len(req)>=4:
            required_field = req
        else:
            required_field ='ENOAH'
    return required_field

def fundMate_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Home Address' or req_val == 'Zip':
                required_field = str(df[column_name][i+5])
                if required_field == 'STREET':
                    required_field = str(df['Col_2'][i+4])
                break
            else:
                required_field = str(df[column_name][i+1]).replace('(mm/dd/yyyy)','ENOAH')
                if required_field == 'ENOAH':
                    required_field =str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                break
    if req_val == 'Zip':
        req = required_field.split()[-1]
        if len(req)>=4:
            required_field = req
        else:
            required_field ='ENOAH'
    return required_field

def Redline_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if required_field == 'ENOAH' and not 'Email Address:' in str(df[column_name][i+2]):
                required_field = str(df[column_name][i+2])
                if (req_val == 'Zip' or req_val == 'TaxID') and required_field=='ENOAH':
                    required_field = str(df[column_name][i+3])
            if req_val == 'Name' and 'DBA:' in required_field:
                required_field = str(df['Col_2'][i+1])
            if req_val == 'DBA' and 'Email Address' in required_field:
                required_field = 'ENOAH'
            if req_val == 'Address' and 'Inc.' in required_field:
                required_field = required_field.split('Inc.')[1]
            if req_val == 'StartDate' and 'ENOAH' in required_field:
                req = str(df['Col_2'][i+1]).split()[-3:]
                required_field = (" ".join(req))
            break
    if req_val == 'Zip':
        req = required_field[-5:]
        required_field = re.sub('[a-zA-Z:%,/]',  '',req)
    return required_field

def Redline_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                if req_val == 'State':
                    required_field = required_field.split()[-2]
                if req_val == 'City':
                    req = required_field.split()[:-2]
                    required_field = (" ".join(req))
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
    if req_val == 'Zip':
        req = required_field[-5:]
        required_field = re.sub('[a-zA-Z:%,/]',  '',req)
    return required_field

def Go_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Company' or req_val == 'Tax':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i-1])
                break
            if req_val == 'Zip1':
                req = str(df[column_name][i])
                required_field = re.sub('[a-zA-Z:@.%,/]', '',req).strip()
                if required_field =='' or required_field == ' ':
                    required_field = re.sub('[a-zA-Z:%,/]', '',str(df[column_name][i+1]))
                break
            else:
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
    return required_field

def Go_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '' or (req_val == 'SSN' and '@' in required_field):
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH' or 'Mobile' in required_field or (req_val == 'SSN' and ' Email:' in required_field):
                        required_field = df[column_name][i+1]
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+1]
                
    if req_val == 'Zip':
        required_field = re.sub('[a-zA-Z:%,/]',  '',required_field)
    return required_field

def Meg_Logic_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Tax':
                required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+3])
                break
            elif req_val == 'Zip':
                req = str(df[column_name][i+1])
                required_field = re.sub('[a-zA-Z:%",/]', '',req)
                if required_field == '':
                    req = str(df[column_name][i+2])
                    required_field = re.sub('[a-zA-Z:%",/]', '',req)
                    if required_field == '' or required_field == 'ENOHA':
                        req = str(df[column_name][i+3])
                        required_field = re.sub('[a-zA-Z:%",/]', '',req)
                        if required_field == '' or required_field == 'ENOHA':
                            req = str(df[column_name][i+4])
                            required_field = re.sub('[a-zA-Z:%",/]', '',req)
                break
            elif req_val == 'State':
                req = str(df[column_name][i+1])
                required_field = re.sub('[0-9]', '',req)
                break
            else:
                required_field = str(df[column_name][i+1]).replace(',',' ').replace('.',' ')
                if req_val == 'Date' and ('Length' in required_field or 'years' in required_field):
                    required_field = str(df[column_name][i+2]).replace(',',' ').replace('.',' ')
                elif required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2]).replace('City:','')
                    if (req_val == 'Address' or req_val == 'Name') and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+4])
                break
    if req_val == 'Tax' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax' in str(df['Col_2'][i]):
                required_field = str(df["Col_3"][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df["Col_3"][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df["Col_3"][i+3])
                break
    if req_val == 'Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'ddress:' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i+2])
    return required_field

def Meg_Logic_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if (req_val == 'Owner' or req_val == 'Zip' or req_val == 'Date Of Birth' or req_val == 'First Name' or req_val == 'Last Name' or req_val == 'SSN' or req_val =='Home Address') and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+4])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
    return required_field

def Ascentium_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'StartDate':
                req = str(df['Col_3'][i])
                if req == 'ENOAH':
                    req = str(df['Col_3'][i+1])
                break
            required_field = str(df[column_name][i]).replace(keyword,'')
            if required_field == '':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH' and req_val == 'Name':
                    required_field = str(df['Col_2'][i+1])
            break
    if req_val == 'StartDate':
        given_years = req.replace('+','')  
        currentTimeDate = int(datetime.now().strftime('%Y'))
        required_field = (int(currentTimeDate)) - (int(given_years))  
    return required_field

def Ascentium_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val =='Zip' or req_val == 'Home Address':
                    req = str(df[column_name][i]).replace(keyword,'')
                    if req == '':
                        req = str(df['Col_2'][i])
                        if req == 'ENOAH':
                            req = str(df['Col_2'][i+1])
                    break
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i+1])
                    break
            else:
                if req_val =='Zip' or req_val == 'Home Address':
                    req = str(df[column_name][i]).replace(keyword,'')
                    if req == '':
                        req = str(df['Col_2'][i])
                        if req == 'ENOAH':
                            req = str(df['Col_2'][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i+1])
    if req_val == 'Zip':
        required_field = req.split()[-1]
    if req_val =='Home Address':
        required_field = req.split(',')[0]
    return required_field

def Qualifier_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('qualifier.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if keyword == 'Federal Tax ID' and required_field == 'ENOAH':#28/08
                for i in range (0,len(df)):
                    if 'Federal Tax ID' in str(df['Col_3'][i]):
                        required_field = df['Col_3'][i+1].replace(keyword,'')
            if keyword == 'Business Start Date' or req_val == 'StartDate':
                required_field = str(df['Col_4'][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_4'][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i+3])
            if keyword == 'Business Start Date' and 'Business Start Date' in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+3])
            if keyword == 'Zip' and 'Zip' in str(df['Col_4'][i]):
                print('testingqli......')
                required_field = str(df['Col_5'][i+1])

                if '/' in required_field:
                    if len(required_field)>10:
                        required_field_1 = str(required_field).split('/')[1]
                        required_field_2 = str(required_field_1)[-1]
                        required_field = (required_field).replace(str(required_field).split('/')[1],required_field_2)
            if required_field == 'ENOAH':
                row_index = i + 2
                if row_index < len(df): 
                    required_field = str(df[column_name][i+2]).replace('Website','')#19/09
                    if required_field == 'ENOAH' and (req_val == 'Address' or req_val == 'City'):
                        required_field = str(df[column_name][i+3])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+2])
                            if required_field == 'ENOAH':
                                required_field = str(df['Col_2'][i+3])
                                if required_field == 'ENOAH':
                                    required_field = str(df[column_name][i+4])

        # else:
        #     if keyword == 'Federal Tax ID' in str(df['Col_3'][i]):
        #         required_field = df['Col_3'][i+1]

            break
    return required_field

def Qualifier_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    
    required_field ='ENOAH'
    df.to_csv('qualifier11.csv')
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            # print('Keyword..................',keyword,str(df[column_name][i]))
            # if str(keyword) != str(df[column_name][i]):
            #     keyword = 'Owner First andLast Name'
        
            if str(keyword).replace(' ','') in str(df[column_name][i]).replace(' ',''):
                
                required_field = str(df[column_name][i+1])
                if keyword == 'DOB':
                    required_field = str(df['Col_2'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+3])
                if keyword == 'SSN' or req_val == 'SS#':
                    required_field = str(df['Col_1'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_1'][i+2])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_1'][i+3])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH' and (req_val == 'Home Address' or req_val == 'First Name' or req_val =='Last Name' or req_val == 'Zip'):
                        required_field = str(df[column_name][i+3])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH' and (req_val == 'Home Address' or req_val == 'First Name' or req_val =='Last Name'):
                        required_field = str(df[column_name][i+3])
    required_field = str(required_field).replace(',',' ').replace('Cell Phone','').replace('Email Address','')
    return required_field

def Click_Loan_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i]).replace(keyword,'')
                req = re.sub('[a-zA-Z:%,/]',  '',req[-6:])
                if re.search('[0-9]',str(req)) and len(req)>=4:
                    required_field = req
#                 else:
#                     req = str(df['Col_2'][i]) 
#                     req = re.sub('[a-zA-Z:%,/]',  '',req[-6:])
#                     if re.search('[0-9]',str(req)) and len(req)>=4:
#                         required_field = req
                break
            else:
                required_field = str(df[column_name][i]).replace('Federal Business ID#:','').replace('Federal Business ID #:','').replace(keyword,'')
                break
    return required_field

def Click_Loan_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('Zip:','').replace('zip:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i]).replace('Zip:','').replace('zip:','').replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
    return required_field

def Dealstruck_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
                if required_field == 'ENOAH' and req_val == 'TaxID':
                    required_field = str(df['Col_5'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH' and req_val == 'Address':
                    required_field = str(df['Col_2'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+2])
                if required_field == 'ENOAH' and req_val == 'Zip':
                    required_field = str(df['Col_4'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i+1])
            if req_val == 'DBA' and ('City' in required_field or required_field == 'ENOAH'):
                required_field = str(df['Col_4'][i+1])
            if req_val == 'Name' and 'Physical Address' in required_field:
                required_field = str(df['Col_2'][i+1])
            break
    if required_field == 'ENOAH' and req_val == 'StartDate':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i+1])
    if required_field == 'ENOAH' and req_val == 'TaxID':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i+1])
    return required_field

def Dealstruck_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if req_val == 'Home Address' and required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i+1])    
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                if req_val == 'Zip' and not re.search('[0-9]',str(required_field)):
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_5'][i+2])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                if req_val == 'Zip' and not re.search('[0-9]',str(required_field)):
                    required_field = str(df[column_name][i+2])
    return required_field

def Mayfair_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'DBA':
                required_field = str(df[column_name][i+1])
                break
            elif req_val == 'Address':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                break
            elif req_val == 'Zip':
                if 'ncorporation' not in str(df[column_name][i+2]):
                    req = str(df[column_name][i+2])
                else:
                    req = str(df[column_name][i+1])
                break
            else:
                required_field = str(df[column_name][i]).replace('Federal Tax ID:','').replace('FederalTaxID:','').replace(keyword,'')
                break
    if req_val == 'Zip':
        if '-' or 'USA' in req:
            required_field = re.sub('[a-zA-Z:%,./]',  '',req[-11:])
        else:
            required_field = re.sub('[a-zA-Z:%,./]',  '',req[-5:])
    return required_field

def Mayfair_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Zip':
                    if 'erchant' not in str(df[column_name][i+2]) and 'documents' not in str(df[column_name][i+2]):
                        req = str(df[column_name][i+2])
                    elif 'erchant' not in str(df[column_name][i+1]):
                        req = str(df[column_name][i+1])
                    else:
                        req = str(df[column_name][i]).replace(keyword,'')
                    break
                else:
                    required_field = str(df[column_name][i]).replace('Date of Birth:','').replace('Dateof Birth:','').replace('Home Address','').replace('HomeAddress','').replace('Name of Owner 1:','').replace('Name of Owner1:','').replace('Nameof Owner1:','').replace('% of 0wnership:','').replace('Name of Owner','').replace('% of0wnership:','').replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i+1])
                    break
        else:
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Zip':
                    if 'erchant' not in str(df[column_name][i+1]):
                        req = str(df[column_name][i+1])
                    elif required_field == 'ENOAH':
                        req = str(df[column_name][i]).replace(keyword,'')
                else:
                    required_field = str(df[column_name][i]).replace('Home Address','').replace('HomeAddress','').replace('Name of Owner 2:','').replace('Name of Owner2:','').replace('% of0wnership:','').replace(keyword,'')
                    if required_field == '':
                        required_field = str(df[column_name][i+1])
                        
    if owner_status == 'Owner #1' and req_val == 'SS#' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'ssn:' in str(df['Col_2'][i]):
                required_field = str(df['Col_2'][i]).replace('ssn:','')
    if req_val == 'Owner' and 'Mobile' in required_field:
        required_field = required_field.split('Mobile')[0]
    if req_val == 'Mobile' and ' 'in required_field:
        required_field = required_field.split(' ')[-1]    
        
    if req_val == 'Zip':
        if len(req) >= 11:
            required_field = re.sub('[a-zA-Z:%,./]',  '',req[-11:])
        else:
            required_field = re.sub('[a-zA-Z:%,./]',  '',req[-5:])
    return required_field

def Circle_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if (req_val == 'DBA' or req_val =='Name') and required_field =='':
                required_field = str(df[column_name][i+1]).replace(':','')
                
            if req_val == 'Zip' or req_val == 'TaxID' or req_val == 'Phone':
                required_field = str(re.sub('[a-zA-Z:%.,()& /]', '',required_field))
            break
    return required_field

def Circle_CapOwner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            if req_val == 'Owner' and required_field == '':
                required_field = str(df[column_name][i+1])
            if req_val == 'Zip' or req_val == 'SS#' or req_val == 'Mobile':
                required_field = str(re.sub('[a-zA-Z:%.,()& /]', '',required_field))
            break
    return required_field

def Creative_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    print('Prema...')
    for i in range(0,len(df)):
        df[column_name][i] = str(df[column_name][i]).replace('egal','Legal')
        # df[column_name][i] = str(df[column_name][i]).replace('ame','Name')
        # df[column_name][i] = str(df[column_name][i]).replace('Busines','Business')
        if str(keyword) in str(df[column_name][i]):
            print('century....',req_val,required_field)
            if req_val == 'Zip':
                required_field = str(df[column_name][i+1])
                required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
                if required_field == 'ENOAH' or required_field == '':
                    required_field = str(df[column_name][i+2])
                    required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
                    if required_field == 'ENOAH' or required_field == '':
                        required_field = str(df[column_name][i+3])
                        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
                print('century1',req_val,required_field)
            elif req_val == 'TaxID' and required_field == 'ENOAH':
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                print('century2',req_val,required_field)
            elif (req_val == 'Name' or req_val == 'StartDate') and required_field == 'ENOAH':
                print('century3',req_val,required_field)
                required_field = str(df[column_name][i]).replace(keyword,'').replace('Business','').replace('Busines','')
                break
            else:
                required_field = str(df[column_name][i+1]).replace(keyword,'')
                
                if required_field == 'ENOAH' and req_val == 'StartDate' and re.search('[0-9]',str(df['Col_3'][i+1])):
                    required_field = str(df['Col_3'][i+1])
                if required_field == 'ENOAH' and req_val != 'StartDate':
                    required_field = str(df[column_name][i+2])
                    if req_val == 'DBA' and required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                break
    return required_field

def Creative_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Email':
                    required_field = str(df['Col_4'][i+1])
                    if required_field == 'ENOAH' and '@' in str(df['Col_4'][i+2]):
                        required_field = str(df['Col_4'][i+2])
                    break
                elif req_val == 'City' or req_val == 'Zip':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field =='':
                        required_field = str(df[column_name][i+1]).replace(':','')
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+2]).replace(':','')
                    break
                # print('umesh',req_val,required_field)
                # if req_val == 'First Name' and required_field == 'ENOAH':
                #     print('msd',df['Col_1'][i],df['Col_1'][i+1])
                #     required_field = str(df['Col_1'][i]).replace(keyword,'').replace(':','').replace('Frt','')
                #     print('Virat',required_field)
                else:
                    required_field = str(df[column_name][i]).replace('State:','').replace(keyword,'').replace('Frt','').replace('First','').replace('Adress','').replace('Address','')
                    if req_val == 'Date Of Birth' and len(required_field.split(' '))>= 2:
                        required_field = str(required_field).split(' ')[0]
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2])
                        if (req_val == 'First Name' or req_val == 'Last Name' or req_val == 'SS#' or req_val == 'Home Address') and required_field == 'ENOAH':
                            required_field = str(df[column_name][i])
                    break
        else:
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Email':
                    required_field = str(df['Col_4'][i+1])
                elif req_val == 'City' or req_val == 'Zip':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field =='':
                        required_field = str(df[column_name][i+1]).replace(':','')
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+2]).replace(':','')
                else:
                    required_field = str(df[column_name][i+1]).replace('05/07/198:','05/07/1982').replace(':','')
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+2]).replace(':','')
                        if (req_val == 'First Name' or req_val == 'Last Name' or req_val == 'SS#' or req_val == 'Home Address') and required_field == 'ENOAH':
                            required_field = str(df[column_name][i+3])
                
    if req_val == 'Zip':
        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
    return required_field

def Reliable_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if required_field == '':
                required_field = str(df[column_name][i-1])
                if req_val == 'Name' and required_field =='ENOAH':
                    required_field = str(df[column_name][i-2])
            break
            
    if 'DocuSign' in required_field:
        required_field = 'ENOAH'
    return required_field

def Reliable_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('State','').replace(keyword,'').strip()
            if required_field =='':
                required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH' and (req_val == 'Home Address' or req_val == 'Email'):
                    required_field = str(df['Col_2'][i-1])
                if req_val == 'City' and required_field == 'ENOAH':
                    required_field = str(df[column_name][i-2])
                if req_val == 'State' and not re.search('[a-zA-Z]',str(required_field)):
                    required_field = str(df[column_name][i-2]) 
            break   
    if req_val == 'Zip':
        required_field = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
    if req_val == 'State':
        required_field = str(re.sub('[0-9:%.,() /]', '',required_field))
    return required_field

def America_Advances_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip' or req_val == 'StartDate':
                required_field = re.sub('[a-zA-Z:%,.]', '',str(df[column_name][i]))
                break
            elif req_val == 'State':
                required_field = re.sub('[0-9%.,() /]', '',str(df[column_name][i]))
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                break
    return required_field

def America_Advances_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = re.sub('[a-zA-Z:%,./]', '',str(df[column_name][i]))
                break
            elif req_val == 'State':
                required_field = re.sub('[0-9%.,() /]', '',str(df[column_name][i]))
                break
            else:
                required_field = str(df[column_name][i]).replace('Date of Birth:','').replace('Date of  Birth:','').replace(keyword,'')
                if req_val == 'SS#' and required_field == '':
                    required_field = str(df[column_name][i+1])
                break
    return required_field

def Bridge_Consol_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if keyword == 'Started:':
                required_field = df[column_name][i].replace(keyword,'')
                if required_field == '':
                    required_field = df[column_name][i-1].replace('Date Business','')
                break
            elif keyword == 'Federal Tax ID:':
                required_field = df[column_name][i+2]
                break
            else:    
                required_field = str(df[column_name][i+1]).replace('Telephone #:','').replace('Date Business','')
                if req_val == 'DBA' and required_field =='ENOAH':
                    required_field = str(df['Col_3'][i+1])
                break
    return required_field

def Bridge_Consol_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if keyword == 'Corporate Officer/Owner':
                    required_field = df['Col_2'][i+1]
                    break
                elif keyword == 'Ownership %:':
                    required_field = df[column_name][i+2]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+3]
                    break
                else:    
                    required_field = str(df[column_name][i+1]).replace('SSN:','').replace('Date of Birth:','')
                    break         
            else: 
                required_field = str(df[column_name][i+1]).replace('SSN:','').replace('Date of Birth:','')
    return required_field

def Newrock_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('Length of Ownership','')
            if required_field == '' and req_val == 'Address':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
            if required_field == '' and req_val == 'Date':
                required_field = str(df[column_name][i-1])
            elif required_field == '':
                required_field = str(df[column_name][i+1])
                if 'PHYSICAL ADDRESS' in required_field and req_val == 'Name':
                    required_field = str(df[column_name][i-1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i-1])
                    if req_val == 'Zip' and required_field == 'ENOAH':
                        required_field = str(df[column_name][i-2])   
            break
    if req_val == 'Date':
        if re.search('[0-9]',str(required_field)):
            required_field = datetime.strptime(required_field, "%m/%y").strftime("%m-%d-%Y")
        else:
            required_field = 'ENOAH'
    if req_val == 'Address' and ('City' in required_field[:4] or 'City' in required_field[-4:]):
        required_field = required_field.replace('City','')
    return required_field    

def Newrock_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if required_field =='' and req_val == 'Home Address':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
            elif required_field == '' and req_val != 'Home Address':
                required_field = str(df[column_name][i-1])
                if req_val == 'Owner' and not re.search('[0-9]',str(required_field)):
                    required_field = str(df[column_name][i+1])
            break
    if req_val == 'Home Address' and ('City' in required_field[:4] or 'City' in required_field[-4:]):
        required_field = required_field.replace('City','')
    return required_field

def Pristine_Factors_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i]).replace(keyword,'')
                print('hangover....',req)
                required_field = req.split()[-1]
                if '-' in req:
                    req = req.split('-')[0]
                    required_field = req.split()[-1]
                break
            elif req_val == 'Tax':
                required_field = str(re.sub('[a-zA-Z:%.#*,() /]', '',str(df[column_name][i])))
                if required_field == '':
                    required_field = str(re.sub('[a-zA-Z:%.#*,() /]', '',str(df[column_name][i+1])))
                    if required_field == '':
                        required_field = str(re.sub('[a-zA-Z:%.#*,() /]', '',str(df[column_name][i-1])))
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
                break
    if req_val == 'Name' and required_field != 'ENOAH':
        required_field = str(required_field).replace('Business','')
    if req_val == 'Address' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'BusinessAddress' in str(df['Col_1'][i]):
                required_field = df['Col_1'][i].replace('BusinessAddress','')
                break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'BusinessAddress' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).split()[-1]
                break
    return required_field
            
def Pristine_Factors_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
   
    for i in range(0,len(df)):
        if str(keyword).replace(' ','') in str(df[column_name][i]).replace(' ',''):
            if owner_status == 'Owner #1':
                print('malai',required_field)
                required_field = str(df[column_name][i]).replace(keyword,'').replace('Zip','')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if 'Section' in required_field:
                        required_field = str(df[column_name][i+1])
                if req_val == 'Zip':
                    
                    required_field = str(required_field).replace('City','').replace('State','').replace('ZIP','').replace(',','').replace(' ','')
                    print('tableeee',required_field)
                    required_field = str(required_field)[-5:]
                if req_val == 'City':
                    print('tableeee222',required_field)
                    required_field = str(required_field).replace('City','').replace('State','').replace('ZIP','').replace(',','')
                    required_field = str(required_field)[:-10]
                break
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if 'Section' in required_field:
                        required_field = str(df[column_name][i+1])
    if req_val == 'Zip':
        if not re.search('[a-zA-Z]',str(required_field).split()[-1]):
            required_field = required_field.split()[-1]
        else:
            required_field = 'ENOAH'
    return required_field

def Berkman_Financ_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('BerkmanB.csv')
    for i in range(0,len(df)):
        if req_val == 'Name' and 'LegaI' in str(df[column_name][i]):
            print('king.......')
            df[column_name][i] = str(df[column_name][i]).replace('LegaI','Legal')
        
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1]).replace('Landlord Name:','').replace(keyword,'ENOAH').replace('Billing Street Address','ENOAH')
            if req_val == 'StartDate' and required_field == 'ENOAH': ##27/06
                print('kurios...')
                required_field = str(df['Col_3'][i+1])
                if 'ENOAH' in str(required_field):#07/10
                    required_field = str(df['Col_2'][i+2])
                if 'ENOAH' in str(required_field):#08/10
                    required_field = str(df['Col_2'][i+3])
                if 'Website' in str(required_field):
                    required_field = str(df['Col_2'][i+2])
            if req_val == 'Name' and 'Business' in required_field:
                required_field = str(df[column_name][i]).replace(keyword,'')
            if required_field == 'ENOAH' and req_val == 'DBA':
                required_field = str(df['Col_4'][i+1])
                if required_field =='ENOAH':
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i+2])
            if required_field == 'Limited' and req_val == 'DBA':
                print('ioioioioio',required_field,(df['Col_3'][i]))
                required_field = str(df['Col_3'][i]).replace('Business DBA Name:','')
                print('ioioioioi11',required_field)
            if required_field == 'ENOAH' and req_val != 'DBA':
                required_field = str(df[column_name][i+2]).replace('(If different than above):','').replace('Type of Business','ENOAH').replace('Billing Street Address','ENOAH').replace('City:','ENOAH').replace('Zip Code:','ENOAH')
                if required_field == 'ENOAH' and (req_val == 'Name' or req_val == 'Address'):
                    required_field = str(df['Col_2'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+2])
                if required_field == 'ENOAH' and (req_val == 'TaxID' or req_val == 'City' or req_val == 'State' or req_val == 'Zip'):
                    required_field = str(df[column_name][i+3]).replace('City:','ENOAH').replace('Zip Code:','ENOAH')
                    if required_field == 'ENOAH' and req_val == 'TaxID':
                        required_field = str(df[column_name][i+4])
            break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Physical Street Address:' in str(df['Col_1'][i]):
                req = str(df["Col_1"][i+1])
                if req == 'ENOAH':
                    req = str(df["Col_2"][i+1])
                req = req.split()[-1]
                if re.search('[0-9]',str(req)):
                    required_field = req
                else:
                    required_field = 'ENOAH'
    return required_field

def Berkman_Financ_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('BerkmanO.csv')
    for i in range(0,len(df)):
        if req_val == 'SS#' and 'ss#' in str(df[column_name][i]):#24/09
            df[column_name][i] = str(df[column_name][i]).replace('ss#','SS#')
   
        if owner_status == 'Owner #1':
            
            if str(keyword) in str(df[column_name][i]):
                    
                if req_val == 'DOB':
                    required_field = str(df['Col_4'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i+2])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_4'][i+3])
                            if required_field == 'ENOAH':
                                required_field = str(df['Col_3'][i+1])
                else:
                    required_field = str(df[column_name][i+1])
                    
                    # if required_field == 'ENOAH' and req_val == 'City':
                    #     required_field = str(df['Col_3'][i]).replace('city:','')
                    # if required_field == 'ENOAH' and req_val == 'State':
                    #     required_field = str(df['Col_4'][i]).replace(str(keyword),'')
                    # if required_field == 'ENOAH' and req_val == 'Zip':
                    #     required_field = str(df['Col_5'][i]).replace(str(keyword),'')
                    if req_val == 'Last Name' and required_field == 'ENOAH':#25/06
                        
                        required_field = str(df['Col_4'][i]).replace(str(keyword),'')
                        print('kklklkl',required_field)
                        if required_field == '':
                            required_field = str(df['Col_4'][i+2]).replace(str(keyword),'')
                    if req_val == 'SS#' and required_field == 'ENOAH': ##27/06
                        required_field = str(df['Col_3'][i+1])
                        if required_field == 'ENOAH': ##27/06
                            required_field = str(df['Col_2'][i+2])
                            if required_field == 'ENOAH': ##27/06
                                required_field = str(df['Col_2'][i+3])

                    if req_val == 'Zip' and '-' in str(required_field):##24/06
                        required_field = str(required_field).split('-')[0]
                    if req_val == 'Zip' and not re.search(r'[0-9]', str(required_field)):
                        required_field = str(df['Col_5'][i+2])
                       
                        
                    if required_field == 'ENOAH' and req_val == 'Home Address' and 'By signing below' not in df['Col_2'][i+2]:
                        required_field = str(df['Col_2'][i+2])
                        print('Berkman...2',required_field)
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_3'][i+1])
                    if required_field == 'ENOAH' and (req_val != 'Email' or req_val != 'Home Address'):
                        required_field = str(df[column_name][i+2])
                        if required_field == 'ENOAH' and (req_val == 'City' or req_val == 'State' or req_val == 'Zip'):
                            required_field = str(df[column_name][i+3])
                break
        else:
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'DOB':
                    required_field = str(df['Col_4'][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_4'][i+2])
                else:
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH' and req_val == 'Home Address' and 'By signing below' not in df['Col_2'][i+2]:
                        required_field = str(df['Col_2'][i+2])
                        print('Berkman...1',required_field)
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_3'][i+1])
                    if required_field == 'ENOAH' and (req_val != 'Email' or req_val != 'Home Address'):
                        required_field = str(df[column_name][i+2])
                        # if required_field == 'ENOAH' and (req_val == 'City' or req_val == 'State' or req_val == 'Zip'):
                        #     required_field = str(df[column_name][i+3])
    if req_val == 'Zip':
        required_field = re.sub('[a-zA-Z:%,/]', '',required_field)
        if required_field == '':
            print('vela....')
            if (i + 2) < len(df[column_name]):
                print('Work....',str(df[column_name][i+2]))
                required_field = str(df[column_name][i+2])
    if req_val == 'State':
        required_field = re.sub('[0-9]', '',required_field)[:2]
    return required_field

def Big_Think_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                required_field = str(df['Col_2'][i-1])
            elif req_val == 'State':
                required_field = re.sub('[0-9]', '',str(df[column_name][i-1]))
            elif req_val == 'Zip':
                required_field = re.sub('[a-zA-Z:%,/]', '',str(df[column_name][i-1]))        
            elif req_val == 'Phone':
                required_field = str(df[column_name][i-1])
            else:
                required_field = str(df[column_name][i]).replace('Business Name:','').replace('BusineSs Name:','').replace('Federal Tax ID:','').replace('DBA Name:','').replace('DBAName:','').replace(keyword,'').strip()
                if required_field == '':
                    required_field = str(df[column_name][i-1]).replace('Business Name:','')
                    if req_val == 'DBA' and 's Name:' in required_field:
                        required_field  = 'ENOAH'
                    if req_val == 'Name' and required_field == 'ENOAH' and not 'Information' in str(df[column_name][i-2]):
                        required_field  = str(df[column_name][i-2])
            break
    if req_val == 'Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Address:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'').replace('Business Address:','')
                if required_field == '':
                    required_field = str(df["Col_2"][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_3'][i-1])
    if req_val == 'City' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'City:' in str(df['Col_2'][i]) and not 'Business Address' in str(df['Col_2'][i-1]):
                required_field = str(df["Col_2"][i-1])
            elif 'City:' in str(df['Col_2'][i]) and 'Business Address' in str(df['Col_2'][i-1]):
                required_field = str(df["Col_2"][i]).replace('City:','') 
                if 'State:' in required_field:
                    required_field = 'ENOAH'
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'City:' in str(df['Col_2'][i]):
                required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_3'][i-1]))
                if required_field == '':
                    required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_3'][i]))
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_3'][i]):
                required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_3'][i]))
                if required_field == '':
                    required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_3'][i-1]))
    if req_val == 'Phone' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Phone Number:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df["Col_2"][i-1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_3'][i])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_3'][i-1])
    return required_field

def Big_Think_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1' and (req_val == 'DOB' or req_val == 'SSN' or req_val == 'Owner' or req_val == 'Home Address' or req_val == 'City'):
                required_field = str(df['Col_1'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_1'][i-1]).replace('007/29','07/29')
                    if req_val == 'Home Address' and required_field == 'ENOAH':
                        required_field = str(df[column_name][i-1])
            elif owner_status == 'Owner #2' and (req_val == 'DOB' or req_val == 'SSN' or req_val == 'Owner' or req_val == 'Home Address' or req_val == 'City'):
                required_field = str(df['Col_3'][i])
                if required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i-1])
            elif keyword == 'Name:' or keyword == 'Cell Phone Number:' or keyword == 'Email Address:':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i-1])
                    if req_val == 'Email' and required_field == 'ENOAH':
                        required_field = str(df['Col_1'][i-1])
            elif req_val == 'State':
                required_field = re.sub('[0-9]', '',str(df[column_name][i-1]))
            elif req_val == 'Zip':
                required_field = re.sub('[a-zA-Z:%,/]', '',str(df[column_name][i-1]))
                if required_field == '':
                    required_field = re.sub('[a-zA-Z:%,/]', '',str(df[column_name][i]))
            break
            
    if keyword == 'Name:' and owner_status == 'Owner #1' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Name:' in str(df['Col_2'][i]):
                required_field = str(df["Col_1"][i]).replace(keyword,'').replace('Name:','')
                if required_field =='' or required_field == 'ENOAH':
                    required_field = str(df["Col_1"][i-1])
    if keyword == 'Name:' and owner_status == 'Owner #2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Name:' in str(df['Col_4'][i]):
                required_field = str(df["Col_3"][i]).replace(keyword,'').replace('Name:','')
                if required_field =='' or required_field == 'ENOAH':
                    required_field = str(df["Col_3"][i-1])
    if req_val == 'Owner' and owner_status == 'Owner #2' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Ownership:' in str(df['Col_3'][i]):
                required_field = str(df["Col_3"][i]).replace(keyword,'').replace('Ownership:','').replace('% Ownership:','')
                if required_field == '':
                    required_field = str(df["Col_3"][i-1])
    
    if (req_val == 'DOB' or req_val == 'SSN' or req_val == 'Owner' or req_val == 'Home Address' or req_val == 'City' or req_val == 'Zip' or req_val == 'Mobile' or req_val == 'Email') and owner_status == 'Owner #1' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_1'][i]):
                if req_val == 'Zip':
                    required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_2'][i-1]))
                    if required_field == '':
                        required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_2'][i]))
                        if required_field == '':
                            required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_2'][i+1]))
                    break                            
                else:
                    required_field = str(df['Col_1'][i]).replace(keyword,'')
                    if req_val == 'Home Address' and 'Email' in required_field:
                        required_field = str(df['Col_1'][i-4]).replace('Address:','').replace('AddreSs:','')
                        if 'Ownership' in required_field:
                            required_field = str(df['Col_1'][i-3]).replace('Address:','').replace('AddreSs:','')
                    if req_val == 'City' and 'State:' in required_field:
                        required_field = 'ENOAH'
                    if required_field == '' and (req_val == 'Mobile' or req_val == 'Email'):
                        required_field = str(df['Col_2'][i])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i-1])
                    elif required_field == '':
                        required_field = str(df['Col_1'][i-1])
                    break
            elif str(keyword) in str(df['Col_3'][i]) and req_val == 'Zip':
                required_field = re.sub('[a-zA-Z:%,/]', '',str(df['Col_2'][i]))

    if (req_val == 'Mobile' or req_val == 'Email' or req_val == 'SSN' or req_val == 'DOB' or req_val == 'Home Address' or req_val == 'Zip') and required_field == 'ENOAH' and owner_status == 'Owner #2':
        for i in range(0,len(df)):
            if str(keyword) in str(df['Col_3'][i]):
                required_field = str(df['Col_3'][i])+''+str(df['Col_4'][i]).replace(keyword,'')
                required_field = required_field.replace('Cell Phone Number:','').replace('ENOAH','').replace('Email Address:','').replace(keyword,'')
                if req_val == 'Zip':
                    required_field = re.sub('[a-zA-Z:%_,/]', '',str(required_field))
                if required_field == '' or required_field == 'ENOAH':
                    required_field = str(df['Col_4'][i-1])
                break
    return required_field

def _BigThink_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            if req_val == 'Name' and required_field == '':
                required_field = str(df['Col_2'][i])
            if req_val == 'Zip':
                required_field = re.findall('\d+', required_field)[0]
                if len(required_field) <= 2:
                    required_field = 'ENOAH'
                elif len(required_field) == 6:
                    required_field = str(required_field)[1:]
                else:
                    required_field = str(required_field)
            break
    if req_val == 'TaxID' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Federal Tax ID:' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i]).replace(keyword,'')
                break
    return required_field

def _BigThink_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'Address':
                    if 'Amount Requested' in str(df[column_name][i-2]):
                        required_field = str(df[column_name][i-1])+' '+str(df[column_name][i]).replace(keyword,'')
                    else:
                        required_field = str(df[column_name][i]).replace(keyword,'')
                elif req_val == 'Email':
                    required_field = str(df['Col_2'][i])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
                    if (req_val == 'SS#' or req_val == 'DOB') and '.' in str(required_field):
                        required_field = str(required_field).replace('.','')
                    if (req_val == 'SS#' or req_val == 'DOB') and required_field == '':
                        required_field = str(df[column_name][i-1]).replace('(Primary Owner)','')
                break
            else:
                if req_val == 'Address':
                    if 'Amount Requested' in str(df[column_name][i-2]):
                        required_field = str(df[column_name][i-1])+' '+str(df[column_name][i]).replace(keyword,'')
                    else:
                        required_field = str(df[column_name][i]).replace(keyword,'')
                elif req_val == 'Email':
                    required_field = str(df['Col_2'][i])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
    if req_val == 'First Name' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'First Name' in str(df['Col_1'][j]):
                if owner_status == 'Owner #1':
                    required_field = df['Col_1'][j].replace('First Name','').replace(':','')
                    break
                else:
                    required_field = df['Col_1'][j].replace('First Name','').replace(':','')
    if req_val == 'Last Name' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Last Name' in str(df['Col_2'][j]):
                if owner_status == 'Owner #1':
                    required_field = df['Col_2'][j].replace('Last Name','').replace(':','')
                    if required_field == '':
                        required_field = df['Col_3'][j]
                    break
                else:
                    required_field = df['Col_2'][j].replace('Last Name','').replace(':','')
                    if required_field == '':
                        required_field = df['Col_3'][j]
    if req_val == 'SS#' and required_field == 'ENOAH':
        for j in range(0,len(df)):
            if 'Cell:' in str(df['Col_3'][j]):
                required_field = str(re.sub('[a-zA-Z:%, .\|]', '',str(df['Col_3'][j]))).replace('-','')
                if len(required_field)==9:
                    required_field = required_field
                else:
                    required_field = 'ENOAH'
    return required_field

def Umbrella_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Address' or req_val == 'Name':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+1])
                        if required_field == 'ENOAH':
                            required_field = str(df['Col_2'][i+1])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
            break
    return required_field

def Umbrella_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('-','').replace(keyword,'')
            if required_field == '' and req_val != 'Home Address':
                required_field = str(df[column_name][i+1])
                if req_val == 'Last Name' and required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
            if req_val == 'Home Address' and required_field == '':
                required_field = str(df['Col_2'][i])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+1])
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i+1])
            if req_val == 'SSN':
                required_field = re.sub('[a-zA-Z:%,/]', '',required_field)
            break
    return required_field

def Express_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
            break
    if req_val == 'Zip':
        required_field = required_field.split(' ')[-1]
    if req_val == 'City':
        required_field = required_field.split(',')[0]
    if req_val == 'StartDate':
        given_years = int(required_field)
        currentTimeDate = int(datetime.now().strftime('%Y'))
        required_field = (int(currentTimeDate)) - given_years
    return required_field

def Express_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field ='ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                break
    if req_val == 'Zip':
        required_field = required_field.split(' ')[-1]
    if req_val == 'City':
        required_field = required_field.split(',')[0]
    return required_field

def SMB_Compass_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = str(re.sub('[a-zA-Z:%, /]', '',df[column_name][i]))
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def SMB_Compass_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DOB':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = str(before_keyword).strip().replace('_','').replace('Date of Birth:','')
            elif req_val == 'SSN':
                mystring = df[column_name][i]
                keyword = keyword
                before_keyword, keyword, after_keyword = mystring.partition(keyword)
                required_field = after_keyword.strip()
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def Madison_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name' or req_val == 'DBA' or req_val == 'TaxID' or req_val =='StartDate':
                required_field = str(df['Col_2'][i]).replace('Office Address Line 2','')
                if req_val == 'Name' and str(df['Col_1'][i+1])=='ENOAH':
                    required_field = str(df['Col_2'][i])+' '+str(df['Col_2'][i+1]).replace('ENOAH','')
                if req_val == 'DBA' and str(df['Col_1'][i+1])=='ENOAH':
                    required_field = str(df['Col_2'][i])+' '+str(df['Col_2'][i+1]).replace('ENOAH','')
            else:
                required_field = str(df['Col_4'][i])
            break
    if req_val =='Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Office Street Address' in str(df['Col_2'][i]):
                required_field = str(df['Col_4'][i])+' '+str(df['Col_4'][i+1])
                
    required_field = str(required_field).replace('Office Street Address','').replace('Office Address Line 2','').replace('ENOAH','')
    return required_field

def Madison_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'Home Address' or req_val =='City' or req_val == 'State':
                required_field = str(df['Col_2'][i]).replace('Ownership %','')
            else:
                required_field = str(df['Col_4'][i])
                if (req_val =='Owner' or req_val == 'Zip') and required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i]).replace('Ownership %','')
            break
    if req_val == 'Owner' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Ownership' in str(df['Col_2'][i]):
                required_field = str(df['Col_4'][i])
    return required_field

def Finest_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'StartDate':
                required_field = str(df[column_name][i]).replace(keyword,'')
                if required_field == '':
                    required_field = str(df[column_name][i+1])
            else:
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Finest_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('State Zip:','').replace('Date f Birth:','').replace('Date of Brth:','').replace('Date of Birth:','').replace(keyword,'')
            if required_field == '':
                required_field = str(df[column_name][i+1])
            break
    if req_val == 'Zip':
        required_field = required_field.split()[-1]
    return required_field

def Mazo_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if (req_val == 'TaxID' or req_val == 'StartDate') and required_field == '':
                required_field = str(df[column_name][i+1])
            break
    if req_val == 'StartDate':
        if len(required_field)<=2:
            given_years = required_field.strip()
            currentTimeDate = int(datetime.now().strftime('%Y'))
            required_field = (int(currentTimeDate)) - int(given_years)
        else:
            required_field = required_field
    return required_field

def Mazo_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('0wnership','').replace(keyword,'')
            if req_val == 'City':
                required_field = required_field.split('/')[0]
            break
    if req_val == 'Zip' and required_field == 'ENOAH' or required_field == '':
        for i in range(0,len(df)):
            if 'Zip:' in str(df['Col_2'][i]):
                required_field = re.sub('[a-zA-Z:%,.[ /]', '',str(df['Col_2'][i]))
    return required_field

def Specialized_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if req_val == 'Name' and 'Doing Business As:' not in str(df[column_name][i+1]):
                required_field = required_field+' '+str(df[column_name][i+1])
            if req_val == 'DBA' and 'Tax ID' not in str(df[column_name][i+1]):
                required_field = required_field+' '+str(df[column_name][i+1])
            break
    if req_val == 'Name' and 'First' in required_field:
        required_field = required_field.split('First')[0]
    return required_field

def Specialized_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            break
    if req_val == 'First Name' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'First Name:' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).split('First Name:')[1]
    return required_field

def Llama_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if req_val == 'Address':
                required_field = required_field.split(',')[0]
            elif req_val == 'City':
                required_field = required_field.split(',')[1]
            elif req_val == 'State':
                required_field = required_field.split(',')[2]
            elif req_val == 'Zip':
                required_field = required_field.split(',')[2:3]
                required_field = re.sub('[a-zA-Z:%,. /]', '',''.join(required_field))
                if required_field =='':
                    required_field = re.sub('[a-zA-Z:%,. /]', '',str(df[column_name][i+2]))
            break
    return required_field

def Llama_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if req_val == 'Home Address':
                required_field = required_field.split(',')[0]
            elif req_val == 'City':
                required_field = required_field.split(',')[1]
            elif req_val == 'State':
                required_field = required_field.split(',')[2]
            elif req_val == 'Zip':
                required_field = required_field.split(',')[2:3]
                required_field = re.sub('[a-zA-Z:%,. /]', '',''.join(required_field))
                if required_field =='':
                    required_field = re.sub('[a-zA-Z:%,. /]', '',str(df[column_name][i+2]))
            break
    return required_field

def Deer_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
                if req_val == 'DBA' and required_field == 'ENOAH':
                    required_field = str(df['Col_3'][i+1])
            break
    return required_field

def Deer_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
            if req_val == 'First Name' or req_val == 'Last Name' or req_val == 'DOB':
                required_field = str(df[column_name][i+1])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
                if required_field=='':
                    required_field = str(df[column_name][i+1])
            break
    return required_field

def FMS_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def FMS_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def FMS_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('("Merchant")','').replace('("Merchant)','').replace('City,','').replace(':','').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1]).replace(':','')
                if  req_val == 'DBA' and required_field == 'ENOAH' and not 'Zip' in str(df[column_name][i+2]):
                    required_field = str(df[column_name][i+2])
            elif req_val == 'Zip' and (required_field == 'ENOAH' or not re.search('[0-9]',str(required_field))):
                required_field = str(df['Col_1'][i][-6:])
            break
    return required_field

def FMS_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
            required_field = str(df[column_name][i]).replace(keyword,'').replace('City,','').replace('0wnership:','').replace(':','').strip()
            if required_field=='' :
                required_field = str(df[column_name][i+1])
            if not re.search('[0-9]',str(required_field)) and req_val == 'Zip':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Corner_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Phone' or req_val == 'TaxID' or req_val == 'StartDate':
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH' and req_val == 'StartDate':
                    required_field = str(df['Col_3'][i+1])
                elif 'Months Ago' in str(required_field) and req_val == 'StartDate':
                    required_field = str(df['Col_3'][i])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
                if req_val == 'DBA' and required_field == '':
                    required_field = str(df['Col_4'][i])
                elif req_val != 'DBA' and required_field == '':
                    required_field = str(df[column_name][i+1])
            break
    return required_field

def Corner_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Home Address' or req_val == 'Zip':
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
                if required_field=='':
                    required_field = str(df[column_name][i+1])
            else:
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Credible_Global_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Credible_Global_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]): 
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Fast_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('Business','').replace('("Merchant")','').replace('("Merchant)','').replace('("Merchant":','').replace(':','').replace('#','').strip()
            if required_field=='':
                required_field = str(df[column_name][i+1])
            break
    if req_val == 'City' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_2'][i]) and not re.search('[0-9]',str(df['Col_2'][i])):
                required_field = df['Col_2'][i].replace('City,','').replace('State','').replace('Zip','').replace(':','').strip()
                if ',' in required_field:
                    required_field = required_field.split(',')[0]
    if req_val == 'State' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Zip' in str(df['Col_2'][i]) and not re.search('[0-9]',str(df['Col_2'][i])):
                required_field = df['Col_2'][i].replace('City,','').replace('State','').replace('Zip','').replace(':','').strip()
                if ',' in required_field:
                    required_field = required_field.split(',')[1]
    return required_field

def Fast_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = re.sub("[a-zA-Z/:,_.]",'',str(df[column_name][i])).strip()
                if required_field == '':
                    required_field = str(df[column_name][i+1])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').replace('#','').strip()
                if required_field=='':
                    required_field = str(df[column_name][i+1]).replace(':','')
            break
    return required_field

def Select_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df['Col_2'][i])
            if required_field=='ENOAH':
                required_field = str(df[column_name][i+1]).replace('Gross Annual Sales','').replace('Company Phone','')
            break
    return required_field

def Select_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'DOB' and str(df['Col_2'][i])!='ENOAH':
                if 'SSN' in str(df[column_name][i+3]):
                    required_field = (str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])+' '+str(df['Col_2'][i+2]))
                elif 'SSN' in str(df[column_name][i+2]):
                    required_field = (str(df['Col_2'][i])+' '+str(df['Col_2'][i+1])).replace('  ',' ')
                elif 'SSN' in str(df[column_name][i+1]):
                    required_field = str(df['Col_2'][i]) 
            elif req_val == 'DOB' and str(df['Col_2'][i])=='ENOAH':
                if 'SSN' in str(df[column_name][i+4]):
                    required_field = (str(df['Col_1'][i+1])+' '+str(df['Col_1'][i+2])+' '+str(df['Col_1'][i+3]))
                elif 'SSN' in str(df[column_name][i+3]):
                    required_field = (str(df['Col_1'][i+1])+' '+str(df['Col_1'][i+2])).replace('  ',' ')
                elif 'SSN' in str(df[column_name][i+2]):
                    required_field = str(df['Col_1'][i+1]).replace('  ',' ')
            else:    
                required_field = str(df['Col_2'][i])
                if required_field=='ENOAH':
                    required_field = str(df[column_name][i+1])
            break
    return required_field

def Simply_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
            break
    return required_field

def Simply_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').strip()
            break
    return required_field

def Hybrid_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if '|' in str(required_field)[0] or ('1'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('L'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('I'in str(required_field)[0] and ' 'in str(required_field)[1]):
                required_field = required_field[1:]
            if required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
            if req_val =='Name' and 'L' in str(required_field)[0]:
                required_field = str(required_field)[1:]
            break
    return required_field

def Hybrid_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if '|' in str(required_field)[0] or ('1'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('L'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('l'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('I'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('J'in str(required_field)[0] and ' 'in str(required_field)[1]):
                required_field = required_field[1:]
            if required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
                if '|' in str(required_field)[0] or ('1'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('L'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('l'in str(required_field)[0] and ' 'in str(required_field)[1]) or ('I'in str(required_field)[0] and ' 'in str(required_field)[1]):
                    required_field = required_field[1:]
                if req_val == 'Zip' and required_field == 'ENOAH':
                    required_field = str(df['Col_1'][i-1])[-5:]
            if (req_val == 'City' or req_val == 'Home Address') and 'I' in str(required_field)[0]:
                required_field = str(required_field)[1:]
            break
    return required_field

def Fundworks_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    df.to_csv('fundworks.csv')
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'StartDate':
                required_field = str(re.sub('[a-zA-Z:%, ]', '',str(df[column_name][i])))
                if required_field == '':
                    required_field = str(re.sub('[a-zA-Z:%, ]', '',str(df[column_name][i-1])))
            else:
                required_field = str(df[column_name][i]).replace('StateZip','').replace('StateZip','').replace(keyword,'').replace('.','').strip()
                if (req_val == 'Zip' or req_val == 'DBA' or req_val == 'Address') and required_field == '':
                    required_field = str(df[column_name][i-1])
            break
    return required_field

def Fundworks_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('StateZip','').replace(keyword,'').replace('.','').strip()
            if req_val == 'Zip' and required_field == '':
                required_field = str(df[column_name][i-1])
            break
    return required_field

def Creditfy_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def Creditfy_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'First Name' or req_val == 'DOB' or req_val == 'Mobile':
                    required_field = str(df['Col_1'][i])
                elif req_val == 'Last Name' or req_val == 'SSN' or req_val == 'Owner':
                    required_field = str(df[column_name][i]).replace(keyword,'').replace('First Name:','').replace('(MM/DD/YY):','').replace('Cell Phone:','')
                elif req_val == 'Home Address' or req_val == 'Email':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df['Col_2'][i])
                elif req_val == 'Zip':
                    required_field = required_field = str(df['Col_2'][i])
                break
            else:
                if req_val == 'First Name' or req_val == 'DOB' or req_val == 'Mobile':
                    required_field = str(df['Col_3'][i])
                elif req_val == 'Last Name' or req_val == 'SSN' or req_val == 'Owner':
                    required_field = str(df[column_name][i]).replace(keyword,'').replace('First Name:','').replace('(MM/DD/YY):','').replace('Cell Phone:','')
                elif req_val == 'Home Address' or req_val == 'Email':
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if required_field == '':
                        required_field = str(df['Col_4'][i])
                elif req_val == 'Zip':
                    required_field = required_field = str(df['Col_4'][i])
                break
    return required_field

def Abstract_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('Federal TaxID:','').replace('Federal Tax ID','').replace(keyword,'').replace(':','').replace('State','')
            print('amar....',req_val,required_field)
            if req_val == 'TaxID' and required_field =='TaxID':
                required_field = str(df[column_name][i+1])
            if req_val == 'Name' and required_field == 'ENOAH':
                required_field = str(df['Col_2'][i])
                if required_field == '':
                    required_field = str(df['Col_1'][i+1])
            if req_val == 'Zip' and required_field == '.':
                required_field = str(df['Col_3'][i+1])
                if required_field == '':
                    required_field = str(df['Col_3'][i+2])
            if required_field == '':
                required_field = str(df[column_name][i+1]).replace('Desired loan amount:','')
                if req_val == 'DBA' and 'State' in str(df['Col_3'][i+1]):
                    required_field = 'ENOAH'
                if req_val == 'Name' and ('Address' in required_field or required_field == 'ENOAH'):
                    required_field = str(df['Col_2'][i+1])
            break
    return required_field

def Abstract_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').replace('%','').replace('Name','').replace('Corporate','').replace('Office/Owner','').strip()
            if required_field == '':
                required_field = str(df[column_name][i+1]).replace('N/A','')
            break
    return required_field

def DiTommaso_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('|','')
            break
    return required_field

def DiTommaso_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if owner_status == 'Owner #1':
            if str(keyword) in str(df[column_name][i]):
                if req_val == 'Last Name' or req_val == 'Home Address':
                    required_field = str(df['Col_2'][i])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if req_val == 'First Name' and required_field == '':
                        required_field = str(df['Col_2'][i])
                break
        else:
            if req_val == 'Last Name' or req_val == 'Home Address':
                required_field = str(df['Col_4'][i])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    return required_field

def Simplified_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            if req_val == 'Name' and 'Entity' in required_field:
                required_field = str(df[column_name][i]).replace('Business Legal Name:','')
            elif req_val == 'Name' and required_field == 'ENOAH':
                required_field = str(df[column_name][i+2])
            if req_val == 'DBA' and 'Tax' in required_field:
                required_field = str(df[column_name][i]).replace('Business DBA Name:','')
            elif req_val == 'DBA' and required_field == 'ENOAH':
                required_field = str(df['Col_4'][i+1])
            break
    return required_field

def Simplified_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1])
            break
    return required_field

def Affinity_new_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'State':
                req = str(df[column_name][i+1]).split(',')
                required_field = req[-2]
            elif req_val == 'City':
                req = str(df[column_name][i+1]).split(',')
                required_field = req[-3]
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if req_val == 'DBA' and required_field == 'ENOAH':
                        required_field = df['Col_3'][i+1]
            break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'CITY, STATE' in str(df['Col_2'][i]):
                required_field = df['Col_2'][i+1]
                if required_field == 'ENOAH' or not re.search('[0-9]',str(required_field)):
                    required_field = df['Col_3'][i+1]
                break
    return required_field

def Affinity_new_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i+1])
                if ',' in str(req):
                    required_field = req.split(',')[-1]
                else:
                    required_field = 'ENOAH'     
            elif req_val == 'State':
                req = str(df[column_name][i+1])
                if ',' in str(req):
                    required_field = req.split(',')[-2]
                else:
                    required_field = 'ENOAH'
            elif req_val == 'City':
                req = str(df[column_name][i+1])
                if ',' in str(req):
                    required_field = req.split(',')[-3]
                else:
                    required_field = str(df['Col_3'][i+1])
            else:
                required_field = str(df[column_name][i+1])
                if required_field == 'ENOAH':
                    required_field = str(df[column_name][i+2])
                    if required_field == 'ENOAH':
                        required_field = str(df[column_name][i+3])
                        if required_field == 'ENOAH':
                            required_field = str(df[column_name][i+4])
            break
    if req_val == 'Zip' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'CITY, STATE' in str(df['Col_3'][i]):
                required_field = df['Col_3'][i+1]
                if required_field == 'ENOAH' or not re.search('[0-9]',str(required_field)):
                    required_field = df['Col_4'][i+1]
    return required_field

def Snap_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                if 'Address'in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])
                elif 'Address'in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])
                elif 'Address'in str(df[column_name][i+5]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])+' '+str(df[column_name][i+4])
                else:
                     required_field = str(df[column_name][i+1])
            elif req_val == 'Address':
                if 'Zip:'in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])
                elif 'Zip:'in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])
                else:
                     required_field = str(df[column_name][i+1])        
            elif req_val == 'TaxID' or req_val == 'Phone':
                required_field = str(df[column_name][i+1])
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    required_field = str(required_field).replace('name:','')
    return required_field

def Snap_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Home Address':
                if 'Zip:'in str(df[column_name][i+2]):
                    required_field = str(df[column_name][i+1])
                elif 'Zip:'in str(df[column_name][i+3]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])
                elif 'Zip:' in str(df[column_name][i+4]):
                    required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])
            elif req_val == 'DOB':
                required_field = str(df[column_name][i+1])
            elif req_val == 'Last Name':
                if 'Home' not in str(df[column_name][i+1]):
                    required_field = str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
                if 'Home' in required_field or'Address' in required_field:
                    required_field = str(required_field).split('Home')[0]
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
                if keyword == 'Name:' and ('Home' in required_field or 'Address' in required_field):
                    required_field = str(required_field).split('Home')[0]
            break
    if req_val == 'Mobile' and required_field == 'ENOAH':
        for i in range (0,len(df)):
            if 'Phone:' in str(df['Col_1'][i]):
                required_field = str(df['Col_1'][i]).split('Phone:')[1]
                break
    return required_field

def AFG_August_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i+1]).replace('Business Address','')
            break
    return required_field
def AFG_August_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val =='DOB':
                required_field = str(df[column_name][i+1])+' '+str(df[column_name][i+2])+' '+str(df[column_name][i+3])
            else:
                required_field = str(df[column_name][i+1])
            break
    return required_field

def IOU_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Address':
                required_field = str(df['Col_1'][i+1])+' '+str(df[column_name][i+1])
            else:
                required_field = df[column_name][i+1]
                if required_field == 'ENOAH':
                    required_field = df[column_name][i+2]
            break
    if req_val == 'StartDate' and required_field != 'ENOAH':
        given_years = required_field               
        currentTimeDate = int(datetime.now().strftime('%Y'))
        required_field = (int(currentTimeDate)) - (int(given_years)) 
    return required_field

def IOU_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'Home Address':
                    required_field = str(df['Col_1'][i+1])+' '+str(df[column_name][i+1])
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
                break
            else:
                if req_val == 'Home Address':
                    required_field = str(df['Col_1'][i+1])+' '+str(df[column_name][i+1])
                else:
                    required_field = df[column_name][i+1]
                    if required_field == 'ENOAH':
                        required_field = df[column_name][i+2]
    return required_field

def Dynamic_Fund_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Name':
                if 'Mailing' not in str(df[column_name][i+1]):   
                    required_field = str(df[column_name][i]).replace(keyword,'')+' '+str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
            elif req_val == 'DBA':
                if 'Street' not in str(df[column_name][i+1]):   
                    required_field = str(df[column_name][i]).replace(keyword,'')+' '+str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
            elif req_val == 'Phone':
                if 'Email' not in str(df[column_name][i+1]):   
                    required_field = str(df[column_name][i]).replace(keyword,'')+' '+str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
            elif req_val == 'Address':
                if 'State' not in str(df[column_name][i+1]):
                    required_field = str(df[column_name][i]).replace(keyword,'')+' '+str(df[column_name][i+1])
                else:
                    required_field = str(df[column_name][i]).replace(keyword,'')
            elif req_val == 'Zip':
                required_field = str(re.sub('[a-zA-Z:%, /]', '',str(df[column_name][i])))
            else:
                 required_field = str(df[column_name][i]).replace(keyword,'')
            break
    if req_val == 'Address' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Business Street Address:' in str(df['Col_2'][i]):
                if 'State' not in str(df["Col_1"][i+1]):
                    required_field = str(df["Col_2"][i]).replace('Business Street Address:','')+' '+str(df["Col_1"][i+1])
                else:
                    required_field = str(df["Col_2"][i]).replace('Business Street Address:','')
                break
    required_field = str(required_field).replace('ENOAH','')
    return required_field

def Dynamic_Fund_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = str(re.sub('[a-zA-Z:%, /]', '',str(df[column_name][i])))
            else:
                required_field = df[column_name][i].replace(keyword,'')
            break
    return required_field

def Goldstone_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i+1])
                required_field =  re.sub('[a-zA-Z.,& :]', '', req)
                if required_field == '':
                    required_field = str(df[column_name][i-1]).split()[-1]
            else:
                required_field = str(df[column_name][i+1])
                if req_val == 'Address' and '-' in required_field:
                    required_field = str(required_field).split('-')[0]
            break
    return required_field

def Goldstone_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                req = str(df[column_name][i])
                required_field =  re.sub('[a-zA-Z.,& :]', '', req)
                if required_field == '':
                    required_field = str(df[column_name][i-1]).split()[-1]
                    if required_field == 'ENOAH':
                        required_field = str(df['Col_2'][i-1]).split()[-1]
            else:
                required_field = str(df[column_name][i]).replace(keyword,'')
            break
    if req_val == 'Home Address' and required_field == 'ENOAH' and owner_status == 'Owner #1':
        for i in range(0,len(df)):
            if 'Address:' in str(df['Col_2'][i]):
                required_field = str(df["Col_2"][i]).replace('Address:','')
                break
    if req_val == 'Home Address' and required_field == 'ENOAH' and owner_status == 'Owner #2':
        for i in range(0,len(df)):
            if 'Address:' in str(df['Col_4'][i]):
                required_field = str(df["Col_4"][i]).replace('Address:','')
                break
    if req_val == 'Zip' and required_field == 'ENOAH' and owner_status == 'Owner #2':
        for i in range(0,len(df)):
            if 'Address:' in str(df['Col_4'][i]):
                req = str(df["Col_4"][i]).replace('Address:','')
                required_field = req.split()[-1]
                break
    return required_field

def Capitalworks_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
  
    for i in range(0,len(df)):
        if 'ZIP' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('ZIP','Zip')    
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('(MM/YYYY)','').replace('(MM/YYY)','').replace('MM/YYYY):','').replace('Suite/FI:','').replace('Suite/FL:','').replace(keyword,'').replace(':','')
            break
    return required_field

def Capitalworks_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    
    for i in range(0,len(df)):
        if 'ZIP' in str(df[column_name][i]):
            df[column_name][i] = str(df[column_name][i]).replace('ZIP','Zip')
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','')
            break
    return required_field

def Citi_Cap_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df['Col_2'][i]).replace('Years','').replace('years','')
            if req_val == 'StartDate' and required_field == 'ENOAH':
                required_field = str(df[column_name][i]).replace(keyword,'').replace('Years','').replace('years','')
            break
    if req_val == 'StartDate':
        given_years = required_field          
        currentTimeDate = int(datetime.now().strftime('%Y'))
        required_field = (int(currentTimeDate)) - (int(given_years)) 
    return required_field

def Citi_Cap_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df['Col_2'][i])
            break
    return required_field

def Corporate_Trust_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('73452','73852').replace('Business DBA Name','').replace('Fax:','').replace("(“Merchant”):",'').replace(keyword,'').replace(':','')
            break
    return required_field

def Corporate_Trust_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('73452','73852').replace(keyword,'')
            break
    return required_field

def Nationwide_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('Date Business Started:',"")
            if req_val == 'Name' and required_field == '':
                required_field = str(df['Col_2'][i])
            if req_val == 'City' and required_field == '':
                required_field = str(df['Col_3'][i])
            if req_val == 'StartDate' and required_field == '':
                required_field = str(df[column_name][i+1]).replace('Month:','').replace('Year:','')
            break
    return required_field

def Nationwide_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if owner_status == 'Owner #1':
                if req_val == 'DOB':
                    required_field = str(df['Col_2'][i]).replace("D(cid:75)(cid:17):",'')
                else: 
                    required_field = str(df[column_name][i]).replace(keyword,'')
                    if (keyword == 'Applicant:' or keyword == 'Ownership % :') and required_field == '':
                        required_field = str(df[column_name][i+1])
                break
            else:
                if req_val == 'DOB':
                    required_field = str(df['Col_2'][i]).replace("D(cid:75)(cid:17):",'')
                else: 
                    required_field = str(df[column_name][i]).replace(keyword,'')
    return required_field

def Lendeavor_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('Business Physical','').replace('#','').replace(':','')
            if required_field == '':
                required_field = str(df[column_name][i+1])
            break
    return required_field

def Lendeavor_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('#','').replace(':','')
            if required_field == '':
                required_field = str(df[column_name][i+1]).replace('Mobile Phone:','')
                if req_val == 'Email' and (required_field == 'ENOAH' or required_field == ''):
                    required_field = str(df['Col_4'][i])
                    if required_field == 'ENOAH':
                     required_field = str(df['Col_4'][i+1])
                elif req_val == 'Home Address' and required_field == 'ENOAH':
                    required_field = str(df['Col_2'][i])
                    if required_field == 'ENOAH':
                     required_field = str(df['Col_2'][i+1])
            break
    return required_field

def Gotham_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'')
            if (req_val == 'Name' or req_val == 'Address') and required_field == '':
               required_field =  str(df['Col_2'][i])
            break
    if req_val == 'StartDate':
        given_years = required_field
        if re.search('[0-9]',str(given_years)):
            given_years = re.findall('\d+', given_years)[0]      
            currentTimeDate = int(datetime.now().strftime('%Y'))
            required_field = (int(currentTimeDate)) - (int(given_years))
        else:
            requried_field = 'ENOAH'
    if req_val == 'DBA' and required_field == 'ENOAH':
        for i in range(0,len(df)):
            if 'Corporation Name:' in str(df['Col_1'][i]):
                required_field = str(df["Col_1"][i-1]).replace('The Business DBA Name:','').replace(keyword,'')
                if required_field == 'ENOAH' or required_field =='':
                    required_field = str(df["Col_2"][i-1])
                break
    return required_field

def Gotham_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace(':','').replace('#','')
            break
    return required_field

def Fundall_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace('Physical Address','').replace('(Under Current Owner)','').replace('[','').replace(']','').replace(keyword,'')
            required_field = str(re.sub("[:%.,()+?_$#|`]", '',required_field))
            if req_val == 'Zip':
                required_field = str(required_field).split('Cod')[1]
            elif req_val == 'Phone':
                required_field = str(required_field).split('Phone')[0]
            break
    return required_field

def Fundall_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(df[column_name][i]).replace(keyword,'').replace('[','').replace(']','')
            required_field = str(re.sub("[:%.,()+?_$#|`]", '',str(required_field)))
            if req_val == 'Last Name' and required_field == '':
                required_field = str(df['Col_3'][i])
            break
    return required_field

def Simmons_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace('city:','').replace('City:','').replace('Physical Adress','').replace('Physical Address','').replace('Physical Adres:','').replace(keyword,'').replace(':','')
            if req_val == 'StartDate' :
                required_field = str(required_field).replace('Month:','').replace('Day:','').replace('Year:','')
                if required_field == '':
                    required_field = str(df[column_name][i+1]).replace('Month:','').replace('Day:','').replace('Year:','')
            break
    return required_field

def Simmons_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'Zip':
                required_field = df[column_name][i-1]
            else:
                required_field = df[column_name][i].replace('city:','').replace('City:','').replace(keyword,'').replace(':','')
                if (req_val == 'DOB' or req_val == 'SS#' or req_val == 'Mobile' or req_val == 'Home Address') and required_field == '':
                    required_field = df[column_name][i+1]
            break                  
    return required_field

def Sapphire_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = str(re.sub('[:%|]', '',str(df[column_name][i+1])))
            break
    return required_field

def Sapphire_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'First Name' or req_val == 'Last Name':
                required_field = df[column_name][i-1]
            else:
                required_field = str(re.sub('[:%|]', '',str(df[column_name][i+1])))
            break
    if req_val == 'Owner':
        required_field = '100'
    return required_field

def Paramount_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'')
            if req_val == 'Name':
                required_field = str(required_field).replace('BusineSS','')
                required_field = str(required_field).replace('Business','')
                            
            if req_val == 'StartDate':
                given_years = str(re.sub('[a-zA-Z:%.,() /]', '',required_field))
                currentTimeDate = int(datetime.now().strftime('%Y'))
                required_field = (int(currentTimeDate)) - int(given_years)
            break
    return required_field

def Paramount_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if req_val == 'DOB' and keyword == 'Date of Birth:':
            df[column_name][i] = str(df[column_name][i]).replace('Date 0f Birth:','Date of Birth:')
            
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace(keyword,'').replace('Zip:','')
            if req_val == 'Mobile' and 'SSN' in required_field:
                required_field = str(required_field).split('SSN')[0]
            break
    
    if req_val == 'SS#' and required_field == 'ENOAH' and owner_status == 'Owner #1':
        for j in range(0,len(df)):
            if 'SSN#:' in str(df['Col_1'][j]):
                required_field = str(df['Col_1'][j]).split('SSN#:')[1]
                break
    if req_val == 'SS#' and required_field == 'ENOAH' and owner_status == 'Owner #2':
        for j in range(0,len(df)):
            if 'SSN#:' in str(df['Col_3'][j]):
                required_field = str(df['Col_3'][j]).split('SSN#:')[1]
                break
    return required_field

def Paramount_Fin_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
   
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df['Col_2'][i]
            if req_val == 'StartDate':
                given_years = str(required_field)
                currentTimeDate = int(datetime.now().strftime('%Y'))
                required_field = (int(currentTimeDate)) - int(given_years)
            break
    return required_field

def Paramount_Fin_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df['Col_2'][i]
            break
    return required_field

def Blackstone_Buss(df,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            required_field = df[column_name][i].replace("(\u201cMerchant\u201d)",'').replace(':','').replace(keyword,'')
            break
    return required_field

def Blackstone_Owner(df,owner_status,keyword,column_name,req_val):
    keyword = keyword.strip()
    required_field = 'ENOAH'
    for i in range(0,len(df)):
        if str(keyword) in str(df[column_name][i]):
            if req_val == 'City':
                required_field = str(df[column_name][i]).split(',')[0]
            elif req_val == 'State':
                required_field = str(df[column_name][i]).split(',')[1]
            elif req_val == 'Zip':
                required_field = str(df[column_name][i]).split(',')[2]
            else:
                required_field = df[column_name][i].replace(keyword,'')
            break
    return required_field

def Crest_Buss(df, keyword, column_name, req_val):
    keyword = keyword.strip().lower()
    required_field = "ENOAH"
   
    for i in range(len(df)):
        if keyword in str(df.loc[i, 'Col_1']).lower():
            value = str(df.loc[i, 'Col_2']).strip()
            next_value = str(df.loc[i+1, 'Col_2']).strip() if i+1 < len(df) else ""
            print('next_value',next_value)
            if req_val in ['City', 'State', 'Zip']: 
                full_address = f"{value} {next_value}".strip()
                print('misple',full_address)
                match = re.search(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*(\d{5})', full_address)
                if match:
                    city, state, zip_code = match.groups()
                    print('unnavida',city, state, zip_code)
                    if req_val == "City":
                        required_field = city.strip()
                    elif req_val == "State":
                        required_field = state.strip()
                    elif req_val == "Zip":
                        required_field = zip_code.strip()
                else:
                    required_field = "ENOAH"
            else:
                required_field = value if pd.notna(df.loc[i, 'Col_2']) else "ENOAH"
            break  
    return required_field



def Crest_Owner(df, owner_status, keyword, column_name, req_val):
    keyword = keyword.strip().lower()
    required_field = "ENOAH" 
    for i in range(len(df)):
        if keyword in str(df.loc[i, column_name]).lower():
            current_line = str(df.loc[i, 'Col_2']).strip()
            next_line = str(df.loc[i+1, 'Col_2']).strip() if i+1 < len(df) else ""
            print('flow...',current_line,next_line)
            if req_val in ['City', 'State', 'Zip']: 
                full_address = f"{current_line} {next_line}".strip()
                print('misple',full_address)
                full_address = full_address.replace('.',', ')##03/06
                match = re.search(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*(\d{5})', full_address)
                if match:
                    city, state, zip_code = match.groups()
                    print('unnavida',city, state, zip_code)
                    if req_val == "City":
                        required_field = city.strip()
                    elif req_val == "State":
                        required_field = state.strip()
                    elif req_val == "Zip":
                        required_field = zip_code.strip()
                else:
                    required_field = "ENOAH"
            else:
                required_field = current_line if pd.notna(df.loc[i, 'Col_2']) else "ENOAH"
            break  
            # if req_val in ["City", "State", "Zip"]:
            #     match_same_line = re.search(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*(\d{5})', current_line)
            #     print('lilly000...',match_same_line)
            #     if match_same_line:
                    
            #         city, state, zip_code = map(str.strip, match_same_line.groups())
            #         if req_val == "City":
            #             return city
            #         elif req_val == "State":
            #             return state
            #         elif req_val == "Zip":
            #             return zip_code
            #     match_next_line = re.search(r'([A-Za-z\s]+),\s*([A-Za-z\s]+),\s*(\d{5})', next_line)
            #     if match_next_line:
            #         print('lilly...',city, state, zip_code)
            #         city, state, zip_code = map(str.strip, match_next_line.groups())
            #         if req_val == "City":
            #             return city
            #         elif req_val == "State":
            #             return state
            #         elif req_val == "Zip":
            #             return zip_code
            # else:
            #     return current_line if current_line else "ENOAH"
    return required_field 

def extract_business_info(Funder,table,table_name,type_column):
    df = extract_table_content(table,table_name,type_column)
    requirements = pd.read_csv(dependencydir + 'Business_Information.csv',on_bad_lines="skip", encoding="cp1252")
    state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv')
    data = requirements[requirements.Funders.str.contains(Funder,na=False)]
    data.reset_index(drop=True, inplace=True)
    df.to_csv("df_Business.csv")
    print(data)
    for i in range(0,len(data.columns)):
        if data.columns[i] == 'Company_Name':
            Company_Name = 'Company Name: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Doing_Business_As':
            Doing_Business_As = 'Doing Business As: '+ str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Business_Tax_ID':
            Business_Tax_ID = 'Business Tax ID: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Date_of_Incorporation':
            Date_of_Incorporation = 'Date of Incorporation: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Street_Address':
            Street_Address = 'Street Address(Company): ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'City':
            City ='City(Company): '+ str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'State':
            State ='State(Company): ' + 'ENOAH'
            State1 = str(eval(data[data.columns[i]][0]))
            for j in range(0,len(state_comparision)):
                if State1.lower().strip() == state_comparision['State'][j].lower().strip() or State1.lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                    State ='State(Company): ' + state_comparision['State Abbreviation'][j]
                    break
        if data.columns[i] == 'Zip_Code':
            Zip = str(eval(data[data.columns[i]][0]))
            Zip_Code = 'Zip Code(Company): ' + Zip
            if not re.search('[0-9]',str(Zip)):
                Zip_Code = 'Zip Code(Company): ' +'ENOAH'
        if data.columns[i] == 'Business_Phone':
            Phone = str(eval(data[data.columns[i]][0]))
            Business_Phone = 'Business Phone: ' + Phone
            if re.search('[a-zA-Z]',str(Phone)):
                Business_Phone = 'Business Phone: ' +'ENOAH'
    return Company_Name, Doing_Business_As, Business_Tax_ID, Date_of_Incorporation, Street_Address, City, State, Zip_Code, Business_Phone

def extract_owner_info(Funder,table,table_name,type_column):
    df = extract_table_content(table,table_name,type_column)
    requirements = pd.read_csv(dependencydir + 'Owner_Information.csv',on_bad_lines="skip", encoding="cp1252")
    state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv')
    data = requirements[requirements.Funders.str.contains(Funder,na=False)]
    data.reset_index(drop=True, inplace=True)
    df.to_csv("owner.csv")
    print(data)
    for i in range(0,len(data.columns)):
        if data.columns[i] == 'First_Name1':
            First_Name1 = 'First Name1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Last_Name1':
            Last_Name1 = 'Last Name1: '+ str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Date_of_Birth1':
            Date_of_Birth1 = 'Date of Birth1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i]== 'Date_of_Incorporation1':
            Date_of_Incorporation1 = 'Date of Incorporation1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Social_Security1':
            SSN1 = str(eval(data[data.columns[i]][0]))
            Social_Security1 = 'Social Security1: ' + SSN1
            if not re.search('[0-9]',str(SSN1)):
                Social_Security1 = 'Social Security1: ' +'ENOAH'            
        if data.columns[i] == 'Mobile_Number1':
            Mobile1 = str(eval(data[data.columns[i]][0]))
            Mobile_Number1 ='Mobile Number1: '+ Mobile1
            if not re.search('[0-9]',str(Mobile1)):
                Mobile_Number1 = 'Mobile Number1: ' +'ENOAH'
        if data.columns[i] == 'Email1':
            Email1 = 'Email1: ' + str(eval(data[data.columns[i]][0]))
            if not '@' in Email1:
                Email1 = 'Email1: ' + 'ENOAH'
        if data.columns[i] == 'Ownership1':
            Ownership1 = 'Ownership Percentage of Business1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Street_Address1':
            Street_Address1 = 'Street Address(Owner)1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'City1':
            City1 = 'City(Owner)1: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'State1':
#             State1 = 'State(Owner)1: ' + str(eval(data[data.columns[i]][0]))
            State1 ='State(Owner)1: ' + 'ENOAH'
            State = str(eval(data[data.columns[i]][0]))
            for j in range(0,len(state_comparision)):
                if State.lower().strip() == state_comparision['State'][j].lower().strip() or State.lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                    State1 ='State(Owner)1: ' + state_comparision['State Abbreviation'][j]
                    break
            print(State1)
        if data.columns[i] == 'Zip_Code1':
            Zip = str(eval(data[data.columns[i]][0]))
            Zip_Code1 = 'Zip Code(Owner)1: ' + str(eval(data[data.columns[i]][0]))
            if not re.search('[0-9]',str(Zip)):
                Zip_Code1 = 'Zip Code(Owner)1: ' +'ENOAH'
        if data.columns[i] == 'First_Name2':
            First_Name2 = 'First Name2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Last_Name2':
            Last_Name2 = 'Last Name2: '+ str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Date_of_Birth2':
            Date_of_Birth2 = 'Date of Birth2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Date_of_Incorporation2':
            Date_of_Incorporation2 = 'Date of Incorporation2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Social_Security2':
            SSN2 = str(eval(data[data.columns[i]][0]))
            Social_Security2 = 'Social Security2: ' + SSN2
            if not re.search('[0-9]',str(SSN2)):
                Social_Security2 = 'Social Security2: ' +'ENOAH'             
        if data.columns[i] == 'Mobile_Number2':
            Mobile2 = str(eval(data[data.columns[i]][0]))
            Mobile_Number2 ='Mobile Number2: '+ Mobile2
            if not re.search('[0-9]',str(Mobile2)):
                Mobile_Number2 = 'Mobile Number2: ' +'ENOAH'  
        if data.columns[i] == 'Email2':
            Email2 = 'Email2: ' + str(eval(data[data.columns[i]][0]))
            if not '@' in Email2:
                Email2 = 'Email2: ' + 'ENOAH'
        if data.columns[i] == 'Ownership2':
            Ownership2 = 'Ownership Percentage of Business2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'Street_Address2':
            Street_Address2 = 'Street Address(Owner)2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'City2':
            City2 = 'City(Owner)2: ' + str(eval(data[data.columns[i]][0]))
        if data.columns[i] == 'State2':
            State2 = 'State(Owner)2: ' + 'ENOAH'
            State = str(eval(data[data.columns[i]][0]))
            for j in range(0,len(state_comparision)):
                if State.lower().strip() == state_comparision['State'][j].lower().strip() or State.lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                    State2 ='State(Owner)2: ' + state_comparision['State Abbreviation'][j]
                    break
            print(State2)
        if data.columns[i] == 'Zip_Code2':
            Zip = str(eval(data[data.columns[i]][0]))
            Zip_Code2 = 'Zip Code(Owner)2: ' + str(eval(data[data.columns[i]][0]))
            if not re.search('[0-9]',str(Zip)):
                Zip_Code2 = 'Zip Code(Owner)2: ' +'ENOAH'
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
    if after_keyword2.strip()!='ENOAH' and after_keyword2.strip()!='-' and after_keyword2.strip()!='' and after_keyword2.strip()!='000-000-0000' and after_keyword2.strip() != after_keyword1.strip():
        return First_Name1, Last_Name1, Date_of_Birth1, Social_Security1, Mobile_Number1, Email1, Ownership1, Street_Address1, City1,State1,Zip_Code1,First_Name2,Last_Name2,Date_of_Birth2,Social_Security2,Mobile_Number2,Email2,Ownership2,Street_Address2,City2,State2,Zip_Code2
    else:
        return First_Name1, Last_Name1, Date_of_Birth1, Social_Security1, Mobile_Number1, Email1, Ownership1, Street_Address1, City1,State1,Zip_Code1

def fetch_entity(Funder,tables):
    entity = ''
    type_column = pd.read_csv(dependencydir + 'entity_info.csv',on_bad_lines="skip", encoding="cp1252")
    type_column = type_column[type_column.Funder.str.contains(Funder,na=False)]
    type_column.reset_index(drop=True, inplace=True)  
    for a in range(0,len(tables)):
        for b in range(0,len(tables[a])):
            df = tables[a][b].df
            print('uiuiiu',type_column)
            df.columns = ast.literal_eval(type_column['Column_Names'][0])
            
            df.to_csv('fetch_ent.csv')
            for k in range(0,len(df)):
                entity_key=ast.literal_eval(type_column['entity_keyword'][0])
                entity_key_loc=ast.literal_eval(type_column['entity_keyword_column'][0])
                entity_val_loc=ast.literal_eval(type_column['entity_value_column'][0])
                for i in range(0,len(entity_key)):
                    for j in range(0,len(entity_key_loc)):
                        for l in range(0,len(entity_val_loc)):
                            if entity_key_loc == entity_val_loc:
                                if type_column['after_keyword'][0]=='Nil':
                                    if type_column['entity_location_+1'][0]==1:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k+1]
                                            if entity == 'LC':
                                                entity = 'LLC'
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==-1:
                                                    entity = df[str(entity_key_loc[j])][k-1]
                                                elif type_column['entity_location_+1_2'][0]==0:
                                                    entity = df[str(entity_key_loc[j])][k]
                                                elif type_column['entity_location_+1_2'][0]==2:
                                                    entity = df[str(entity_key_loc[j])][k+2]
                                                    if Funder == 'OnPoint Solutions' and entity == '':
                                                        entity = df[str(entity_key_loc[j])][k+3]
                                                    if Funder =='Cast Capital Funding' and 'Phone' in entity:
                                                        entity = ''
                                                    if Funder == 'CREATIVE CAPITAL SOLUTIONS' and entity == '':
                                                        entity = df[str(entity_key_loc[j])][k+3]
                                            break
                                    elif type_column['entity_location_+1'][0]==2:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k+2]
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==1:
                                                    entity = df[str(entity_key_loc[j])][k+1]
                                                elif type_column['entity_location_+1_2'][0]==3:
                                                    entity = df[str(entity_key_loc[j])][k+3]
                                                elif type_column['entity_location_+1_2'][0]==-1:
                                                    entity = df[str(entity_key_loc[j])][k-1]
                                            break
                                    elif type_column['entity_location_+1'][0]==3:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k+3]
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==2:
                                                    entity = df[str(entity_key_loc[j])][k+2]
                                                elif type_column['entity_location_+1_2'][0]==4:
                                                    entity = df[str(entity_key_loc[j])][k+4]
                                            break
                                    elif type_column['entity_location_+1'][0]==-1:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k-1]
                                            if entity == '':
                                                if type_column['entity_location_+1_2'][0]==0:
                                                    entity = df[str(entity_key_loc[j])][k]
                                                elif type_column['entity_location_+1_2'][0]==1:
                                                    entity = df[str(entity_key_loc[j])][k+1]
                                                elif type_column['entity_location_+1_2'][0]==-2:
                                                    entity = df[str(entity_key_loc[j])][k-2]
                                            break
                                    elif type_column['entity_location_+1'][0]==-2:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k-2]
                                            break
                                    elif type_column['entity_location_+1'][0]==4:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k-4]
                                            break
                                    elif type_column['entity_location_+1'][0]==-3:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k-3]
                                            break
                                    elif type_column['entity_location_+1'][0]==0:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_key_loc[j])][k]
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==1:
                                                    entity = df[str(entity_key_loc[j])][k+1]
                                                elif type_column['entity_location_+1_2'][0]==-1:
                                                    entity = df[str(entity_key_loc[j])][k-1]
                                                elif type_column['entity_location_+1_2'][0]==2:
                                                    entity = df[str(entity_key_loc[j])][k+2]
                                            break
                                    else:
                                        entity = '' 
                                        break
                                else:
                                    before_key=ast.literal_eval(type_column['after_keyword'][0])
                                    for z in range(0,len(before_key)):
                                        if type_column['entity_location_+1'][0]==1:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                if len(df) != k+1:         
                                                    mystring = df[str(entity_key_loc[j])][k+1] 
                                                    keyword = str(before_key[z])
                                                    before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                    entity = after_keyword
                                                    break
                                        elif type_column['entity_location_+1'][0]==2:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k+2] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==3:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k+3] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-1:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k-1] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-2:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k-2] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==4:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k+4] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-3:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k-3] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==0:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_key_loc[j])][k] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword.replace(str(entity_key[i]),'')
                                                if entity =='':
                                                    entity = before_keyword.replace(str(entity_key[i]),'')
                                                    if entity == '' :
                                                        if type_column['entity_location_+1_2'][0]==1:
                                                            entity = df[str(entity_key_loc[j])][k+1]
                                                            if Funder == 'Reliant_Funding' and ('Business Start Date Under Current Ownership:' in entity):
                                                                entity = ''
                                                        elif type_column['entity_location_+1_2'][0]==-1:
                                                            entity = df[str(entity_key_loc[j])][k-1]
                                                            if Funder == 'Lendzi' and (('ENOAH' in entity) or ('Website:' in entity)):
                                                                entity = df[str(entity_key_loc[j])][k+1]
                                                break
                                        else:
                                            entity = '' 
                                            break
                            else:
                                if type_column['after_keyword'][0]=='Nil':
                                    if type_column['entity_location_+1'][0]==1:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k+1]
                                            if entity == 'LC':
                                                entity = 'LLC'
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==-1:
                                                    entity = df[str(entity_val_loc[l])][k-1]
                                                elif type_column['entity_location_+1_2'][0]==0:
                                                    entity = df[str(entity_val_loc[l])][k]
                                                elif type_column['entity_location_+1_2'][0]==2:
                                                    entity = df[str(entity_val_loc[l])][k+2]
                                            break
                                    elif type_column['entity_location_+1'][0]==2:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k+2]
                                            break
                                    elif type_column['entity_location_+1'][0]==3:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k+3]
                                            break
                                    elif type_column['entity_location_+1'][0]==-1:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k-1]
                                            if entity == '':
                                                if type_column['entity_location_+1_2'][0]==0:
                                                    entity = df[str(entity_val_loc[l])][k]
                                                elif type_column['entity_location_+1_2'][0]==1:
                                                    entity = df[str(entity_val_loc[l])][k+1]
                                                elif type_column['entity_location_+1_2'][0]==-2:
                                                    entity = df[str(entity_val_loc[l])][k-2]
                                            break
                                    elif type_column['entity_location_+1'][0]==-2:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k-2]
                                            break
                                    elif type_column['entity_location_+1'][0]==4:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k-4]
                                            break
                                    elif type_column['entity_location_+1'][0]==-3:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k-3]
                                            break
                                    elif type_column['entity_location_+1'][0]==0:
                                        if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                            entity = df[str(entity_val_loc[l])][k]
                                            if entity == '' :
                                                if type_column['entity_location_+1_2'][0]==1:
                                                    entity = df[str(entity_val_loc[l])][k+1]
                                                    if (Funder == 'Lexington Capital Holdings' or Funder == 'Ma Here Inc') and entity =='':
                                                        entity = df[str(entity_val_loc[l])][k-1]
                                                elif type_column['entity_location_+1_2'][0]==-1:
                                                    entity = df[str(entity_val_loc[l])][k-1]
                                                elif type_column['entity_location_+1_2'][0]==2:
                                                    entity = df[str(entity_val_loc[l])][k+2]
                                            break
                                    else:
                                        entity = '' 
                                        break
                                else:
                                    before_key=ast.literal_eval(type_column['after_keyword'][0])
                                    for z in range(0,len(before_key)):
                                        if type_column['entity_location_+1'][0]==1:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                if len(df) != k+1:         
                                                    mystring = df[str(entity_val_loc[l])][k+1] 
                                                    keyword = str(before_key[z])
                                                    before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                    entity = after_keyword
                                                    break
                                        elif type_column['entity_location_+1'][0]==2:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k+2] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==3:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k+3] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-1:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k-1] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-2:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k-2] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==4:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k+4] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==-3:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k-3] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword
                                                break
                                        elif type_column['entity_location_+1'][0]==0:
                                            if str(entity_key[i]) in df[str(entity_key_loc[j])][k]:
                                                mystring = df[str(entity_val_loc[l])][k] 
                                                keyword = str(before_key[z])
                                                before_keyword,keyword, after_keyword = mystring.partition(keyword)
                                                entity = after_keyword.replace(str(entity_key[i]),'')
                                                if entity =='':
                                                    entity = before_keyword.replace(str(entity_key[i]),'')
                                                break
                                        else:
                                            entity = ''
                                            break
    entity = str(entity).replace('Telephone #:','')
    return entity
    
def fetch_company_info(pdf):
    #fetch the funder
    entity_table = []
    Funder = template_check(pdf)
    print(Funder)
    #read required table and take necessary records for easy processing
    type_column = pd.read_csv(dependencydir + 'Funder_Template.csv',on_bad_lines="skip", encoding="cp1252")
    type_column = type_column[type_column.Funder.str.contains(Funder,na=False)]
    type_column.reset_index(drop=True, inplace=True)
    print(type_column)
    #if type_column.empty:
    #    raise ValueError(f"No matching funder found in template for: {Funder}")
    #fetch required tables for fetching business and owner informations
    Business_Information = table_extract(pdf,'Business_Information',type_column)
    Owner_Information = table_extract(pdf,'Owner_Information',type_column)
    entity_info = pd.read_csv(dependencydir + 'entity_info.csv')
    exceptional_keys = entity_info['Funder'].tolist()  
    if Funder in exceptional_keys :
        entity_table.append(table_extract(pdf,'Entity',type_column))
        entity = fetch_entity(Funder,entity_table)
    else:
        entity = ''

    industry_info = pd.read_csv(dependencydir + 'industry_info.csv',on_bad_lines='skip',encoding='cp1252')
    industry_keys = industry_info['Funder'].tolist()
    if Funder in industry_keys :
        industry_info = industry_info[industry_info.Funder.str.contains(Funder,na=False)]
        industry_info.reset_index(drop=True, inplace=True)
        try:
            industry_information = table_extract(pdf,'industry_information',industry_info)
            Business_Industry, Business_Email, Business_Proceeds, Loan_Amount = IST.extract_industry_info_template(Funder,industry_information,'industry_information',industry_info)
            print('222222222222QQQQQQQQQQQQQQQQQQQ',Business_Industry, Business_Email, Business_Proceeds, Loan_Amount)
        except:
            import traceback
            errortype = traceback.format_exc()
            print("111111111########",errortype)
            Business_Industry = Business_Email = Business_Proceeds = Loan_Amount = ''
    else:
        Business_Industry = Business_Email = Business_Proceeds = Loan_Amount = ''

    #fetch business information for the given statement based on the funder found
    #format_type = type_column['Format'].iloc[0] if not type_column.empty else 'Normal'
    if type_column['Format'][0] == 'Normal':
    #if format_type == 'Normal':
        business_info = extract_business_info(Funder,Business_Information,'Business_Information',type_column)
        Owner_Information = extract_owner_info(Funder,Owner_Information,'Owner_Information',type_column)
    else:
        print("extracting business info")
        business_info = IST.extract_business_info_template(Funder,Business_Information,'Business_Information',type_column)
        Owner_Information = IST.extract_owner_info_template(Funder,Owner_Information,'Owner_Information',type_column)
        
    return Funder,business_info,Owner_Information,entity,Business_Industry, Business_Email, Business_Proceeds, Loan_Amount

def fetch_data_1(pdf):
        head, tail = os.path.split(pdf)
        pdf = decrypt_pdf(pdf)
        Funder,business_info,Owner_Information,entity = fetch_company_info(pdf)
        result = pd.concat([pd.DataFrame(business_info),pd.DataFrame(Owner_Information)])
        result = result.reset_index(drop = True)
        print(result)
        result.columns = ['Particulars']
        state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv')
        city_com = pd.read_csv(dependencydir + "uszips.csv")
        city_com.to_csv('hell.csv')
        address_com = pd.read_csv(dependencydir + "Address_validation.csv")
        if len(result)<30:
            report = pd.read_csv(dependencydir + "Extracted_Data.csv")
        else:
            report = pd.read_csv(dependencydir + "All_Owners_extracted_data.csv")
        report = report.append(pandas.Series(), ignore_index=True)
        report['ISO Name'][0] = Funder
        print(report)
        for i in range(1,len(report.columns)):
            for j in range(0,len(result)):
                if report.columns[i] in result['Particulars'][j]:
                    mystring = result['Particulars'][j]
                    keyword = ':'
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    report[report.columns[i]][0] = after_keyword.strip()
            if report[report.columns[i]][0] == 'ENOAH' or report[report.columns[i]][0] == '':
                report[report.columns[i]][0] = '-'
            if report.columns[i] == 'First Name' or report.columns[i] == 'First Name1' or report.columns[i] == 'First Name2':
                report[report.columns[i]][0] = re.sub('[0-9]',  '', report[report.columns[i]][0])
                if len(report[report.columns[i]][0].split())>1:
                    first_name = report[report.columns[i]][0].split()
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
                elif len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False:
                    first_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
            if report.columns[i] == 'Last Name' or report.columns[i] == 'Last Name1' or report.columns[i] == 'Last Name2':
                report[report.columns[i]][0] = re.sub('[0-9]',  '', report[report.columns[i]][0])
                if len(report[report.columns[i]][0].split())>1:
                    last_name = report[report.columns[i]][0].split()
                    n = len(last_name)
                    if n > 1 and len(last_name[n-2])>3:
                        report[report.columns[i]][0] = last_name[n-1]
                    elif n>1 and len(last_name[n-2])==1:
                        report[report.columns[i]][0] = last_name[n-1]
                    elif n>1 and len(last_name)==2:
                        report[report.columns[i]][0] = last_name[n-1]
                    else:
                        report[report.columns[i]][0] = ' '.join(last_name[n-2:n])
                elif len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False:
                    last_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    n = len(last_name)
                    if len(last_name[n-2])>3:
                        report[report.columns[i]][0] = last_name[n-1]
            if report.columns[i] == 'City(Owner)' or report.columns[i] == 'City(Company)' or report.columns[i] == 'City(Owner)1' or report.columns[i] == 'City(Owner)2':
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace(',','')
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('-','')
            if report.columns[i] == 'State(Owner)' or report.columns[i] == 'State(Company)' or report.columns[i] == 'State(Owner)1' or report.columns[i] == 'State(Owner)2':
                count = 0
                for j in range(0,len(state_comparision)):
                    if report[report.columns[i]][0].lower().strip() == state_comparision['State'][j].lower().strip() or report[report.columns[i]][0].lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                        report[report.columns[i]][0] =state_comparision['State Abbreviation'][j]
                        count = 1
                        break
                if count == 0:
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == 'Zip Code(Owner)' or report.columns[i] == 'Zip Code(Company)' or report.columns[i] == 'Zip Code(Owner)1' or report.columns[i] == 'Zip Code(Owner)2':
                if re.search('[a-zA-Z/]',str(report[report.columns[i]][0])) and not re.search('[0-9]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] = '-'

            if report.columns[i] == 'Business Phone' or report.columns[i] == 'Mobile Number' or report.columns[i] == 'Mobile Number1' or report.columns[i] == 'Mobile Number2':
                if re.search('[a-zA-Z]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == "Doing Business As":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[-]',  '', str(report[report.columns[i]][0])).strip()
            if report.columns[i] == "Date of Incorporation":
                if report[report.columns[i]][0]!='-':
                    try:
                        val = "%m%d%Y".replace(' ','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                    except:
                        report[report.columns[i]][0] = report[report.columns[i]][0]
            if report.columns[i] == "Date of Birth" or report.columns[i] == "Date of Birth1" or report.columns[i] == "Date of Birth2":
                if report[report.columns[i]][0]!='-':
                    try:
                        report[report.columns[i]][0] = report[report.columns[i]][0].replace('=','-').replace(' ','')
                        val = "%m%d%Y".replace(' ','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                        new = date.today().strftime('%m-%d-%Y')
                        newdate1 = datetime.strptime(report[report.columns[i]][0], "%m-%d-%Y")
                        newdate2 = datetime.strptime(new, "%m-%d-%Y")
                        if newdate1>newdate2:
                            report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('-20','-19')
                    except:
                        report[report.columns[i]][0] = report[report.columns[i]][0]
            if report.columns[i] == "Business Tax ID":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z:%.,() +_/]',  '', str(report[report.columns[i]][0]))
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    report[report.columns[i]][0] = report[report.columns[i]][0][:2] + "-" + report[report.columns[i]][0][2:]
            if report.columns[i] == "Social Security" or report.columns[i] == "Social Security1" or report.columns[i] == "Social Security2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    if len(report[report.columns[i]][0])<9:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(9)

                    report[report.columns[i]][0] = report[report.columns[i]][0][:3] + "-" + report[report.columns[i]][0][3:5]+"-"+report[report.columns[i]][0][5:]
            if report.columns[i] == "Business Phone" or report.columns[i] == "Mobile Number" or report.columns[i] == "Mobile Number1" or report.columns[i] == "Mobile Number2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    if len(report[report.columns[i]][0]) == 10:
                        report[report.columns[i]][0] = "(" + report[report.columns[i]][0][:3] + ")" + report[report.columns[i]][0][3:6]+"-"+report[report.columns[i]][0][6:]
                    elif len(report[report.columns[i]][0]) == 11 and '1' in report[report.columns[i]][0][0]:
                        report[report.columns[i]][0] = report[report.columns[i]][0][1:]
                        report[report.columns[i]][0] = "(" + report[report.columns[i]][0][:3] + ")" + report[report.columns[i]][0][3:6]+"-"+report[report.columns[i]][0][6:]
                    else:
                        report[report.columns[i]][0] = '-'
            if report.columns[i] == "Ownership Percentage of Business" or report.columns[i] == "Ownership Percentage of Business1" or report.columns[i] == "Ownership Percentage of Business2":
                if re.search('[a-zA-Z:%]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] =  re.sub('[a-zA-Z:% ]',  '', str(report[report.columns[i]][0]))
                print("report[report.columns[i]][0]",[report[report.columns[i]][0]])
                if report[report.columns[i]][0]!='-' and report[report.columns[i]][0].strip()!='':
                    if re.search('[a-zA-Z:]',str(report[report.columns[i]][0])):
                        report[report.columns[i]][0] =  re.sub('[a-zA-Z:]',  '', str(report[report.columns[i]][0]))
                    if report[report.columns[i]][0]!='-' and report[report.columns[i]][0].strip()!='':
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("%",'')
                        report[report.columns[i]][0] = re.sub(' +', ' ', str(report[report.columns[i]][0]))
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("50 50",'50')
                        report[report.columns[i]][0] = int(float(report[report.columns[i]][0].strip()))
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]) + "%"
            if report.columns[i] != "State(Company)" and report.columns[i] != "State(Owner)" and report.columns[i] != "Email" and report.columns[i] != "State(Owner)1" and report.columns[i] != "Email1" and report.columns[i] != "State(Owner)2" and report.columns[i] != "Email2":
                report[report.columns[i]][0] = report[report.columns[i]][0].title()
            if report.columns[i] == "Email" or report.columns[i] == "Email1" or report.columns[i] == "Email2":
                if is_valid_email(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = (report[report.columns[i]][0]).lower()
                else:
                    report[report.columns[i]][0] = 'gftest@enoahisolution.com'
            if report.columns[i] == "Zip Code(Company)":
                print("printing zip code company",report[report.columns[i]][0])
                
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,.]','',str(report[report.columns[i]][0])).strip()
                    print('printtttttt',report[report.columns[i]][0])
                    
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    if len(report[report.columns[i]][0])>5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0])[-1:-6]
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Company)'][0] == 'ENOAH' or report['City(Company)'][0] == '' or report['City(Company)'][0] == '-':
                                report['City(Company)'][0] = city_com['city'][k]
                            if report['State(Company)'][0] == 'ENOAH' or report['State(Company)'][0] == '' or report['State(Company)'][0] == '-' or len(report['State(Company)'][0]) <= 1 or (city_com['state_id'][k] in report['State(Company)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Company)'][0].lower()):
                                report['State(Company)'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,.]','',str(report[report.columns[i]][0])).strip()
                    print("printing zip code owner",report[report.columns[i]][0])
                    zip = report[report.columns[i]][0]
                    zip = ast.literal_eval(zip)
                    if len(zip)>1:
                        if '-' not in zip[-1]:
                            report[report.columns[i]][0] = zip[-1]
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)'][0] == 'ENOAH' or report['City(Owner)'][0] == '' or report['City(Owner)'][0] == '-':
                                report['City(Owner)'][0] = city_com['city'][k]
                            if report['State(Owner)'][0] == 'ENOAH' or report['State(Owner)'][0] == '' or report['State(Owner)'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)'][0].lower()):
                                report['State(Owner)'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)1":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,.]','',str(report[report.columns[i]][0])).strip()
                    print("printing zip code owner",report[report.columns[i]][0])
                    zip = report[report.columns[i]][0]
                    zip = ast.literal_eval(zip)
                    if len(zip)>1:
                        if '-' not in zip[-1]:
                            report[report.columns[i]][0] = zip[-1]
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)1'][0] == 'ENOAH' or report['City(Owner)1'][0] == '' or report['City(Owner)1'][0] == '-':
                                report['City(Owner)1'][0] = city_com['city'][k]
                            if report['State(Owner)1'][0] == 'ENOAH' or report['State(Owner)1'][0] == '' or report['State(Owner)1'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)1'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)1'][0].lower()):
                                report['State(Owner)1'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,.]','',str(report[report.columns[i]][0])).strip()
                    zip = report[report.columns[i]][0]
                    zip = ast.literal_eval(zip)
                    if len(zip)>1:
                        if '-' not in zip[-1]:
                            report[report.columns[i]][0] = zip[-1]
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)2'][0] == 'ENOAH' or report['City(Owner)2'][0] == '' or report['City(Owner)2'][0] == '-':
                                report['City(Owner)2'][0] = city_com['city'][k]
                            if report['State(Owner)2'][0] == 'ENOAH' or report['State(Owner)2'][0] == '' or report['State(Owner)2'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)2'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)2'][0].lower()):
                                report['State(Owner)2'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Street Address(Owner)":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()):
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].lower()
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace('0,','')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].title().strip()
                        break
                report['Street Address(Owner)'][0] = re.sub('\s+',' ',report['Street Address(Owner)'][0])
                report['Street Address(Owner)'][0] = ''.join([s for s in report['Street Address(Owner)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)1":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()):
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].lower()
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace('0,','')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].title().strip()
                        break
                report['Street Address(Owner)1'][0] = re.sub('\s+',' ',report['Street Address(Owner)1'][0])
                report['Street Address(Owner)1'][0] = ''.join([s for s in report['Street Address(Owner)1'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)1'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)1'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)2":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()):
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].lower()
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace('0,','')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].title().strip()
                        break
                report['Street Address(Owner)2'][0] = re.sub('\s+',' ',report['Street Address(Owner)2'][0])
                report['Street Address(Owner)2'][0] = ''.join([s for s in report['Street Address(Owner)2'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)2'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)2'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Company)":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in str(report['Street Address(Company)'][0]).lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Company)'][0].lower()):
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].lower()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].title().strip()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace('0,','')
                        break
                report['Street Address(Company)'][0] = re.sub('\s+',' ',report['Street Address(Company)'][0])
                report['Street Address(Company)'][0] = ''.join([s for s in report['Street Address(Company)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Company)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                            break
                report['Street Address(Company)'][0] = ' '.join(datatosplit)
        report['Filename'] = tail
        return report

def fetch_data_batch(pdf,start_time,filepath,estimated_time):
    head, tail = os.path.split(pdf)
    pdf = decrypt_pdf(pdf)
    try:
        Funder,business_info,Owner_Information,entity = fetch_company_info(pdf)
        result = pd.concat([pd.DataFrame(business_info),pd.DataFrame(Owner_Information)])
        result = result.reset_index(drop = True)
        print(result)
        result.columns = ['Particulars']
        result.to_csv("result.csv")
        print("length of result",len(result))
        state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv')
        city_com = pd.read_csv(dependencydir + "uszips.csv",on_bad_lines="skip", encoding="cp1252")
        address_com = pd.read_csv(dependencydir + "Address_validation.csv")
        if len(result)<30:
            report = pd.read_csv(dependencydir + "Extracted_Data.csv")
        else:
            report = pd.read_csv(dependencydir + "All_Owners_extracted_data.csv")
        report = report.append(pandas.Series(), ignore_index=True)
        report['ISO Name'][0] = Funder
        print(report)
        for i in range(1,len(report.columns)):
            for j in range(0,len(result)):
                if report.columns[i] in result['Particulars'][j]:
                    mystring = result['Particulars'][j]
                    keyword = ':'
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    report[report.columns[i]][0] = after_keyword.strip()
            if report[report.columns[i]][0] == 'ENOAH' or report[report.columns[i]][0] == '':
                report[report.columns[i]][0] = '-'
            if report.columns[i] == 'First Name' or report.columns[i] == 'First Name1' or report.columns[i] == 'First Name2':
                if len(report[report.columns[i]][0].split())>1:
                    first_name = report[report.columns[i]][0].split()
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
                if len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False:
                    first_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
            if report.columns[i] == 'Last Name' or report.columns[i] == 'Last Name1' or report.columns[i] == 'Last Name2':
                if len(report[report.columns[i]][0].split())>1:
                    last_name = report[report.columns[i]][0].split()
                    n = len(last_name)
                    report[report.columns[i]][0] = last_name[n-1]
                if len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False:
                    last_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    n = len(last_name)
                    if len(last_name[n-2])>3:
                        report[report.columns[i]][0] = last_name[n-1]
            if report.columns[i] == 'City(Owner)' or report.columns[i] == 'City(Company)' or report.columns[i] == 'City(Owner)1' or report.columns[i] == 'City(Owner)2':
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace(',','')
            if report.columns[i] == 'State(Owner)' or report.columns[i] == 'State(Company)' or report.columns[i] == 'State(Owner)1' or report.columns[i] == 'State(Owner)2':
                count = 0
                for j in range(0,len(state_comparision)):
                    if report[report.columns[i]][0].lower().strip() == state_comparision['State'][j].lower().strip() or report[report.columns[i]][0].lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                        report[report.columns[i]][0] =state_comparision['State Abbreviation'][j]
                        count = 1
                        break
                if count == 0:
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == 'Zip Code(Owner)' or report.columns[i] == 'Zip Code(Company)' or report.columns[i] == 'Zip Code(Owner)1' or report.columns[i] == 'Zip Code(Owner)2':
                if re.search('[a-zA-Z]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] = '-'

            if report.columns[i] == 'Business Phone' or report.columns[i] == 'Mobile Number' or report.columns[i] == 'Mobile Number1' or report.columns[i] == 'Mobile Number2':
                if re.search('[a-zA-Z]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == "Doing Business As":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[-]',  '', str(report[report.columns[i]][0])).strip()
            if report.columns[i] == "Date of Incorporation":
                if report[report.columns[i]][0]!='-':
                    try:
                        val = "%m%d%Y".replace(' ','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                    except:
                        report[report.columns[i]][0] = report[report.columns[i]][0]
            if report.columns[i] == "Date of Birth" or report.columns[i] == "Date of Birth1" or report.columns[i] == "Date of Birth2":
                if report[report.columns[i]][0]!='-':
                    try:
                        report[report.columns[i]][0] = report[report.columns[i]][0].replace('=','-').replace(' ','')
                        val = "%m%d%Y".replace(' ','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                        new = date.today().strftime('%m-%d-%Y')
                        newdate1 = datetime.strptime(report[report.columns[i]][0], "%m-%d-%Y")
                        newdate2 = datetime.strptime(new, "%m-%d-%Y")
                        if newdate1>newdate2:
                            report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('-20','-19')
                    except:
                        report[report.columns[i]][0] = report[report.columns[i]][0]
            if report.columns[i] == "Business Tax ID":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z&]',  '', str(report[report.columns[i]][0]))
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    report[report.columns[i]][0] = report[report.columns[i]][0][:2] + "-" + report[report.columns[i]][0][2:]
            if report.columns[i] == "Social Security" or report.columns[i] == "Social Security1" or report.columns[i] == "Social Security2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    report[report.columns[i]][0] = report[report.columns[i]][0][:3] + "-" + report[report.columns[i]][0][3:5]+"-"+report[report.columns[i]][0][5:]
            if report.columns[i] == "Business Phone" or report.columns[i] == "Mobile Number" or report.columns[i] == "Mobile Number1" or report.columns[i] == "Mobile Number2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    report[report.columns[i]][0] = "(" + report[report.columns[i]][0][:3] + ")" + report[report.columns[i]][0][3:6]+"-"+report[report.columns[i]][0][6:]
            if report.columns[i] == "Ownership Percentage of Business" or report.columns[i] == "Ownership Percentage of Business1" or report.columns[i] == "Ownership Percentage of Business2":
                if re.search('[a-zA-Z:]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] =  re.sub('[a-zA-Z: ]',  '', str(report[report.columns[i]][0]))
                print("report[report.columns[i]][0]",[report[report.columns[i]][0]])
                if report[report.columns[i]][0]!='-' and report[report.columns[i]][0].strip()!='':
                    if re.search('[a-zA-Z:]',str(report[report.columns[i]][0])):
                        report[report.columns[i]][0] =  re.sub('[a-zA-Z:]',  '', str(report[report.columns[i]][0]))
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("%",'')
                    report[report.columns[i]][0] = int(float(report[report.columns[i]][0].strip()))
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]) + "%"
            if report.columns[i] != "State(Company)" and report.columns[i] != "State(Owner)" and report.columns[i] != "Email" and report.columns[i] != "State(Owner)1" and report.columns[i] != "Email1" and report.columns[i] != "State(Owner)2" and report.columns[i] != "Email2":
                report[report.columns[i]][0] = report[report.columns[i]][0].title()
            if report.columns[i] == "Email" or report.columns[i] == "Email1" or report.columns[i] == "Email2":
                if is_valid_email(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = (report[report.columns[i]][0]).lower()
                else:
                    report[report.columns[i]][0] = 'gftest@enoahisolution.com'
            if report.columns[i] == "Zip Code(Company)":
                print("printing zip code company",report[report.columns[i]][0])
                if report[report.columns[i]][0]!='-':
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    print("printing zip code company after appending",report[report.columns[i]][0])
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            print("printing zip code company after validation",report[report.columns[i]][0],[report['City(Company)'][0]])
                            if report['City(Company)'][0] == 'ENOAH' or report['City(Company)'][0] == '' or report['City(Company)'][0] == '-':
                                report['City(Company)'][0] = city_com['city'][k]
                            if report['State(Company)'][0] == 'ENOAH' or report['State(Company)'][0] == '' or report['State(Company)'][0] == '-' or (city_com['state_id'][k] in report['State(Company)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Company)'][0].lower()):
                                report['State(Company)'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)":
                if report[report.columns[i]][0]!='-':
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)'][0] == 'ENOAH' or report['City(Owner)'][0] == '' or report['City(Owner)'][0] == '-':
                                report['City(Owner)'][0] = city_com['city'][k]
                            if report['State(Owner)'][0] == 'ENOAH' or report['State(Owner)'][0] == '' or report['State(Owner)'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)'][0].lower()):
                                report['State(Owner)'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)1":
                if report[report.columns[i]][0]!='-':
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)1'][0] == 'ENOAH' or report['City(Owner)1'][0] == '' or report['City(Owner)1'][0] == '-':
                                report['City(Owner)1'][0] = city_com['city'][k]
                            if report['State(Owner)1'][0] == 'ENOAH' or report['State(Owner)1'][0] == '' or report['State(Owner)1'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)1'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)1'][0].lower()):
                                report['State(Owner)1'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)2":
                if report[report.columns[i]][0]!='-':
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if report['City(Owner)2'][0] == 'ENOAH' or report['City(Owner)2'][0] == '' or report['City(Owner)2'][0] == '-':
                                report['City(Owner)2'][0] = city_com['city'][k]
                            if report['State(Owner)2'][0] == 'ENOAH' or report['State(Owner)2'][0] == '' or report['State(Owner)2'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)2'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)2'][0].lower()):
                                report['State(Owner)2'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Street Address(Owner)":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()):
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].lower()
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace('0,','')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].title().strip()
                        break
                report['Street Address(Owner)'][0] = re.sub('\s+',' ',report['Street Address(Owner)'][0])
                report['Street Address(Owner)'][0] = ''.join([s for s in report['Street Address(Owner)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)1":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()):
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].lower()
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace('0,','')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].title().strip()
                        break
                report['Street Address(Owner)1'][0] = re.sub('\s+',' ',report['Street Address(Owner)1'][0])
                report['Street Address(Owner)1'][0] = ''.join([s for s in report['Street Address(Owner)1'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)1'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)1'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)2":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()):
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].lower()
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace('0,','')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].title().strip()
                        break
                report['Street Address(Owner)2'][0] = re.sub('\s+',' ',report['Street Address(Owner)2'][0])
                report['Street Address(Owner)2'][0] = ''.join([s for s in report['Street Address(Owner)2'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)2'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)1'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Company)":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in str(report['Street Address(Company)'][0]).lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Company)'][0].lower()):
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].lower()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].title().strip()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace('0,','')
                        break
                report['Street Address(Company)'][0] = re.sub('\s+',' ',report['Street Address(Company)'][0])
                report['Street Address(Company)'][0] = ''.join([s for s in report['Street Address(Company)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Company)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                            break
                report['Street Address(Company)'][0] = ' '.join(datatosplit)
        report['Filename'] = tail
        os.remove(pdf, dir_fd=None)
        print("printing extracted_data",report)
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        time_taken = str(end - start)
        print("printing time_taken",time_taken)
        estimated_time = estimated_time - dti.timedelta(seconds=int((end-start).total_seconds()))
        datetime_obj = datetime.strptime(str(estimated_time), "%Y-%m-%d %H:%M:%S")
        time = datetime_obj.time()
        isoapi.to_update_batch_estimated_time(str(time),filepath)
        return report
    except:
        os.remove(pdf, dir_fd=None)
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        time_taken = str(end - start)
        estimated_time = estimated_time - dti.timedelta(seconds=int((end-start).total_seconds()))
        datetime_obj = datetime.strptime(str(estimated_time), "%Y-%m-%d %H:%M:%S")
        time = datetime_obj.time()
        isoapi.to_update_batch_estimated_time(str(time),filepath)
        report = pd.DataFrame([[tail,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-']],columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1'])
        
        return report

def fetch_data(pdf):
    head, tail = os.path.split(pdf)
    #pdf = decrypt_pdf(pdf)
    from pypdf import PdfReader, PdfWriter
    pdf_reader = PdfReader(pdf)
    print('pdf_reader',pdf_reader)
    metadata = pdf_reader.metadata
    print('Teamout...',metadata)                      # Check if the producer is "Adobe Acrobat Pro (32-bit)"
    producer = metadata.get('/Producer', '').lower()
    print('wwwwwwwwwwwwwwwwwwwwww')
    print(producer)
    if "adobe acrobat pro (32-bit)" in producer:
        #print("Stopping execution: File was produced by Adobe Acrobat Pro (32-bit).")
        #report = pd.DataFrame([[tail,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-']],columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1'])
        #print('report')
        #return report
        #from pypdf import PdfReader, PdfWriter
        #pdf = str(pdf) + '_decrypted_statement.pdf'
        input_pdf = PdfReader(pdf)
        output_pdf = PdfWriter()
        for page in input_pdf.pages[1:]:
            output_pdf.add_page(page)
        output_pdf.write(pdf)
        print("Processing successful", pdf)
    #    report = pd.DataFrame([[tail,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-']],columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1'])
    #    print('report')
    #    return report
    #else:
    #    print('jjjjjjjjjjjjjjjjjj')
    try:
        Funder,business_info,Owner_Information,entity,Business_Industry, Business_Email, Business_Proceeds, Loan_Amount = fetch_company_info(pdf)
        result = pd.concat([pd.DataFrame(business_info),pd.DataFrame(Owner_Information)])
        result = result.reset_index(drop = True)
        print(result)
        result.to_csv('meet.csv')
        result.columns = ['Particulars']
        state_comparision = pd.read_csv(dependencydir + 'State_Comparision.csv')
        city_com = pd.read_csv(dependencydir + "uszips.csv", encoding="cp1252")
        city_com.to_csv('hell.csv')
        address_com = pd.read_csv(dependencydir + "Address_validation.csv")
        if len(result)<30:
            report = pd.read_csv(dependencydir + "Extracted_Data.csv")
        else:
            report = pd.read_csv(dependencydir + "All_Owners_extracted_data.csv")
        new_row = {
            'ISO Name': Funder,
            'Entity Type': entity,
            'Industry': Business_Industry,
            'Business Email': Business_Email,
            'Proceeds': Business_Proceeds,
            'Amount': Loan_Amount
        }
        report = pd.concat([report, pd.DataFrame([new_row])], ignore_index=True)
        report.loc[0, 'ISO Name'] = Funder
        report.loc[0, 'Entity Type'] = entity
        report.loc[0, 'Industry'] = Business_Industry
        report.loc[0, 'Business Email'] = Business_Email
        report.loc[0, 'Proceeds'] = Business_Proceeds
        report.loc[0, 'Amount'] = Loan_Amount
        print(report)
        #report = report.append(pandas.Series(), ignore_index=True)
        #report['ISO Name'][0] = Funder
        #report['Entity Type'][0] = entity
        #report = pd.concat([report, pd.DataFrame([{}])], ignore_index=True)
        #report.at[0, 'ISO Name'] = Funder
        #report.at[0, 'Entity Type'] = entity
        # print(report)
        for i in range(1,len(report.columns)):
            for j in range(0,len(result)):
                if report.columns[i] in result['Particulars'][j]:
                    mystring = result['Particulars'][j]
                    keyword = ':'
                    before_keyword, keyword, after_keyword = mystring.partition(keyword)
                    report.loc[0,report.columns[i]] = after_keyword.strip()
            if report[report.columns[i]][0] == 'ENOAH' or str(report[report.columns[i]][0]).strip() == '':
                report[report.columns[i]][0] = '-'
            if report.columns[i] == 'First Name' or report.columns[i] == 'First Name1' or report.columns[i] == 'First Name2':
                report[report.columns[i]][0] = re.sub('[0-9[_]', '', report[report.columns[i]][0])
                print('btcccc',report[report.columns[i]][0])
                if ',' in str(report[report.columns[i]][0]):#17/06
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace(',','')
                if len(report[report.columns[i]][0].split())>1 and Funder!='American Capital - AMC Capital':
                    first_name = report[report.columns[i]][0].split()
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
                    print('btcc11',report[report.columns[i]][0])
                elif len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False and Funder!='American Capital - AMC Capital':
                    first_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    m = len(first_name)
                    report[report.columns[i]][0] = ' '.join(first_name[0:m-1])
                    print('btcc22',report[report.columns[i]][0])
            if report.columns[i] == 'Last Name' or report.columns[i] == 'Last Name1' or report.columns[i] == 'Last Name2':
                report[report.columns[i]][0] = re.sub('[0-9[_]', '', report[report.columns[i]][0])
                if len(report[report.columns[i]][0].split())>1:
                    last_name = report[report.columns[i]][0].split()
                    n = len(last_name)
                    if n > 1 and len(last_name[n-2])>3:
                        report[report.columns[i]][0] = last_name[n-1]
                    elif n>1 and len(last_name[n-2])==1:
                        report[report.columns[i]][0] = last_name[n-1]
                    elif n>1 and len(last_name)==2:
                        report[report.columns[i]][0] = last_name[n-1]
                    else:
                        report[report.columns[i]][0] = ' '.join(last_name[n-2:n])
                elif len(re.findall('.[^A-Z]*', report[report.columns[i]][0]))>1 and str(report[report.columns[i]][0]).isupper() == False:
                    last_name = re.findall('.[^A-Z]*', report[report.columns[i]][0])
                    n = len(last_name)
                    if len(last_name[n-2])>3:
                        report[report.columns[i]][0] = last_name[n-1]
            if report.columns[i] == 'City(Owner)' or report.columns[i] == 'City(Company)' or report.columns[i] == 'City(Owner)1' or report.columns[i] == 'City(Owner)2':
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace(',','')
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('City','')
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('city','')
                if str(report[report.columns[i]][0])!='-':
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('-','')
            if report.columns[i] == 'State(Owner)' or report.columns[i] == 'State(Company)' or report.columns[i] == 'State(Owner)1' or report.columns[i] == 'State(Owner)2':
                count = 0
                for j in range(0,len(state_comparision)):
                    if report[report.columns[i]][0].lower().strip() == state_comparision['State'][j].lower().strip() or report[report.columns[i]][0].lower().strip() == state_comparision['State Abbreviation'][j].lower().strip():
                        report[report.columns[i]][0] =state_comparision['State Abbreviation'][j]
                        count = 1
                        break
                if count == 0:
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == 'Zip Code(Owner)' or report.columns[i] == 'Zip Code(Company)' or report.columns[i] == 'Zip Code(Owner)1' or report.columns[i] == 'Zip Code(Owner)2':
                # print('barathion',report[report.columns[i]][0])
                # zip_code = report[report.columns[i]][0]
                if re.search('[a-zA-Z/]',str(report[report.columns[i]][0])) and not re.search('[0-9]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] = '-'
            
            if report.columns[i] == "Doing Business As":
                if report[report.columns[i]][0]!='-':
                    report.loc[0,report.columns[i]] = re.sub('[-]',  '', str(report.loc[0,report.columns[i]])).strip().replace('RMATION','').replace('Name','')#20/08
                
                if 'dba' in str(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('dba','')
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('/','')
                if 'Entity Type' in str(report[report.columns[i]][0]):#22/07
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('Entity Type','')
                if 'Date of Incorporation' in str(report[report.columns[i]][0]):#22/07
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('Date of Incorporation','')
            report.to_csv('one.csv')
            # if report.columns[i] == "Zip Code(Owner)1":
            #     print('jilll')
            #     zipppp = report[report.columns[i]][0]
            #     print('zippp',zipppp)
            if '{{' in report[report.columns[i]][0] or '}}' in report[report.columns[i]][0]:
                print('report[report.columns[i]][0]',report[report.columns[i]][0])
                report[report.columns[i]][0] = '-'
            if report.columns[i] == "Company Name":
                print('lllll',report[report.columns[i]][0])
                if '[' in report[report.columns[i]][0]:
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('[','')
            if report.columns[i] == "Street Address(Owner)1":#04/06
                print('cleaner...',report[report.columns[i]][0])
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('[')[0]
                if 'ENOAH' in str(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('ENOAH','')
                if 'enoah' in str(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('enoah','')
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("['",' ')
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("']",'')
            if report.columns[i] == "Street Address(Company)":
                print('killer...',str(report[report.columns[i]][0]))
                report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('no PO Boxes','')
                if '[' in report[report.columns[i]][0]:
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('[','')
                if 'business' in str(report[report.columns[i]][0]):
                    
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('business','')       
            if report.columns[i] == "Date of Incorporation":
                if '0ct' in str(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('0ct','Oct')#28/08
                if report[report.columns[i]][0]!='-':
                    if '/' in report[report.columns[i]][0]:
                        report[report.columns[i]][0] = report[report.columns[i]][0].replace('b','').replace("Date", "")
                    try:
                        val = "%m%d%Y".replace(' ','').replace('[','').replace(']','').replace('Month','').replace('Year','').replace("⁰", "").replace("Date", "").replace('yy','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                        print('bharath....',report[report.columns[i]][0])
                    except:
                        print('bharath1111',report[report.columns[i]][0])
                        report.loc[0,report.columns[i]] = report.loc[0,report.columns[i]].replace('[','').replace(' ','').replace("Date", "").replace('month','').replace('year','').replace('Month','').replace('Year','').replace("⁰", "").replace('yy','')#30/07
            if report.columns[i] == "Date of Birth" or report.columns[i] == "Date of Birth1" or report.columns[i] == "Date of Birth2":
                if '0ct' in str(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('0ct','Oct').replace('D0B','')#28/08
                if report[report.columns[i]][0]!='-':
                    try:
                        report[report.columns[i]][0] = report[report.columns[i]][0].split('.')[1]#09/07
                        report[report.columns[i]][0] = report[report.columns[i]][0].replace('=','-').replace(' ','').replace('D0B','').replace('[','').replace(']','').replace('O','0').replace('com','').replace('Date','')#08/07
                        val = "%m%d%Y".replace(' ','')
                        d = datetime.strptime(report[report.columns[i]][0].strip(), val)
                        report[report.columns[i]][0] = d.strftime('%m/%d/%Y')
                        yourdate = dateutil.parser.parse(report[report.columns[i]][0])
                        datetimeobject = datetime.strptime(str(yourdate),'%Y-%m-%d  %H:%M:%S')
                        report[report.columns[i]][0] = datetimeobject.strftime('%m-%d-%Y')
                        new = date.today().strftime('%m-%d-%Y')
                        newdate1 = datetime.strptime(report[report.columns[i]][0], "%m-%d-%Y")
                        newdate2 = datetime.strptime(new, "%m-%d-%Y")
                        
                        if newdate1>newdate2:
                            report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace('-20','-19')
                    except:
                        report[report.columns[i]][0] = report[report.columns[i]][0].replace('Dateof','').replace('Date','').replace('D0B','')
            if report.columns[i] == "Business Tax ID":
                if report[report.columns[i]][0]!='-':
                    report.loc[0,report.columns[i]] = str(re.sub('[a-zA-Z:%.,() +?`_/]', '',report.loc[0,report.columns[i]]))
                    report.loc[0,report.columns[i]] = ''.join(e for e in report.loc[0,report.columns[i]] if e.isalnum())
                    report.loc[0,report.columns[i]] = report.loc[0,report.columns[i]][:2] + "-" + report.loc[0,report.columns[i]][2:]
            if report.columns[i] == "Social Security" or report.columns[i] == "Social Security1" or report.columns[i] == "Social Security2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = str(re.sub('[a-zA-Z:%.,() ?`+_/]', '',report[report.columns[i]][0]))
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    if len(report[report.columns[i]][0])<9:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(9)
                    # report[report.columns[i]][0] = report[report.columns[i]][0]
            if report.columns[i] == "Business Phone" or report.columns[i] == "Mobile Number" or report.columns[i] == "Mobile Number1" or report.columns[i] == "Mobile Number2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = str(re.sub('[a-zA-Z:%.,_?/]', '',report[report.columns[i]][0]))
                    report[report.columns[i]][0] = ''.join(e for e in report[report.columns[i]][0] if e.isalnum())
                    if len(report[report.columns[i]][0]) == 10:
                        report[report.columns[i]][0] = "(" + report[report.columns[i]][0][:3] + ")" + report[report.columns[i]][0][3:6]+"-"+report[report.columns[i]][0][6:]
                    elif len(report[report.columns[i]][0]) == 11 and '1' in report[report.columns[i]][0][0]:
                        report[report.columns[i]][0] = report[report.columns[i]][0][1:]
                        report[report.columns[i]][0] = "(" + report[report.columns[i]][0][:3] + ")" + report[report.columns[i]][0][3:6]+"-"+report[report.columns[i]][0][6:]
                    else:
                        report[report.columns[i]][0] = '-'
            if report.columns[i] == "Ownership Percentage of Business" or report.columns[i] == "Ownership Percentage of Business1" or report.columns[i] == "Ownership Percentage of Business2":
                if re.search('[a-zA-Z:%,/`? _|-]',str(report[report.columns[i]][0])):
                    report[report.columns[i]][0] =  re.sub('[a-zA-Z:%,/`? _|-]',  '', str(report[report.columns[i]][0]))
                # print("report[report.columns[i]][0]",[report[report.columns[i]][0]])
                if report[report.columns[i]][0]!='-' and report[report.columns[i]][0].strip()!='':
                    if re.search('[a-zA-Z:]',str(report[report.columns[i]][0])):
                        report[report.columns[i]][0] =  re.sub('[a-zA-Z:]',  '', str(report[report.columns[i]][0]))
                    if report[report.columns[i]][0]!='-' and report[report.columns[i]][0].strip()!='':
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("%",'')
                        report[report.columns[i]][0] = re.sub(' +', ' ', str(report[report.columns[i]][0]))
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).replace("50 50",'50')
                        print('stringggg', report[report.columns[i]][0])
                        report[report.columns[i]][0] = int(float(report[report.columns[i]][0].strip()))
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]) + "%"
            if report.columns[i] != "State(Company)" and report.columns[i] != "State(Owner)" and report.columns[i] != "Email" and report.columns[i] != "State(Owner)1" and report.columns[i] != "Email1" and report.columns[i] != "State(Owner)2" and report.columns[i] != "Email2":
                report.loc[0,report.columns[i]] = str(report.loc[0,report.columns[i]]).title()
            if report.columns[i] == "Email" or report.columns[i] == "Email1" or report.columns[i] == "Email2":
                if is_valid_email(report[report.columns[i]][0]):
                    report[report.columns[i]][0] = (report[report.columns[i]][0]).lower()
                else:
                    # report[report.columns[i]][0] = 'gftest@enoahisolution.com'
                    report[report.columns[i]][0] = '-'
            if report.columns[i] == "Zip Code(Company)":
                print("printing zip code company",report[report.columns[i]][0])
                print('smoke...',type(report[report.columns[i]][0]))
                if '[' in str(report[report.columns[i]][0]) and ']' in str(report[report.columns[i]][0]) and "''" in str(report[report.columns[i]][0]):
                    print('listing....')
                    zip = report[report.columns[i]][0]
                    zip = ast.literal_eval(zip)
                    print('problem',)
                    if len(zip)>1:
                        # if '-' not in zip[-1]:
                        report[report.columns[i]][0] = zip[-1]#20/06
                        print('print......',report[report.columns[i]][0])
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub(r"[^\d]",'',str(report[report.columns[i]][0])).strip()
                    print('sword11',str(report[report.columns[i]][0]))
                    if '-' not in str(report[report.columns[i]][0]) and len(report[report.columns[i]][0])>5:#17/06
                        report[report.columns[i]][0] = str(report[report.columns[i]][0])[0:5]
                        print('food11..',report[report.columns[i]][0])
                    if '-' in str(report[report.columns[i]][0]):
                        print('sword',str(report[report.columns[i]][0]))
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if ']' not in str(report[report.columns[i]][0]):
                        if len(report[report.columns[i]][0])<5:
                            report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    if len(report[report.columns[i]][0])>5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0])[-5:]
                    print('sword12',str(report[report.columns[i]][0]))  
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if Funder == 'Hylender':
                                report['City(Company)'][0] = city_com['city'][k]
                            if (Funder == 'Fundmate' or Funder == 'JCT Group LLC' or Funder == 'Regal Capital Group' or Funder == 'Fidelity Funding Group LLC' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'Flexibility Capital' or Funder == 'Clara Capital' or Funder == 'BBF_Better Business Funding' or Funder == 'Reliant_Funding' or Funder == 'Platform Funding' or Funder == 'MPF Inc' or Funder == '4 Pillar Consulting LLC' or Funder == 'Circadian Funding' or Funder == 'Kapitus' or Funder == 'August Funding Group' or Funder == 'Stream Capital' or Funder == 'Ace Capital Funding Source' or Funder == 'EMC Financial' or Funder == 'Empower Group' or Funder == 'Steady Capital Solutions' or Funder == 'AFFINITY BEYOND CAPITAL') and (report['City(Company)'][0].lower() in ''.join(city_com['city'][k].split()).lower()):
                                report['City(Company)'][0] = city_com['city'][k]
                            elif (Funder == 'PREMIUM MERCHANT FUNDING' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'Clara Capital' or Funder == 'August Funding Group' or Funder == 'Ace Capital Funding Source' or Funder == 'Circadian Funding'or Funder == 'Premium Merchant Funding' or Funder == 'Stream Capital') and city_com['state_id'][k] in report['State(Company)'][0]:
                                report['City(Company)'][0] = city_com['city'][k] 
                            if report['City(Company)'][0] == 'ENOAH' or report['City(Company)'][0] == '' or report['City(Company)'][0] == '-':
                                report['City(Company)'][0] = city_com['city'][k]
                            if report['State(Company)'][0] == 'ENOAH' or report['State(Company)'][0] == '' or report['State(Company)'][0] == '-' or len(report['State(Company)'][0]) <= 1 or (city_com['state_id'][k] in report['State(Company)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Company)'][0].lower()):
                                report['State(Company)'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,;[ .`?_+]','',str(report[report.columns[i]][0])).strip()
                    print("printing zip code owner",report[report.columns[i]][0])
                    zip = report[report.columns[i]][0]
                    zip = ast.literal_eval(zip)
                    if len(zip)>1:
                        if '-' not in zip[-1]:
                            report[report.columns[i]][0] = zip[-1]
                            print('print1......',report[report.columns[i]][0])
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if (Funder == 'Fundmate' or Funder == 'JCT Group LLC' or Funder == 'Fidelity Funding Group LLC' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'Flexibility Capital' or Funder == 'BBF_Better Business Funding' or Funder == 'Reliant_Funding' or Funder == 'Platform Funding' or Funder == 'MPF Inc' or Funder == '4 Pillar Consulting LLC' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Stream Capital' or Funder == 'Ace Capital Funding Source' or Funder == 'EMC Financial' or Funder == 'Empower Group' or Funder == 'Steady Capital Solutions' or Funder == 'AFFINITY BEYOND CAPITAL') and (report['City(Owner)'][0].lower() in ''.join(city_com['city'][k].split()).lower()):
                                report['City(Owner)'][0] = city_com['city'][k]
                            elif (Funder == 'PREMIUM MERCHANT FUNDING' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Ace Capital Funding Source' or Funder == 'Premium Merchant Funding' or Funder == 'Stream Capital') and city_com['state_id'][k] in report['State(Owner)'][0]:
                                report['City(Owner)'][0] = city_com['city'][k] 
                            if report['City(Owner)'][0] == 'ENOAH' or report['City(Owner)'][0] == '' or report['City(Owner)'][0] == '-':
                                report['City(Owner)'][0] = city_com['city'][k]
                            if report['State(Owner)'][0] == 'ENOAH' or report['State(Owner)'][0] == '' or report['State(Owner)'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)'][0].lower()):
                                report['State(Owner)'][0] = city_com['state_id'][k]
                            if (Funder == 'PREMIUM MERCHANT FUNDING' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Ace Capital Funding Source' or Funder == 'Premium Merchant Funding' or Funder == 'Stream Capital') and city_com['state_id'][k] in report['State(Owner)'][0]:
                                report['City(Owner)'][0] = city_com['city'][k] 
                            break
            if report.columns[i] == "Zip Code(Owner)1":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub(r"[^\d]",'',str(report[report.columns[i]][0])).strip()
                    print("printing zip code owner",report[report.columns[i]][0])
                    if '[' in str(report[report.columns[i]][0]) and ']' in str(report[report.columns[i]][0]) and "''" in str(report[report.columns[i]][0]):#17/06
                        zip = report[report.columns[i]][0]
                        zip = str(ast.literal_eval(zip))
                        if len(zip)>1:
                            if '-' not in zip[-1]:
                                report[report.columns[i]][0] = zip[-1]
                                print('print2......',report[report.columns[i]][0])
                    # if '-' not in str(report[report.columns[i]][0]) and len(report[report.columns[i]][0])>5:#17/06
                    #     report[report.columns[i]][0] = str(report[report.columns[i]][0])[0:5]
                    #     print('food..',report[report.columns[i]][0])
                    if '-' in str(report[report.columns[i]][0]):
                        print('notall',report[report.columns[i]][0])
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    if len(report[report.columns[i]][0])>5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0])[-5:]
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if (Funder == 'Fundmate' or Funder == 'JCT Group LLC' or Funder == 'Fidelity Funding Group LLC' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'Flexibility Capital' or Funder == 'BBF_Better Business Funding' or Funder == 'Reliant_Funding' or Funder == 'Platform Funding' or Funder == 'MPF Inc' or Funder == '4 Pillar Consulting LLC' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Stream Capital' or Funder == 'Ace Capital Funding Source' or Funder == 'EMC Financial' or Funder == 'Empower Group' or Funder == 'Steady Capital Solutions' or Funder == 'AFFINITY BEYOND CAPITAL') and (report['City(Owner)1'][0].lower() in ''.join(city_com['city'][k].split()).lower()):
                                report['City(Owner)1'][0] = city_com['city'][k]
                            elif (Funder == 'PREMIUM MERCHANT FUNDING' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Ace Capital Funding Source' or Funder == 'Premium Merchant Funding' or Funder == 'Stream Capital') and city_com['state_id'][k] in report['State(Owner)1'][0]:
                                report['City(Owner)1'][0] = city_com['city'][k] 
                            if report['City(Owner)1'][0] == 'ENOAH' or report['City(Owner)1'][0] == '' or report['City(Owner)1'][0] == '-':
                                report['City(Owner)1'][0] = city_com['city'][k]
                            if report['State(Owner)1'][0] == 'ENOAH' or report['State(Owner)1'][0] == '' or report['State(Owner)1'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)1'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)1'][0].lower()):
                                report['State(Owner)1'][0] = city_com['state_id'][k]
                            break
            if report.columns[i] == "Zip Code(Owner)2":
                if report[report.columns[i]][0]!='-':
                    report[report.columns[i]][0] = re.sub('[a-zA-Z/:,;[ `.?+_]','',str(report[report.columns[i]][0])).strip()
                    # zip = report[report.columns[i]][0]
                    # zip = ast.literal_eval(zip)
                    # if len(zip)>1:
                    #     if '-' not in zip[-1]:
                    #         report[report.columns[i]][0] = zip[-1]
                    #         print('print3......',report[report.columns[i]][0])
                    if '-' in str(report[report.columns[i]][0]):
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).split('-')[0]
                    if len(report[report.columns[i]][0])<5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0]).zfill(5)
                    if len(report[report.columns[i]][0])>5:
                        report[report.columns[i]][0] = str(report[report.columns[i]][0])[-5:]
                    for k in range(0,len(city_com)):
                        if int(city_com['zip'][k]) == int(report[report.columns[i]][0]):
                            if (Funder == 'Fundmate' or Funder == 'JCT Group LLC' or Funder == 'Fidelity Funding Group LLC' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'Flexibility Capital' or Funder == 'BBF_Better Business Funding' or Funder == 'Reliant_Funding' or Funder == 'Platform Funding' or Funder == 'MPF Inc' or Funder == '4 Pillar Consulting LLC' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Stream Capital' or Funder == 'Ace Capital Funding Source' or Funder == 'EMC Financial' or Funder == 'Empower Group' or Funder == 'Steady Capital Solutions' or Funder == 'AFFINITY BEYOND CAPITAL') and (report['City(Owner)2'][0].lower() in ''.join(city_com['city'][k].split()).lower()):
                                report['City(Owner)2'][0] = city_com['city'][k]
                            elif (Funder == 'PREMIUM MERCHANT FUNDING' or Funder == 'South Coast Capital' or Funder == 'SHIFT BUSINESS SOLUTIONS' or Funder == 'August Funding Group' or Funder == 'Circadian Funding' or Funder == 'Ace Capital Funding Source' or Funder == 'Premium Merchant Funding' or Funder == 'Stream Capital' or Funder == 'Nuwave') and city_com['state_id'][k] in report['State(Owner)2'][0]:
                                report['City(Owner)2'][0] = city_com['city'][k] 
                            if report['City(Owner)2'][0] == 'ENOAH' or report['City(Owner)2'][0] == '' or report['City(Owner)2'][0] == '-':
                                report['City(Owner)2'][0] = city_com['city'][k]
                            if report['State(Owner)2'][0] == 'ENOAH' or report['State(Owner)2'][0] == '' or report['State(Owner)2'][0] == '-' or (city_com['state_id'][k] in report['State(Owner)2'][0]) or (''.join(city_com['state_id'][k].split()).lower() in report['State(Owner)2'][0].lower()):
                                report['State(Owner)2'][0] = city_com['state_id'][k]                         
                            break
            if report.columns[i] == "Street Address(Owner)":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)'][0].lower()):
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].lower()
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].replace('0,','')
                        report['Street Address(Owner)'][0] = report['Street Address(Owner)'][0].title().strip()
                        break
                
                report['Street Address(Owner)'][0] = str(report['Street Address(Owner)'][0]).replace('no Po Boxes','').replace('()','')#20/08   
                report['Street Address(Owner)'][0] = re.sub('\s+',' ',report['Street Address(Owner)'][0])
                report['Street Address(Owner)'][0] = ''.join([s for s in report['Street Address(Owner)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)1":
                report.to_csv('Trust.csv')
                for k in range(0,len(city_com)):
                    # print('Concern...',report['Street Address(Owner)1'][0].lower())
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)1'][0].lower()):
                        print('entered...',city_com['city'][k], city_com['zip'][k], report['Street Address(Owner)1'][0])
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].lower()
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].replace('0,','')
                        report['Street Address(Owner)1'][0] = report['Street Address(Owner)1'][0].title().strip()
                        report.to_csv('Trust1.csv')
                        break
                print('weekend..',report['Street Address(Owner)1'][0])
                report['Street Address(Owner)1'][0] = str(report['Street Address(Owner)1'][0]).replace('No Po Boxes','').replace('()','')
                report['Street Address(Owner)1'][0] = re.sub('\s+',' ',report['Street Address(Owner)1'][0])
                report['Street Address(Owner)1'][0] = ''.join([s for s in report['Street Address(Owner)1'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)1'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)1'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Owner)2":
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Owner)2'][0].lower()):
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].lower()
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'') 
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].replace('0,','')
                        report['Street Address(Owner)2'][0] = report['Street Address(Owner)2'][0].title().strip()
                        break
                report['Street Address(Owner)2'][0] = re.sub('\s+',' ',report['Street Address(Owner)2'][0]).replace('()','')
                report['Street Address(Owner)2'][0] = ''.join([s for s in report['Street Address(Owner)2'][0].split(',') if s])
                datatosplit = str(report['Street Address(Owner)2'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                report['Street Address(Owner)2'][0] = ' '.join(datatosplit)
            if report.columns[i] == "Street Address(Company)":
                if 'Business Address' in str(report['Street Address(Company)'][0]) or 'Physical Address' in str(report['Street Address(Company)'][0]):
                    report['Street Address(Company)'][0] = str(report['Street Address(Company)'][0]).replace('Business Address','').replace('Physical Address','')
                for k in range(0,len(city_com)):
                    if ((str(city_com['city'][k]).strip()).lower() in str(report['Street Address(Company)'][0]).lower()) and ((str(city_com['zip'][k]).strip()).lower() in report['Street Address(Company)'][0].lower()):
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].lower()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['zip'][k]).lower().strip())),'') 
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_name'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['state_id'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace(((str(city_com['city'][k]).lower().strip())),'')
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].title().strip()
                        report['Street Address(Company)'][0] = report['Street Address(Company)'][0].replace('0,','')
                        break
                report['Street Address(Company)'][0] = re.sub('\s+',' ',report['Street Address(Company)'][0])
                report['Street Address(Company)'][0] = ''.join([s for s in report['Street Address(Company)'][0].split(',') if s])
                datatosplit = str(report['Street Address(Company)'][0]).split()
                for t in range(0,len(address_com)):
                    for s in range(0,len(datatosplit)):
                        if address_com['Address_Abbs'][t].strip().lower() == str(datatosplit[s]).strip().lower():
                            print("printing expansion",str(datatosplit[s]),address_com['Address_Abbs'][t])
                            datatosplit[s] = address_com['Expansion'][t]
                            break
                report['Street Address(Company)'][0] = ' '.join(datatosplit)
        report['Filename'] = tail
        df = pd.read_csv(dependencydir + "uszips.csv",on_bad_lines="skip", encoding="cp1252")
        for j in range(0,len(report)):
            for i in range(0,len(df)):
                if report['Zip Code(Owner)1'][j] == '-' or report['Zip Code(Owner)1'][j] == 'ENOAH' or report['Zip Code(Owner)1'][j] == '':
                    if report['City(Owner)1'][j].lower() == df['city'][i].lower() and report['State(Owner)1'][j].lower() == df['state_id'][i].lower():
                        report['Zip Code(Owner)1'][j] = int(df['zip'][i])
                        break
        json_data = report.to_json(orient="records")  # list of dicts
        # print('json_data',json_data)
        report.to_json("Application.json", orient="records", indent = 4)
        return report
    except:
        import traceback
        errortype = traceback.format_exc()
        print("errortype",errortype)
        report = pd.DataFrame([[tail,'-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-']],columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1'])
        return report