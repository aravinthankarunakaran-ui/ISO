# In[Libraries]:
from flask import Flask, render_template, json, request, send_file,Response, jsonify
import requests
import os,signal
import pandas as pd
from flask_login import LoginManager,login_user,login_required,logout_user,current_user
import tempfile
import shutil
import logging
from logging.handlers import RotatingFileHandler
import cryptocode
from datetime import datetime
import io
import ast
import xlsxwriter
import glob 
from fnmatch import fnmatch
#import ray
#ray.init()
import sys
import traceback
from datetime import datetime
from datetime import date
import datetime as dts
from werkzeug.utils import secure_filename
import os
#import magic
import urllib.request
import ISO as iso
import dataModel as dm

from sqlalchemy import create_engine, MetaData,Table, Column, Integer, String,Boolean,DateTime
from sqlalchemy.orm import scoped_session, sessionmaker,mapper

# in[Postgres engine]
#engine = create_engine('postgresql://postgres:admin123@localhost:5432/iso_data',pool_pre_ping=True, convert_unicode=True, pool_size=20000, max_overflow=0)
# engine = create_engine('postgresql://postgres:eNoah@123@localhost:5432/iso_data',pool_pre_ping=True, convert_unicode=True, pool_size=20000, max_overflow=0)
# engine = create_engine('postgresql://postgres:eNoah@123@localhost:5432/iso_data',pool_pre_ping=True, convert_unicode=True, pool_size=20000, max_overflow=0)

engine = create_engine('postgresql://postgres:Mohana!01@localhost:5432/iso',
                       pool_pre_ping=True, 
                       pool_size=20000, 
                       convert_unicode=True,
                       max_overflow=0)  #convert_unicode=True,
#connection = engine.connect() 
metadata = MetaData()
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))

app = Flask(__name__)
#app.config["DEBUG"] = True
#app.config['JSON_SORT_KEYS'] = False

app.secret_key = b'\xaa\t$)\xf7\xee\xef\xa2)\x8bY\xf6QN\xed\xf2'

# In[Login Manager]
login_manager = LoginManager()
login_manager.init_app(app)

login_manager.login_view = "/login"
login_manager.session_protection = "strong"

@login_manager.user_loader
def load_user(userid):
    return dm.User.query.get(userid)
    # return dm.session.get(dm.User, userid)

# In[Error Handling]:
if app.debug is not True:   
    
    file_handler = RotatingFileHandler('errorlog.log', maxBytes=1024 * 1024 * 100, backupCount=2000)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("\n-------------------------------------------------------------------------\n Datetime - %(asctime)s & Filename - %(name)s & Type - %(levelname)s \n-------------------------------------------------------------------------\n %(message)s")
    file_handler.setFormatter(formatter)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.DEBUG)

# In[Login]:
@app.route("/login")
def main():
    
    #return render_template(app.config['SUMMARY_REPORT_URL'],data = json.dumps(jsonData, sort_keys=False))
    return render_template('login_iso.html')

# In[Logout]:
@app.route("/logout",methods=['GET','POST'])
@login_required
def logout():
    
    logout_user()
    return {'redirect':'true','redirect_url':'/login'}

# In[validate user]:
# Login Functionality Start
# In[validate user]:
# Login Functionality Start
@app.route("/validate_user",methods=['GET','POST'])
def validate_users():
    
    data = json.loads(request.get_data())
    print(data)
    username = data['username']
    password = data['password']
    user = dm.User.query.filter_by(username = username).first()
    # user = pd.read_sql_query("select * from userhistory where username = '"+username+"'" ,connection)
    print(user)
    if (user.password == password):
        login_user(user)
        return {'redirect':'true','redirect_url':'/summary'}
    else:
        return "failure"
 # Login Functionality End 
# In[summary]:
@app.route("/summary")
@login_required
def summary():
    
    return render_template("isodata.html",data = json.dumps({"username":str(current_user)}))

# Login Page
@app.route("/", methods = ['POST', 'GET'])
def starting_url():
    return render_template("isodata.html")

ALLOWED_EXTENSIONS = set(['txt', 'pdf'])

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/file_upload_1', methods = ['POST', 'GET'])
#@login_required
def file_upload_1():
    # files = request.files.getlist('files[]')
    files = request.files.getlist('file')
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    print("currentuser",current_user)
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    types = ('*.pdf','*.PDF') # the tuple of file types
    pdfs= []
    for files in types:
        #to fetch all files from temp directory of all mentioned types
        pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
    data_final = pd.DataFrame()
    pdfs= list(dict.fromkeys(pdfs))
    for i in range(0,len(pdfs)):
        data = iso.fetch_data(pdfs[i])
        print("data",data)
        data_final = pd.concat([data_final,data],ignore_index= True)
    data_final = data_final.rename_axis('S.No').reset_index()
    data_todb = data_final.copy()
    data_todb['username'] = str(current_user)
    #data_todb.columns = ['sno','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','username']
    #engine.execute("delete from extracted_iso_data where username = '"+str(current_user)+"'")
    #data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
    JSON =data_final.to_json(orient='records')
    shutil.rmtree(temp_dir)
    data = {"data":JSON}
    return json.dumps(data,sort_keys=False)
    
    # JSON =json.loads(data_final.to_json(orient='records'))
    # summary= json.dumps(JSON,sort_keys=False)
    # shutil.rmtree(temp_dir)
    # print(summary)
    # return summary

# In[uploadfolderpath]:
@app.route("/upload_folderpath")
@login_required
def upload_folderpath():
    return {'redirect':'true','redirect_url':'/path'}
@app.route("/path")
@login_required
def path():
    return render_template('uploadfolderpath.html')


def cal_time(seconds):
    from datetime import date
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    date = date.today().strftime("%Y-%m-%d")
    estimated_time = date + " %d:%02d:%02d" % (hour, minutes, seconds)
    estimated_time = datetime.strptime(estimated_time, '%Y-%m-%d %H:%M:%S')
    return estimated_time

# In[To process summary]:
@app.route('/process_summary_folder', methods=['GET','POST'])
@login_required
def process_summary_folder(): 
    print("ssss")  
    data = json.loads(request.get_data())
    filepath = data['filepath']
    print("filepath",filepath)
    estimated_time='0'
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    upload_id=to_insert_batch_details(data,estimated_time)
    print("upload_id",upload_id)
    if filepath!='':
        pdfs= []
        for path, subdirs,files in os.walk(filepath):
            for name in files:
                report_dir=os.path.join(path, name)
                types = ('*.pdf', '*.PDF') # the tuple of file types
                for files in types:
                    pdfs.extend(glob.glob(os.path.join(path, name)))
                estimated_time = cal_time(len(pdfs)*5) 
                print('wwwwwwwwwwwwwwwww',estimated_time)  
                to_update_batch_estimated_time(str(estimated_time),filepath)
            data_final = pd.DataFrame(columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1','First Name2','Last Name2','Date of Birth2','Social Security2','Mobile Number2','Email2','Ownership Percentage of Business2','Street Address(Owner)2','City(Owner)2','State(Owner)2','Zip Code(Owner)2'])
            pdfs= list(dict.fromkeys(pdfs))
            for pdf in pdfs:
                print("pdf",pdf)
                data = iso.fetch_data(pdf)
                data_final = pd.concat([data_final,data],ignore_index= True)
            data_final = data_final.rename_axis('S.No').reset_index()
            data_final = data_final.fillna('-')
            # changing date format only to the particular user loggin.
            if str(current_user) == 'Ganesh':
                for i in range(0,len(data_final)):
                    data_final['Date of Incorporation'][i] = iso.date_format_change(data_final['Date of Incorporation'][i])
                    data_final['Date of Birth1'][i] = iso.date_format_change(data_final['Date of Birth1'][i])
                    if data_final['Date of Birth2'][i] != '-':
                        data_final['Date of Birth2'][i] = iso.date_format_change(data_final['Date of Birth2'][i])
            data_todb = data_final.copy()
            data_todb['filepath'] = filepath
            data_todb['username'] = str(current_user)
            if len(data_todb.columns)<35:
                data_todb.columns = ['sno','filepath','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filepath','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                # engine.execute("delete from extracted_iso_data where username = '"+str(current_user)+"'")
                # data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
                # data_todb.to_sql('extracted_iso_data_duplicates', engine,if_exists='append',index=False)
            else:
                data_todb.columns = ['sno','filepath','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filepath','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                # engine.execute("delete from extracted_iso_data_dualowners where username = '"+str(current_user)+"'")
                # # data_todb.to_csv("datatodb.csv")
                # data_todb.to_sql('extracted_iso_data_dualowners', engine,if_exists='append',index=False)
                # data_todb.to_sql('extracted_iso_data_duplicates_dualowners', engine,if_exists='append',index=False) 

            # data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','filepath','username']
            # aa = engine.execute("select from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'")
            # aa = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'", connection)
            # print('bfr_deletion',aa)
            with engine.connect() as conn:
                engine.execute("delete from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'")
            # bb = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'", connection)
            # print('afr_deletion',bb)
            #with engine.begin() as conn:
                data_todb.to_sql('extracted_iso_data_dualowners_bulk', conn,if_exists='append',index=False,method='multi')
                data_todb.to_sql('extracted_iso_data_duplicates_dualowners_bulk', conn,if_exists='append',index=False,method='multi')
            # cc = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'", connection)
            # print('afr_insertion',cc)
            print("data_final",data_final)
            if str(current_user) == 'Ganesh':
                #removing some columns as requested not to view in auto download csv/json
                data_final = data_final.drop(columns=['Filename','ISO Name','Business Tax ID','Social Security1','Social Security2','Ownership Percentage of Business1','Ownership Percentage of Business2','Business Email','Proceeds','Amount'])
                # insert default value to Industry as requested by Ganesh.....
                # data_final['Industry'] = 'Working Capital'
            data_final.to_csv(os.path.join(path ,'extracted_data.csv'))
            data_json = data_final.to_dict(orient='records')
            json_content = json.dumps(data_json, sort_keys=False)
            user_filename = os.path.join(path ,'extracted_json.json')
            with open(user_filename, 'w') as file:
                # Write your input to the file
                file.write(json_content)


    # start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    # end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    # estimated_time = estimated_time - dts.timedelta(seconds=180)
    # datetime_obj = datetime.strptime(str(estimated_time), "%Y-%m-%d %H:%M:%S")
    # time = datetime_obj.time()
    # to_update_batch_estimated_time(str(time),filepath)

    start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    time_taken = str(end - start)
    estimated_time = estimated_time - dts.timedelta(seconds=int((end-start).total_seconds()))
    datetime_obj = datetime.strptime(str(estimated_time), "%Y-%m-%d %H:%M:%S")
    time = datetime_obj.time()
    to_update_batch_estimated_time(str(time),filepath)
    to_update_folder_status(upload_id)
    return 'success'

# In[To insert batch details]:   
def to_insert_batch_details(data,estimated_time):
    filepath = data['filepath']
    status = 'Processing...'
    document = dm.uploadfolderpathmaster (data['filepath'],'Processing...',estimated_time)
    dm.db_session.add(document)
    dm.db_session.flush()
    dm.db_session.commit()
    return document.upload_id

# In[To insert batch details time]:   
def to_update_batch_estimated_time(estimated_time,filepath):
    estimated_time = estimated_time
    filepath = str(filepath)    
    dm.engine.execute("UPDATE uploadfolderpathmaster SET estimated_time='"+estimated_time+"' WHERE filepath = '"+filepath+"'")
    return "Success"

# In[to_update_folder_status]:   
def to_update_folder_status(upload_id):
    status = 'Completed'    
    upload_id=str(upload_id)
    dm.engine.execute("UPDATE uploadfolderpathmaster SET status='"+status+"' WHERE upload_id='"+upload_id+"'")
    return "Success"
    
def to_select_upload_estimated_time(data):
    filepath = data['filepath']    
    data = pd.read_sql_query("select estimated_time from uploadfolderpathmaster where filepath= '"+filepath+"'" ,dm.connection)         
    myDict=data.to_dict()
    myList = [myDict [i][0] for i in sorted(myDict.keys())]
    data = {"estimated_time" : myList[0]}
    return json.dumps(data,sort_keys=False)
 #In[get_estimated_time]:
@app.route("/get_estimated_time",methods=['GET','POST'])
@login_required
def get_estimated_time():
    data = json.loads(request.get_data())
    data = to_select_upload_estimated_time(data)
    return data

@app.route('/file_upload_api_1', methods = ['POST', 'GET'])
def file_upload_api_1():
    images = request.files.getlist('file')
    files_password= request.files.getlist('password')
    user = request.form['username']
    user = cryptocode.encrypt(user,"password_1")
    files = []
    for image in images:
        files.append(("file", (image.filename, image.read(), image.content_type)))
    for image in files_password:
        files.append(("password", (image.filename, image.read(), image.content_type)))
    temp = {"username":user}
    files.append(('username', ('username', json.dumps(temp), 'application/json')))
    res = requests.post(url="https://iso_extraction.eisipl.com:5002/file_upload",files=files, verify=False)
    print(res.json())
    return json.dumps(res.json(),sort_keys=False)
@app.route('/file_upload_api_1_old', methods = ['POST', 'GET'])
def file_upload_api_1_old():
    
    images = request.files.getlist('file')
    user = request.form['user']
    password = request.form['password']
    user = cryptocode.encrypt(user,"password_1")
    password= cryptocode.encrypt(password,"password_2")
    print(user,password)
    print("images",images)
    files = []
    for image in images:
        files.append(("file", (image.filename, image.read(), image.content_type)))
    temp = {"user":user,"password":password}
    files.append(('user', ('user', json.dumps(temp), 'application/json')))

    res = requests.post(url="https://iso_extraction.eisipl.com:5002/file_upload",files=files, verify=False)
    # res = requests.post(url="https://10.0.9.145:5002/file_upload",files=files)
    # res = requests.post(url="http://10.0.9.144:5002/file_upload_API",files=files)
    print("mail",res)
    print("mail_1",res.json())
    return json.dumps(res.json(),sort_keys=False)

@app.route('/file_upload_API', methods = ['POST', 'GET'])
def file_upload_API():
    
    files = request.files.getlist('images')
    data = json.load(request.files['user'])
    
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    print(files)
    for file in files:       
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    current_user = data['b']
    print("current_user",current_user)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
        data_final = pd.DataFrame(columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1','First Name2','Last Name2','Date of Birth2','Social Security2','Mobile Number2','Email2','Ownership Percentage of Business2','Street Address(Owner)2','City(Owner)2','State(Owner)2','Zip Code(Owner)2'])
        pdfs= list(dict.fromkeys(pdfs))
        for i in range(0,len(pdfs)):
            data = iso.fetch_data(pdfs[i])
            print("data",data)
            data_final = pd.concat([data_final,data],ignore_index= True)
        data_final = data_final.rename_axis('S.No').reset_index()
        data_final = data_final.fillna('-')
        data_todb = data_final.copy()
        data_todb['username'] = str(current_user)
        
        
        if len(data_todb.columns)<30:
            data_todb.columns = ['sno','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','username']
            engine.execute("delete from extracted_iso_data where username = '"+str(current_user)+"'")
            data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
            data_todb.to_sql('extracted_iso_data_duplicates', engine,if_exists='append',index=False)
        else:
            data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','username']
            engine.execute("delete from extracted_iso_data_dualowners where username = '"+str(current_user)+"'")
            data_todb.to_sql('extracted_iso_data_dualowners', engine,if_exists='append',index=False)
            data_todb.to_sql('extracted_iso_data_duplicates_dualowners', engine,if_exists='append',index=False)            
        JSON =data_final.to_json(orient='records')
        shutil.rmtree(temp_dir)
        data = {"data":JSON}
        print("data_final",data)
        return json.dumps(data,sort_keys=False)
        # return jsonify(data)
    except:
        shutil.rmtree(temp_dir)
        summary = {"data":"failure"}
        print("summary",summary)
        return summary
        # return jsonify(summary)

@app.route('/file_upload', methods = ['POST', 'GET'])
@login_required
def file_upload():
    #files = request.files.getlist('files[]')
    files = request.files.getlist('file')
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    print(files)
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
        data_final = pd.DataFrame(columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1','First Name2','Last Name2','Date of Birth2','Social Security2','Mobile Number2','Email2','Ownership Percentage of Business2','Street Address(Owner)2','City(Owner)2','State(Owner)2','Zip Code(Owner)2'])
        pdfs= list(dict.fromkeys(pdfs))

        for i in range(0,len(pdfs)):
            data = iso.fetch_data(pdfs[i])
            print("data",data)
            data_final = pd.concat([data_final,data],ignore_index= True)
            Funder = data['ISO Name'].iloc[0] if 'ISO Name' in data.columns else None
            try:
                from pypdf import PdfReader, PdfWriter
                pdf_reader = PdfReader(pdfs[i])
                metadata = pdf_reader.metadata
                Producer = metadata.get('/Producer', 'Unknown')  # Extract PDF producer
                print(f"PDF Metadata Producer: {Producer}")
            except Exception as e:
                print(f"Error extracting PDF metadata: {e}")
                Producer = "Unknown"
            if Funder and Producer:
                engine.execute('INSERT INTO producer (funder, producer) VALUES (%s, %s)', (Funder, Producer))
                print(f"Inserted into producer table: Funder - {Funder}, Producer - {Producer}")

        data_final = data_final.rename_axis('S.No').reset_index()
        data_final = data_final.fillna('-')
        # changing date format only to the particular user loggin.
        if str(current_user) == 'Ganesh':
            for i in range(0,len(data_final)):
                data_final['Date of Incorporation'][i] = iso.date_format_change(data_final['Date of Incorporation'][i])
                data_final['Date of Birth1'][i] = iso.date_format_change(data_final['Date of Birth1'][i])
                if data_final['Date of Birth2'][i] != '-':
                    data_final['Date of Birth2'][i] = iso.date_format_change(data_final['Date of Birth2'][i])

        data_todb = data_final.copy()
        data_todb['username'] = str(current_user)
    
        # data_todb.to_csv('todbdata.csv')
        if len(data_todb.columns)<35:
            print('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
            data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
            exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
            data_todb = data_todb.reindex(exc_col, axis=1)
            current_user_str = str(current_user)
            print(f"Executing SQL: delete from extracted_iso_data_dualowners where username = {current_user_str}")  
            print(f"Parameters: {current_user_str}")  
            engine.execute("delete from extracted_iso_data where username = %s", (current_user_str,))
            data_to_insert = data_todb.fillna('').values.tolist()
            for row in data_to_insert:
                engine.execute('INSERT INTO extracted_iso_data (sno, filename, isoname, companyname, dbaname, taxid, '
                           'startdate, streetaddress, city_company, state_company, zip_company, phone, firstname, '
                           'lastname, dob, ssn, mobile, email, ownership, homeaddress, city_owner, state_owner, '
                           'zip_owner, entity_type, industry, business_email, proceeds, amount, username) VALUES %s',
                           (tuple(row)))
            engine.execute("delete from extracted_iso_data_duplicates where username = %s" , (current_user_str,)) 
        
            data_to_insert_duplicates = data_todb.fillna('').values.tolist()
            for row in data_to_insert_duplicates:
                engine.execute('INSERT INTO extracted_iso_data_duplicates (sno, filename, isoname, companyname, '
                           'dbaname, taxid, startdate, streetaddress, city_company, state_company, zip_company, '
                           'phone, firstname, lastname, dob, ssn, mobile, email, ownership, homeaddress, city_owner, '
                           'state_owner, zip_owner, entity_type, industry, business_email, proceeds, amount, username) VALUES %s',
                           (tuple(row)))
        
            #data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
            #data_todb.to_sql('extracted_iso_data_duplicates', engine,if_exists='append',index=False)
        else:
            print('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',len(data_todb.columns))
            print('ffffffffffffffffffffffffff',data_todb)
            print("Actual column count:", len(data_todb.columns))
            print("Column names before renaming:", data_todb.columns.tolist())

            missing_columns = ['entity_type','industry','business_email','proceeds','amount']
            for col in missing_columns:
                if col not in data_todb.columns:
                    data_todb[col] = None
            if len(data_todb.columns) > 40:
                data_todb = data_todb.iloc[:, :40]  

            data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
            exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
            data_todb = data_todb.reindex(exc_col, axis=1)
            current_user_str = str(current_user)
            print('gggggggggggggggggggggggg',data_todb)
            engine.execute("delete from extracted_iso_data_dualowners where username = %s", (current_user_str,))
            data_to_insert_dualowners = data_todb.fillna('').values.tolist()
            #for row in data_to_insert_dualowners:
                #engine.execute('INSERT INTO extracted_iso_data_dualowners (column1, column2, ..., columnN) VALUES %s', (tuple(row)))
            #engine.execute("delete from extracted_iso_data_duplicates_dualowners where username = %s" , (current_user_str,))
            
            data_to_insert_duplicates_dualowners = data_todb.fillna('').values.tolist()
            #for row in data_to_insert_duplicates_dualowners:
            #    engine.execute('INSERT INTO extracted_iso_data_duplicates_dualowners (column1, column2, ..., columnN) VALUES %s', (tuple(row)))
            
            #data_todb.to_csv("datatodb.csv")
            #data_todb.to_sql('extracted_iso_data_dualowners', engine, if_exists='append', index=False)
            #data_todb.to_sql('extracted_iso_data_duplicates_dualowners', engine, if_exists='append', index=False)
        print('cccccccccccccccccccccccccccccccccc',data_final)
        JSON =data_final.to_json(orient='records')
        shutil.rmtree(temp_dir)
        data = {"data":JSON}
        return json.dumps(data,sort_keys=False)
    except Exception as e:
        import traceback
        errortype = traceback.format_exc()
        print('dddddddddddddddddddddddd', e,errortype)
        shutil.rmtree(temp_dir)
        summary = {"data":"failure"}
        print("summary",summary)
        return summary
  




@app.route('/s3_sf_toget_funder_list', methods = ['POST', 'GET'])
def s3_sf_toget_funder_list():
    Funder = ''
    application_pdfs = []
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    files = request.files.getlist('files')
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
            pdfs= list(dict.fromkeys(pdfs))
        for k in range(0,len(pdfs)):
            Funder = iso.template_check(pdfs[k])
            if Funder!='No format found':
                application_pdfs.append(pdfs[k])
                break
    except:
        application_pdfs = application_pdfs
    return json.dumps({'data':application_pdfs})
@app.route('/s3_sf_file_upload', methods = ['POST', 'GET'])
def s3_sf_file_upload():
    #files = request.files.getlist('files[]')
    current_user = 'Select_Funding_S3'
    files = request.files.getlist('files')
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    print(files)
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
        data_final = pd.DataFrame(columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1','First Name2','Last Name2','Date of Birth2','Social Security2','Mobile Number2','Email2','Ownership Percentage of Business2','Street Address(Owner)2','City(Owner)2','State(Owner)2','Zip Code(Owner)2'])
        pdfs= list(dict.fromkeys(pdfs))
        for i in range(0,len(pdfs)):
            data = iso.fetch_data_s3(pdfs[i])
            print("data",data)
            data_final = pd.concat([data_final,data],ignore_index= True)
        data_final = data_final.rename_axis('S.No').reset_index()
        data_final = data_final.fillna('-')
        # changing date format only to the particular user loggin.
        if str(current_user) == 'Ganesh':
            for i in range(0,len(data_final)):
                data_final['Date of Incorporation'][i] = iso.date_format_change(data_final['Date of Incorporation'][i])
                data_final['Date of Birth1'][i] = iso.date_format_change(data_final['Date of Birth1'][i])
                if data_final['Date of Birth2'][i] != '-':
                    data_final['Date of Birth2'][i] = iso.date_format_change(data_final['Date of Birth2'][i])

        data_todb = data_final.copy()
        data_todb['username'] = str(current_user)
        # data_todb.to_csv('todbdata.csv')
        try:
            if len(data_todb.columns)<35:
                data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                engine.execute("delete from extracted_iso_data where username = '"+str(current_user)+"'")
                data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
                data_todb.to_sql('extracted_iso_data_duplicates', engine,if_exists='append',index=False)
            else:
                data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                engine.execute("delete from extracted_iso_data_dualowners where username = '"+str(current_user)+"'")
                # data_todb.to_csv("datatodb.csv")
                data_todb.to_sql('extracted_iso_data_dualowners', engine,if_exists='append',index=False)
                data_todb.to_sql('extracted_iso_data_duplicates_dualowners', engine,if_exists='append',index=False)  
        except:
            print("DB insertion")
        JSON =data_final.to_json(orient='records')
        shutil.rmtree(temp_dir)
        data = {"data":JSON}
        print("data",data)
        return json.dumps(data,sort_keys=False)
        # return data
    except Exception as e:
        import traceback
        errortype = traceback.format_exc()
        print("error@@@@@@@@@@@@@@",e, errortype)
        shutil.rmtree(temp_dir)
        summary = {"data":"failure"}
        print("summary",summary)
        return 'failure'
@app.route('/s3_fc_toget_funder_list', methods = ['POST', 'GET'])
def s3_fc_toget_funder_list():
    Funder = ''
    application_pdfs = []
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    files = request.files.getlist('files')
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
            pdfs= list(dict.fromkeys(pdfs))
        for k in range(0,len(pdfs)):
            Funder = iso.template_check(pdfs[k])
            if Funder!='No format found':
                application_pdfs.append(pdfs[k])
                break
    except:
        application_pdfs = application_pdfs
    return json.dumps({'data':application_pdfs})
@app.route('/s3_fc_file_upload', methods = ['POST', 'GET'])
def s3_fc_file_upload():
    #files = request.files.getlist('files[]')
    current_user = 'Flexibility_Capital_S3'
    files = request.files.getlist('files')
    temp_dir = tempfile.mkdtemp() 
    os.chmod(temp_dir, 0o777)
    print(files)
    for file in files:          
        bad_chars = [';', ':', '!', "*","'",'"','%']
        for i in bad_chars :
            file.filename =  file.filename.replace(i, '')
            print("file",file.filename)
        file.save(os.path.join(temp_dir, file.filename))
    print("temp_dir",temp_dir)
    try:
        types = ('*.pdf','*.PDF') # the tuple of file types
        pdfs= []
        for files in types:
            #to fetch all files from temp directory of all mentioned types
            pdfs.extend(glob.glob(os.path.join(str(temp_dir),files)))
        data_final = pd.DataFrame(columns = ['Filename','ISO Name','Company Name','Doing Business As','Business Tax ID','Date of Incorporation','Street Address(Company)','City(Company)','State(Company)','Zip Code(Company)','Business Phone','First Name1','Last Name1','Date of Birth1','Social Security1','Mobile Number1','Email1','Ownership Percentage of Business1','Street Address(Owner)1','City(Owner)1','State(Owner)1','Zip Code(Owner)1','First Name2','Last Name2','Date of Birth2','Social Security2','Mobile Number2','Email2','Ownership Percentage of Business2','Street Address(Owner)2','City(Owner)2','State(Owner)2','Zip Code(Owner)2'])
        pdfs= list(dict.fromkeys(pdfs))
        for i in range(0,len(pdfs)):
            data = iso.fetch_data_s3(pdfs[i])
            print("data",data)
            data_final = pd.concat([data_final,data],ignore_index= True)
        data_final = data_final.rename_axis('S.No').reset_index()
        data_final = data_final.fillna('-')
        # changing date format only to the particular user loggin.
        if str(current_user) == 'Ganesh':
            for i in range(0,len(data_final)):
                data_final['Date of Incorporation'][i] = iso.date_format_change(data_final['Date of Incorporation'][i])
                data_final['Date of Birth1'][i] = iso.date_format_change(data_final['Date of Birth1'][i])
                if data_final['Date of Birth2'][i] != '-':
                    data_final['Date of Birth2'][i] = iso.date_format_change(data_final['Date of Birth2'][i])

        data_todb = data_final.copy()
        data_todb['username'] = str(current_user)
        # data_todb.to_csv('todbdata.csv')
        try:
            if len(data_todb.columns)<35:
                data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname','lastname','dob','ssn','mobile','email','ownership','homeaddress','city_owner','state_owner','zip_owner','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                engine.execute("delete from extracted_iso_data where username = '"+str(current_user)+"'")
                data_todb.to_sql('extracted_iso_data', engine,if_exists='append',index=False)
                data_todb.to_sql('extracted_iso_data_duplicates', engine,if_exists='append',index=False)
            else:
                data_todb.columns = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                exc_col = ['sno','filename','isoname','companyname','dbaname','taxid','startdate','streetaddress','city_company','state_company','zip_company','phone','firstname1','lastname1','dob1','ssn1','mobile1','email1','ownership1','homeaddress1','city_owner1','state_owner1','zip_owner1','firstname2','lastname2','dob2','ssn2','mobile2','email2','ownership2','homeaddress2','city_owner2','state_owner2','zip_owner2','entity_type','industry','business_email','proceeds','amount','username']
                data_todb = data_todb.reindex(exc_col, axis=1)
                engine.execute("delete from extracted_iso_data_dualowners where username = '"+str(current_user)+"'")
                # data_todb.to_csv("datatodb.csv")
                data_todb.to_sql('extracted_iso_data_dualowners', engine,if_exists='append',index=False)
                data_todb.to_sql('extracted_iso_data_duplicates_dualowners', engine,if_exists='append',index=False)  
        except:
            print("DB insertion")
        JSON =data_final.to_json(orient='records')
        shutil.rmtree(temp_dir)
        data = {"data":JSON}
        print("data",data)
        return json.dumps(data,sort_keys=False)
        # return data
    except Exception as e:
        import traceback
        errortype = traceback.format_exc()
        print("error@@@@@@@@@@@@@@",e, errortype)
        shutil.rmtree(temp_dir)
        summary = {"data":"failure"}
        print("summary",summary)
        return 'failure'
# In[doenload report]:
@app.route("/download_report",methods=['GET','POST'])
def download_report():
    #excel = pd.read_csv("data_final.csv",index_col = 0)
    excel = pd.read_sql_query("select * from extracted_iso_data_dualowners where username = '"+str(current_user)+"'",connection) 
    excel.drop("username", axis=1, inplace=True)
    print("after data%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%",excel)
    user_filename = str(current_user)+"_iso_report.csv"
    return Response(excel.to_csv(index=False),mimetype="text/xlsx",headers={"Content-disposition":"attachment; filename=iso_report.csv"})
   
@app.route("/download_report_xml",methods=['GET','POST'])
def download_report_xml():
    #df = pd.read_csv("data_final.csv",index_col=0)
    df = pd.read_sql_query("select * from extracted_iso_data_dualowners where username = '"+str(current_user)+"'",connection) 
    df.drop("username", axis=1, inplace=True)
    print("xml",df)
    # if os.path.exists('iso_report.xml') == True:
    if fnmatch('/','iso_report.xml') == True:
        print("*********")
        os.remove("iso_report.xml")
    def func(row):
        xml = ['<item>']
        for field in row.index:
            xml.append('  <field name="{0}">{1}</field>'.format(field, row[field]))
        xml.append('</item>')
        return '\n'.join(xml)

    req_format = ' '.join(df.apply(func, axis=1))
    with open(str(current_user) + "_iso_report.xml", "w") as f:
        f.write(req_format)
    path = str(current_user) + "_iso_report.xml"
    # return send_file(path, as_attachment=True,cache_timeout=0)
    return send_file(path, as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


def to_select_subfolder_report(data):
    filepath = data['filepath']    
    data = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'",dm.connection)        
    data.drop("username", axis=1, inplace=True)
    data.drop("filepath", axis=1, inplace=True)
    data = data.to_dict(orient='records')  
    return json.dumps(data,sort_keys=False)

#In[Get Report Documenthistory data]:
@app.route("/get_subfolder_report",methods=['GET','POST'])
@login_required
def get_subfolder_report():
    data = json.loads(request.get_data())
    data = to_select_subfolder_report(data)
    return data

# In[doenload report]:
@app.route("/download_subfolder_report",methods=['GET','POST'])
def download_subfolder_report():
    filepath =  request.args.get('filepath')
    excel = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'",connection) 
    excel.drop("username", axis=1, inplace=True)
    excel.drop("filepath", axis=1, inplace=True)
    print("after data%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%",excel)
    user_filename = str(current_user)+"_iso_report.csv"
    return Response(excel.to_csv(index=False),mimetype="text/xlsx",headers={"Content-disposition":"attachment; filename=iso_report.csv"})

# In[doenload report]:
@app.route("/export_to_crm",methods=['GET','POST'])
def export_to_crm():
    excel = pd.read_sql_query("select * from extracted_iso_data_dualowners where username = '"+str(current_user)+"'",connection) 
    excel.drop("username", axis=1, inplace=True)
    excel = excel.to_dict(orient='records')
    return json.dumps(excel,sort_keys=False)

# In[download report] json:
@app.route("/download_to_json", methods=['GET','POST'])
def download_to_json():
    filepath =  request.args.get('filepath')
    excel = pd.read_sql_query("select * from extracted_iso_data_dualowners_bulk where username = '"+str(current_user)+"' and filepath = '"+filepath+"'", connection) 
    excel.drop("username", axis=1, inplace=True)
    excel.drop("filepath", axis=1, inplace=True)
    excel = excel.to_dict(orient='records')
    json_content = json.dumps(excel, sort_keys=False)
    user_filename = str(current_user) + "_bulk_json.json"  # Changed file extension to .json or .text

    # Create a response with JSON content as an attachment
    response = Response(json_content, content_type='application/json')
    response.headers['Content-Disposition'] = 'attachment; filename=' + user_filename

    return response

if __name__ == "__main__":
    app.run( host='0.0.0.0',port=5002, debug=False, threaded=True) # host='0.0.0.0',
