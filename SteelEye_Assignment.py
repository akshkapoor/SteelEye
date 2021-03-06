import requests
import xmltodict
import zipfile
import os
from pandas.io.json import json_normalize
import boto3

curr_dir=os.path.dirname(os.path.realpath(__file__))

url="https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"

response=requests.get(url)

with open('Root_XML','wb') as rxml:
    rxml.write(response.content)

with open('Root_XML', 'rb') as f:
    xml_content = xmltodict.parse(f)

str_list=xml_content['response']['result']['doc'][0]['str']

link=''
for item in str_list:
    if item['@name']=='download_link':
       link=item['#text']

if link:
    zipf=requests.get(link)
    with open('Zipfile.zip','wb') as zip_f:
        zip_f.write(zipf.content)

with zipfile.ZipFile('Zipfile.zip', 'r') as zip_ref:
    zip_ref.extractall(curr_dir)

filename=''
dir_list=os.listdir(curr_dir)
for file in dir_list:
    if file.startswith('DLTINS'):
        filename=file

filename=curr_dir+'\\'+filename
with open(filename, 'rb') as f:
    main_xml_dict = xmltodict.parse(f)
    
df=json_normalize(main_xml_dict)
df=df['BizData.Pyld.Document.FinInstrmRptgRefDataDltaRpt.FinInstrm']
df.to_csv('result.csv')

s3 = boto3.resource('s3')
BUCKET = "test"

s3.Bucket(BUCKET).upload_file("result.csv", "dump/file")

