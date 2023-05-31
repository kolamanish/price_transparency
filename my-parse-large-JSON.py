import boto3,sys,os,JSON,iJSON,decimal,io,jmespath,gzip
import pandas as pd
import awswrangler as wr
from datetime import datetime
from boto3 import Session
print( 'Start Time : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

AWS_PROFILE="default"
ACCESS_KEY = Session().get_credentials().get_frozen_credentials().access_key
SECRET_KEY = Session().get_credentials().get_frozen_credentials().secret_key
SESSION_TOKEN = Session().get_credentials().get_frozen_credentials().token
AWS_REGION = "us-east-1"

boto3_session = boto3.session.Session()

s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY,aws_session_token=SESSION_TOKEN)
app_bucket = "my-app-bucket"

filename='input/largezipJSON/uhc_ps1_50_c2.JSON.gz'
outLoc1='s3://my-app-bucket/input/uncomp_JSON/split_out/new-5000/data/'

# =================================================================================
# Read S3 Gz File
# =================================================================================

def readS3(filename):
    print("Reading source file => " , filename)
    if filename.split('.')[-1] == 'gz':
        print('Gz File Name is : ' + filename )
        s3_clientobj = s3.get_object(Bucket=app_bucket, Key=filename)
        readData = gzip.decompress(s3_clientobj['Body'].read())
    else :
        print('File Name is : ' + filename )
        s3_clientobj = s3.get_object(Bucket=app_bucket, Key=filename)
        readData = s3_clientobj['Body'].read().decode('utf-8')
    return readData

# =================================================================================
# Split vertically,filter for needed column and chunk by rows
# =================================================================================

# Splitting the data and filtering the columns needed
def readInNetSubset(readData,outDir):
    colname=[' billing_code_type','billing_code','billing_class','billing_code_modifier','negotiated_rate','negotiated_type','expiration_date','provi der_references']
    records = []
    fileext = 0
    counter = 0
    # Batch Size is 500 here. You can change is based on the memory need.
    for JSONData in iJSON.items(readData, 'in_network.item'):
        filterdata=jmespath.search("[ billing_code_type,billing_code,negotiated_rates[].negotiated_prices[].billing_class,negotiated_rates[].negotiated_prices[].billing _code_modifier,negotiated_rates[].negotiated_prices[].negotiated_rate,negotiated_rates[].negotiated_prices[].negotiated_type , negotiated_rates[].negotiated_prices[].expiration_date, negotiated_rates[].provider_references[]]",JSONData)
        if counter % 500 == 0 :
            newdata=pd.DataFrame(records,columns=colname)
            print('My new dataframe is in File : {}'.format(str(fileext)))
            if newdata.empty :
                pass
            else:
                outfile= outDir +'in_network/data_'+ str(fileext)+ '.parquet'
                wr.s3.to_parquet(df=newdata,path=outfile)
                records = []
                fileext = fileext + 1
                records.append((filterdata))
                counter = counter + 1

# Splitting the provider_references data and filtering the columns needed
def readProvSubset(readData,outDir):
    colname=[' provider_group_id','npi','tin_type','tin_value']
    records = []
    fileext = 0
    counter = 0
    for JSONData in iJSON.items(readData, 'provider_references.item'):
        filterdata=jmespath.search("[ provider_group_id,provider_groups[].npi[],provider_groups[].tin.type,provider_groups[].tin.value]",JSONData)
        # Batch Size is 5000 here. You can change is based on the memory need.
        if counter % 5000 == 0 :
            newdata=pd.DataFrame(records,columns=colname)
            print('My new dataframe is in File : {}'.format(str(fileext)))
            if newdata.empty :
                pass
            else:
                outfile= outDir +'provider/data_'+ str(fileext)+ '.parquet'
                wr.s3.to_parquet(df=newdata,path=outfile)
                records = []
                fileext = fileext + 1
                records.append((filterdata))
                counter = counter + 1

readData=readS3(filename)

print( 'Process 1 Start Time : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
readInNetSubset(readData,outLoc1)
readProvSubset(readData,outLoc1)
print( 'Process 1 End Time : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
print( 'End Time : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

