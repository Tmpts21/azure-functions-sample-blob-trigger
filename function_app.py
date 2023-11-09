import azure.functions as func  
import requests  
import pandas as pd 
import logging  



def insert_and_send(df , url , headers) :

   # insert data to dictionary then send post request
    payload = {
               'cityName' : None ,
               'year2015' : None ,
               'year2016' : None ,
               'year2017' : None ,
               'year2018' : None ,
               'year2019' : None
            }
    payload["cityName"] = df[0]
    payload["year2015"] = int(df[1])
    payload["year2016"] = int(df[2])
    payload["year2017"] = int(df[3])
    payload["year2018"] = int(df[4])
    payload["year2019"] = int(df[5])

    res = requests.post(url , json=payload , headers=headers)

    return res.text

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="myblobstorage/{name}.xlsx",
                               connection="BlobStorageConnectionString") 
def sample_app_blob_trigger(myblob: func.InputStream): 
    print(myblob)   
    #read the blob storage
    df = pd.read_excel(myblob.read(), engine='openpyxl')    
    
    #call external api to update/insert the database values based on the excel file    
    url = "https://ianvincentsampleapplication.azurewebsites.net/api/insertData"
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}   
    for row in range(len(df)):
        insert_and_send(list(df.iloc[row]) , url , headers) 
    
    logging.info(f"{df}")
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}" 
                f"Blob Size: {myblob.length} bytes") 
