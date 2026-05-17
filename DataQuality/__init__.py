from azure.identity import DefaultAzureCredential
import azure.functions as func
import requests
import SQLFile as SQL
from datetime import datetime, timedelta
import pandas as pd
import traceback
import Credential as cr
from dotenv import load_dotenv, dotenv_values
import pytz
load_dotenv()
import logging

logging.basicConfig(#Logging setup
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.WARNING,
    filename = 'Quality.txt',
    datefmt='%Y-%m-%d %H:%M:%S')

current_year = datetime.now().year
three_years_ago = current_year - 3
date_three_years_ago = datetime(three_years_ago, 1, 1)
Scriptstatus={"TableName":""}
for name in logging.Logger.manager.loggerDict.keys():
    if 'snowflake' in name or "urllib3" in name or "filelock" in name:
        logging.getLogger(name).setLevel(logging.WARNING)
        logging.getLogger(name).propagate = False
        
logger = logging.getLogger(__name__)
logger = logging.getLogger('azure')


# class FilterOutSpecificMessage(logging.Filter):
#     def filter(self, record):
#         # Suppress logs containing this specific message
#         return "Number of results in first chunk" not in record.getMessage()
kuala_lumpur_tz = pytz.timezone('Asia/Kuala_Lumpur')
KL_timestamp = datetime.now(kuala_lumpur_tz)#Getting Kuala Lumppur date time

logger.setLevel(logging.INFO)
# logger.addFilter(FilterOutSpecificMessage())
  
def Payload(imo,page,size):
    IMO = [imo]
    query_payload = {
        "pageIndex": page,
        "pageSize": size,
        "isBaseDataset": False,
        "createdAfter": "2023-01-01T00:00:00.000Z",
        "queryFilters": [{"column": "IMO", "filterValues": IMO}],
        "sorting": {"column": "IMO", "order": "Ascending"}
    }
    return query_payload


def Payload2(page,size):
    query_payload = {
        "pageIndex": page,
        "pageSize": size,
        "isBaseDataset": False,
        "createdAfter": "2023-01-01T00:00:00.000Z",
        "sorting": {
            "column": "IMO",
            "order": "Ascending"
        }}
    return query_payload

def fetch_paginated_data(url, headers, payload_func, key=None, values=None):
    DF = pd.DataFrame()
    if key and values:
        for value in values:
            payload = payload_func(value, 1, 1)
            response = call_query_endpoint(url, payload, headers)
            if not response or "pagination" not in response:
                continue
            count = response["pagination"]["totalCount"]
            for i in range(1, int(count / 1000) + 2):
                page_payload = payload_func(value, i, 1000)
                data = call_query_endpoint(url, page_payload, headers)
                if data and "data" in data:
                    DF = pd.concat([DF, pd.DataFrame(data["data"])], ignore_index=True)
    else:
        payload = payload_func(1, 1)
        response = call_query_endpoint(url, payload, headers)
        if not response or "pagination" not in response:
            return DF
        count = response["pagination"]["totalCount"]
        for i in range(1, int(count / 1000) + 2):
            page_payload = payload_func(i, 1000)
            data = call_query_endpoint(url, page_payload, headers)
            if data and "data" in data:
                DF = pd.concat([DF, pd.DataFrame(data["data"])], ignore_index=True)
    return DF
def call_query_endpoint(url, payload, Eheaders):
    try:
        # print(url)
        print(payload)
        response = requests.post(url=url, headers=Eheaders, json=payload)

        response.raise_for_status()
        if response.status_code != 200:
            return response
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred during the request: {e}")
    except Exception as e:
        print(f"error occured: {e}")


def fetch_paginated_data(url, headers, payload_func, key=None, values=None):
    DF = pd.DataFrame()
    if key and values:
        for value in values:
            payload = payload_func(value, 1, 1)
            response = call_query_endpoint(url, payload, headers)
            if not response or "pagination" not in response:
                continue
            count = response["pagination"]["totalCount"]
            for i in range(1, int(count / 1000) + 2):
                page_payload = payload_func(value, i, 1000)
                data = call_query_endpoint(url, page_payload, headers)
                if data and "data" in data:
                    DF = pd.concat([DF, pd.DataFrame(data["data"])], ignore_index=True)
    else:
        payload = payload_func(1, 1)
        response = call_query_endpoint(url, payload, headers)
        if not response or "pagination" not in response:
            return DF
        count = response["pagination"]["totalCount"]
        for i in range(1, int(count / 1000) + 2):
            page_payload = payload_func(i, 1000)
            data = call_query_endpoint(url, page_payload, headers)
            if data and "data" in data:
                DF = pd.concat([DF, pd.DataFrame(data["data"])], ignore_index=True)
    return DF


def process_and_upsert(df, table, key_columns):
    df["APIDate"] = datetime.now(kuala_lumpur_tz).strftime('%Y-%m-%d')
    logger.info(f"Processing {len(df)} records into {table}")
    SQL.CreateTable(df, table)
    for col in df.columns:
        SQL.InsertIfColumnNotExist(table, col)
    SQL.upsert(df, key_columns, table, f"{table}_delta")

complete=False
def main(mytimer: func.TimerRequest) -> None:
    try:
        DatasetName="DNV_DATAQUALITY_DATASET"
        _, DatasetList,Query,Credential = SQL.selectTable(DatasetName, f"{DatasetName}.ID,{DatasetName}.NAME,{DatasetName}.COMPANY",
                                     f"{DatasetName}.NAME='DCS Period Summary V1' or {DatasetName}.NAME='Monthly emissions summary V1'")
        logger.info(f"{Query},{Credential}")
        logger.info(f"{DatasetList}")
        _, IMOList,Query,Credential = SQL.selectTable( "STAGING.stage_DNV_MasterVessel", "VESSELIMO, COMPANYID", "")
        table_map = {
            "DCS Period Summary V1": ["DNV_API_Quality_DCSPeriodSummaryV1", Payload2, None],
            "Monthly emissions summary V1": ["DNV_API_Quality_MonthlyemissionssummaryV1", Payload, IMOList]
        }        
        logger.info(Query, Credential)
        logger.info(DatasetList)

        for idnum, name, company in DatasetList:
            print(name)
            datasetId = idnum
            try:
                token_url, url, token_payload, Eheaders = cr.get_Credential(company)
            except (Exception, TypeError, NameError) as Err:
                logger.error(f"error:{Err}")
                logging.error("An error on getting token occurred:\n%s", traceback.format_exc())
            except Exception as err:
                logger.error(f"Credential error for {company}: {err}")
                continue
            datasetName = "DNV_API_Quality_" + "".join(name.replace("Data Quality", "DQ").replace("-", "").split(" "))
            URL = url + "/datasets/" + datasetId + "/query"
            table_name, payload_func, key_vals = table_map.get(name, [None, None, None])
            if not table_name:
                logger.warning(f"Unknown dataset: {name}")
                continue

            logger.info(f"{datasetName}:{company}")
            if "Period Summary" in name:
                # print("period summary ya")
                companyList = ['LTD', 'BSM Germany']
                for companyid in companyList:
                    if companyid != company:
                        continue
                    urlpayload = Payload2(1,1)
                    DF=pd.DataFrame()
                    logger.info(f"{URL},{urlpayload},{Eheaders}")
                    Response=call_query_endpoint(URL, urlpayload)
                    if Response is None:
                        logger.info(f"Error:{name}-{companyid}:{Response}")
                        continue
                    if "pagination" in Response:
                        count = Response["pagination"]["totalCount"]
                    else:
                        logger.info(f"{name}-{companyid}:{Response}")
                        continue
                    df = fetch_paginated_data(URL, Eheaders, payload_func, key="IMO", values=[imo for imo, cid in IMOList if cid == company] if key_vals else None)

                    logger.info(f"{count}:{URL}")
                    page=int(count/1000)+(count%1000>0)
                    for i in range (1,page+1):
                        urlpayload = Payload2(i,1000)
                        Data = call_query_endpoint(URL, urlpayload, Eheaders)
                        if not isinstance(Data, (list, tuple, dict)):
                            continue
                        if len(Data['data']) == 0:
                            continue
                        #print(Data["pagination"])
                        df = pd.DataFrame(Data["data"])
                        DF = pd.concat([DF, df], ignore_index=True)
                    KeyColumn = ["IMO", "Vessel_Name", "Period_Start_Date"]
            else:
                DF = pd.DataFrame()
                for imo, companyid in IMOList:
                    if companyid != company:
                        continue
                    urlpayload = Payload(imo,1,1)
                    
                    Response=call_query_endpoint(URL, urlpayload, Eheaders)
                    if Response is None:
                        logger.info(f"Error:{name}-{companyid}:{Response}")
                        continue
                    if "pagination" in Response:
                        count = Response["pagination"]["totalCount"]

                    else:
                        logger.info(f"{name}-{companyid}:{Response}")
                        continue                    
                    logger.info(f"{companyid}-{imo}:{count}")
                    page = int(count / 1000) + (count % 1000 > 0)
                    for i in range (1,page+1):
                        urlpayload = Payload(imo,i,1000)
                        Data = call_query_endpoint(URL, urlpayload, Eheaders)
                        if not isinstance(Data, (list, tuple, dict)):
                            logger.info(f"{imo}:{Data}")
                            # print(imo,":",data)
                            continue
                        if len(Data['data']) == 0:
                            continue
                        #print(Data["pagination"])
                        df = pd.DataFrame(Data["data"])
                        DF = pd.concat([DF, df], ignore_index=True)
                    KeyColumn = ["IMO", "Vessel_Name", "Year_Month","PERIOD_START_DATE","PERIOD_END_DATE"]
            print(len(DF))
            DF["APIDate"]=str(datetime.now(kuala_lumpur_tz).strftime('%Y-%m-%d'))
            logger.info(f"{len(DF)}to {datasetName}")
            max_lengths = DF.astype(str).apply(lambda col: col.str.len().max())
            col = max_lengths.index.tolist()


            SQL.CreateTable(DF, datasetName)
            for col in DF.columns:
                SQL.InsertIfColumnNotExist(datasetName, col)
            SQL.upsert(DF, KeyColumn, datasetName, datasetName+"_delta")
            print(f"update:{len(DF)} to {datasetName}")
            logger.info(f"update:{len(DF)}to {datasetName}")

    except (Exception, TypeError, NameError) as Err:

        logger.error(f"error:{Err}")
        logger.error("An error occurred:\n%s", traceback.format_exc())
    finally:
        
        logger.info("Done")


        
