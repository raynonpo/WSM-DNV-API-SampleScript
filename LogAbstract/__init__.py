from azure.identity import DefaultAzureCredential
import azure.functions as func
import requests
import time

import SQLFile as SQL
from datetime import datetime, timedelta
import pandas as pd
import traceback
import Credential as cr
from dotenv import load_dotenv, dotenv_values
import pytz
load_dotenv()
import logging
import asyncio
import aiohttp
import psutil
import os
logging.basicConfig(#Logging setup
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.WARNING,
    filename = 'Log.txt',
    datefmt='%Y-%m-%d %H:%M:%S')

current_year = datetime.now().year
three_years_ago = current_year - 3
date_three_years_ago = datetime(three_years_ago, 1, 1)
        
logger = logging.getLogger(__name__)
logger = logging.getLogger('azure')

kuala_lumpur_tz = pytz.timezone('Asia/Kuala_Lumpur')
KL_timestamp = datetime.now(kuala_lumpur_tz)#Getting Kuala Lumppur date time

logger.setLevel(logging.DEBUG)

def log_memory_usage():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return f"Memory usage: RSS={mem_info.rss / (1024 * 1024):.2f} MB, VMS={mem_info.vms / (1024 * 1024):.2f} MB"

def PayloadPagWTFilter(page, pagesize):
    # IMO=[imo]

    query_payload = {
        "pageIndex": page,
        "pageSize": pagesize,
        "isBaseDataset": False,
        "columnFilter": ['LAST_UPDATED', 'SOURCE_SYSTEM', 'EVENT_DATETIME_UTC',
                         'IMO', 'Date_UTC', 'Time_UTC', 'Voyage_From', 'Voyage_To', 'Voyage_Number', 'Latitude_Degree',
                         'Latitude_Minutes', 'Latitude_North_South', 'Longitude_Degree', 'Longitude_Minutes',
                         'Longitude_East_West',
                         'Event', 'Time_Since_Previous_Report', 'Time_Elapsed_Anchoring', 'Distance', 'Passengers',
                         'Cargo_Mt',
                         'ME_Consumption_HFO', 'ME_Consumption_LFO', 'ME_Consumption_MGO', 'ME_Consumption_MDO',
                         'ME_Consumption_LNG', 'ME_Consumption_LPGP', 'ME_Consumption_LPGB', 'AE_Consumption_HFO',
                         'AE_Consumption_LFO', 'AE_Consumption_MGO', 'AE_Consumption_MDO', 'AE_Consumption_LNG',
                         'AE_Consumption_LPGP', 'AE_Consumption_LPGB',
                         'Boiler_Consumption_HFO', 'Boiler_Consumption_LFO', 'Boiler_Consumption_MGO',
                         'Boiler_Consumption_MDO', 'Boiler_Consumption_LNG', 'Boiler_Consumption_LPGP',
                         'Boiler_Consumption_LPGB',
                         'Inert_gas_Consumption_HFO', 'Inert_gas_Consumption_LFO', 'Inert_gas_Consumption_MGO',
                         'Inert_gas_Consumption_MDO', 'Inert_gas_Consumption_LNG', 'Inert_gas_Consumption_LPGP',
                         'Inert_gas_Consumption_LPGB',
                         'HFO_ROB', 'LFO_ROB', 'MGO_ROB', 'MDO_ROB', 'LNG_ROB', 'LPGP_ROB', 'LPGB_ROB'

                         ],
        "sorting": {
            "column": "IMO",
            "order": "Ascending"
        }}
    print(query_payload)
    return query_payload





def call_query_endpoint1(url, payload, Eheaders):
    try:
        # print(url)
        response = requests.post(url=url, headers=Eheaders, json=payload)

        response.raise_for_status()
        if response.status_code != 200:
            return response
        return response.json()
    except requests.exceptions.HTTPError as e:
        logger.info(f"HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        logger.info(f"An error occurred during the request: {e}")
    except Exception as e:
        logger.info(f"error occured: {e}")


async def call_query_endpoint(url, payload, Eheaders):
    """Asynchronous function to call an API endpoint."""
    try:
        async with aiohttp.ClientSession() as session:
            memory=log_memory_usage()
            logger.info(f" {payload}, {memory}")
            async with session.post(url, headers=Eheaders, json=payload) as response:
                response.raise_for_status()
                if response.status != 200:
                    return await response.text()  # Return response content in case of error
                return await response.json()
    except aiohttp.ClientResponseError as e:
        logger.info(f"HTTP error occurred at page {payload['pageIndex']}: {e}")
    except aiohttp.ClientError as e:
        logger.info(f"An error occurred during the request at page {payload['pageIndex']} : {e}")
    except Exception as e:
        logger.info(f"Error occurred at page {payload['pageIndex']}: {e}")

def PhraseData(Data):
    item = []
    Item = []
    for j in Data["data"]:
        item = []
        for i in j:
            item.append(str(j[i]))
        item.append(str(datetime.now().strftime('%Y-%m-%d')))
        Item.append(item)

    column = [j for j in Data["data"][0]]
    column.append("APIDate")

    # print(Item[0])
    return [column, Item]

async def process_batch(PageCount, pagesize, URL):
    """Processes API requests in batches of 15 using asyncio."""
    semaphore = asyncio.Semaphore(15)  # Limit concurrency to 15 requests

    async def fetch_page(i,Eheaders):
        async with semaphore:  # Limit concurrent requests

            data = await call_query_endpoint(URL, PayloadPagWTFilter(i, pagesize), Eheaders)
            if data:
                return PhraseData(data)  # Process the data
            return None, None

    token_url, url, token_payload, eheaders = cr.get_Credential("LTD")
    last_generated_time = time.time()
    tasks=[]
    for i in range(1, PageCount + 1):
        current_time = time.time()
        elapsed_time = current_time - last_generated_time
        if elapsed_time >= 60:
            last_generated_time = time.time()
            token_url, url, token_payload, eheaders = cr.get_Credential("LTD")
        tasks.append(fetch_page(i,eheaders))

    results = await asyncio.gather(*tasks)

    header, Line = None, []
    for head, lin in results:
        if head:
            header = head
        if lin:
            Line.extend(lin)

    return header, Line  # Return processed data


async def ProcessLA(URL, Pagecount, pagesize):  # Replace with actual credential handler
    header, Line = await process_batch(Pagecount, pagesize, URL)
    return header, Line







    
def main(mytimer: func.TimerRequest) -> None:
    try:
        _, DatasetList,Query,credential = SQL.selectTable("API.DNV_DataQuality_Dataset", "ID,Name,Company", "Name='Log Abstract V1'")
        Tablename = "DNV_API_LogAbstract"
        logger.info(f"{Query},{credential}")
        logger.info(f"{DatasetList}")
        datasetId = DatasetList[0][0]
        
        [token_url, url, token_payload, Eheaders] = cr.get_Credential("LTD")
        URL = url + "/datasets/" + datasetId + "/query"
        page = 1
        checkpagesize = 1
        pagesize = 1000
        response = call_query_endpoint1(URL, PayloadPagWTFilter(page, checkpagesize), Eheaders)["pagination"][ "totalCount"]
        MemoryUsed=log_memory_usage()
        logger.info(f"{URL},{MemoryUsed}")
        logger.info(f"{response}")
        TotalCount=response
        PageCount = int(TotalCount / pagesize) + (TotalCount % pagesize > 0)
        header, lin = asyncio.run(ProcessLA(URL, PageCount, pagesize))
        df = pd.DataFrame(data=lin, columns=header)  # hide for testing
        SQL.upsert(df, ["SOURCE_SYSTEM", "IMO", "EVENT", "DATE_UTC", "TIME_UTC"], Tablename, Tablename + "_DELTA")
        
        logger.info(f"insert {len(lin)} to {Tablename}")
    except (Exception, TypeError, NameError) as Err:
        traceback.print_exc()
        logger.error("An error occurred:\n%s", traceback.format_exc())
        logger.exception("error")
    finally:
        logger.info("Done")

        
