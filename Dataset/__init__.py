from azure.identity import DefaultAzureCredential
import azure.functions as func
import requests
import time
import json
import SQLFile as SQL
from datetime import datetime, timedelta
import pandas as pd
import traceback
import Credential as cr
from dotenv import load_dotenv, dotenv_values
import pytz
load_dotenv()
import logging


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.WARNING,
    filename='Log\\LogAbstract1.log',
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


malaysia_tz = pytz.timezone("Asia/Kuala_Lumpur")

def get_token(token_url, token_payload):
    try:
        response = requests.post(token_url, data=token_payload)
        return response.json()["access_token"]
    except Exception as e:
        print(f"Failed to retrieve access token: {e}")


def callAPI(APIURL, headers, method="get", payload=None):
    if not payload:
        payload = {}
    requestFun = requests.get if method == "get" else requests.post
    data = json.dumps(payload)
    response = requestFun(APIURL, headers=headers, data=data)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"{response.status_code}:{response.reason}--")


def get_datasets(Company):
    payload = {
        # "isBaseDataset": True,
        "Predefine": True,
        "pageIndex": 1,
        "pageSize": 100,
        "sortColumn": "Name",
        "sortDirection": "Ascending",
        # "datasetName": "S",
        "createdAfter": "2023-01-01T00:00:00.000Z",
        # "schemaVersionIds": ["89153dc4-2323-45c9-b70e-241503697315"]
    }
    token_url, url, token_payload, headers = cr.get_Credential(Company)
    try:
        required_fields = ["id", "name", "description"]
        data = callAPI(f"{url}/datasets/query", headers, payload=payload, method="post")
        data_df = pd.DataFrame(data["result"])[required_fields]
        data_df['Company'] = Company
        data_df['APIDAte'] = datetime.now(malaysia_tz).date()
        data_df.columns = data_df.columns.str.upper()  # Convert all column names to uppercase to be safe
        return data_df
    except Exception as e:
        print(f"error occured: {e}")


def main(mytimer: func.TimerRequest) -> None:
    try:
        Companylist = ['LTD', 'BSM Germany']
        company_data_list = [get_datasets(company) for company in Companylist]
        companies_df = pd.concat(company_data_list).reset_index(drop=True)  # Standardize row index
        # companies_df.columns = companies_df.columns.str.upper()  # Convert all column names to uppercase to be safe
        # header = companies_df.columns  # pd.Index
        data_quality_table_name = "DNV_DataQuality_Dataset".upper()
        # logger.info(f"{header},{data_quality_table_name}")
        
        SQL.CreateTable( companies_df, data_quality_table_name)  # Snowflake autocommits any successful statements by default.
        # SQL.TruncateTable(data_quality_table_name)
        SQL.insert_dataframe_to_snowflake(companies_df,data_quality_table_name)
        logger.info(f"No of data update:{len(companies_df)}")
        print("done")

    except (Exception, TypeError, NameError) as Err:
        print(Err)
        traceback.print_exc()
        logger.error("An error occurred:\n%s", traceback.format_exc())
        logger.exception("error")
    finally:
        logger.info("Done")

        
