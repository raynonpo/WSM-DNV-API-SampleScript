import requests
# import time
# import json
# from datetime import datetime
# import pandas as pd
# import pyodbc as p
import os
from dotenv import load_dotenv  # , dotenv_values

load_dotenv()


def get_token(token_url, token_payload):
    try:
        response = requests.post(token_url, data=token_payload)
        return response.json()["access_token"]
    except Exception as e:
        print(f"Failed to retrieve access token: {e}")


def get_Credential(Company):
    client_id = ""
    client_secret = ""
    subscription_key = ""
    workspace_id = ""
    dataset_id = ""
    print(Company)
    if Company == 'LTD':
        print("True")
        client_id = os.getenv("client_idLTD")
        client_secret = os.getenv("client_secretLTD")
        subscription_key = os.getenv("subscription_keyLTD")
        workspace_id = os.getenv("workspace_idLTD")
    elif Company == 'BSM Germany':
        client_id = os.getenv("client_idBSM")
        client_secret = os.getenv("client_secretBSM")
        subscription_key = os.getenv("subscription_keyBSM")
        workspace_id = os.getenv("workspace_idBSM")
        dataset_id = os.getenv("dataset_idBSM")

    token_url = os.getenv("Token")
    url = f"https://api.veracity.com/veracity/dw/gateway/api/v2/workspaces/{workspace_id}"
    token_payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://dnvglb2cprod.onmicrosoft.com/83054ebf-1d7b-43f5-82ad-b2bde84d7b75"
    }
    print(token_url, token_payload)
    Token = get_token(token_url, token_payload)
    headers = {
        'Content-Type': 'application/json',
        'Ocp-Apim-Subscription-Key': subscription_key,
        'Authorization': f"Bearer {Token}"
    }
    return token_url, url, token_payload, headers
