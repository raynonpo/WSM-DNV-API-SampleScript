import os
import snowflake.connector as sf
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas, pd_writer
import numpy as np
from dotenv import load_dotenv
load_dotenv()

snowflake_user = os.getenv("snowflake_user")
snowflake_password = os.getenv("snowflake_password")
snowflake_account = os.getenv("snowflake_account")
snowflake_warehouse = os.getenv("snowflake_warehouse")
snowflake_database = os.getenv("snowflake_database")
snowflake_schema = os.getenv("snowflake_schema")
snowflake_role = os.getenv("snowflake_role")
credentiallist={snowflake_user, snowflake_password, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema, snowflake_role}
conn = sf.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse,
    role=snowflake_role,
    client_session_keep_alive=True
)
mycursor = conn.cursor()
def CreateTable(df, TableName):
    max_lengths = df.astype(str).apply(lambda col: col.str.len().max())
    col = max_lengths.index.tolist()
    length = max_lengths.values.tolist()
    TableCreate = f"CREATE TABLE IF NOT EXISTS {TableName} (" + ",".join(
        [str(i) + " Varchar(" + str(j) + ")" for i, j in zip(col, length)]) + ")"
    # print(TableCreate)
    mycursor.execute(TableCreate)
    conn.commit()
def DeleteTable(TableName):
    DeleteTable = f"Truncate Table IF EXISTS {TableName} "
    # print(DeleteTable)
    mycursor.execute(DeleteTable)
    print(mycursor.rowcount, "record(s) deleted")

def TruncateTable(TableName):
    Truncate = f"truncate if EXISTS  {TableName}"
    mycursor.execute(Truncate)
    count = mycursor.rowcount
    # mycursor.close()
    conn.commit()

def InsertData(header, Data, TableName):
    tem2 = ",".join([i for i in header])
    # print(tem2)
    for line in Data:
        tem = ",".join(["NULL" if i == "None" else "'" + str(i) + "'" for i in line])
        # print(tem)
        mycursor.execute(f"""INSERT INTO {TableName} ({tem2}) VALUES ({tem})""")
    conn.commit()
def bulk_insert_from_df(table_name, df):
    pd.set_option('future.no_silent_downcasting', True)
    df.columns = df.columns.str.upper()
    df.replace('', np.nan, inplace=True)
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name.upper(), database=snowflake_database,
                                              schema=snowflake_schema,auto_create_table=True, overwrite=True)
    # Convert DataFrame to a list of tuple
    print(f"Inserted {nrows} rows into {table_name}.")


def upsert(delta_table_df: pd.DataFrame, key_columns: list[str], main_table_name, delta_table_name):
    TruncateTable(delta_table_name)
    CreateTable(delta_table_df, delta_table_name)
    count = bulk_insert_from_df(delta_table_name, delta_table_df)
    non_key_columns = [col for col in delta_table_df.columns if col not in key_columns]
    set_updates = ", ".join([f"target.{col} = source.{col}" for col in non_key_columns])
    insert_columns = ", ".join(delta_table_df.columns)
    insert_values = ", ".join([f"source.{col}" for col in delta_table_df.columns])
    on_condition = " AND ".join([f"target.{key} = source.{key}" for key in key_columns])
    merge_sql = f"""
        MERGE INTO {main_table_name} AS target
        USING {delta_table_name} AS source
          ON {on_condition}
        WHEN MATCHED THEN 
          UPDATE SET {set_updates}
        WHEN NOT MATCHED THEN 
          INSERT ({insert_columns}) 
          VALUES ({insert_values});
        """
    print(merge_sql)
    mycursor.execute(merge_sql)
    upsert_result = mycursor.fetchall()
    print(upsert_result)
    drop_temp_table_query = f"DROP TABLE {delta_table_name}"
    mycursor.execute(drop_temp_table_query)
    conn.commit()
    return count
    # column_names = [desc[0] for desc in mycursor.description]
    


def selectTable(tableName, Key, condition=''):
    whereclause = ""
    if condition:
        whereclause = f"where {condition}"
    query=f"""Select {Key} from {tableName} {whereclause}"""
    mycursor.execute(query)
    mylist = [list(i) for i in mycursor.fetchall()]
    return [mycursor.description, mylist,query,credentiallist]


def InsertIfColumnNotExist(TableName, ColumnName):
    exe = f""" ALTER TABLE {TableName} ADD COLUMN IF NOT EXISTS {ColumnName} varchar(4000)"""
    mycursor.execute(exe)

    conn.commit()
def insert_dataframe_to_snowflake(df, table_name):

    try:
        # Use write_pandas to insert data efficiently
        success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
        if success:
            print(f"Successfully inserted {nrows} rows into {table_name}.")
        else:
            print("Failed to insert data into Snowflake.")

    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")