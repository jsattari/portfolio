#!/usr/bin/env python3

import snowflake.connector
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import csv


# load env
load_dotenv(os.path.abspath(os.getcwd()) + '/.env')

# snowflake creds
SNOWFLAKE_ACCOUNT = os.getenv('SNOWA')
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_WAREHOUSE = "SOURCE_INTEGRATIONS"
SNOWFLAKE_STAGE_DATABASE = "SOURCE_STAGING"
# ecomm db creds
pg_host = os.getenv('BRIDGE_HOST')
pg_database = os.getenv('BRIDGE_DB')
pg_port = os.getenv('BRIDGE_PORT')
pg_user = os.getenv('BRIDGE_U')
pg_password = os.getenv('BRIDGE_P')
# ssh
ssh_user = os.getenv('SSH_U')
ssh_pw = os.getenv('SSH_PW')
ssh_host = os.getenv('SSH_H')
ssh_p = os.getenv('SSH_P')


class SnowflakeUpload:
    """
    A Snowflake Upload Object
        Methods:
            init:  (filePath, tableName)
            add_column:  Adds a column to the table definition (name, dataType, isKey)
            upload:  Executes a snowflake upload of a CSV file
    """

    def __init__(self, filePath, tableName):
        self.filePath = filePath
        self.tableName = tableName.replace(".", "")
        self.stage_table_sql = ""
        self.columns = []

    def __build_table_sql(self):
        self.stage_table_sql = "CREATE OR REPLACE TABLE " + \
            self.tableName + "("
        for col in self.columns:
            self.stage_table_sql = self.stage_table_sql + \
                col["name"] + " " + col["dataType"] + ", "
        return self.stage_table_sql + "_Created DATETIME)"

    def add_column(self, name, dataType, isKey):
        self.columns.append(
            {"name": name, "dataType": dataType, "isKey": isKey})
        self.stage_table_sql = self.__build_table_sql()

    def __upload(self, cur, schema,  header_rows=0):
        """
            A Snowflake upload method
                Args:
                    cur - Open Snowflake cursor
                    schema - The schema the upload should be staged in
                    header_rows - Number of header rows to skip in upload, defaults to 0
        """
        print("Using Snowflake Warehouse " + SNOWFLAKE_WAREHOUSE +
              " and stage " + schema + ".GENERAL_STAGE")
        cur.execute("USE WAREHOUSE " + SNOWFLAKE_WAREHOUSE)
        cur.execute("USE SCHEMA " + SNOWFLAKE_STAGE_DATABASE + "." + schema)
        cur.execute("CREATE OR REPLACE FILE FORMAT python_csv_format type = csv field_delimiter = ',' escape_unenclosed_field='\\\\' FIELD_OPTIONALLY_ENCLOSED_BY='\"' empty_field_as_null = true null_if = ('') ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE SKIP_HEADER = " + str(header_rows) + ";")
        cur.execute(self.stage_table_sql)
        print("Copying file to Snowflake")
        cur.execute("PUT 'file://" + self.filePath + "' @" +
                    schema + ".GENERAL_STAGE" + " OVERWRITE = TRUE")
        print("Copying data from csv to staging table")
        cur.execute("COPY INTO " + self.tableName + " FROM @" + schema + ".GENERAL_STAGE FILES = ('" +
                    self.tableName + ".csv.gz') FILE_FORMAT = (FORMAT_NAME = 'python_csv_format')")
        cur.execute("UPDATE " + self.tableName +
                    " SET _CREATED = CAST(convert_timezone('UTC', current_timestamp()) AS DATETIME) WHERE _CREATED IS NULL")

    def data_upload(self):
        """
            Executes an upload for a Google Analytics submission
        """
        table_sql = "CREATE TABLE IF NOT EXISTS SE_BRIDGE.dbo." + \
            self.tableName + "("
        for table_col in self.columns:
            table_sql = table_sql + \
                table_col["name"] + " " + table_col["dataType"] + ", "
        table_sql = table_sql[:-2] + ")"

        # Build the MERGE statement that will copy the data from the staging table into the production table.
        # Production table should have the same name and columns as the staging table.
        merge_sql = "MERGE INTO SE_BRIDGE.dbo." + self.tableName + \
            " t USING (SELECT * FROM SOURCE_STAGING.SE_BRIDGE." + \
            self.tableName + ") x ON "
        for key_col in self.columns:
            if key_col["isKey"] == True:
                merge_sql = merge_sql + "t." + \
                    key_col["name"] + " = x." + key_col["name"] + " AND "
        merge_sql = merge_sql[:-4] + "WHEN MATCHED THEN UPDATE SET "
        for update_col in self.columns:
            if update_col["isKey"] == False:
                merge_sql = merge_sql + \
                    update_col["name"] + " = x." + update_col["name"] + ", "
        merge_sql = merge_sql[:-2] + " WHEN NOT MATCHED THEN INSERT ("
        for insert_col in self.columns:
            merge_sql = merge_sql + insert_col["name"] + ", "
        merge_sql = merge_sql[:-2] + ") VALUES ("
        for val_col in self.columns:
            merge_sql = merge_sql + "x." + val_col["name"] + ", "
        merge_sql = merge_sql[:-2] + ");"

        con = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT
        )
        cur = con.cursor()

        try:
            self.__upload(cur, "SE_BRIDGE")
            cur.execute("USE SCHEMA SE_BRIDGE.dbo")

            # Create production table if it doesn't exist
            cur.execute(table_sql)

            # Merge the data from staging to production table
            print("Merging data into Production table")
            cur.execute(merge_sql)

        finally:
            cur.close()
            con.close()


def data_type_chooser(typeName):
    '''
    Converts the given pandas datatype to a Snowflake datatype
        Returns: String
        Known types:
            integer, float, datetime, varchar
    '''
    typeDict = {
        "int64": "NUMBER(38,0)",
        "float64": "NUMBER(28,9)",
        "datetime64[ns]": "DATETIME",
        "object": "VARCHAR",
        "bool": "BOOLEAN"
    }
    return typeDict.get(typeName)


def make_pg_query(filename):
    '''
    Open query file, create tunnel,
    create engine, then query db,
    return dataframe
    '''

    query = open(os.path.abspath(os.getcwd()) + f'/bridge_query/{filename}', 'r')

    tunnel = SSHTunnelForwarder(
        (ssh_host, int(ssh_p)),
        ssh_username=ssh_user,
        ssh_password=ssh_pw,
        ssh_pkey='~/.ssh/id_rsa',
        remote_bind_address=(pg_host, int(pg_port)),
    )

    # bug fix to allow .close() method
    tunnel.daemon_forward_servers = True

    # start le tunnel
    tunnel.start()

    # make the query engine
    engine = create_engine(
        f'postgresql://{pg_user}:{pg_password}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{pg_database}')

    # save query results to dataframe
    df = pd.read_sql_query(query.read(), con=engine)

    # close connection to db
    tunnel.close()

    return df


def save_file(dataframe, folder, report):
    '''
    Saves file to designated location
    '''
    dataframe.to_csv(f'{folder}{report}.csv', index=False,
                     header=False, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
