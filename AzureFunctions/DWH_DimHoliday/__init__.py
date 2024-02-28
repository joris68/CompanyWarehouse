import requests
import json
from datetime import datetime as dt
import logging
import pyodbc
import hashlib

from common import  truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        # -----------------------------------------------------------
        # Get the public holidays from Website https://feiertage-api.de/ 
        # and store them into variables for the DWH. 
        # -----------------------------------------------------------

        # Variables
        bundesland = []
        holidayDates = []
        holidayNames= []
        
        # Get the current year and the two years before
        year = dt.now().year
        yearlist = [str(year),str(year-1),str(year-2)]
        
        #Bundesländer Berlin and Nordrheinwestfalen for Münster
        bundesländer = ["BE","NW"]

        for item in bundesländer:
            for year in yearlist:

                url = f"https://www.feiertage-api.de/api/?jahr={year}&nur_land={item}"

                payload = {}
                headers = {
                }

                response = requests.request("GET", url, headers=headers, data=payload)

                data = json.loads(response.text)

                for datas in data:
                    holidayNames.append(datas)
                    holidayDates.append((data[datas]["datum"]))
                    bundesland.append(item)
                    
        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyValue_list = [hashlib.sha256((str(holidayNames[x]) + str(holidayDates[x]) + str(bundesland[x])).encode()).hexdigest() for x in range(0, len(holidayNames))]

        dataset = list(zip(holidayNames, holidayDates, bundesland, hashkeyValue_list))
        
        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimHolidays"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimHolidays"

        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        Insert_Query = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (HolidayName, HolidayDate, State, InsertDate, HashkeyValue) VALUES (?,?,?,GETDATE(),?)"

        insert_into_tmp_table(dataset, Insert_Query, cursor, conn)

        cursor.execute(f"""
            -- Synchronize the target table with refreshed data from the source table
            WITH CTE AS (
                SELECT *,
                    CAST(CONVERT(VARCHAR(10), [HolidayDate], 112) AS INT) AS [HolidayDateID]
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
            )
            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
            USING CTE AS SOURCE
            ON TARGET.HashkeyValue = SOURCE.HashkeyValue

            WHEN NOT MATCHED BY TARGET THEN 
                INSERT (HolidayName, HolidayDateID, [State], InsertDate, HashkeyValue)
                VALUES (SOURCE.HolidayName, SOURCE.HolidayDateID, SOURCE.State, SOURCE.InsertDate, SOURCE.HashkeyValue);
        """)
        
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)  
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
