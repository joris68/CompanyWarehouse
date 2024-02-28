#Autor: MC, Changes: PSC
import logging
import os
import pyodbc
from xml.etree import ElementTree as ET
import hashlib
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import traceback




def main(req: func.HttpRequest) -> func.HttpResponse:
 
    try:
    
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area "blueantprojectservice"
        # -----------------------------------------------------------
    
        CONTAINER = 'blueanthumanservice'

        now = get_time_in_string()[0]
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()  

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        xml_content = ET.fromstring(blob_content)

        PersonIDs = []
        Initials = []
        FirstNames = []
        LastNames = []
        Emails = []
        datetime_list =[]

        for child in xml_content.iter('{http://human.blueant.axis.proventis.net/}GetPersonResponseParameter'):
            
            personid = child.find('{http://human.blueant.axis.proventis.net/}personID').text
            initials = child.find('{http://human.blueant.axis.proventis.net/}initials').text
            firstname = child.find('{http://human.blueant.axis.proventis.net/}firstname').text
            lastname = child.find('{http://human.blueant.axis.proventis.net/}lastname').text
            email = child.find('{http://human.blueant.axis.proventis.net/}email').text
            
            if personid not in PersonIDs: 
                PersonIDs.append(personid)
                Initials.append(initials)
                FirstNames.append(firstname)
                LastNames.append(lastname)
                Emails.append(email)
            datetime_list.append(now)

        Initials = ['N/A' if value is None else value for value in Initials]
        FirstNames = ['N/A' if value is None else value for value in FirstNames]
        LastNames = ['N/A' if value is None else value for value in LastNames]
        Emails = ['N/A' if value is None else value for value in Emails]

        # -----------------------------------------------------------
        # Create hashkeys for the BK and the Value, create and add them to the dataset
        # -----------------------------------------------------------
        
        hashkeyBK_list = [hashlib.sha256(str(PersonIDs[x]).encode()).hexdigest() for x in range(0, len(PersonIDs))]

        hashkeyValue_list = [hashlib.sha256((str(Initials[x]) + str(FirstNames[x]) + str(LastNames[x]) + str(Emails[x])).encode()).hexdigest() for x in range(0, len(Initials))]

        dataset = list(zip(PersonIDs, Initials, FirstNames, LastNames, Emails, hashkeyBK_list, hashkeyValue_list,datetime_list,datetime_list))
                
        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------
    
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntUser"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntUser"

        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)
        
        InsertQuery = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (BlueAntUserBK, UserInitials, FirstName, LastName, UserEmail, HashkeyBK, HashkeyValue, InsertDate, UpdateDate) VALUES (?,?,?,?,?,?,?,?,?)"
        
        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)
        
        # -----------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table.  
        # -----------------------------------------------------------

        cursor.execute(f"""
            --Synchronize the target table with refreshed data from source table
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));

                MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
                USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS SOURCE
                ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
                WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
                THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue],
                    TARGET.[BlueAntUserBK] = SOURCE.[BlueAntUserBK], 
                    TARGET.[UserInitials] = SOURCE.[UserInitials],
                    TARGET.[FirstName] = SOURCE.[FirstName],
                    TARGET.[LastName] = SOURCE.[LastName],
                    TARGET.[UserEmail] = SOURCE.[UserEmail],
                    TARGET.[UpdateDate] = SOURCE.[UpdateDate]
                WHEN NOT MATCHED BY TARGET 
                THEN INSERT ([BlueAntUserBK], [UserInitials], [FirstName],[LastName], [UserEmail], [HashkeyBK], [HashkeyValue],InsertDate,UpdateDate)
                VALUES (SOURCE.[BlueAntUserBK], SOURCE.[UserInitials], SOURCE.[FirstName], SOURCE.[LastName], SOURCE.[UserEmail], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue],SOURCE.InsertDate,SOURCE.UpdateDate)
                OUTPUT  $action as [Status] into @MergeLog;

            INSERT INTO [tmp].[changed] ([Status],Anzahl)
            SELECT [Status], count(*) as Anzahl FROM @MergeLog
            GROUP BY [Status];
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
