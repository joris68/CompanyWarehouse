#Author: Pascal Schütze
from datetime import datetime
import logging
import azure.functions as func
from xml.etree import ElementTree as ET
import hashlib
import pandas as pd
from common import get_relative_blob_path, get_latest_blob_from_staging, insert_into_tmp_table, lookup_tmp_table, truncate_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueanthumanservice'
    
        #-------------------------------------------------------------------------------------------------------------------
        # First, retrieve the data from the blob storage
        #-------------------------------------------------------------------------------------------------------------------

        # Download the blob content as text
        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()
        
        #----------------------------------------------------------------------------------------
        # Now, retrive the data from xml and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        # Parse the XML content
        xml = ET.fromstring(blob_content)

        BlueAntUserIDs = []
        WorkTimeCalendarIDs = []
        EnddateIDs = []
        StartdateIDs = []

        # Loop through all the GetPersonResponseParameter elements
        for person in xml.findall('.//{http://human.blueant.axis.proventis.net/}GetPersonResponseParameter'):
            dateOfLeaving = person.find('{http://human.blueant.axis.proventis.net/}dateOfLeaving').text
            if dateOfLeaving is not None:
                # Convert the date of leaving to an end date ID
                timestamp = datetime.strptime(str(dateOfLeaving), '%Y-%m-%d%z')
                enddateID = int(str(timestamp.year) + f'{timestamp.month:02d}' + f'{timestamp.day:02d}')
            else:
                enddateID = 99991231

            # Loop through all the WorktimeCalendar elements
            for calendar in person.findall('.//{http://human.blueant.axis.proventis.net/}WorktimeCalendar'):
                # Get the calendar ID, valid date and person ID
                calendarID = calendar.find('{http://human.blueant.axis.proventis.net/}calendarID').text
                valid_value = calendar.find('{http://human.blueant.axis.proventis.net/}validDate').text
                personID = person.find('{http://human.blueant.axis.proventis.net/}personID').text
                
                # Convert the valid date to a start date ID
                timestamp = datetime.fromtimestamp(int(valid_value) / 1000)
                startDateID = int(str(timestamp.year) + f'{timestamp.month:02d}' + f'{timestamp.day:02d}')

                # Append the extracted IDs to the respective lists
                BlueAntUserIDs.append(personID)
                WorkTimeCalendarIDs.append(calendarID)
                StartdateIDs.append(startDateID)
                EnddateIDs.append(enddateID)
            
        # Create a DataFrame with columns 'BlueAntUserID', 'WorkTimeCalendarID', 'StartDateID', 'EndDateID'
        WorktimeDetail_df = pd.DataFrame({'BlueAntUserID': BlueAntUserIDs, 'WorkTimeCalendarID': WorkTimeCalendarIDs, 'StartDateID': StartdateIDs, 'EndDateID': EnddateIDs})

        # Convert StartDateID to datetime type
        WorktimeDetail_df['StartDateID'] = pd.to_datetime(WorktimeDetail_df['StartDateID'], format='%Y%m%d')

        # Rename 'EndDateID' column to 'DateOfLeavingID'
        WorktimeDetail_df = WorktimeDetail_df.rename(columns={'EndDateID': 'DateOfLeavingID'})

        # Identify rows where StartDateID is not duplicated for each BlueAntUserID
        mask = ~WorktimeDetail_df.groupby('BlueAntUserID')['StartDateID'].transform(lambda x: x.duplicated(keep=False))

        # Update the EndDateID for non-duplicated StartDateID rows by shifting the next StartDateID and subtracting 1 day
        WorktimeDetail_df.loc[mask, 'EndDateID'] = WorktimeDetail_df.groupby('BlueAntUserID')['StartDateID'].shift(-1) - pd.DateOffset(days=1)

        # Fill empty fields in EndDateID with '2000-01-01'
        WorktimeDetail_df['EndDateID'] = WorktimeDetail_df['EndDateID'].fillna(pd.to_datetime('19900101', format='%Y%m%d'))

        # Convert the modified columns back to the original integer type
        WorktimeDetail_df['StartDateID'] = WorktimeDetail_df['StartDateID'].dt.strftime('%Y%m%d').astype(int)
        WorktimeDetail_df['EndDateID'] = WorktimeDetail_df['EndDateID'].dt.strftime('%Y%m%d').astype(int)

        # Replace '19900101' with '99991231' in EndDateID column
        WorktimeDetail_df['EndDateID'] = WorktimeDetail_df['EndDateID'].replace(19900101, 99991231)

        # Reorder the columns
        WorktimeDetail_df = WorktimeDetail_df[['BlueAntUserID', 'WorkTimeCalendarID', 'StartDateID', 'EndDateID', 'DateOfLeavingID']]

        # Get the maximum EndDateID for each BlueAntUserID
        date_of_leaving_ids = WorktimeDetail_df.groupby('BlueAntUserID')['DateOfLeavingID'].max()

        # Update the last EndDateID for each BlueAntUserID that has already left
        mask = (WorktimeDetail_df['DateOfLeavingID'] != 99991231) & (WorktimeDetail_df['EndDateID'] == 99991231)
        WorktimeDetail_df.loc[mask, 'EndDateID'] = WorktimeDetail_df.loc[mask, 'BlueAntUserID'].map(date_of_leaving_ids)
        
        # Create a new column 'HashkeyBK' and 'HashkeyValue' for each row and compute the hash values
        WorktimeDetail_df['HashkeyBK'] = WorktimeDetail_df.apply(lambda row: hashlib.sha256((str(row['BlueAntUserID']) + str(row['WorkTimeCalendarID']) + str(row['StartDateID'])).encode()).hexdigest(), axis=1)
        WorktimeDetail_df['HashkeyValue'] = WorktimeDetail_df.apply(lambda row: hashlib.sha256((str(row['StartDateID']) + str(row['EndDateID']) + str(row['DateOfLeavingID'])).encode()).hexdigest(), axis=1)

        #Filter the duplicate for Markus Gleich WorktimeCalendar (40h/Week)
        WorktimeDetail_df = WorktimeDetail_df[~((WorktimeDetail_df['BlueAntUserID'] == str(736157976)) & (WorktimeDetail_df['WorkTimeCalendarID'] == str(3)))]

        # The dataset to be loaded into the DWH.   
        dataset = list(WorktimeDetail_df.itertuples(index=False, name=None))    
            
        logging.info(f"Data successfully stored into variables for DWH Load.")

        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables. #BA UserBK: 736157976,  WorkTimeCalendarBK: 3
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntWorktimeCalendar"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntWorktimeCalendar"

        conn, cursor = init_DWH_Db_connection()
    
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        InsertQuery = f"""
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                (BlueAntUserID, WorkTimeCalendarID, StartDateID, EndDateID, DateOfLeavingID, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
            VALUES
                (?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?)
            """
        
        insert_into_tmp_table(dataset, InsertQuery, cursor, conn)
        
        LookUpQuery = f"""
            WITH CTE AS (
                SELECT 
                    u.BlueAntUserID AS BlueAntUserID,
                    wcd.BlueAntWorkTimeCalendarDetailID AS BlueAntWorkTimeCalendarID,
                    wc.StartDateID,
                    wc.EndDateID,
                    wc.DateOfLeavingID,
                    wc.HashkeyBK AS OldHashkeyBK,  -- Renaming for clarity
                    wc.HashkeyValue,
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                                    CONCAT(CONVERT(VARCHAR, u.BlueAntUserID), 
                                            CONVERT(VARCHAR, wcd.BlueAntWorkTimeCalendarDetailID), 
                                            CONVERT(VARCHAR, wc.StartDateID))), 2)) AS NewHashKeyBK  -- New hashkey generation
                                            
                FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] wc
                JOIN dwh.DimBlueAntWorkTimeCalendarDetail wcd ON wc.WorkTimeCalendarID = wcd.BlueAntWorkTimeCalendarBK
                JOIN dwh.DimBlueAntUser u ON wc.BlueAntUserID = u.BlueAntUserBK 
                WHERE NOT (u.BlueAntUserID = 23 AND wcd.BlueAntWorkTimeCalendarDetailID = 2) --Filter Markus Gleich 40h/Week
            )
            UPDATE tmp
            SET
                tmp.BlueAntUserID = c.BlueAntUserID,
                tmp.WorkTimeCalendarID = c.BlueAntWorkTimeCalendarID,
                tmp.HashkeyBK = c.NewHashKeyBK  -- Using the new hashkey
                        
            FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp
            JOIN CTE c ON tmp.HashkeyBK = c.OldHashkeyBK;
        """
        
        lookup_tmp_table(LookUpQuery, cursor, conn)
        
        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table. 
        #----------------------------------------------------------------------------------------

        cursor.execute(f"""
        -- Synchronize the target table with refreshed data from the source table
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));
        
        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
        USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS SOURCE
        ON TARGET.[HashkeyBK] = SOURCE.[HashkeyBK]
        
        WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
        THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue], 
                        TARGET.[StartDateID] = SOURCE.[StartDateID],
                        TARGET.[EndDateID] = SOURCE.[EndDateID],
                        TARGET.[DateOfLeavingID] = SOURCE.[DateOfLeavingID],
                        TARGET.[UpdateDate] = SOURCE.[UpdateDate]
        
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT ([BlueAntUserID], [WorkTimeCalendarID], [StartDateID], [EndDateID], [DateOfLeavingID], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
        VALUES (SOURCE.[BlueAntUserID], SOURCE.[WorkTimeCalendarID], SOURCE.[StartDateID], SOURCE.[EndDateID], SOURCE.[DateOfLeavingID], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
        
        OUTPUT $action as [Status] INTO @MergeLog;
        
        INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
    """)
    
        # Commit transaction and log success
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

    #----------------------------------------------------------------------------------------
    # Now, update the Log Table. 
    #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn)

        # Close the database connection and log disconnection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
