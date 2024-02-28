#Author: Pascal Schütze 
import logging
from xml.etree import ElementTree as ET
import hashlib
from common import get_relative_blob_path, create_file_name, init_DWH_Db_connection, close_DWH_DB_connection, get_latest_blob_from_staging, insert_into_loadlogging, DWH_table_logging
import traceback
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantworktimecalendar'

        # Download the blob content as text
        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()
    
    #----------------------------------------------------------------------------------------
    # Now, retrive the data from xml and pack it into variables that are loaded into DB 
    #----------------------------------------------------------------------------------------
    
        # Parse the XML content
        xml = ET.fromstring(blob_content)
        
        # Lists to store extracted data
        CalendarIDs = []
        CalendarNames = []
        CalendarDurations = []
        WorkTimesMonday = []
        WorkTimesTuesday = []
        WorkTimesWednesday = []
        WorkTimesThursday = []
        WorkTimesFriday = []

        # Namespace prefix for XML elements
        xml_link_ns9 = '{http://project.blueant.axis.proventis.net/}'

        # Iterate over each WtCalendar element in the XML content
        for calendar in xml.findall(f'.//{xml_link_ns9}WtCalendar'):
    
            # Extract calendar ID, name, and duration
            calenderID = int(calendar.find(f'{xml_link_ns9}ID').text)
            calenderName = calendar.find(f'{xml_link_ns9}name').text
            calenderDuration = int(calendar.find(f'{xml_link_ns9}duration').text) / 60000
            
            # Initialize variables for each day of the week, empty days are set to 0!
            timeMonday, timeTuesday, timeWednesday, timeThursday, timeFriday = 0, 0, 0, 0, 0    
            
            # Iterate over each WtCalendarDay element within the current calendar
            for calendarday in calendar.findall(f'.//{xml_link_ns9}WtCalendarDay'):
            
                # Extract the day name and duration, duration is converted from milliseconds to minutes
                if calendarday.find(f'{xml_link_ns9}dayName').text == 'monday':
                    timeMonday = int(calendarday.find(f'{xml_link_ns9}duration').text) / 60000
                    continue
                elif calendarday.find(f'{xml_link_ns9}dayName').text == 'tuesday':
                    timeTuesday = int(calendarday.find(f'{xml_link_ns9}duration').text) / 60000
                    continue
                elif calendarday.find(f'{xml_link_ns9}dayName').text == 'wednesday':
                    timeWednesday = int(calendarday.find(f'{xml_link_ns9}duration').text) / 60000
                    continue
                elif calendarday.find(f'{xml_link_ns9}dayName').text == 'thursday':
                    timeThursday = int(calendarday.find(f'{xml_link_ns9}duration').text) / 60000
                    continue
                elif calendarday.find(f'{xml_link_ns9}dayName').text == 'friday':
                    timeFriday = int(calendarday.find(f'{xml_link_ns9}duration').text) / 60000
                    continue
        
            # Append extracted data to respective lists
            CalendarIDs.append(calenderID)
            CalendarNames.append(calenderName)
            CalendarDurations.append(calenderDuration)
            WorkTimesMonday.append(timeMonday)
            WorkTimesTuesday.append(timeTuesday)
            WorkTimesWednesday.append(timeWednesday)
            WorkTimesThursday.append(timeThursday)
            WorkTimesFriday.append(timeFriday)
        
        logging.info(f"Data successfully stored into variables for DWH Load.")

    #----------------------------------------------------------------------------------------
    # Now, load the data from the variables into the tmp tables. 
    #----------------------------------------------------------------------------------------

        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntWorktimeCalendarDetail"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntWorktimeCalendarDetail"

        conn, cursor = init_DWH_Db_connection()

        # Log the start of the data loading process
        logging.info(f"Starting data loading process...")

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        # Truncate the temporary table
        cursor.execute(f"TRUNCATE TABLE [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info(f"Temporary table truncated.")
    
        for x in range(len(CalendarIDs)):
            # Concatenate the values to be inserted into the tmp table
            value = str(CalendarIDs[x]) + str(CalendarNames[x]) 
            
            # Hash the concatenated value using SHA-256
            hash_object = hashlib.sha256(value.encode())
            hex_dig = hash_object.hexdigest()
            
            # Insert a new row into the temporary table
            cursor.execute(f"""
                INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                    ([BlueAntWorkTimeCalendarBK], [WorkTimeCalendarName], [WorktimeCalendarDuration], [Monday], [Tuesday], [Wednesday], [Thursday], [Friday], [InsertDate], [HashkeyValue])
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?)""", 
                CalendarIDs[x], CalendarNames[x], CalendarDurations[x], WorkTimesMonday[x], WorkTimesTuesday[x], WorkTimesWednesday[x], WorkTimesThursday[x], WorkTimesFriday[x], hex_dig)
        
        # Commit the transaction
        conn.commit()
        logging.info( "Data inserted into temporary table.")

    #----------------------------------------------------------------------------------------
    # Now, merge data from tmp Table to dwh Table. 
    #----------------------------------------------------------------------------------------
    
        cursor.execute(f"""
        -- Synchronize the target table with refreshed data from the source table
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));
        
        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
        USING [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS SOURCE
        ON TARGET.[HashkeyValue] = SOURCE.[HashkeyValue]
        
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT ([BlueAntWorkTimeCalendarBK], [WorkTimeCalendarName], [WorktimeCalendarDuration], [Monday], [Tuesday], [Wednesday], [Thursday], [Friday], [InsertDate], [HashkeyValue])
        VALUES (SOURCE.[BlueAntWorkTimeCalendarBK], SOURCE.[WorkTimeCalendarName], SOURCE.[WorktimeCalendarDuration], SOURCE.[Monday], SOURCE.[Tuesday], SOURCE.[Wednesday], SOURCE.[Thursday], SOURCE.[Friday], SOURCE.[InsertDate], SOURCE.[HashkeyValue]) 
        
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

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
    