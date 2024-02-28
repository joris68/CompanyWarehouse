#Author: Pascal Schütze
from datetime import datetime
import logging
from xml.etree import ElementTree as ET
import hashlib
import azure.functions as func
from common import get_relative_blob_path, create_file_name, get_latest_blob_from_staging, insert_into_loadlogging, init_DWH_Db_connection, close_DWH_DB_connection, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try: 

        CONTAINER = 'blueantinvoice'

        now = get_time_in_string()[0]

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
  
        blob_content = blob.content_as_text()

        #----------------------------------------------------------------------------------------
        # Now, retrive the data from xml and pack it into variables that are loaded into DB 
        #----------------------------------------------------------------------------------------

        # Parse the XML content
        xml = ET.fromstring(blob_content)

        # Initialize empty lists to store extracted data
        InvoiceIDs = []
        InvoiceNumbers = []
        InvoiceDates = []
        InvoiceStateIDs = []
        InvoiceAmounts = []
        StartDates = []
        EndDates = []
        ProjectNumbers = []
        WorkTimeAccountableInHours = []
        WorkTimeTravelAccountableInHours = []
        WorkTimeNotAccountableInHours = []
        WorkTimeTravelNotAccountableInHours = []

        datetime_list = []
        hashkeyvalue_list= []
        hashkeybk_list = []

        # Define namespace for XML elements
        xml_link_ns13 = '{http://invoice.blueant.axis.proventis.net/}'

        # Iterate through each <Invoice> element in the XML data
        for invoice in xml.findall(f'.//{xml_link_ns13}Invoice'):
            # Extract invoice data
            invoiceID = int(invoice.find(f'{xml_link_ns13}invoiceID').text)
            invoiceNumber = invoice.find(f'{xml_link_ns13}invoiceNumber').text
            invoiceDate_date = datetime.strptime(invoice.find(f'{xml_link_ns13}invoiceDate').text, '%Y-%m-%d%z')
            invoiceDate = int(str(invoiceDate_date.year) + f'{invoiceDate_date.month:02d}' + f'{invoiceDate_date.day:02d}')
            invoiceStateID = int(invoice.find(f'{xml_link_ns13}invoiceStateID').text)
            invoiceAmount = float(invoice.find(f'{xml_link_ns13}invoiceAmount').text)

            # Extract performance period data, different logic with try/except to handle missing data 
            # it was not possible to check if performancePeriod tag was inside the invoice tag with if/else
            performanceperiod = invoice.find(f'.//{xml_link_ns13}performancePeriod')
            try:
                # Get the StartDate of the performance period for the invoice, convert it to an integer
                startDate_date = datetime.strptime(performanceperiod.find(f'{xml_link_ns13}from').text, '%Y-%m-%d%z')
                startDate = int(str(startDate_date.year) + f'{startDate_date.month:02d}' + f'{startDate_date.day:02d}')
                
                # Get the EndDate of the performance period for the invoice, convert it to an integer
                endDate_date = datetime.strptime(str(performanceperiod.find(f'{xml_link_ns13}to').text), '%Y-%m-%d%z')
                endDate = int(str(endDate_date.year) + f'{endDate_date.month:02d}' + f'{endDate_date.day:02d}')
            except Exception:
                startDate = 00000000
                endDate = 00000000
        
            # Extract project number
            projectNumber = invoice.find(f'{xml_link_ns13}projectNumber').text
            
            # Extract worktime accountable data
            for worktimeaccountable in invoice.findall(f'.//{xml_link_ns13}worktimeAccountable'):
                worktimeaccountableinhours = float(worktimeaccountable.find(f'{xml_link_ns13}worktimeInHours').text)
            
            # Extract worktime travel accountable data
            for worktimetravelaccountable in invoice.findall(f'.//{xml_link_ns13}worktimeTravelAccountable'):
                worktimetravelaccountableinhours = float(worktimetravelaccountable.find(f'{xml_link_ns13}worktimeInHours').text)
            
            # Extract worktime not accountable data
            for worktimenotaccountable in invoice.findall(f'.//{xml_link_ns13}worktimeNotAccountable'):
                worktimenotaccountableinhours = float(worktimenotaccountable.find(f'{xml_link_ns13}worktimeInHours').text)
            
            # Extract worktime travel not accountable data
            for worktimetravelnotaccountable in invoice.findall(f'.//{xml_link_ns13}worktimeTravelNotAccountable'):
                worktimetravelnotaccountableinhours = float(worktimetravelnotaccountable.find(f'{xml_link_ns13}worktimeInHours').text)
            
            # Append extracted data to respective lists
            InvoiceIDs.append(invoiceID)
            InvoiceNumbers.append(invoiceNumber)
            InvoiceDates.append(invoiceDate)
            InvoiceStateIDs.append(invoiceStateID)
            InvoiceAmounts.append(invoiceAmount)
            StartDates.append(startDate)
            EndDates.append(endDate)
            ProjectNumbers.append(projectNumber)
            WorkTimeAccountableInHours.append(worktimeaccountableinhours)
            WorkTimeTravelAccountableInHours.append(worktimetravelaccountableinhours)
            WorkTimeNotAccountableInHours.append(worktimenotaccountableinhours)
            WorkTimeTravelNotAccountableInHours.append(worktimetravelnotaccountableinhours)

            bk = str(invoiceID) 
            bk_hash = hashlib.sha256(bk.encode()).hexdigest()
            hashkeybk_list.append(bk_hash)
            
            value = str(invoiceNumber) + str(invoiceDate) + str(invoiceStateID) + str(invoiceAmount) + str(startDate) + str(endDate) + str(projectNumber) + str(worktimeaccountableinhours) + str(worktimetravelaccountableinhours) + str(worktimenotaccountableinhours) + str(worktimetravelnotaccountableinhours)
            value_hash = hashlib.sha256(value.encode()).hexdigest()
            hashkeyvalue_list.append(value_hash)

            datetime_list.append(now)
        
        dataset = list(zip(
                InvoiceIDs
                ,InvoiceNumbers
                ,InvoiceDates
                ,InvoiceStateIDs
                ,InvoiceAmounts
                ,StartDates
                ,EndDates
                ,ProjectNumbers
                ,WorkTimeAccountableInHours
                ,WorkTimeTravelAccountableInHours
                ,WorkTimeNotAccountableInHours
                ,WorkTimeTravelNotAccountableInHours
                ,datetime_list
                ,datetime_list
                ,hashkeybk_list
                ,hashkeyvalue_list))
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables. 
        #----------------------------------------------------------------------------------------
    
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntInvoice"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntInvoice"

        conn, cursor = init_DWH_Db_connection()

        # Log the start of the data loading process
        logging.info(f": Starting data loading process...")

        insert_into_loadlogging(SCHEMA_NAME,TABLE_NAME, cursor, conn)

        # Truncate the temporary table
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement= f"""
                INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                    (
                        InvoiceBK
                        , InvoiceNumber
                        , InvoiceDateID
                        , BlueAntInvoiceStateID
                        , InvoiceAmount
                        , StartDateID
                        , EndDateID
                        , BlueAntProjectID
                        , WorktimeAccountableInHours
                        , WorktimeTravelAccountableInHours
                        , WorktimeNotAccountableInHours
                        , WorktimeTravelNotAccountableInHours
                        , InsertDate
                        , UpdateDate
                        , HashkeyBK
                        , HashkeyValue
                    )
                VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?)
            """
    
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 

        logging.info("Data inserted into temporary table.")

        #----------------------------------------------------------------------------------------
        # Now, merge data from tmp Table to dwh Table. 
        #----------------------------------------------------------------------------------------
    
        cursor.execute(f"""
        -- Synchronize the target table with refreshed data from the source table
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));
        WITH CTE AS (
            SELECT 
                       tmp.InvoiceBK
                       , tmp.InvoiceNumber
                       , tmp.InvoiceDateID
                       , ISNULL(dwh_baInSt.BlueAntInvoiceStateID, -1) AS BlueAntInvoiceStateID
                       , tmp.InvoiceAmount
                       , tmp.StartDateID
                       , tmp.EndDateID
                       , ISNULL(dwh_baPr.BlueAntProjectID, -1) AS BlueAntProjectID
                       , tmp.WorktimeAccountableInHours
                       , tmp.WorktimeTravelAccountableInHours
                       , tmp.WorktimeNotAccountableInHours
                       , tmp.WorktimeTravelNotAccountableInHours
                       , tmp.InsertDate
                       , tmp.UpdateDate
                       , tmp.HashkeyBK
                       , CONCAT(tmp.HashkeyValue,'|',dwh_baInSt.BlueAntInvoiceStateID,'|',dwh_baPr.BlueAntProjectID) as [HashkeyValue]
            FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp 
                LEFT JOIN [{SCHEMA_NAME}].DimBlueAntProject dwh_baPr          ON  tmp.BlueAntProjectID = dwh_baPr.ProjectNumber
                LEFT JOIN [{SCHEMA_NAME}].DimBlueAntInvoiceState dwh_baInSt   ON  tmp.BlueAntInvoiceStateID = dwh_baInSt.InvoiceStateBK
        )
        
        MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
        USING CTE AS SOURCE
        ON TARGET.[HashkeyBK] = SOURCE.[HashkeyBK]
        
        WHEN MATCHED AND TARGET.[HashkeyValue] <> SOURCE.[HashkeyValue]
        THEN UPDATE SET TARGET.[HashkeyValue] = SOURCE.[HashkeyValue], 
                        TARGET.[InvoiceNumber] = SOURCE.[InvoiceNumber],
                        TARGET.[InvoiceDateID] = SOURCE.[InvoiceDateID],
                        TARGET.[BlueAntInvoiceStateID] = SOURCE.[BlueAntInvoiceStateID],
                        TARGET.[InvoiceAmount] = SOURCE.[InvoiceAmount],
                        TARGET.[StartDateID] = SOURCE.[StartDateID],
                        TARGET.[EndDateID] = SOURCE.[EndDateID],
                        TARGET.[BlueAntProjectID] = SOURCE.[BlueAntProjectID],
                        TARGET.[WorktimeAccountableInHours] = SOURCE.[WorktimeAccountableInHours],
                        TARGET.[WorktimeTravelAccountableInHours] = SOURCE.[WorktimeTravelAccountableInHours],
                        TARGET.[WorktimeNotAccountableInHours] = SOURCE.[WorktimeNotAccountableInHours],
                        TARGET.[WorktimeTravelNotAccountableInHours] = SOURCE.[WorktimeTravelNotAccountableInHours],
                        TARGET.[UpdateDate] = SOURCE.[UpdateDate]
                        
        WHEN NOT MATCHED BY TARGET
        THEN INSERT 
        (
                [InvoiceBK]
                , [InvoiceNumber]
                , [InvoiceDateID]
                , [BlueAntInvoiceStateID]
                , [InvoiceAmount]
                , [StartDateID]
                , [EndDateID]
                , [BlueAntProjectID]
                , [WorktimeAccountableInHours]
                , [WorktimeTravelAccountableInHours]
                , [WorktimeNotAccountableInHours]
                , [WorktimeTravelNotAccountableInHours]
                , [InsertDate]
                , [UpdateDate]
                , [HashkeyBK]
                , [HashkeyValue]
                )
        VALUES (
                SOURCE.[InvoiceBK]
                , SOURCE.[InvoiceNumber]
                , SOURCE.[InvoiceDateID]
                , SOURCE.[BlueAntInvoiceStateID]
                , SOURCE.[InvoiceAmount]
                , SOURCE.[StartDateID]
                , SOURCE.[EndDateID]
                , SOURCE.[BlueAntProjectID]
                , SOURCE.[WorktimeAccountableInHours]
                , SOURCE.[WorktimeTravelAccountableInHours]
                , SOURCE.[WorktimeNotAccountableInHours]
                , SOURCE.[WorktimeTravelNotAccountableInHours]
                , SOURCE.[InsertDate]
                , SOURCE.[UpdateDate]
                , SOURCE.[HashkeyBK]
                , SOURCE.[HashkeyValue]
                )
        
        OUTPUT $action as [Status] INTO @MergeLog;
        
        INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
    """)
    
        # Commit transaction and log success
        conn.commit()

        cursor.commit()
        
        logging.info( ": Data successfully inserted in dwh.")

    #----------------------------------------------------------------------------------------
    # Now, update the Log Table. 
    #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn)
        
        close_DWH_DB_connection(conn, cursor)
        
        # If all code is processed successfully, return a response with an appropriate status code
        return func.HttpResponse( ": Function execution completed.", status_code=200) 
    
    except Exception as e:
            # If an exception occurs, capture the error traceback
            error_traceback = traceback.format_exc()

            # Return an HTTP response with the error message and traceback, separated by a line break
            return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)