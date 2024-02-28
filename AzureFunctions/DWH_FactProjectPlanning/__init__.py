#Author: Pascal Schütze
from datetime import datetime
import logging
import hashlib
import pandas as pd
import numpy as np
import pytz
import azure.functions as func
from common import get_latest_blob_from_staging, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:


        timezone = pytz.timezone('Europe/Berlin')
        # Get current date and time components
        now = datetime.now(timezone)
        
        month = now.month
        year = now.year

        # Add leading zero to single digit values
        month = f'{month:02d}'

        CONTAINER = 'sharepointprojectplanningexcel'

        blob_content = get_latest_blob_from_staging(CONTAINER)

        # Read the blob content (excel) into a pandas dataframe
        data = pd.read_excel(blob_content.readall(), sheet_name='Sprint', index_col=0 , header=None)

        #-------------------------------------------------------------------------------------------------------------------
        # Now, retrieve the relevant data from the dataframe and write it into variables
        #-------------------------------------------------------------------------------------------------------------------

        # Reset the index and rename the column
        data = data.reset_index().rename(columns={"index": "RowNumber"})

        # Find column indices for required columns
        nr_col, project_col, ma_col, dez_col, jan_col = 0, 0, 0, 0, 0
        year = datetime.now(timezone).year

        for row_number, row in data.iterrows():
            for col_index, value in row.items():
                if project_col == 0 or ma_col == 0 or dez_col == 0 or jan_col == 0 or nr_col == 0:
                    if value == 'Nr.':
                        nr_col = col_index
                        start_row = row_number
                    if value == 'Projektname':
                        project_col = col_index
                    if value == 'MA':
                        ma_col = col_index
                    if value == 'Jan':
                        jan_col = col_index
                    if value == 'Dez':
                        dez_col = col_index
                        break
                else:
                    break

        # Extract relevant data into a new DataFrame
        data_extract = data.iloc[start_row:, nr_col:dez_col + 1]

        # Find the end row based on NaN values
        end_row = None
        for index, row in data_extract.iterrows():
            if data_extract.isna().all(axis=1).iloc[index]:
                end_row = data_extract.index[index]
                break

        # Create cleaned_data DataFrame
        cleaned_data = pd.DataFrame()
        start_row += 1
        cleaned_data['Projektname'] = data.iloc[start_row:end_row, project_col]
        cleaned_data['Projektname'].fillna(method='ffill', inplace=True)
        cleaned_data['MA'] = data.iloc[start_row:end_row, ma_col]

        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        for i, month in enumerate(months):
            cleaned_data[month] = data.iloc[start_row:end_row, jan_col + i]

        # Reset index and reorder columns
        cleaned_data = cleaned_data.reset_index(drop=False).drop(columns=['index'])
        cleaned_data.index += 1
        columns = cleaned_data.columns.tolist()
        columns = columns[2:] + columns[:2]
        cleaned_data = cleaned_data[columns]

        # Replace '-' with NaN and fill NaN values with 0
        cleaned_data = cleaned_data.replace('-', np.nan).fillna(value=0)
        
        # Remove rows where 'MA' contains 'gesamt'
        cleaned_data ['MA'] = cleaned_data['MA'].astype(str)
        cleaned_data = cleaned_data[~cleaned_data['MA'].str.contains('gesamt', case=False)]

        # Remove rows where strings 
        string_mask = cleaned_data[months].applymap(lambda value: isinstance(value, str))
        cleaned_data = cleaned_data[~string_mask.any(axis=1)]

        dwh_data = pd.DataFrame()  # Initialize an empty DataFrame to store the final dwh data
        filtered_rows = pd.DataFrame()  # Initialize an empty DataFrame to store the filtered rows  

        # Iterate over the 12 months
        for i in range(0, 12):
            month_column = cleaned_data.columns[i]

            # Check if the current column is not 'Projektname' or 'MA'
            if month_column != 'Projektname' or month_column != 'MA':
                filtered_rows = cleaned_data[cleaned_data[month_column] > 0]  # Filter rows where the value in the month column is greater than 0

                month = i + 1
                dateID = int(f"{year}{month:02d}01")  # Generate a dateID based on the current year and month

                # Create a new DataFrame with repeated 'dateID' values
                dateID_column = pd.DataFrame({'DateID': np.repeat(dateID, len(filtered_rows))})

                filtered_column = pd.DataFrame({
                    'Projektname': filtered_rows['Projektname'],
                    'MA': filtered_rows['MA'],
                    'PlanningValue': filtered_rows[month_column]
                })  # Create a DataFrame with the required columns from the filtered rows

                filtered_column.reset_index(drop=True, inplace=True)  # Reset the index of the filtered_column DataFrame

                # Append the 'dateID' column to 'filtered_column'
                insert_data = pd.concat([filtered_column[['Projektname', 'MA', 'PlanningValue']].reset_index(drop=True), dateID_column], axis=1)

                # Extract the required columns and assign values to the new DataFrame 'dwh_data'
                dwh_data = pd.concat([dwh_data, insert_data[['Projektname', 'MA', 'DateID', 'PlanningValue']]], ignore_index=True)

        logging.info("Data successfully cleaned and transformed for DWH.")
        
        #----------------------------------------------------------------------------------------
        # Now, load the data from the variables into the tmp tables. Note:
        # Projekname is the Business Key and the Column is ProjectID in the Fact table
        # MA is the EmployeeID in the Fact table
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactProjectPlanning"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactProjectPlanning"
        

        conn, cursor = init_DWH_Db_connection()

        # Log the start of the data loading process
        logging.info(" Starting data loading process...")


        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        # Truncate the temporary table
        cursor.execute(f"TRUNCATE TABLE [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info(": Temporary table truncated.")

        # Load data into the temporary table
        for index in dwh_data.index:
            # Compute the hash values for the business key and the value
            bk = str(dwh_data['Projektname'][index]) + str(dwh_data['DateID'][index]) 
            bk_hash = hashlib.sha256(bk.encode()).hexdigest()

            value = str(dwh_data['PlanningValue'][index])
            value_hash = hashlib.sha256(value.encode()).hexdigest()

            cursor.execute(f"""
                INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                    (BlueAntProjectID, ProjectName, EmployeeID, DateID, PlanningValue, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
                VALUES
                    (NULL, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?)
            """, (dwh_data['Projektname'][index], dwh_data['MA'][index], int(dwh_data['DateID'][index]), float(dwh_data['PlanningValue'][index]), bk_hash, value_hash))

        # Commit the transaction
        conn.commit()
        logging.info("The cleaned data inserted into temporary table.")

        #----------------------------------------------------------------------------------------
        # Update the Hashkey BK of the Tmp table with the joined data from the DWH table DimBlueAntProject and DimUser
        # Replace the Values of EmployeeID with MA-N.N with 0, that MA-N.N is not lost in the join
        #----------------------------------------------------------------------------------------
        
        # Update the Tmp table with the joined data from the DWH table DimBlueAntProject and DimUser, replace not found values with -1!  
        cursor.execute(f"""
            SELECT ISNULL(dwh_bp.BlueAntProjectID, -1) as BlueAntProjectID, tmp.ProjectName, 
                CASE WHEN tmp.EmployeeID = 'MA-N.N' THEN 0 
                ELSE ISNULL(dwh_u.UserID, -1) 
                END as EmployeeID, tmp.DateID, tmp.PlanningValue, tmp.InsertDate, tmp.UpdateDate, tmp.HashkeyBK, tmp.HashkeyValue
            FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp
            LEFT JOIN dwh.DimBlueAntProject 	    dwh_bp 	ON 	tmp.ProjectName = dwh_bp.ProjectName
            LEFT JOIN dwh.DimUser 				    dwh_u 	ON 	tmp.EmployeeID = dwh_u.UserInitials
            WHERE tmp.ProjectName NOT LIKE 'DEAL%' 
            AND tmp.ProjectName NOT LIKE 'INTERN%'
        """)
        
        tmp_table = cursor.fetchall()
        
        # Convert the cursor object into a list of lists
        tmp_table = [list(row) for row in tmp_table]
        
        # Create a DataFrame from the cursor object
        df = pd.DataFrame(tmp_table ,columns=['BlueAntProjectID', 'ProjectName', 'EmployeeID', 'DateID', 'PlanningValue', 'InsertDate', 'UpdateDate', 'HashkeyBK', 'HashkeyValue'])
        
        # Group by 'DateID', 'BlueAntProjectID', and 'EmployeeID', and sum the 'PlanningValue'
        df_grouped = df.groupby(['DateID', 'BlueAntProjectID', 'EmployeeID']).agg({'PlanningValue': 'sum'}).reset_index()

        # Merge the grouped DataFrame back into the original DataFrame, the sum values are in the column 'PlanningValue_sum'
        df = df.merge(df_grouped, on=['DateID', 'BlueAntProjectID', 'EmployeeID'], how='left', suffixes=('', '_sum'))
        
        # The lambda function takes each row and concatenates the values from 'HashkeyBK', 'EmployeeID', and 'BlueAntProjectID'. Then, it hashes the concatenated string using SHA-256 and returns the hashed value. 
        df['HashkeyBK'] = df.apply(lambda row: hashlib.sha256(f"{row['HashkeyBK']}{row['EmployeeID']}{row['BlueAntProjectID']}".encode()).hexdigest(), axis=1)
        
        # Update the 'HashkeyValue' column with the new hashed values based on the summed 'PlanningValue'
        df['HashkeyValue'] = df.apply(lambda row: hashlib.sha256(f"{row['PlanningValue_sum']}".encode()).hexdigest(), axis=1)

        # Remove the column Planning Value and rename the column PlanningValue_sum to PlanningValue
        df.drop(['PlanningValue'], axis=1, inplace=True)
        df.rename(columns={'PlanningValue_sum': 'PlanningValue'}, inplace=True)
        
        # Drop duplicates based on a subset of columns
        subset_columns = ['BlueAntProjectID', 'ProjectName', 'EmployeeID', 'DateID']
        df = df.drop_duplicates(subset=subset_columns)
        
        # Truncate the temporary table
        cursor.execute(f"TRUNCATE TABLE [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        
        # Insert the data from the joins and updated BKs and HaskeyValues into the temporary table
        for index in df.index:
            
            cursor.execute(f"""
                INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]
                    (BlueAntProjectID, ProjectName, EmployeeID, DateID, PlanningValue, InsertDate, UpdateDate, HashkeyBK, HashkeyValue)
                VALUES
                    (?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?)
            """, (int(df['BlueAntProjectID'][index]), df['ProjectName'][index], int(df['EmployeeID'][index]), int(df['DateID'][index]), float(df['PlanningValue'][index]), df['HashkeyBK'][index], df['HashkeyValue'][index]))

        # Commit the transaction
        conn.commit()
        logging.info("HashkeyBK and Value is updated per Project, DateID and EmployeeID for the temporary table.")
        
        #----------------------------------------------------------------------------------------
        # Merge the data from the temporary table into the DWH table, ignore planning values where EmployeeID is -1
        #----------------------------------------------------------------------------------------

        # Truncate the temporary table
        cursor.execute(f"TRUNCATE TABLE [{SCHEMA_NAME}].[{TABLE_NAME}]")

        cursor.execute(f"""
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));
        WITH CTE AS (
            SELECT tmp.BlueAntProjectID, tmp.ProjectName, tmp.EmployeeID, tmp.DateID, tmp.PlanningValue, tmp.InsertDate, tmp.UpdateDate, tmp.HashkeyBK, tmp.HashkeyValue
            FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] tmp 
            WHERE tmp.EmployeeID != -1
        )
        
        -- MERGE statement
        MERGE  [{SCHEMA_NAME}].[{TABLE_NAME}] AS TARGET
        USING CTE AS SOURCE
        ON (TARGET.[HashkeyBK] = SOURCE.[HashkeyBK])
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT ([BlueAntProjectID], [ProjectName], [EmployeeID], [DateID], [PlanningValue], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
        VALUES (SOURCE.[BlueAntProjectID], SOURCE.[ProjectName] ,SOURCE.[EmployeeID], SOURCE.[DateID], SOURCE.[PlanningValue], SOURCE.[InsertDate], SOURCE.[UpdateDate], SOURCE.[HashkeyBK], SOURCE.[HashkeyValue])
        OUTPUT  $action as [Status] into @MergeLog;

        INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
        SELECT [Status], count(*) as Anzahl FROM @MergeLog
        GROUP BY [Status];

        """)

        # Commit transaction and log success
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        #----------------------------------------------------------------------------------------
        # Now, update the Log Table for DimJiraSprint. 
        #----------------------------------------------------------------------------------------

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension= False  )

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
     # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
