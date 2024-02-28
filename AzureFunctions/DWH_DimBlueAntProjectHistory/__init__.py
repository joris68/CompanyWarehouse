import logging
import azure.functions as func
import pandas as pd
from common import get_latest_blob_from_staging, init_DWH_Db_connection, close_DWH_DB_connection,truncate_tmp_table,get_time_in_string,DWH_table_logging
import traceback
import hashlib


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        now = get_time_in_string()[0]

        CONTAINER = 'blueantprojecthistory'

        blob_content = get_latest_blob_from_staging(CONTAINER)

        xl = pd.read_excel(blob_content.content_as_bytes(),sheet_name="sheet1",header=1,na_filter = False)

        conn , cursor = init_DWH_Db_connection()

        projects = xl.values.tolist()

        for x in range(len(projects)):
            hashkeyValue = str(projects[x][0]) + str(projects[x][1]) +str(projects[x][2]) +str(projects[x][4]) +str(projects[x][5]) +str(projects[x][6]) 
            bk_value = str(projects[x][3])
            hash_objectbk = hashlib.sha256(bk_value.encode())
            hash_objectValue = hashlib.sha256(hashkeyValue.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hex_digVal = hash_objectValue.hexdigest()
            projects[x].append(now)
            projects[x].append(now)
            projects[x].append(hex_digbk)
            projects[x].append(hex_digVal)

        

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimBlueAntProject"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimBlueAntProject"
        
        insert_tmp_statement = f"INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]  ([ProjectNumber], [ProjectLeaderID], [CustomerBK], [ProjectBK], [ProjectName], [StartDate], [EndDate], [ProjectStateBK], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue]) VALUES (?,?,?,?,?,?,?,'25204', ?,?, ?,?)"
        
       # cursor.fast_executemany = True  #Set fast_executemany  = True
        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        cursor.executemany(insert_tmp_statement, projects) #load data into azure sql db
        logging.info("insertet")
        cursor.commit() #Close the cursor and connection
        conn.commit()

        cursor.execute(f"""
            DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

            WITH CTE AS (
                SELECT 
                    [ProjectNumber],
                    ISNULL(u.[UserID], -1) AS ProjectManagerID,
                    ISNULL(c.[BlueAntCustomerID], -1) AS CustomerID,
                    [ProjectBK],
                    [ProjectName],
                    CAST(CONVERT(varchar(10), [StartDate], 112) AS INT) AS [StartDate],
                    CAST(CONVERT(varchar(10), [EndDate], 112) AS INT) AS [EndDate],
                    ISNULL(s.ProjectStateID,-1) as ProjectStateID,
                    p.[InsertDate],
                    p.[UpdateDate],
                    p.[HashkeyBK],
                    CONCAT(p.HashkeyValue, '|', ISNULL(u.[UserID], -1), '|', ISNULL(c.[BlueAntCustomerID], -1),'|',ISNULL(s.ProjectStateID,-1)) AS HashkeyValue
                FROM
                    [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] AS p
                    LEFT JOIN dwh.DimBlueAntCustomer AS c   ON p.CustomerBK = c.CustomerBK
                    LEFT JOIN dwh.DimUser AS u              ON p.ProjectLeaderID = u.BlueAntUserBK
                    LEFT JOIN dwh.DimBlueAntProjectState s  ON p.ProjectStateBK = s.ProjectStateBK
            )
            
            MERGE [{SCHEMA_NAME}].[{TABLE_NAME}] AS tgt
            USING CTE AS src
            ON tgt.ProjectBK = src.ProjectBK
            WHEN MATCHED AND tgt.HashkeyValue <> src.HashkeyValue THEN
                UPDATE SET 
                    tgt.[ProjectNumber] = src.[ProjectNumber],
                    tgt.[ProjectManagerID] = src.[ProjectManagerID],
                    tgt.[CustomerID] = src.[CustomerID],
                    tgt.[ProjectBK] = src.[ProjectBK],
                    tgt.[ProjectName] = src.[ProjectName],
                    tgt.[ProjectStateID] = src.[ProjectStateID],
                    tgt.[StartDateID] = src.[StartDate],
                    tgt.[EndDateID] = src.[EndDate],
                    tgt.InsertDate = src.InsertDate,
                    tgt.UpdateDate = src.UpdateDate,
                    tgt.HashKeyBK = src.HashKeyBK,
                    tgt.HashkeyValue = src.HashkeyValue
            WHEN NOT MATCHED THEN
                INSERT ([ProjectNumber], [ProjectManagerID], [CustomerID], [ProjectBK], [ProjectName], [ProjectStateID],[StartDateID], [EndDateID], [InsertDate], [UpdateDate], [HashkeyBK], [HashkeyValue])
                VALUES (src.[ProjectNumber], src.[ProjectManagerID], src.[CustomerID], src.[ProjectBK], src.[ProjectName], src.[ProjectStateID], src.[StartDate], src.[EndDate], src.InsertDate, src.UpdateDate, src.HashkeyBK, src.HashkeyValue)
            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO [tmp].[changed] ([Status], Anzahl)
            SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
            GROUP BY [Status];
        """)

        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)

        close_DWH_DB_connection(conn, cursor)  
    
        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

