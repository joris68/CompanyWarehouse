#Autor:MC
import logging
import azure.functions as func
import hashlib
from common import init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "DimUser"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_DimUser"

        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME,TABLE_NAME, cursor, conn)

    #-------------------------------------------------------------------------- JiraUser 

        #truncate für fullload
        cursor.execute(f"Truncate Table [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info("Truncated the tmp Table to Merge")

        query = """
        --mit Absprache mit MGO und SMO nehmen wir die untenstehenden Emails raus
            WITH cte_ba AS (
                SELECT 
                    BlueAntUserBK,
                    UserInitials,
                    FirstName,
                    LastName,
                    UserEmail
                FROM dwh.DimBlueAntUser 
                WHERE 
                    UserEmail != 'itbestellungen@ceteris.ag'
                    AND BlueAntUserBK NOT IN (-2, -1)
            ),
            cte_jira AS (
                SELECT 
                    JiraUserBK,
                    UserEmail,
                    UserName
                FROM dwh.DimJiraUser 
                WHERE 
                    UserEmail NOT IN (N'N/A', 'itbestellungen@ceteris.ag', '<N/A>')
            )
            SELECT 
                ISNULL(JiraUserBK, N'N/A') AS JiraUserBK,
                ISNULL(BlueAntUserBK, -1) AS BlueAntUserBK,
                ISNULL(a.UserEmail, b.UserEmail) AS UserEmail,
                ISNULL(UserName, N'N/A') AS UserName,
                ISNULL(UserInitials, N'N/A') AS UserInitials,
                ISNULL(FirstName, N'N/A') AS FirstName,
                ISNULL(LastName, N'N/A') AS LastName
            FROM cte_ba a
            FULL OUTER JOIN cte_jira b
            ON a.UserEmail = b.UserEmail
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        conn.commit()
    # Auf einzelne Spalten zugreifen
        for row in results:
            bk=row[2]
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()

            value= str(row[0]) + str(row[1])+ str(row[3])+ str(row[4])+ str(row[5])+ str(row[6])
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            cursor.execute(f"INSERT INTO  [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (JiraUserBK , BlueAntUserBK , UserEmail , UserName ,UserInitials ,[FirstName],LastName,InsertDate,UpdateDate,HashkeyBK,HashkeyValue) VALUES (?, ?, ?, ?,?,?,?,GETDATE(),GETDATE(),?,?)",row[0],row[1],row[2],row[3],row[4],row[5],row[6],hex_digbk,hex_digvalue )
        conn.commit()
        logging.info("Data Inserted in tmp Table")

        cursor.execute("""
        --Synchronize the target table with refreshed data from source table
        DECLARE @MergeLog TABLE([Status] VARCHAR(20));

        MERGE [dwh].[DimUser] AS TARGET
        USING tmp.dwh_DimUser AS SOURCE 
        ON (TARGET.HashkeyBK = SOURCE.HashkeyBK) 
        --When records are matched, update the records if there is any change
        WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue
        THEN UPDATE SET TARGET.HashkeyValue = SOURCE.HashkeyValue, 
            TARGET.JiraUserBK = Source.JiraUserBK,
            TARGET.BlueAntUserBK = CONVERT(int,SOURCE.BlueAntUserBK),
            TARGET.UserEmail = SOURCE.UserEmail,
            TARGET.UserName = SOURCE.UserName,
            TARGET.UserInitials = SOURCE.UserInitials,
            TARGET.FirstName = SOURCE.FirstName,
            TARGET.LastName = SOURCE.LastName,
            TARGET.UpdateDate = SOURCE.UpdateDate
        --When no records are matched, insert the incoming records from source table to target table
        WHEN NOT MATCHED BY TARGET 
        THEN INSERT (JiraUserBK, BlueAntUserBK, UserEmail,UserName,UserInitials,FirstName,LastName,InsertDate,UpdateDate,HashkeyBK,HashkeyValue ) VALUES (SOURCE.JiraUserBK, CONVERT(int,SOURCE.BlueAntUserBK), SOURCE.UserEmail,SOURCE.UserName,SOURCE.UserInitials,SOURCE.FirstName,SOURCE.LastName,SOURCE.InsertDate,SOURCE.UpdateDate,SOURCE.HashkeyBK,SOURCE.HashkeyValue)
        --When there is a row that exists in target and same record does not exist in source then delete this record target
        --WHEN NOT MATCHED BY SOURCE 
        --THEN DELETE 
        OUTPUT  $action as [Status] into @MergeLog;

        INSERT INTO [tmp].[changed] ([Status],Anzahl)
        SELECT [Status], count(*) as Anzahl FROM @MergeLog
        GROUP BY [Status];
    """)
        conn.commit()
        logging.info("The Merge statement was successfully executed.")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=True)

        # Close the database connection
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed."  , status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
    

      
