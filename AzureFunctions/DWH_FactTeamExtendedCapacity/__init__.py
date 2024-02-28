import logging
import azure.functions as func
import pandas as pd
from common import insert_into_loadlogging, truncate_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        #----------------------------------------------------------------------------------------
        # Now, update the Table FactTeamExtendedCapacity and insert the data into tmp table. 
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactTeamExtendedCapacity"
        
        conn, cursor = init_DWH_Db_connection()

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
        truncate_tmp_table(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
        InsertQuery = f""" 
        WITH 
            cte_Date_Month AS (
                SELECT 
                    MAX(DateID) AS MaxDate,
                    MIN(DateID) AS MinDate,
                    YearMonthID
                FROM 
                    dwh.dimdate
                GROUP BY 
                    YearMonthID
            ),
            cte_Date_Week AS (
                SELECT 
                    MAX(DateID) AS MaxDate,
                    MIN(DateID) AS MinDate,
                    CalendarWeekYearText
                FROM 
                    dwh.dimdate
                GROUP BY 
                    CalendarWeekYearText
            ),
            cte_basis AS (
                SELECT 
                    Mitarbeiter AS UserID,
                    Wert AS Capacity,
                    SUBSTRING(Tag, 0, 5) AS [Year],
                    CASE WHEN Tag LIKE '%-W%' THEN 'Week'
                    WHEN LEN(Tag) = 8 THEN 'Month'
                    ELSE 'Day' END AS [Difference],
                --  IIF(Tag LIKE '%-W%', 'Week', 'Month') AS [Difference],
                    Tag AS Datum
                FROM 
                    [pln].[KAPAZITAET]
            ),
            cte_week1 AS (
                SELECT 
                    UserID,
                    Capacity,
                    [Year],
                    SUBSTRING(Datum, CHARINDEX('W', Datum) + 1, 2) AS [Week]
                FROM 
                    cte_basis
                WHERE 
                    [Difference] = 'Week'
            ),
            cte_Month1 AS (
                SELECT 
                    UserID,
                    Capacity,
                    [Year],
                    SUBSTRING(Datum, CHARINDEX('-', Datum) + 1, 2) AS [Month]
                FROM 
                    cte_basis
                WHERE 
                    [Difference] = 'Month'
            ),
            cte_final AS (
                SELECT 
                    UserID,
                    Capacity,
                    MinDate AS StartDateID,
                    MaxDate AS EndDateID
                FROM 
                    cte_week1 a
                LEFT JOIN 
                    cte_Date_Week b ON CONCAT(a.[Year], ' ', a.[Week]) = b.CalendarWeekYearText
                UNION ALL
                SELECT 
                    UserID,
                    Capacity,
                    MinDate AS StartDateID,
                    MaxDate AS EndDateID
                FROM 
                    cte_Month1 a
                LEFT JOIN 
                    cte_Date_Month b ON CONCAT(a.[Year], a.[Month]) = b.YearMonthID
                
                UNION ALL
                Select
                    UserID,
                    Capacity,
                    b.DateID as StartDateID,
                    b.DateID as EndDateID
                FROM 
                    cte_basis a
                LEFT JOIN dwh.DimDate b on a.Datum = b.DateBK
                WHERE 
                    [Difference] = 'Day'

            )

        INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}]
        SELECT 
            UserID
            ,Capacity
            ,StartDateID
            ,EndDateID
        FROM cte_final;  
        """
        
        try:
            # Execute the SQL query
            cursor.execute(InsertQuery)
            conn.commit()
            logging.info(f"Data was successfully inserted into the Fact Table: {TABLE_NAME}")

        except Exception as e:
            # If an error occurs, rollback changes
            conn.rollback()
            logging.error(f"An error occurred during the insert: {e}")
        
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)

