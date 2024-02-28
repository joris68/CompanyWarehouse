#Autor:MC, Changed by: PSC (08.09.2023)
import traceback
import azure.functions as func
import logging
from common import init_DWH_Db_connection, close_DWH_DB_connection


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
        
        #----------------------------------------------------------------------------------------
        # Now, update the Table FactTeamExtendedCapacity and insert the data into tmp table. 
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactPlanning"
        
        conn, cursor = init_DWH_Db_connection() 

        cursor.execute(f"TRUNCATE TABLE {SCHEMA_NAME}.{TABLE_NAME}")
        conn.commit()

        InsertQuery = """
DECLARE @time DATETIME = CONVERT(DATETIME, GETDATE());

        WITH cte_Date_Month AS (
            SELECT 
                MAX(DateID) AS MaxDate,
                MIN(DateID) AS MinDate,
                YearMonthID
            FROM dwh.dimdate
            GROUP BY YearMonthID
        ),
        cte_Date_Week AS (
            SELECT 
                MAX(DateID) AS MaxDate,
                MIN(DateID) AS MinDate,
                CalendarWeekYearText
            FROM dwh.dimdate
            GROUP BY CalendarWeekYearText
        ),
        cte_basis AS (
            SELECT 
				ISNULL(b.BlueAntProjectID,-1) BlueAntProjectID,
                ISNULL(d.DealID, -1) AS DealID,
                a.Projekt,
                Mitarbeiter AS UserID,
                IIF(Fakturiert = 'nicht fakturiert', 0, 1) AS Billable,
                IIF(Reserviert_Geplant = 'Geplant', 0, 1) AS PlanningType,
                CONVERT(DECIMAL(9, 4), Wert) AS PlanningValue,
                SUBSTRING(Datum, 0, 5) AS [Year],
                IIF(Datum LIKE '%-W%', 'Week', 'Month') AS [Difference],
                Datum
            FROM [pln].[PLANPRJMA] a
            LEFT JOIN dwh.DimDeal d ON 
                CASE 
                    WHEN LEFT(a.Projekt, 3) = 'de-' THEN CAST(RIGHT(a.Projekt, LEN(a.Projekt) - 3) AS INT)
                    ELSE NULL 
                END = d.DealID

			LEFT JOIN [pln].[pln_DimProjectInternal] p ON 
                CASE 
                    WHEN LEFT(a.Projekt, 3) = 'in-' THEN CAST(RIGHT(a.Projekt, LEN(a.Projekt) - 3) AS INT)
                    ELSE NULL 
                END = p.DealID

			LEFT JOIN dwh.dimblueantproject b ON a.Projekt = b.ProjectNumber OR p.ProjectNumber = b.ProjectNumber
        ),
        cte_week1 AS (
            SELECT 
                BlueAntProjectID,
                DealID,
                UserID,
                Billable,
                PlanningType,
                PlanningValue,
                [Year],
                SUBSTRING(Datum, CHARINDEX('W', Datum) + 1, 2) AS [Week],
                Projekt
            FROM cte_basis
            WHERE [Difference] = 'Week'
        ),
        cte_Month1 AS (
            SELECT 
                BlueAntProjectID,
                DealID,
                UserID,
                Billable,
                PlanningType,
                PlanningValue,
                [Year],
                SUBSTRING(Datum, CHARINDEX('-', Datum) + 1, 2) AS [Month],
                Projekt
            FROM cte_basis
            WHERE [Difference] = 'Month'
        ),
        cte_final AS (
            SELECT 
                BlueAntProjectID,
                DealID,
                UserID,
                MinDate AS StartDateID,
                MaxDate AS EndDateID,
                Billable,
                PlanningValue,
                PlanningType,
                @time AS InsertDate,
                @time AS UpdateDate
            FROM cte_week1 a
            LEFT JOIN cte_Date_Week b ON CONCAT(a.[Year], ' ', a.[Week]) = b.CalendarWeekYearText

            UNION ALL

            SELECT 
                BlueAntProjectID,
                DealID,
                UserID,
                MinDate AS StartDateID,
                MaxDate AS EndDateID,
                Billable,
                PlanningValue,
                PlanningType,
                @time AS InsertDate,
                @time AS UpdateDate
            FROM cte_Month1 a
            LEFT JOIN cte_Date_Month b ON CONCAT(a.[Year], a.[Month]) = b.YearMonthID
        )
        
        INSERT INTO dwh.FactPlanning
		SELECT 
		BlueAntProjectID,
		DealID,
        UserID,
        StartDateID,
        EndDateID,
        Billable,
        PlanningValue,
        PlanningType,
        InsertDate,
        UpdateDate
		
		from cte_final;
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


