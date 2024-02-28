#Autor:PSC
import traceback
import azure.functions as func
import logging
from common import init_DWH_Db_connection, DWH_table_logging, close_DWH_DB_connection


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        #----------------------------------------------------------------------------------------
        # Now, update the Table FactPlanningSCD2 and insert the data into tmp table. 
        #----------------------------------------------------------------------------------------
        
        # Define constants for database schemas
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactPlanningSCD2"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactPlanningSCD2"
                
        conn, cursor = init_DWH_Db_connection() 

        cursor.execute(f"TRUNCATE TABLE {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME}")
        conn.commit()

        InsertQueryTmp = f"""
            DECLARE @time DATETIME = CONVERT(DATETIME, GETDATE());
            DECLARE @rowstarttime INT = CONVERT(INT, CONVERT(VARCHAR(8), GETDATE(), 112));
            DECLARE @rowendtime INT = 99991231;
            DECLARE @iscurrent BIT = 1;
            DECLARE @RowCountDst INT = (SELECT COUNT(1) FROM {SCHEMA_NAME}.{TABLE_NAME});

        
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
                FROM [pln].PLANPRJMA a
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
                    @rowstarttime AS RowStartDate,
                    @rowendtime AS RowEndDate,
                    @iscurrent AS RowIsCurrent,
                    @time AS InsertDate,
                    @time AS UpdateDate,

                    --DBR 20241801 doppelte Berechnung, 'wenn wir etwas doppelt machen, ist es einmal zu viel'        
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(BlueAntProjectID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(DealID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(UserID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(MinDate,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(MaxDate,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(Billable,'-1')), 
                            '|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK,

                    --DBR 20241801 doppelte Berechnung, 'wenn wir etwas doppelt machen, ist es einmal zu viel'                        
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
                        CONVERT(VARCHAR(30), ISNULL(PlanningValue,-1))
                    ), 2)) AS HashKeyValue
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
                    @rowstarttime AS RowStartDate,
                    @rowendtime AS RowEndDate,
                    @iscurrent AS RowIsCurrent,
                    @time AS InsertDate,
                    @time AS UpdateDate,

                    --DBR 20241801 doppelte Berechnung, 'wenn wir etwas doppelt machen, ist es einmal zu viel'         
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            CONVERT(VARCHAR(30), ISNULL(BlueAntProjectID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(DealID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(UserID,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL( MinDate,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(MaxDate,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(Billable,'-1')),
                            '|',CONVERT(VARCHAR(30), ISNULL(PlanningType,'-1'))
                        )
                    ), 2)) AS HashKeyBK,

                    --DBR 20241801 doppelte Berechnung, 'wenn wir etwas doppelt machen, ist es einmal zu viel'        
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256',
                        CONVERT(VARCHAR(30), ISNULL(PlanningValue,-1)
                    )), 2)) AS HashKeyValue
                FROM cte_Month1 a
                LEFT JOIN cte_Date_Month b ON CONCAT(a.[Year], a.[Month]) = b.YearMonthID
            )

            INSERT INTO {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME}
            (
                    BlueAntProjectID, 
                    DealID, 
                    UserID, 
                    StartDateID, 
                    EndDateID, 
                    Billable, 
                    PlanningType, 
                    PlanningValue,
                    RowStartDate,
                    RowEndDate,
                    RowIsCurrent,
                    InsertDate,
                    UpdateDate,
                    HashKeyBK,
                    HashKeyValue

            )
                SELECT 
                    BlueAntProjectID, 
                    DealID, 
                    UserID, 
                    StartDateID, 
                    EndDateID, 
                    Billable, 
                    PlanningType, 
                    SUM(PlanningValue) AS PlanningValue,                --DBR 20240117 Wir brauchen die Summe, da -1 Projekte/Deals sauber aufgelöst werden sollen
                    CASE
                        WHEN @RowCountDst = 0  THEN 19000101
                        ELSE MAX(RowStartDate)                          --DBR 20240117 Das ist ein fixer Paramter der innerhalb des Ladeprozesses für alle Zeilen gesetzt wird und deswegen kann MAX verwenden werden; MAX(1;1;1;1;1;1)  = 1
                    END AS RowStartDate, 
                    MAX(RowEndDate) AS RowEndDate,                      --DBR 20240117 Das ist ein fixer Paramter der innerhalb des Ladeprozesses für alle Zeilen gesetzt wird und deswegen kann MAX verwenden werden; MAX(1;1;1;1;1;1)  = 1
                    MAX(CAST(RowIsCurrent AS INT)) AS RowIsCurrent, 	--DBR 20240117 Das ist ein fixer Paramter der innerhalb des Ladeprozesses für alle Zeilen gesetzt wird und deswegen kann MAX verwenden werden; MAX(1;1;1;1;1;1)  = 1
                    MAX(InsertDate) AS InsertDate, 						--DBR 20240117 Das ist ein fixer Paramter der innerhalb des Ladeprozesses für alle Zeilen gesetzt wird und deswegen kann MAX verwenden werden; MAX(1;1;1;1;1;1)  = 1
                    MAX(UpdateDate) AS UpdateDate, 						--DBR 20240117 Das ist ein fixer Paramter der innerhalb des Ladeprozesses für alle Zeilen gesetzt wird und deswegen kann MAX verwenden werden; MAX(1;1;1;1;1;1)  = 1
                    MAX(HashKeyBK) AS HashKeyBK, 						--DBR 20240117 An der Stelle können wir den Hashkey Max verwenden; da die Hashkeys aktuell alle im Group By enthalten sind)
                    MAX(HashKeyValue) AS HashKeyValue					--DBR 20240117 Hier bräuchten wir nicht den MAX sondern müssten den HashKey nach der Gruppierung berechnen; SUM(1+2) = 3; MAX(Hashkey(1), Haskey(2) )!= Hashkey(3)
                    
                FROM cte_final

                /*

                    DBR 20240117
                    fachliche Anforderung, das sind Interne Projekte die in TM1 angelegt wurden aber ohne Referenz
                    Weil diese 2mal einen -1 SK haben, müssen wir diese Werte Aggregieren sodass wir nur eine Zeile Pro HashkeyBK im Ziel haben

                    Die Having Klausel kann fachlich nicht vorkommen, dass ein Projekt oder Deal doppelte Schlüssel haben. 
                    Falls doch dann sind das Aufgrund von technischen Problemen und dann wollen wir auf einen Fehler laufen.


                */
                GROUP BY 
                    BlueAntProjectID
                    , DealID
                    , UserID
                    , StartDateID
                    , EndDateID
                    , Billable
                    , PlanningType
                HAVING NOT (DealID = -1 AND BlueAntProjectID = -1);     
        """
        
        try:
            # Execute the SQL query
            cursor.execute(InsertQueryTmp)
            conn.commit()
            logging.info(f"Data was successfully inserted into the Tmp Table: {TMP_TABLE_NAME}")

        except Exception as e:
            # If an error occurs, rollback changes
            conn.rollback()
            logging.error(f"An error occurred during the insert: {e}")
            raise
        
        
        # ----------------------------------------------------------------------------------------
        # Update just values that chnaged during the same day! 
        # ----------------------------------------------------------------------------------------
        
        UpdateQueryDWH = f"""
            UPDATE dst 
            SET 
                dst.PlanningValue = src.PlanningValue
                , dst.RowIsCurrent = 1
                , dst.UpdateDate = src.UpdateDate
                , dst.HashKeyValue = src.HashKeyValue
                , dst.RowEndDate = src.RowEndDate
            FROM {SCHEMA_NAME}.{TABLE_NAME} dst
                INNER JOIN {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} src ON dst.HashKeyBK = src.HashKeyBK
            WHERE 
                (
                    dst.RowStartDate = src.RowStartDate
                    OR 
                    (
                        dst.RowStartDate = 19000101 -- Initial Loading Date
                        AND CONVERT(DATE,dst.InsertDate) = CONVERT(DATE,GETDATE())
                    )
                ) 
                --and dst.RowIsCurrent = 1  
                /*
                    DBR 20240117 durch die Einschränkung auf das Startdatum müsste diese Bedigung obsolet sein; für den Fall das am Tag etwas deaktiviertes wieder aktivert wird
                    in diesem Intraday Update werden lediglich die RowIsCurrent auf 1 und die EndDates korrigiert.
                    Im späteren UpdateQueryDWHNotExistInSource werden die Werte die 1 waren auf 0 gesetzt 

                    --Start
                    Wert		Wert	RowIsCurrent	RowStartDate	RowEndDate
                    fakturiert	13.0000	1				20231016		99991231	

                    --untergägiges Update 1 fakturiert -> nicht fakturiert
                    Wert		Wert	RowIsCurrent	RowStartDate	RowEndDate
                    fakturiert	13.0000	1				20231016		99991231	
                    nicht		13.0000	0				20231016		20231016	

                    --untergägiges Update 1 nicht fakturiert -> fakturiert
                    Wert		Wert	RowIsCurrent	RowStartDate	RowEndDate
                    fakturiert	13.0000	0				20231016		20231016	
                    nicht		13.0000	1				20231016		99991231	


                */
                and 
				 (
					dst.HashKeyValue <> src.HashKeyValue
					OR dst.RowIsCurrent != src.RowIsCurrent  --multiple changes within bk
				)
        """
        # ----------------------------------------------------------------------------------------
        # Update Records that do not exist in Source! 
        # ----------------------------------------------------------------------------------------
        UpdateQueryDWHNotExistInSource = f"""
            DECLARE @time DATETIME = CONVERT(DATETIME, GETDATE());
            DECLARE @endtimeid INT = CONVERT(INT, CONVERT(VARCHAR(8), DATEADD(day,-1,GETDATE()), 112));
            
            UPDATE dst 
            SET 
                dst.RowIsCurrent = 0
	            ,dst.UpdateDate = @time
                ,dst.RowEndDate = CASE
                                    WHEN @endtimeid < dst.RowStartDate THEN dst.RowStartDate --DBR 20240118 this is only the case when we have multiply intraday changes within BK see UpdateQueryDWH for sample 
                                    ELSE @endtimeid
                                END  
            FROM {SCHEMA_NAME}.{TABLE_NAME} dst
                LEFT JOIN {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} src ON dst.HashKeyBK = src.HashKeyBK
            WHERE src.HashKeyBK is null
                AND dst.RowIsCurrent = 1
        """

        # ----------------------------------------------------------------------------------------
        # SCD2 Merge Statement: Merge TMP into DWH Table
        # ----------------------------------------------------------------------------------------
            
        MergeStatement = f"""
             DECLARE @MergeLog TABLE([Status] VARCHAR(20));
             DECLARE @endtimeid INT = CONVERT(INT, CONVERT(VARCHAR(8), DATEADD(day,-1,GETDATE()), 112));
            
            
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                BlueAntProjectID, 
                DealID, 
                UserID, 
                StartDateID, 
                EndDateID, 
                Billable, 
                PlanningType,
                PlanningValue, 
                RowStartDate, 
                RowEndDate, 
                RowIsCurrent, 
                InsertDate, 
                UpdateDate, 
                HashKeyBK, 
                HashKeyValue
            )
            SELECT 
                BlueAntProjectID, 
                DealID, 
                UserID, 
                StartDateID, 
                EndDateID, 
                Billable, 
                PlanningType, 
                PlanningValue, 
                RowStartDate, 
                RowEndDate, 
                RowIsCurrent, 
                InsertDate, 
                UpdateDate, 
                HashKeyBK, 
                HashKeyValue
            FROM (
                MERGE {SCHEMA_NAME}.{TABLE_NAME} AS dst
                USING {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} AS src
                ON dst.HashKeyBK = src.HashKeyBK
                    AND dst.RowIsCurrent = 1 
                WHEN MATCHED 
                    AND src.RowStartDate > dst.RowStartDate 
                    AND dst.HashKeyValue <> src.HashKeyValue 
    
                THEN UPDATE SET dst.RowEndDate = @endtimeid, 
					            dst.UpdateDate = src.UpdateDate,
					            dst.RowIsCurrent = 0
                
                WHEN NOT MATCHED BY TARGET 
                THEN INSERT (
					BlueAntProjectID,
					DealID, 
					UserID, 
					StartDateID, 
					EndDateID, 
					Billable, 
					PlanningValue, 
					PlanningType, 
					RowStartDate, 
					RowEndDate, 
					RowIsCurrent, 
					InsertDate, 
					UpdateDate, 
					HashKeyBK, 
					HashKeyValue)
                VALUES (
					src.BlueAntProjectID, 
					src.DealID, 
					src.UserID, 
					src.StartDateID, 
					src.EndDateID, 
					src.Billable, 
					src.PlanningValue, 
					src.PlanningType, 
					src.RowStartDate, 
					src.RowEndDate, 
					src.RowIsCurrent, 
					src.InsertDate, 
					src.UpdateDate, 
					src.HashKeyBK, 
					src.HashKeyValue)
                
            	OUTPUT $Action AS Action
                    ,src.BlueAntProjectID AS BlueAntProjectID 
                    ,src.DealID AS DealID
                    ,src.UserID AS UserID 
                    ,src.StartDateID AS StartDateID 
                    ,src.EndDateID AS EndDateID 
                    ,src.Billable  AS Billable
                    ,src.PlanningType AS PlanningType
                    ,src.PlanningValue AS PlanningValue 
                    ,src.RowStartDate AS RowStartDate 
                    ,src.RowEndDate AS RowEndDate 
                    ,src.RowIsCurrent AS RowIsCurrent 
                    ,src.InsertDate AS InsertDate
                    ,src.UpdateDate AS UpdateDate 
                    ,src.HashKeyBK AS HashKeyBK 
                    ,src.HashKeyValue AS HashKeyValue
            ) MergeStatement
            WHERE Action = 'Update';
            
            /*
                --DBR 20240118 Sinn nicht erkannt, ggf. überbleibsel vom Debugging
                INSERT INTO [{TMP_SCHEMA_NAME}].[changed] ([Status],Anzahl)
                SELECT [Status], count(*) as Anzahl FROM @MergeLog
                GROUP BY [Status];
            */
        """
        
        try:
            # Execute the SQL query
            cursor.execute(UpdateQueryDWH)
            conn.commit()
            logging.info(f"Executed Update Query successfully, for changes during the day. Table: {TABLE_NAME}")
            
            cursor.execute(UpdateQueryDWHNotExistInSource)
            conn.commit()
            logging.info(f"Executed Update Query successfully, for records that are not on source: {TABLE_NAME}")
            


            # Execute the SQL query
            cursor.execute(MergeStatement)
            conn.commit()
            logging.info(f"Executed Merge Query successfully, merge data into Fact Table: {TABLE_NAME}")

        except Exception as e:
            # If an error occurs, rollback changes
            conn.rollback()
            logging.error(f"An error occurred during the merge: {e}")
            raise
            
        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)   
        
        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)
    
    except Exception as e:

        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)


