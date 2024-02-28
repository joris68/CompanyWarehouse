#Autor:MC
from datetime import datetime
import logging
import azure.functions as func
from xml.etree import ElementTree as ET
import hashlib
from common import init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_latest_blob_from_staging,insert_into_tmp_table,truncate_tmp_table,get_time_in_string
import traceback
import pandas as pd
from io import StringIO

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'personio'
   
        #Time      
        now = get_time_in_string()[0]

        blob = get_latest_blob_from_staging(CONTAINER)

        blob_content=blob.readall().decode('utf-8')

        column_names = ["name", "mail", "absenceType","startDate","endDate"]

        #skiprows ignoriert die erste Zeile da diese die Spaltennamen enthalten und wir sie selber hier definieren um mehr Kontrolle zu haben.
        df = pd.read_csv(StringIO(blob_content), header=None,skiprows=1, names=column_names)

        # Anwendung von lambda-Funktion auf jede Zeile des DataFrame
        df['HashkeyBK'] = df.apply(lambda row: (hashlib.sha256((str(row['mail'])+'|'+str(row['startDate'])+'|'+str(row['endDate'])).encode())).hexdigest(), axis=1)
        df['HashkeyValue'] = df.apply(lambda row: (hashlib.sha256((str(row['name'])+'|'+str(row['absenceType'])).encode())).hexdigest(), axis=1)
        
        df['InsertDate'] = now
        df['UpdateDate'] = now

        #Daten aus dem DataFrame in eine Liste von Tupeln umwandeln für Insert mit cursor.execute
        dataset = [tuple(row) for row in df.values]

        conn , cursor = init_DWH_Db_connection()

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntAbsence"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntAbsence"

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement = f"""
            INSERT INTO {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} 
            (
                Name, 
                UserEmail, 
                AbsenceType, 
                StartDate, 
                EndDate,
                HashkeyBK,
                HashkeyValue,
                InsertDate,
                UpdateDate
            ) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 

        MergeQueryTmp = f"""
            -- Synchronize the target table with refreshed data from source table
            DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

            WITH cte AS (
                SELECT
                    -1 AS AbsenceID, -- Damit es ohne große Probleme in die bestehende Tabelle gemerged werden kann
                    ISNULL(b.UserID, -1) AS UserID,
                    ISNULL(c.BlueAntAbsenceTypeID, -1) AS BlueAntAbsenceTypeID,
                    CAST(CONVERT(varchar(10), CONVERT(date, a.StartDate), 112) AS INT) AS DateFromID,
                    CAST(CONVERT(varchar(10), CONVERT(date, a.EndDate), 112) AS INT) AS DateToID,
                    'released' AS [State],
                    convert(bit,0) as IsDeleted,
                    LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                        CONCAT
                        (
                            a.HashkeyValue    --DBR 20240130; nicht sauber dem im Vorfeld zu berechnen und dann im Nachgang nochmal
                        , '|', ISNULL(b.UserID, -1)
                        , '|', ISNULL(c.BlueAntAbsenceTypeID, -1)
                        )
                    ), 2)) AS HashkeyValue,
                    a.HashkeyBK,
                    a.InsertDate,
                    a.UpdateDate
                FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} a
                LEFT JOIN {SCHEMA_NAME}.dimuser b ON a.UserEmail = b.UserEmail
                LEFT JOIN {SCHEMA_NAME}.DimBlueAntAbsenceType c ON a.AbsenceType = c.AbsenceTypeName
            ) 

            MERGE {SCHEMA_NAME}.{TABLE_NAME} AS DST
            USING cte AS src
            ON DST.HashkeyBK = src.HashkeyBK
            -- and DST.HashkeyValue != src.HashkeyValue
            WHEN MATCHED AND DST.HashkeyValue <> src.HashkeyValue
            THEN UPDATE SET
                DST.HashkeyValue = src.HashkeyValue,
                DST.AbsenceID = src.AbsenceID,
                DST.UserID = src.UserID,
                DST.BlueAntAbsenceTypeID = src.BlueAntAbsenceTypeID,
                DST.DateFromID = src.DateFromID,
                DST.DateToID = src.DateToID,
                DST.[State] = src.[State],
                DST.IsDeleted = src.IsDeleted,
                DST.UpdateDate = src.UpdateDate
            -- When no records are matched, insert the incoming records from source table to target table
            WHEN NOT MATCHED BY TARGET 
            THEN INSERT (
                AbsenceID, 
                UserID, 
                BlueAntAbsenceTypeID,
                DateFromID, 
                DateToID, 
                [State],
                IsDeleted,
                HashkeyBK, 
                HashkeyValue, 
                InsertDate, 
                UpdateDate
            ) 
            VALUES (
                src.AbsenceID, 
                src.UserID, 
                src.BlueAntAbsenceTypeID,
                src.DateFromID,
                src.DateToID, 
                src.[State],
                src.IsDeleted,
                src.HashkeyBK, 
                src.HashkeyValue, 
                src.InsertDate, 
                src.UpdateDate
            )
            OUTPUT $action AS [Status] INTO @MergeLog;

            INSERT INTO tmp.[changed] ([Status], Anzahl)
            SELECT [Status], COUNT(*) AS Anzahl
            FROM @MergeLog
            GROUP BY [Status];
                       
        """

        #Update die DeleteFlags. Kommentare sind im SQL-Skript.
        UpdateQueryDeleted = f"""
                       
            -- DBR: 20240124
            -- Ich kann auch immer alle Abwesenheiten für euch auslesen (oder zum Beispiel heute - 30 Tage),
            -- dann bekommst du die verlängerten Abwesenheiten immer mit bzw. innerhalb des 30 Tage Limits,
            -- aber das erhöht bei euch den Aufwand für das Aussortieren bereits verarbeiteter Abwesenheiten.
            -- => würde ich dann die Variante mit Rückwirkend 1 Jahr nehmen. 
            -- und technisch von uns eine Annahme ist, dass damit das Startdatum (DateFromID) gemeint ist.
            DECLARE @AbsenceDate INT = (
                SELECT CAST(
                    CONVERT(varchar(10),
                    GREATEST(convert(date,'2024-01-01'), DATEADD(year,-1, convert(date,'{now}'))), 112) as INT
                )
            );

            -- Update erstmal alle Zeilen bis vor 1 Jahr Mindestens aber 01.01.2024
            -- wo der HashkeyBK übereinstimmt in Quelle und Ziel mit den StandardValues.
            -- So achte ich auch auf die Anforderung von DBR:
            -- Springer berücksichtigen (einmal gelöscht, kommt aber warum auch immer wieder)
            -- Erwartung  ISIsDeleted wird von 1 auf 0 zurückgesetzt und das IsDeletedat wird genullt
            UPDATE {SCHEMA_NAME}.{TABLE_NAME}
            SET 
                --Wenn schon davor IsDeleted war dann wird UpdateDate geupdated aber sonst nicht.
                UpdateDate = CONVERT(datetime, CONVERT(datetimeoffset,'{now}')),
                IsDeleted = 0,
                DeletedTimestamp = null
            WHERE DateFromID >= @AbsenceDate
                AND HashkeyBK IN (SELECT HashkeyBK FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME})
                AND IsDeleted =1;

            -- Update alle Zeilen, welche die mal gelöscht waren, jetzt aber nicht mehr
            -- (mindestens aber 01.01.2024) 
            UPDATE {SCHEMA_NAME}.{TABLE_NAME}
            SET UpdateDate = CONVERT(datetime, CONVERT(datetimeoffset,'{now}')),
                IsDeleted = 1,
                DeletedTimestamp = CONVERT(datetime, CONVERT(datetimeoffset,'{now}'))
                WHERE DateFromID >= @AbsenceDate
                    AND HashkeyBK NOT IN (SELECT HashkeyBK FROM {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME})
                    AND IsDeleted = 0; 
        """

        try:
            # Execute the SQL query
            cursor.execute(MergeQueryTmp)

            logging.info("The Merge statement was successfully executed.")

            cursor.execute(UpdateQueryDeleted)

            logging.info("The Update statement was successfully executed.")

            conn.commit()

            logging.info(f"Data was successfully inserted into the dwh Table: {TABLE_NAME}")

        except Exception as e:
            # If an error occurs, rollback changes
            conn.rollback()

            logging.error(f"An error occurred during the insert: {e}")
            
            raise


        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed."  ,status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
    
"""
#DBR 20240118 In BlueAnt werden die Abwesenheiten nicht mehr gespeichert, wir wollen aber weiterhin den Code behalten deswegen haben wir ihn auskommentiert

from datetime import datetime
import logging
import azure.functions as func
import pytz
from xml.etree import ElementTree as ET
import hashlib
from common import get_relative_blob_path, create_file_name, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_latest_blob_from_staging,insert_into_tmp_table,truncate_tmp_table
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantabsences'
   
        timezone = pytz.timezone('Europe/Berlin')
        # Get current date and time components
        now = datetime.now(timezone)
        
        blob = get_latest_blob_from_staging(CONTAINER,folder_name=get_relative_blob_path() )
        blob_content = blob.content_as_text()

        xml_content = ET.fromstring(blob_content)

        personid_list = []
        absenceid_list = []
        absencetypeid_list = []
        from_list = []
        to_list = []
        state_list = []
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        # Namespace definieren
        namespace = {'ns5': 'http://absence.blueant.axis.proventis.net/'}

        # Absence-Elemente auswählen
        absences = xml_content.findall('.//ns5:Absence', namespace)

        # AbsenceID und PersonID auslesen
        for absence in absences:
            absence_id = absence.find('ns5:absenceID', namespace).text
            person_id = absence.find('ns5:personID', namespace).text
            absencetype_id = absence.find('ns5:absenceTypeID', namespace).text
            from_date = absence.find('ns5:from', namespace).text
            to_date = absence.find('ns5:to', namespace).text
            state = absence.find('ns5:state', namespace).text

            absenceid_list.append(absence_id)
            personid_list.append(person_id)
            absencetypeid_list.append(absencetype_id)
            from_list.append(from_date)
            to_list.append(to_date)
            state_list.append(state)

            #hashkeybk und haskeyvalue
            bk=absence_id
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)

            value= str(person_id) + str(absencetype_id) + str(from_date)+ str(to_date)+ str(state)
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)

            datetime_list.append(now)

        dataset = list(zip(absenceid_list,personid_list,absencetypeid_list,from_list,to_list,state_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))
    
        conn , cursor = init_DWH_Db_connection()

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntAbsence"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntAbsence"

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement = f"INSERT INTO  [tmp].[dwh_FactBlueAntAbsence] (AbsenceId , PersonId , AbsenceTypeBk , DateFrom ,DateTo ,State,HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?, ?, ?, ?,?,?,?,?,?,?)"
        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn) 

        cursor.execute(f"""
"""
            --Synchronize the target table with refreshed data from source table
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
            with cte as(
            select 
                    CONVERT(INT,AbsenceID) as AbsenceID
                    ,ISNULL(b.UserID,-1) as UserID
                    ,ISNULL(c.BlueAntAbsenceTypeID,-1) as BlueAntAbsenceTypeID
                    ,CAST(CONVERT(varchar(10),convert(date,a.DateFrom),112) as INT) DateFromID
                    ,CAST(CONVERT(varchar(10),convert(date,a.DateTo),112) as INT) DateToID
                    ,convert(nvarchar(20),[State]) as [State]
                    ,a.HashkeyBK
                    ,CONCAT(a.HashkeyValue,'|',ISNULL(b.UserID,-1),'|',ISNULL(c.BlueAntAbsenceTypeID,-1)) as HashkeyValue
                    ,a.InsertDate
                    ,a.UpdateDate
            from [tmp].[dwh_FactBlueAntAbsence] a
            LEFT JOIN dwh.DimUser b
            on a.PersonId = b.BlueAntUserBK
            LEFT JOIN dwh.DimBlueAntAbsenceType c
            on a.AbsenceTypeBk =c.AbsenceTypeBK
            ) 
                merge dwh.[FactBlueAntAbsence] as TARGET
                using cte as SOURCE
                on TARGET.HashkeyBK = SOURCE.HashkeyBK --and tgt.HashkeyValue != src.HashkeyValue
                WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue
                THEN UPDATE SET TARGET.HashkeyValue = SOURCE.HashkeyValue,
                    TARGET.AbsenceID = SOURCE.AbsenceID,
                    TARGET.UserID = SOURCE.UserID,
                    TARGET.BlueAntAbsenceTypeID = SOURCE.BlueAntAbsenceTypeID,
                    TARGET.DateFromID = SOURCE.DateFromID,
                    TARGET.DateToID = SOURCE.DateToID,
                    TARGET.[State] = SOURCE.[State],
                    TARGET.UpdateDate = SOURCE.UpdateDate
            --When no records are matched, insert the incoming records from source table to target table
            WHEN NOT MATCHED BY TARGET 
            THEN INSERT (AbsenceID, UserID, BlueAntAbsenceTypeID,DateFromID,DateToID,[State],HashkeyBK,HashkeyValue,InsertDate,UpdateDate ) 
                VALUES (SOURCE.AbsenceID, SOURCE.UserID, SOURCE.BlueAntAbsenceTypeID,SOURCE.DateFromID,SOURCE.DateToID,SOURCE.[State],SOURCE.HashkeyBK,SOURCE.HashkeyValue,SOURCE.InsertDate,SOURCE.UpdateDate)
            --When there is a row that exists in target and same record does not exist in source then delete this record target
            WHEN NOT MATCHED BY SOURCE 
            THEN DELETE 
            OUTPUT  $action as [Status] into @MergeLog;

            INSERT INTO tmp.[changed] ([Status],Anzahl)
            SELECT [Status], count(*) as Anzahl FROM @MergeLog
            GROUP BY [Status];
        """
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

"""