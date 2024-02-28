#Change: Pascal Schütze 
import logging
import hashlib
from xml.etree import ElementTree as ET
import azure.functions as func
from common import  get_relative_blob_path, get_latest_blob_from_staging, truncate_tmp_table, insert_into_tmp_table, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging,get_time_in_string
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:
    
    try:
    
        # -----------------------------------------------------------
        # Load the latest xml-file from the staging area
        # -----------------------------------------------------------
    
        CONTAINER = 'blueantabsencetypes'

        now = get_time_in_string()[0]
    
        relative_path = get_relative_blob_path()
        
        blob_content = get_latest_blob_from_staging(CONTAINER, folder_name=relative_path).content_as_text()

        # -----------------------------------------------------------
        # Store the blob_content into variables to load into the DWH
        # -----------------------------------------------------------

        tree = ET.fromstring(blob_content)

        name_list = []
        id_list = []
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        # Namespace-Deklarationen
        namespaces = {
            'ns0': 'http://schemas.xmlsoap.org/soap/envelope/',
            'ns1': 'http://absence.blueant.axis.proventis.net/'
        }

        # AbsenceType-Elemente finden
        absence_types = tree.findall('.//ns1:AbsenceType', namespaces)

        # AbsenceTypeID und Namen auslesen
        for absence_type in absence_types:
            absence_type_id = absence_type.find('ns1:absenceTypeID', namespaces).text
            name = absence_type.find('ns1:name', namespaces).text
            name_list.append(name)
            id_list.append(absence_type_id)

            datetime_list.append(now)
            #hashkeybk und haskeyvalue
            bk=absence_type_id
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)

            value= str(name) 
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)

        dataset = list(zip(id_list,name_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        # -----------------------------------------------------------
        # Load the variables into the tmp table and merge into the DWH
        # -----------------------------------------------------------
        
        SCHEMA_NAME = 'dwh'
        TABLE_NAME = 'DimBlueAntAbsenceType'
        TMP_SCHEMA_NAME = 'tmp'
        TMP_TABLE_NAME = 'dwh_DimBlueAntAbsenceType'
        
        conn, cursor = init_DWH_Db_connection()
        
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        truncate_tmp_table(TMP_SCHEMA_NAME, TMP_TABLE_NAME, cursor, conn)

        insert_tmp_statement = f""" INSERT INTO {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} (AbsenceTypeBK, AbsenceTypeName, HashkeyBK, HashkeyValue, InsertDate, UpdateDate) VALUES (?,?,?,?,?,?)"""

        insert_into_tmp_table(dataset, insert_tmp_statement, cursor, conn)

        cursor.execute(f"""
        -- Synchronize the target table with refreshed data from the source table
        DECLARE @MergeLog TABLE ([Status] VARCHAR(20));

        MERGE {SCHEMA_NAME}.{TABLE_NAME} AS TARGET
        USING {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} AS SOURCE
        ON TARGET.HashkeyBK = SOURCE.HashkeyBK --and tgt.HashkeyValue != src.HashkeyValue
        WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue 
        THEN UPDATE SET TARGET.HashkeyValue = SOURCE.HashkeyValue,
                        TARGET.AbsenceTypeName = SOURCE.AbsenceTypeName,
                        TARGET.UpdateDate = SOURCE.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (AbsenceTypeBK, AbsenceTypeName, HashkeyBK, HashkeyValue, InsertDate, UpdateDate)
            VALUES (SOURCE.AbsenceTypeBK, SOURCE.AbsenceTypeName, SOURCE.HashkeyBK, SOURCE.HashkeyValue, SOURCE.InsertDate, SOURCE.UpdateDate)
        OUTPUT $action AS [Status] INTO @MergeLog;

        INSERT INTO tmp.[changed] ([Status], Anzahl)
        SELECT [Status], COUNT(*) AS Anzahl FROM @MergeLog
        GROUP BY [Status];
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