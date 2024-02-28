from datetime import datetime
import logging
import azure.functions as func
import pytz
from xml.etree import ElementTree as ET
import hashlib
from common import get_relative_blob_path, create_file_name, get_latest_blob_from_staging, init_DWH_Db_connection, close_DWH_DB_connection, insert_into_loadlogging, DWH_table_logging
import traceback


def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'blueantbudgetvalues'

        timezone = pytz.timezone('Europe/Berlin')
        # Get current date and time components
        now = datetime.now(timezone)
    
        

        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())
        blob_content = blob.content_as_text()
  
        xml_content = ET.fromstring(blob_content)


        project_id_list = []
        projectpropertyType_list = []
        costtypeID_list = []
        investtypeID_list = []
        value_list = []
        hashkeyvalue_list = []
        hashkeybk_list = []
        datetime_list =[]

        # Namespace-Deklarationen
        ns = {'ns1': 'http://budget.blueant.axis.proventis.net/'}

        # Alle <ns1:BudgetValues>-Elemente auswählen
        budget_values = xml_content.findall('.//ns1:BudgetValues', namespaces=ns)

        # Informationen aus den Elementen extrahieren
        for budget_value in budget_values:
            project_id = budget_value.find('ns1:projectID', namespaces=ns).text
            budget_value_list = budget_value.findall('ns1:budgetValueList/ns1:BudgetValue', namespaces=ns)
            
            # Informationen aus der BudgetValue-Liste extrahieren
            for value in budget_value_list:
                project_property_type = value.find('ns1:projectPropertyType', namespaces=ns).text
                value_text = value.find('ns1:value', namespaces=ns).text
                costtypeidvalue = value.find('ns1:costTypeID', namespaces=ns).text
                investtypeidvalue = value.find('ns1:investTypeID', namespaces=ns).text

                project_id_list.append(project_id)
                projectpropertyType_list.append(project_property_type)
                value_list.append(value_text)
                costtypeID_list.append(costtypeidvalue)
                investtypeID_list.append(investtypeidvalue)

                #hashkeybk und haskeyvalue
                bk=str(project_property_type)+str(costtypeidvalue)+str(investtypeidvalue)+str(project_id)
                hash_objectbk = hashlib.sha256(bk.encode())
                hex_digbk = hash_objectbk.hexdigest()
                hashkeybk_list.append(hex_digbk)

                value= str(value_text)
                hash_objectvalue = hashlib.sha256(value.encode())
                hex_digvalue = hash_objectvalue.hexdigest()
                hashkeyvalue_list.append(hex_digvalue)

                datetime_list.append(now)

        alldata = list(zip(project_id_list,projectpropertyType_list,value_list,hashkeybk_list,hashkeyvalue_list,datetime_list,datetime_list))

        conn , cursor = init_DWH_Db_connection()

        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntBudgetValues"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntBudgetValues"

    
        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)

        cursor.execute(f"Truncate Table  [tmp].[dwh_FactBlueAntBudgetValues]")
        logging.info("Truncated [tmp].[dwh_FactBlueAntBudgetValues] table")
        conn.commit()

        cursor.fast_executemany = True
        insert = f"INSERT INTO  {TMP_SCHEMA_NAME}.{TMP_TABLE_NAME} (ProjectBK , ProjectPropertyType ,[Value],HashkeyBK,HashkeyValue,InsertDate,UpdateDate) VALUES (?,?,?,?,?,?,?)"
        cursor.executemany(insert, alldata) #load data into azure sql db
    #  logging.info(datetime.datetime.now())
        conn.commit()

        cursor.execute(f"""
            --Synchronize the target table with refreshed data from source table
            DECLARE @MergeLog TABLE([Status] VARCHAR(20));
            with cte as(
                select 
                    ISNULL(b.BlueAntProjectID,-1) as BlueAntProjectID
                    ,a.ProjectPropertyType
                    ,a.[Value]
                    ,a.InsertDate
                    ,a.UpdateDate
                    ,a.HashkeyBK
                    ,CONCAT(a.HashkeyValue,'|',ISNULL(b.BlueAntProjectID,-1)) as HashkeyValue
                from tmp.dwh_FactBlueAntBudgetValues a
                LEFT JOIN dwh.DimBlueAntProject b
                on a.ProjectBK = b.ProjectBK
            )
                Merge dwh.FactBlueAntBudgetValues as TARGET
                using cte as SOURCE
                on TARGET.HashkeyBK = SOURCE.HashkeyBK --and tgt.HashkeyValue != src.HashkeyValue
                WHEN MATCHED AND TARGET.HashkeyValue <> SOURCE.HashkeyValue
                THEN UPDATE SET TARGET.HashkeyValue = SOURCE.HashkeyValue,
                        TARGET.[BlueAntProjectID] = SOURCE.[BlueAntProjectID],
                        TARGET.ProjectPropertyType = SOURCE.ProjectPropertyType,
                        TARGET.[Value] = SOURCE.[Value],
                        TARGET.UpdateDate = SOURCE.UpdateDate
            --When no records are matched, insert the incoming records from source table to target table
                WHEN NOT MATCHED BY TARGET 
                THEN INSERT (BlueAntProjectID, ProjectPropertyType,[Value],HashkeyBK,HashkeyValue,InsertDate,UpdateDate ) 
                VALUES (SOURCE.BlueAntProjectID, SOURCE.ProjectPropertyType, SOURCE.[Value],SOURCE.HashkeyBK,SOURCE.HashkeyValue,SOURCE.InsertDate,SOURCE.UpdateDate)
                --When there is a row that exists in target and same record does not exist in source then delete this record target
                --WHEN NOT MATCHED BY SOURCE 
                --THEN DELETE 
                OUTPUT  $action as [Status] into @MergeLog;

                INSERT INTO tmp.[changed] ([Status],Anzahl)
                SELECT [Status], count(*) as Anzahl FROM @MergeLog
                GROUP BY [Status];
    """)
        conn.commit()
        
        logging.info("Data merged in dwh")

        DWH_table_logging(TABLE_NAME, cursor, conn, is_Dimension=False)

        close_DWH_DB_connection(conn ,cursor)

        return func.HttpResponse("Successfull ETL Process", status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)
