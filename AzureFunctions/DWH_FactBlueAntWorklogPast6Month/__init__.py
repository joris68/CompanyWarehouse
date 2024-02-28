#Autor:MC
import logging
import azure.functions as func
from xml.etree import ElementTree as ET
import hashlib
from common import get_relative_blob_path, init_DWH_Db_connection, close_DWH_DB_connection, get_latest_blob_from_staging, insert_into_loadlogging, DWH_table_logging,get_time_in_string,insert_into_tmp_table
import traceback
from datetime import datetime
import pytz

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:
   
        CONTAINER = 'blueantworktime'

        now = get_time_in_string()[0]
 
        blob = get_latest_blob_from_staging(CONTAINER, folder_name=get_relative_blob_path())

        blob_content = blob.content_as_text()
    
        xml_content = ET.fromstring(blob_content)

        first_Value = []
        second_Value = []
        third_Value = []
        personID = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}personID')]
        workTimeID = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}workTimeID')]
        date = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}date')]
        duration = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}duration')]
        projectID = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}projectID')]
        taskID = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}taskID')]
        activityID = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}activityID')]
        comment = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}comment')]
        billable = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}billable')]
        lastChangedDate = [elem.text for elem in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}lastChangedDate')]

        new_comment = []

        for x in comment:
            if len(x) > 255:
                x = x[0:255]
                new_comment.append(x)
            else:
                new_comment.append(x)
        
        datetime_list = []
        hashkeyvalue_list= []
        hashkeybk_list = []

        for worktime in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}WorkTime'):
            # Greifen Sie auf das erste und dritte stringValue beim customFieldList zu
            custom_fields = worktime.find('.//{http://cost.blueant.axis.proventis.net/}customFieldList')
            # if custom_fields is not None:
            fields = custom_fields.findall('.//{http://masterdata.blueant.axis.proventis.net/}stringValue')
            # if len(fields) >= 4:
            firstvalue = str(fields[0].text).replace("None","N/A")
            secondvalue = str(fields[1].text).replace("None","N/A")
            thirdvalue = str(fields[2].text).replace("None","N/A")

            first_Value.append(firstvalue)
            second_Value.append(secondvalue)
            third_Value.append(thirdvalue)

            datetime_list.append(now)

        conn, cursor = init_DWH_Db_connection()

        # Define constants for database schemasStartDat
        SCHEMA_NAME = "dwh"
        TABLE_NAME = "FactBlueAntWorklog"
        TMP_SCHEMA_NAME = "tmp"
        TMP_TABLE_NAME = "dwh_FactBlueAntWorklog"

        insert_into_loadlogging(SCHEMA_NAME, TABLE_NAME, cursor, conn)
        
    #    cursor.execute("Truncate Table dwh.DimJiraProject")
        cursor.execute(f"Truncate Table  [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}]")
        logging.info("Truncated [tmp].[dwh_FactBlueAntWorklog] table")

        for x in range(len(workTimeID)):
            bk=workTimeID[x]
            hash_objectbk = hashlib.sha256(bk.encode())
            hex_digbk = hash_objectbk.hexdigest()
            hashkeybk_list.append(hex_digbk)
            
            value= str(first_Value[x]) + str(second_Value[x]) + (str(third_Value[x]))+ str(personID[x])+ str((date[x]))+str(duration[x])+str(projectID[x])+str(taskID[x])+str(activityID[x])+str(new_comment[x])+str(billable[x])+str(lastChangedDate[x])
            hash_objectvalue = hashlib.sha256(value.encode())
            hex_digvalue = hash_objectvalue.hexdigest()
            hashkeyvalue_list.append(hex_digvalue)
        
        now = datetime.now(pytz.timezone('Europe/Berlin'))

        dataset = list(zip(workTimeID,date,duration,projectID,taskID,activityID,new_comment,billable,personID,lastChangedDate,first_Value,second_Value,third_Value,datetime_list,datetime_list,hashkeybk_list,hashkeyvalue_list))

        # ich habe hier ID zu BK gemacht bei projectID und BlueantTaskID, personID zu BLueAntUserBK
        insert_tmp_statement = f"""
            INSERT INTO [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] (
                BlueAntWorklogID,
                [Date],
                BlueAntTimeSpent,
                [ProjectBK],
                BlueAntTaskBK,
                BlueAntActivityID,
                Comment,
                Billable,
                BlueAntUserBK,
                BlueAntLastChangedDate,
                FirstValue,
                SecondValue,
                ThirdValue,
                InsertDate,
                UpdateDate,
                HashkeyBK,
                HashkeyValue
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.fast_executemany = True

        cursor.executemany(insert_tmp_statement, dataset)

        conn.commit()

        cursor.execute(f"""
                       
            BEGIN TRANSACTION; 
            BEGIN TRY 
            BEGIN     

                DECLARE @Today DATE = CONVERT(DATE, '{now}'); -- Aktuelles Datum

                -- 1. Tag des aktuellen Monats
                DECLARE @FirstDayOfCurrentMonth DATE = DATEADD(DAY, 1 - DATEPART(DAY, @Today), @Today);

                -- 1. Tag des Monats vor 6 Monaten
                DECLARE @FirstDayOfSixMonthAgo DATE = DATEADD(MONTH, -6, @FirstDayOfCurrentMonth);

                DECLARE @FirstDayOfSixMonthAgoID INT = CAST(CONVERT(varchar(10), CONVERT(date, @FirstDayOfSixMonthAgo), 112) as INT);

                SELECT @FirstDayOfSixMonthAgoID;

                DELETE FROM [{SCHEMA_NAME}].[{TABLE_NAME}]
                WHERE WorklogDateID >= @FirstDayOfSixMonthAgoID;      
                
                -- Synchronize the target table with refreshed data from source table
                WITH cte AS (
                    SELECT 
                        a.*,
                        ISNULL(d.BlueAntProjectTaskID,-1) as BlueAntProjectTaskID,
                        ISNULL(b.BlueAntProjectID,-1) as BlueAntProjectID,
                        CAST(CONVERT(varchar(10), CONVERT(date, a.[date]), 112) as INT) WorklogDateID,
                        ISNULL(c.UserID,-1) as UserID,
                        CONVERT(datetime2(0), [BlueAntLastChangedDate]) AS BlueAntLastChangedDatetime,
                        CONVERT(decimal(9,2), (CAST(BlueAntTimeSpent AS FLOAT) / 1000) / 60) AS BlueAntTimeSpentMinute,
                        CONVERT(INT, BlueAntWorklogID) AS BlueAntWorklogIDs,
                        CASE 
                            WHEN Billable = N'false' THEN CONVERT(BIT, 0)
                            ELSE CONVERT(BIT, 1) 
                        END AS Bill,
                        CONCAT(a.HashkeyValue, '|', ISNULL(c.UserID,-1), '|', ISNULL(b.BlueAntProjectID,-1), '|', ISNULL(d.BlueAntProjectTaskID,-1)) AS MergeHashkeyValue
                    FROM [{TMP_SCHEMA_NAME}].[{TMP_TABLE_NAME}] a
                    LEFT JOIN [{SCHEMA_NAME}].DimBlueAntProject b ON a.ProjectBK = b.ProjectBK
                    LEFT JOIN [{SCHEMA_NAME}].DimUser c ON a.BlueAntUserBK = c.BlueAntUserBK
                    LEFT JOIN [{SCHEMA_NAME}].DimBlueAntProjectTask d ON a.BlueAntTaskBK = d.BlueAntTaskBK
                )

                INSERT INTO [{SCHEMA_NAME}].[{TABLE_NAME}] (
                    [BlueAntWorklogID], 
                    WorklogDateID, 
                    BlueAntTimeSpentMinute,
                    BlueAntProjectID,
                    [BlueAntProjectTaskID],
                    [BlueAntActivityID],
                    [Comment],
                    [Billable],
                    UserID,
                    [BlueAntLastChangedDate],
                    [FirstValue],
                    [SecondValue],
                    [ThirdValue],
                    InsertDate,
                    UpdateDate,
                    HashkeyBK,
                    HashkeyValue
                ) 
                SELECT 
                    SRC.BlueAntWorklogIDs, 
                    SRC.WorklogDateID, 
                    SRC.BlueAntTimeSpentMinute,
                    SRC.BlueAntProjectID,
                    SRC.[BlueAntProjectTaskID],
                    SRC.[BlueAntActivityID],
                    SRC.[Comment],
                    SRC.[Bill],
                    SRC.UserID,
                    SRC.[BlueAntLastChangedDatetime],
                    SRC.[FirstValue],
                    SRC.[SecondValue],
                    SRC.[ThirdValue],
                    SRC.InsertDate,
                    SRC.UpdateDate,
                    SRC.HashkeyBK,
                    SRC.MergeHashkeyValue 
                FROM cte SRC;

            END
                                    
            IF @@TRANCOUNT > 0 
                COMMIT TRANSACTION; 
            END TRY 
            BEGIN CATCH 
                IF @@TRANCOUNT > 0 
                    ROLLBACK TRANSACTION; 
                THROW; 
            END CATCH; 
                   
        """)
        
        conn.commit()
        
        cursor.commit()

        logging.info("The Merge statement was successfully executed.")
        
        DWH_table_logging(TABLE_NAME, cursor, conn)

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Function was successfully executed"  ,status_code=200)

    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)