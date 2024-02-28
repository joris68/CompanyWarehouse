#Autor:MC
import azure.functions as func
from xml.etree import ElementTree as ET
from common import get_relative_blob_path, get_latest_blob_from_staging, init_DWH_Db_connection, insert_into_loadlogging, close_DWH_DB_connection, DWH_table_logging,get_time_in_string,truncate_tmp_table,insert_into_tmp_table,get_blob_start_with
import traceback
import pyodbc
import os 

def main(req: func.HttpRequest) -> func.HttpResponse:

    try:

        CONTAINER = 'initialloads'
        
        blob = get_blob_start_with(CONTAINER,'blueant')

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

        for worktime in xml_content.findall('.//{http://cost.blueant.axis.proventis.net/}WorkTime'):
            # Greifen Sie auf das erste und dritte stringValue beim customFieldList zu
            custom_fields = worktime.find('.//{http://cost.blueant.axis.proventis.net/}customFieldList')
    #        if custom_fields is not None:
            fields = custom_fields.findall('.//{http://masterdata.blueant.axis.proventis.net/}stringValue')
    #            if len(fields) >= 4:
            firstvalue = str(fields[0].text).replace("None","<N/A>")
            secondvalue = str(fields[1].text).replace("None","<N/A>")
            thirdvalue = str(fields[2].text).replace("None","<N/A>")

            first_Value.append(firstvalue)
            second_Value.append(secondvalue)
            third_Value.append(thirdvalue)

        alldata = list(zip(workTimeID,date,duration,projectID,taskID,activityID,comment,billable,personID,lastChangedDate,first_Value,second_Value,third_Value))

        connection_string = os.environ['DBDWHConnectionString']
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        cursor.execute("truncate table tmp.[dwh_FactBlueAntWorklogHistory]")
        cursor.execute("truncate table dwh.[FactBlueAntWorklogHistory]")
        cursor.fast_executemany = True
        testinsert = f"INSERT INTO  tmp.[dwh_FactBlueAntWorklogHistory] (BlueAntWorklogID , [Date] , BlueAntTimeSpent , [ProjectBK] ,BlueAntTaskBK ,BlueAntActivityID, Comment,Billable,BlueAntUserBK,BlueAntLastChangedDate,FirstValue,SecondValue,ThirdValue) VALUES (?, ?, ?, ?,?,?, ?, ?, ?,?,?, ?, ?)"
        cursor.executemany(testinsert, alldata) #load data into azure sql db

        conn.commit()

        cursor.execute("""
With cte as(
SELECT 
            CONVERT(INT, a.BlueAntWorklogID) AS BlueAntWorklogID,
            CONVERT(INT, ISNULL(b.BlueAntProjectID, -1)) AS BlueAntProjectID,
            --CONVERT(INT, a.ProjectID) AS ProjectID,
            IIF(FirstValue = N'<N/A>', ThirdValue, FirstValue) AS JiraIssue,
            CONVERT(INT, ISNULL(d.[BlueAntProjectTaskID],-1)) AS [BlueAntProjectTaskID],
           -- CONVERT(INT, a.[BlueAntActivityID]) AS [BlueAntActivityID],
            CONVERT(NVARCHAR(950), Comment) AS BlueAntWorklogSummary,
            CAST(CONVERT(varchar(10), CONVERT(date, a.[Date]), 112) AS INT) AS BlueAntWorklogDateID,
            CONVERT(INT, ISNULL(c.UserID, -1)) AS UserID,
            --CONVERT(INT, a.PersonID) AS PersonID,
            CONVERT(datetime2(0), [BlueAntLastChangedDate]) AS BlueAntLastChangedDatetime,
            CONVERT(DECIMAL(9, 2), (CAST(BlueAntTimeSpent AS FLOAT) / 1000) / 60) AS BlueAntTimeSpentMinute,
            CASE WHEN Billable = N'false' THEN CONVERT(BIT, 0)
                ELSE CONVERT(BIT, 1) END AS Billable 
        FROM [tmp].[dwh_FactBlueAntWorklogHistory] a
        LEFT JOIN dwh.DimBlueAntProject b ON a.ProjectBK = b.ProjectBK
        LEFT JOIN dwh.DimUser c ON a.BlueAntUserBK = c.BlueAntUserBK
        LEFT JOIN dwh.DimBlueAntProjectTask d on a.BlueAntTaskBK = d.BlueAntTaskBK
)

INSERT INTO dwh.[FactBlueAntWorklogHistory] ([BlueAntWorklogID], [BlueAntProjectID], [JiraIssue], [BlueAntProjectTaskID], [BlueAntWorklogSummary], [BlueAntWorklogDateID], [UserID], [BlueAntLastChangedDatetime], [BlueAntTimeSpentMinute], [Billable])
SELECT 
BlueAntWorklogID
,BlueAntProjectID
,JiraIssue
,[BlueAntProjectTaskID]
,[BlueAntWorklogSummary]
,BlueAntWorklogDateID
,UserID
,BlueAntLastChangedDatetime
,BlueAntTimeSpentMinute
,[Billable]
FROM cte
        """)

        conn.commit()

        close_DWH_DB_connection(conn, cursor)

        return func.HttpResponse("The Merge statement was successfully executed.", status_code=200)
    
    except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)