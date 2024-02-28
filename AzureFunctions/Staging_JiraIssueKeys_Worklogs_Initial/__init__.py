#Autor:MC
import traceback
import json
import azure.functions as func
from common import get_relative_blob_path, create_file_name,  upload_blob_to_storage, jira_api_request,get_time_in_string
from datetime import datetime
import config_urls as URL



def main(req: func.HttpRequest) -> func.HttpResponse:

  try:
    
    now = get_time_in_string()[0].date()

    #---------------------------------- API CALL ----------------------------------
    # Get the toal amount of issues in the last 3 sprints
    #---------------------------------- API CALL ---------------------------------- 
  

    response_maxissues = jira_api_request(URL.JIRA_WORKLOGS_MAXTOTAL_ISSUES, 'GET')
    
    jsondata = json.loads(response_maxissues.text)

    totals = jsondata["total"]
    
    #---------------------------------- API CALL ----------------------------------
    # Get all issues in the last 3 sprints
    #---------------------------------- API CALL ---------------------------------- 

    # Create a list to split the totals in 100 steps, because we can only show 100 issues per API Call
    liste = [x for x in range(0,totals,100)]
    
    bigJSONissues = []            # Create a empty list JSON file
    worklogissuekeys = []   # Crerate a empty list to store the Issuekeys for the worklogs
    
    CONTAINER1 = 'initialloads'

    # Second API Call to get all IssueKeys and to create the first JSON file
    for items in liste:

      url = URL.JIRA_ISSUES_RANGE.format(items)

      # API Call to get all issues
      response_allIssues = jira_api_request(url , 'GET')
      jsondata_issues = json.loads(response_allIssues.text)
      issues = jsondata_issues["issues"]
        
      for issue in issues:
 #         fields= issue["fields"]
          bigJSONissues.append(issue)
          worklogissuekeys.append(issue["key"])

    # Json list convert to string to store in Blob Storage 
    jsonIssues_string = json.dumps(bigJSONissues)
           
    upload_blob_to_storage(f'{CONTAINER1}',  jsonIssues_string, str(now)+('-jiraissuekeys.json'))

    #---------------------------------- API CALL ----------------------------------
    # Get all worklogs for the issues in the last 3 sprints
    #---------------------------------- API CALL ---------------------------------- 
    
    bigJSONworklog = []       # Second JSON file for the worklogs
      
    CONTAINER2 = 'initialloads'
      
    for keys in worklogissuekeys:
        
        url = URL.JIRA_WORKLOG.format(keys)
        
        # API Call to get all worklogs
        response_worklog = jira_api_request(url , 'GET')
        worklogdata = json.loads(response_worklog.text)
        bigJSONworklog.append(worklogdata)

    jsonstring2 = json.dumps(bigJSONworklog)
  
    upload_blob_to_storage(f'{CONTAINER2}',  jsonstring2,str(now)+('-jiraworklog.json'))

    return func.HttpResponse("Function was successfully executed"  ,status_code=200)
  
  except Exception as e:
        # If an exception occurs, capture the error traceback
        error_traceback = traceback.format_exc()

        # Return an HTTP response with the error message and traceback, separated by a line break
        return func.HttpResponse(str(e) + "\n--\n" + error_traceback, status_code=400)




