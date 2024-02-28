#   Das ist der Python-file mit dem wir die Urls Zentral Pflegen möchten.
#   Dort sind alle URLS, die wir für die API-Requests benötigen, aufgeteilt nach System und API
#   Von hier aus werden wir die Konstanten in die Jeweiligen Python-File importieren
#  jede Anpassung an den URls (z.B. eine Anpassung an den Parametern für die Rest-Abfragen wird hier voorgenommen!!)



# Base URLs for the Azure Functions

BLUEANT_REST_BASE_URL = "https://ceteris.blueant.cloud/rest/"
BLUEANT_SOAP_BASE_URL = "https://ceteris.blueant.cloud/services/"
JIRA_BASE_URL = 'https://ceteris.atlassian.net/rest/'

# Relative URLs Jira
JIRA_WORKLOGS_MAXTOTAL_ISSUES = 'https://ceteris.atlassian.net/rest/api/3/search?maxResults=1'
JIRA_ISSUES_RANGE = 'https://ceteris.atlassian.net/rest/api/3/search?maxResults=100&startAt={}'
JIRA_WORKLOG = 'https://ceteris.atlassian.net/rest/api/3/issue/{}/worklog'
JIRA_SPRINT = f"https://ceteris.atlassian.net/rest/agile/1.0/board/6/sprint"
JIRA_PROJECT = f"https://ceteris.atlassian.net/rest/api/3/project/search?maxResults=50&startAt=0"
JIRA_USER = f"https://ceteris.atlassian.net/rest/api/3/users/search?query"


# Relative URL BlueAnt Rest API

BLUEANT_REST_CUSTOMER = f"{BLUEANT_REST_BASE_URL}v1/masterdata/customers"
BLUEANT_REST_PROJECTS = f"{BLUEANT_REST_BASE_URL}v1/projects"  
BLUEANT_REST_PROJECTRESOURCE = f"{BLUEANT_REST_BASE_URL}v1/human/persons"
BLUEANT_REST_PLANNINGENTRIES = "https://ceteris.blueant.cloud/rest/v1/projects/{}/planningentries" 
BLUEANT_REST_PROJECTSTATE = f"{BLUEANT_REST_BASE_URL}v1/masterdata/projects/statuses"

# Relative URL BlueAnt Soap API: Hier bitte merken !! Alle Soap-URLS !! heißen Service

BLUEANT_ABSENCESERVICE = f"{BLUEANT_SOAP_BASE_URL}AbsenceService"
BLUEANT_BUDGETSERVICE = f"{BLUEANT_SOAP_BASE_URL}BudgetService"
BLUEANT_HUMANSERVICE = f"{BLUEANT_SOAP_BASE_URL}HumanService/"
BLUEANT_INVOICESERVICE = f"{BLUEANT_SOAP_BASE_URL}InvoiceService/"
BLUEANT_MASTERDATASERVICE = f"{BLUEANT_SOAP_BASE_URL}MasterDataService"
BLUEANT_PROJECTSERVICE =  f"{BLUEANT_SOAP_BASE_URL}ProjectsService"
BLUEANT_WORKTIME_ACCOUNTING_SERVICE = f"{BLUEANT_SOAP_BASE_URL}WorktimeAccountingService/"

