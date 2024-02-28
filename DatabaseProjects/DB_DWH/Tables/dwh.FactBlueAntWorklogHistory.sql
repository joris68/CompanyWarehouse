CREATE TABLE [dwh].[FactBlueAntWorklogHistory] (
    [BlueAntWorklogID]           INT            NULL,
    [BlueAntProjectID]           INT            NULL,
    [JiraIssue]                  NVARCHAR (255) NULL,
    [BlueAntProjectTaskID]       INT            NULL,
    [BlueAntWorklogSummary]      NVARCHAR (950) NULL,
    [BlueAntWorklogDateID]       INT            NULL,
    [UserID]                     INT            NULL,
    [BlueAntLastChangedDatetime] DATETIME2 (0)  NULL,
    [BlueAntTimeSpentMinute]     DECIMAL (9, 2) NULL,
    [Billable]                   BIT            NULL
);

