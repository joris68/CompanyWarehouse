CREATE TABLE [dwh].[FactWorklog] (
    [BlueAntBk]             NVARCHAR (600) NULL,
    [BlueAntTimeMinutes]    DECIMAL (9, 2) NULL,
    [BlueAntIssue]          NVARCHAR (50)  NULL,
    [BlueAntUserID]         INT            NULL,
    [BlueAntProjectID]      INT            NULL,
    [BlueAntWorklogDateID]  INT            NULL,
    [Billable]              BIT            NULL,
    [BlueAntProjectTaskID]  INT            NULL,
    [JiraBK]                NVARCHAR (600) NULL,
    [JiraUserID]            INT            NULL,
    [JiraTimeSpentMinutes]  DECIMAL (9, 2) NULL,
    [JiraIssue]             NVARCHAR (50)  NULL,
    [JiraWorklogDateID]     INT            NULL,
    [BlueAntWorklogSummary] NVARCHAR (500) NULL,
    [JiraWorklogSummary]    NVARCHAR (500) NULL
);

