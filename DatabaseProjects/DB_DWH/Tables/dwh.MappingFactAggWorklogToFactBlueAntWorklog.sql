CREATE TABLE [dwh].[MappingFactAggWorklogToFactBlueAntWorklog] (
    [BlueAntWorklogID]       INT            NULL,
    [JiraIssueID]            INT            NULL,
    [BaIssue]                NVARCHAR (30)  NULL,
    [UserID]                 INT            NULL,
    [BlueAntProjectID]       INT            NULL,
    [WorklogSummary]         NVARCHAR (255) NULL,
    [Billable]               BIT            NULL,
    [WorklogDateID]          INT            NULL,
    [BlueAntActivityID]      INT            NULL,
    [BlueAntTaskID]          INT            NULL,
    [BaSumTimeMinutes]       DECIMAL (9, 2) NULL,
    [BlueAntTimeSpentMinute] DECIMAL (9, 2) NULL,
    [MappingBk]              NVARCHAR (50)  NULL
);

