CREATE TABLE [dwh].[MappingFactAggWorklogToFactJiraWorklog] (
    [JiraWorklogID]        INT            NULL,
    [JiraIssueID]          INT            NULL,
    [WorklogSummary]       NVARCHAR (255) NULL,
    [WorklogEntryDateID]   INT            NULL,
    [UserID]               INT            NULL,
    [JiraTimeSpentMinutes] DECIMAL (9, 2) NULL,
    [SumJiraTimeSpentMin]  DECIMAL (9, 2) NULL,
    [JiraIssue]            NVARCHAR (30)  NULL,
    [Factbk]               NVARCHAR (50)  NULL
);

