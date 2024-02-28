CREATE TABLE [tmp].[dwh_FactJiraWorklog] (
    [JiraWorklogID]    NVARCHAR (255)  NULL,
    [JiraIssueID]      NVARCHAR (255)  NULL,
    [JiraTimeSpent]    NVARCHAR (255)  NULL,
    [WorklogSummary]   NVARCHAR (1000) NULL,
    [WorklogEntryDate] NVARCHAR (80)   NULL,
    [JiraUserBK]       NVARCHAR (255)  NULL,
    [InsertDate]       DATETIME        NOT NULL,
    [UpdateDate]       DATETIME        NOT NULL,
    [HashkeyBK]        VARCHAR (255)   NOT NULL,
    [HashkeyValue]     VARCHAR (255)   NOT NULL
);

