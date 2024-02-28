CREATE TABLE [dwh].[DimJiraIssue] (
    [JiraIssueID]       INT            IDENTITY (1, 1) NOT NULL,
    [JiraIssueBK]       INT            NOT NULL,
    [JiraParentIssueBK] INT            NOT NULL,
    [JiraIssue]         NVARCHAR (50)  NOT NULL,
    [JiraIssueName]     NVARCHAR (150) NOT NULL,
    [UserID]            INT            NOT NULL,
    [JiraIssueTypeID]   INT            NOT NULL,
    [JiraProjectID]     INT            NOT NULL,
    [JiraSprintID]      INT            NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL,
    [Deleted]           BIT            NULL,
    [DeletedTimestamp]  DATETIME       NULL,
    CONSTRAINT [PK_dwh_DimJiraIssue_JiraIssueID] PRIMARY KEY CLUSTERED ([JiraIssueID] ASC)
);

