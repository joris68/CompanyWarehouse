CREATE TABLE [tmp].[dwh_DimJiraIssue] (
    [JiraIssueBK]       INT            NOT NULL,
    [JiraParentIssueBK] INT            NOT NULL,
    [JiraIssue]         NVARCHAR (255) NOT NULL,
    [JiraIssueName]     NVARCHAR (255) NOT NULL,
    [UserAssignedID]    NVARCHAR (255) NOT NULL,
    [JiraIssueTypeID]   INT            NOT NULL,
    [JiraProjectID]     INT            NOT NULL,
    [SprintID]          INT            NOT NULL,
    [InsertDate]        DATETIME       NOT NULL,
    [UpdateDate]        DATETIME       NOT NULL,
    [HashkeyBK]         VARCHAR (255)  NOT NULL,
    [HashkeyValue]      VARCHAR (255)  NOT NULL
);

