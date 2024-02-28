CREATE TABLE [dwh].[DimJiraUser] (
    [JiraUserId]   INT            IDENTITY (1, 1) NOT NULL,
    [JiraUserBK]   NVARCHAR (100) NULL,
    [UserEmail]    NVARCHAR (100) NULL,
    [UserName]     NVARCHAR (100) NULL,
    [InsertDate]   DATETIME       NOT NULL,
    [UpdateDate]   DATETIME       NOT NULL,
    [HashkeyBK]    VARCHAR (255)  NOT NULL,
    [HashkeyValue] VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimJiraUser_JiraUserId] PRIMARY KEY CLUSTERED ([JiraUserId] ASC)
);

