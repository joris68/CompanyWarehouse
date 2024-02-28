CREATE TABLE [tmp].[dwh_DimJiraUser] (
    [JiraUserBK]   NVARCHAR (255) NULL,
    [UserEmail]    NVARCHAR (255) NULL,
    [UserName]     NVARCHAR (255) NULL,
    [HashkeyBK]    VARCHAR (255)  NULL,
    [HashkeyValue] VARCHAR (255)  NULL,
    [InsertDate]   DATETIME       NOT NULL,
    [UpdateDate]   DATETIME       NOT NULL
);

