CREATE TABLE [tmp].[dwh_DimJiraIssueType] (
    [IssueTypeId]   INT           NOT NULL,
    [IssueTypeName] VARCHAR (20)  NOT NULL,
    [InsertDate]    DATETIME      NOT NULL,
    [UpdateDate]    DATETIME      NOT NULL,
    [HashkeyBK]     VARCHAR (255) NULL,
    [HashkeyValue]  VARCHAR (255) NULL
);

