CREATE TABLE [tmp].[dwh_FactJiraIssue] (
    [JiraIssueBK]          INT           NULL,
    [Timeoriginalestimate] INT           NULL,
    [Timespent]            INT           NULL,
    [Timeestimate]         INT           NULL,
    [InsertDate]           DATETIME      NOT NULL,
    [UpdateDate]           DATETIME      NOT NULL,
    [HashkeyBK]            VARCHAR (255) NOT NULL,
    [HashkeyValue]         VARCHAR (255) NOT NULL
);

