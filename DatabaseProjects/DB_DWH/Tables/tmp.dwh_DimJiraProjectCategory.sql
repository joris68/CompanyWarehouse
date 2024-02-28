CREATE TABLE [tmp].[dwh_DimJiraProjectCategory] (
    [id]           INT           NOT NULL,
    [name]         VARCHAR (30)  NOT NULL,
    [description]  VARCHAR (30)  NOT NULL,
    [InsertDate]   DATETIME      NOT NULL,
    [UpdateDate]   DATETIME      NOT NULL,
    [HashkeyBK]    VARCHAR (255) NULL,
    [HashkeyValue] VARCHAR (255) NULL
);

