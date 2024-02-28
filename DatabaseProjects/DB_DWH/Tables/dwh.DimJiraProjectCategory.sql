CREATE TABLE [dwh].[DimJiraProjectCategory] (
    [ProjectCategoryID]          INT            IDENTITY (1, 1) NOT NULL,
    [ProjectCategoryBK]          INT            NOT NULL,
    [ProjectCategoryName]        NVARCHAR (50)  NOT NULL,
    [ProjectCategoryDescription] NVARCHAR (100) NOT NULL,
    [InsertDate]                 DATETIME       NOT NULL,
    [UpdateDate]                 DATETIME       NOT NULL,
    [HashkeyBK]                  VARCHAR (255)  NOT NULL,
    [HashkeyValue]               VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimJiraProjectCategory_ProjectCategoryID] PRIMARY KEY CLUSTERED ([ProjectCategoryID] ASC)
);

