CREATE TABLE [dwh].[DimUser] (
    [UserID]        INT            IDENTITY (1, 1) NOT NULL,
    [BlueAntUserBK] INT            NOT NULL,
    [JiraUserBK]    NVARCHAR (100) NOT NULL,
    [UserInitials]  NVARCHAR (20)  NOT NULL,
    [FirstName]     NVARCHAR (50)  NOT NULL,
    [LastName]      NVARCHAR (50)  NOT NULL,
    [UserName]      NVARCHAR (100) NOT NULL,
    [UserEmail]     NVARCHAR (100) NOT NULL,
    [InsertDate]    DATETIME       NOT NULL,
    [UpdateDate]    DATETIME       NOT NULL,
    [HashkeyValue]  VARCHAR (255)  NOT NULL,
    [HashkeyBK]     VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimUser_UserID] PRIMARY KEY CLUSTERED ([UserID] ASC)
);

