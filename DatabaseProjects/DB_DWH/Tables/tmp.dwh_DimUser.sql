CREATE TABLE [tmp].[dwh_DimUser] (
    [JiraUserBK]    NVARCHAR (255) NULL,
    [BlueAntUserBK] NVARCHAR (255) NOT NULL,
    [UserEmail]     NVARCHAR (255) NULL,
    [UserName]      NVARCHAR (255) NULL,
    [UserInitials]  NVARCHAR (20)  NOT NULL,
    [FirstName]     NVARCHAR (50)  NOT NULL,
    [LastName]      NVARCHAR (50)  NOT NULL,
    [InsertDate]    DATETIME       NOT NULL,
    [UpdateDate]    DATETIME       NOT NULL,
    [HashkeyBK]     VARCHAR (255)  NULL,
    [HashkeyValue]  VARCHAR (255)  NULL
);

