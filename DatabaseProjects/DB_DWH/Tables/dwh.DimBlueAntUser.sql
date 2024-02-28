CREATE TABLE [dwh].[DimBlueAntUser] (
    [BlueAntUserID] INT            IDENTITY (1, 1) NOT NULL,
    [BlueAntUserBK] INT            NULL,
    [UserInitials]  NVARCHAR (20)  NULL,
    [FirstName]     NVARCHAR (50)  NULL,
    [LastName]      NVARCHAR (50)  NULL,
    [UserEmail]     NVARCHAR (100) NULL,
    [HashkeyBK]     VARCHAR (255)  NULL,
    [HashkeyValue]  VARCHAR (255)  NULL,
    [InsertDate]    DATETIME       NULL,
    [UpdateDate]    DATETIME       NULL,
    CONSTRAINT [PK_dwh_DimBlueAntUser_BlueAntUserID] PRIMARY KEY CLUSTERED ([BlueAntUserID] ASC)
);

