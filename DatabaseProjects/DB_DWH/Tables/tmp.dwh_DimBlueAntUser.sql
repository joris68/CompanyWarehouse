CREATE TABLE [tmp].[dwh_DimBlueAntUser] (
    [BlueAntUserBK] NVARCHAR (30)  NULL,
    [UserInitials]  NVARCHAR (20)  NULL,
    [FirstName]     NVARCHAR (50)  NULL,
    [LastName]      NVARCHAR (50)  NULL,
    [UserEmail]     NVARCHAR (255) NULL,
    [HashkeyBK]     VARCHAR (255)  NULL,
    [HashkeyValue]  VARCHAR (255)  NULL,
    [InsertDate]    DATETIME       NULL,
    [UpdateDate]    DATETIME       NULL
);

