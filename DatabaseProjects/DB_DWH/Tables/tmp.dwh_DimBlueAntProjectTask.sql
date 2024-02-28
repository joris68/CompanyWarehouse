CREATE TABLE [tmp].[dwh_DimBlueAntProjectTask] (
    [BlueAntTaskBK] INT            NULL,
    [ProjectBK]     INT            NULL,
    [TaskName]      NVARCHAR (255) NULL,
    [TaskParent]    INT            NULL,
    [HashkeyBK]     VARCHAR (255)  NOT NULL,
    [HashkeyValue]  VARCHAR (255)  NOT NULL,
    [InsertDate]    DATETIME       NOT NULL,
    [UpdateDate]    DATETIME       NOT NULL
);

