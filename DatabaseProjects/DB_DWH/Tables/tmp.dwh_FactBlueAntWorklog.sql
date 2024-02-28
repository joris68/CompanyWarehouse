CREATE TABLE [tmp].[dwh_FactBlueAntWorklog] (
    [BlueAntWorklogID]       NVARCHAR (100) NULL,
    [Date]                   NVARCHAR (80)  NULL,
    [BlueAntTimeSpent]       NVARCHAR (100) NULL,
    [ProjectBK]              NVARCHAR (100) NULL,
    [BlueAntTaskBK]          NVARCHAR (100) NULL,
    [BlueAntActivityID]      NVARCHAR (100) NULL,
    [Comment]                NVARCHAR (500) NULL,
    [Billable]               NVARCHAR (100) NULL,
    [BlueAntUserBK]          NVARCHAR (100) NULL,
    [BlueAntLastChangedDate] NVARCHAR (100) NULL,
    [FirstValue]             NVARCHAR (200) NULL,
    [SecondValue]            NVARCHAR (200) NULL,
    [ThirdValue]             NVARCHAR (200) NULL,
    [InsertDate]             DATETIME       NOT NULL,
    [UpdateDate]             DATETIME       NOT NULL,
    [HashkeyBK]              VARCHAR (255)  NOT NULL,
    [HashkeyValue]           VARCHAR (255)  NOT NULL
);

