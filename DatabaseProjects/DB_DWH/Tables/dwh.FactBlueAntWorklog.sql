CREATE TABLE [dwh].[FactBlueAntWorklog] (
    [BlueAntWorklogID]       INT            NOT NULL,
    [WorklogDateID]          INT            NULL,
    [BlueAntTimeSpentMinute] DECIMAL (9, 2) NULL,
    [BlueAntProjectID]       INT            NULL,
    [BlueAntProjectTaskID]   INT            NULL,
    [BlueAntActivityID]      INT            NULL,
    [Comment]                NVARCHAR (500) NULL,
    [Billable]               BIT            NULL,
    [UserID]                 INT            NULL,
    [BlueAntLastChangedDate] DATETIME2 (0)  NULL,
    [FirstValue]             NVARCHAR (200) NULL,
    [SecondValue]            NVARCHAR (200) NULL,
    [ThirdValue]             NVARCHAR (200) NULL,
    [InsertDate]             DATETIME       NOT NULL,
    [UpdateDate]             DATETIME       NOT NULL,
    [HashkeyBK]              VARCHAR (255)  NOT NULL,
    [HashkeyValue]           VARCHAR (255)  NOT NULL
);

