CREATE TABLE [tmp].[dwh_FactBlueAntWorklogHistory] (
    [BlueAntWorklogID]       NVARCHAR (255)  NULL,
    [Date]                   NVARCHAR (255)  NULL,
    [BlueAntTimeSpent]       NVARCHAR (255)  NULL,
    [ProjectBK]              NVARCHAR (255)  NULL,
    [BlueAntTaskBK]          NVARCHAR (255)  NULL,
    [BlueAntActivityID]      NVARCHAR (255)  NULL,
    [Comment]                NVARCHAR (1000) NULL,
    [Billable]               NVARCHAR (255)  NULL,
    [BlueAntUserBK]          NVARCHAR (255)  NULL,
    [BlueAntLastChangedDate] NVARCHAR (255)  NULL,
    [FirstValue]             NVARCHAR (255)  NULL,
    [SecondValue]            NVARCHAR (255)  NULL,
    [ThirdValue]             NVARCHAR (255)  NULL
);

