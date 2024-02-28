CREATE TABLE [tmp].[dwh_FactBlueAntProjectTask] (
    [BlueAntTaskBK]        INT            NULL,
    [WorkActualMinutes]    DECIMAL (9, 2) NULL,
    [WorkActualDays]       DECIMAL (9, 4) NULL,
    [WorkPlannedMinutes]   DECIMAL (9, 2) NULL,
    [WorkPlannedDays]      DECIMAL (9, 4) NULL,
    [WorkEstimatedMinutes] DECIMAL (9, 2) NULL,
    [WorkEstimatedDays]    DECIMAL (9, 4) NULL,
    [HashkeyBK]            VARCHAR (255)  NULL,
    [HashkeyValue]         VARCHAR (255)  NULL,
    [InsertDate]           DATETIME       NULL,
    [UpdateDate]           DATETIME       NULL
);

