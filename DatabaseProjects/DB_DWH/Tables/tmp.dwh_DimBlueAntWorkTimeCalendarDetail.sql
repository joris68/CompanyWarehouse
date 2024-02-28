CREATE TABLE [tmp].[dwh_DimBlueAntWorkTimeCalendarDetail] (
    [BlueAntWorkTimeCalendarBK] INT            NOT NULL,
    [WorkTimeCalendarName]      NVARCHAR (100) NOT NULL,
    [WorktimeCalendarDuration]  INT            NOT NULL,
    [Monday]                    INT            NOT NULL,
    [Tuesday]                   INT            NOT NULL,
    [Wednesday]                 INT            NOT NULL,
    [Thursday]                  INT            NOT NULL,
    [Friday]                    INT            NOT NULL,
    [InsertDate]                DATETIME       NOT NULL,
    [HashkeyValue]              VARCHAR (255)  NOT NULL
);

