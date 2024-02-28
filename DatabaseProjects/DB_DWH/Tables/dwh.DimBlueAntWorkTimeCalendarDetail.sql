CREATE TABLE [dwh].[DimBlueAntWorkTimeCalendarDetail] (
    [BlueAntWorkTimeCalendarDetailID] INT            IDENTITY (1, 1) NOT NULL,
    [BlueAntWorkTimeCalendarBK]       INT            NOT NULL,
    [WorkTimeCalendarName]            NVARCHAR (100) NOT NULL,
    [WorktimeCalendarDuration]        INT            NOT NULL,
    [Monday]                          INT            NOT NULL,
    [Tuesday]                         INT            NOT NULL,
    [Wednesday]                       INT            NOT NULL,
    [Thursday]                        INT            NOT NULL,
    [Friday]                          INT            NOT NULL,
    [InsertDate]                      DATETIME       NOT NULL,
    [HashkeyValue]                    VARCHAR (255)  NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntWorkTimeCalendarDetail_BlueAntWorkTimeCalendarDetailID] PRIMARY KEY CLUSTERED ([BlueAntWorkTimeCalendarDetailID] ASC)
);

