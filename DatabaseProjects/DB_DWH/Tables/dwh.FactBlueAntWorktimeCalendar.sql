CREATE TABLE [dwh].[FactBlueAntWorktimeCalendar] (
    [BlueAntUserID]      INT           NOT NULL,
    [WorkTimeCalendarID] INT           NOT NULL,
    [StartDateID]        INT           NOT NULL,
    [EndDateID]          INT           NOT NULL,
    [DateOfLeavingID]    INT           NULL,
    [InsertDate]         DATETIME      NOT NULL,
    [UpdateDate]         DATETIME      NOT NULL,
    [HashkeyBK]          VARCHAR (255) NOT NULL,
    [HashkeyValue]       VARCHAR (255) NOT NULL
);

