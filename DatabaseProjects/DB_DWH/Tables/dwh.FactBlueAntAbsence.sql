CREATE TABLE [dwh].[FactBlueAntAbsence] (
    [AbsenceID]            INT           NOT NULL,
    [UserID]               INT           NOT NULL,
    [BlueAntAbsenceTypeID] INT           NOT NULL,
    [DateFromID]           INT           NOT NULL,
    [DateToID]             INT           NOT NULL,
    [State]                NVARCHAR (50) NOT NULL,
    [IsDeleted]             bit NOT NULL,
    [DeletedTimestamp]      DATETIME NULL,
    [HashkeyBK]            VARCHAR (255) NOT NULL,
    [HashkeyValue]         VARCHAR (255) NOT NULL,
    [InsertDate]           DATETIME      NOT NULL,
    [UpdateDate]           DATETIME      NOT NULL
);

