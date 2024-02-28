CREATE TABLE [tmp].[dwh_FactPlanning] (
    [BlueAntProjectID] INT            NOT NULL,
    [DealID]           INT            NOT NULL,
    [UserID]           INT            NOT NULL,
    [StartDateID]      INT            NULL,
    [EndDateID]        INT            NULL,
    [Billable]         BIT            NOT NULL,
    [PlanningValue]    DECIMAL (9, 4) NULL,
    [PlanningType]     BIT            NOT NULL,
    [RowStartDate]     INT            NULL,
    [RowEndDate]       INT            NULL,
    [RowIsCurrent]     BIT            NULL,
    [InsertDate]       DATETIME       NULL,
    [UpdateDate]       DATETIME       NULL,
    [HashKeyBK]        VARCHAR (255)  NULL,
    [HashKeyValue]     VARCHAR (255)  NULL
);

