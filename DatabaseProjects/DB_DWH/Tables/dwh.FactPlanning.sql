CREATE TABLE [dwh].[FactPlanning] (
    [BlueAntProjectID] INT            NOT NULL,
    [DealID]           INT            NOT NULL,
    [UserID]           INT            NOT NULL,
    [StartDateID]      INT            NULL,
    [EndDateID]        INT            NULL,
    [Billable]         BIT            NOT NULL,
    [PlanningValue]    DECIMAL (9, 4) NULL,
    [PlanningType]     BIT            NOT NULL,
    [InsertDate]       DATETIME       NULL,
    [UpdateDate]       DATETIME       NULL
);

