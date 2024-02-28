CREATE TABLE [dwh].[FactProjectPlanning] (
    [BlueAntProjectID] INT           NULL,
    [ProjectName]      VARCHAR (250) NOT NULL,
    [EmployeeID]       INT           NOT NULL,
    [DateID]           INT           NOT NULL,
    [PlanningValue]    FLOAT (53)    NOT NULL,
    [InsertDate]       DATETIME      NOT NULL,
    [UpdateDate]       DATETIME      NOT NULL,
    [HashkeyBK]        VARCHAR (255) NOT NULL,
    [HashkeyValue]     VARCHAR (255) NOT NULL
);

