CREATE TABLE [dwh].[FactBlueAntProjectResource] (
    [ResourceID]       INT           NOT NULL,
    [ProjectRoleID]    INT           NOT NULL,
    [UserID]           INT           NOT NULL,
    [BlueAntProjectID] INT           NOT NULL,
    [HashkeyBK]        VARCHAR (255) NOT NULL,
    [HashkeyValue]     VARCHAR (255) NOT NULL,
    [InsertDate]       DATETIME      NOT NULL,
    [UpdateDate]       DATETIME      NOT NULL
);

