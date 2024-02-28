CREATE TABLE [dwh].[DimBlueAntProjectRoles] (
    [ProjectRoleID]   INT            IDENTITY (1, 1) NOT NULL,
    [ProjectRoleBK]   INT            NOT NULL,
    [ProjectRoleName] NVARCHAR (100) NOT NULL,
    [ExternalCost]    DECIMAL (9, 4) NULL,
    [TravelCost]      DECIMAL (9, 4) NULL,
    [HashkeyBK]       VARCHAR (255)  NOT NULL,
    [HashkeyValue]    VARCHAR (255)  NOT NULL,
    [InsertDate]      DATETIME       NOT NULL,
    [UpdateDate]      DATETIME       NOT NULL,
    CONSTRAINT [PK_dwh_DimBlueAntProjectRoles_ProjectRoleID] PRIMARY KEY CLUSTERED ([ProjectRoleID] ASC)
);

