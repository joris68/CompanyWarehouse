CREATE TABLE [tmp].[dwh_DimUserExtended] (
    [BlueAntUserBK]     NVARCHAR (255) NOT NULL,
    [TeamName]          NVARCHAR (50)  NOT NULL,
    [State]             NVARCHAR (50)  NOT NULL,
    [TeamGroup]         NVARCHAR (50)  NULL,
    [TeamNameSortId]    INT            NULL,
    [TeamGroupSortId]   INT            NULL,
    [BlueAntUserSortId] INT            NULL,
    CONSTRAINT [BlueAntUserBK] PRIMARY KEY CLUSTERED ([BlueAntUserBK] ASC)
);

