CREATE TABLE [dwh].[DimTarget] (
    [TargetId] INT            IDENTITY (1, 1) NOT NULL,
    [YearID]   INT            NOT NULL,
    [UserID]   INT            NULL,
    [Target]   DECIMAL (9, 2) NULL,
    CONSTRAINT [PK_FactTarget] PRIMARY KEY CLUSTERED ([TargetId] ASC)
);

