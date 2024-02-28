CREATE TABLE [tmp].[dwh_FactNoWork] (
    [NoWorkID]   INT            IDENTITY (1, 1) NOT NULL,
    [UserID]     INT            NOT NULL,
    [DateFromID] INT            NULL,
    [DateToID]   INT            NULL,
    [Name]       NVARCHAR (100) NULL,
    CONSTRAINT [PK_tmp_FactNoWork_NoWorkID] PRIMARY KEY CLUSTERED ([NoWorkID] ASC)
);

