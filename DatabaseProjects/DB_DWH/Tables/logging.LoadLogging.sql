CREATE TABLE [logging].[LoadLogging] (
    [RowID]         INT           IDENTITY (1, 1) NOT NULL,
    [SchemaName]    NVARCHAR (20) NOT NULL,
    [TableName]     NVARCHAR (50) NOT NULL,
    [StartDateTime] DATETIME      NULL,
    [EndDateTime]   DATETIME      NULL,
    [InsertedRows]  INT           NULL,
    [UpdatedRows]   INT           NULL,
    [DeletedRows]   INT           NULL,
    CONSTRAINT [PK_logging_LoadLogging_RowID] PRIMARY KEY CLUSTERED ([RowID] ASC)
);

