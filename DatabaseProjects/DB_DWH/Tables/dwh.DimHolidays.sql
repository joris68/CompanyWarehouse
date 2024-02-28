CREATE TABLE [dwh].[DimHolidays] (
    [HolidayId]     INT            IDENTITY (1, 1) NOT NULL,
    [HolidayName]   NVARCHAR (100) NULL,
    [HolidayDateID] INT            NULL,
    [State]         NVARCHAR (10)  NULL,
    [InsertDate]    DATETIME       NULL,
    [HashkeyValue]  VARCHAR (255)  NULL,
    CONSTRAINT [PK_dwh_DimHolidays_HolidayId] PRIMARY KEY CLUSTERED ([HolidayId] ASC)
);

