CREATE TABLE [tmp].[datevBWA]
(
/*
	Diese Tabelle ist manuell erstellt und befüllt worden weil aktuell kein Ladeprozess existiert. 
	Es ist angedacht. Das wir aus Datev irgendwann die Daten automatisch bekommen 
*/


	[Zeile1] [nvarchar](500) NULL,
	[Bezeichnung] [nvarchar](500) NULL,
	[Konto] [nvarchar](500) NULL,
	[Vorzeichen] [nvarchar](500) NULL,
	[Zeile2] [nvarchar](500) NULL,
	[Zeilenbeschriftung] [nvarchar](500) NULL,
	[Wert] [nvarchar](500) NULL,
	[Wert2] [nvarchar](500) NULL,
	[Wert3] [nvarchar](500) NULL,
	[Wert4] [nvarchar](500) NULL,
	[FktSchl] [nvarchar](500) NULL
) ON [PRIMARY]
GO