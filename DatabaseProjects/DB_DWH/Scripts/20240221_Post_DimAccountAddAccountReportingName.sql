-- Author:DBR
-- Name: 20240221_Post_DimAccountAddAccountReportingName.sql
-- Function: NewColumn
/*
*
*		DEV: 
*		PROD: 
*
*/

UPDATE mart.DimAccount 
SET 
	[AccountReportingName] = N'N/A'
	--,HashkeyValue = N'N/A' --20240222 DBR Manuelles Attribut deswegen muss der Hashkeyvalue nicht angepasst werden
	,UpdateDate = GETDATE()
WHERE AccountId != -1


-- Wir müssen einmalig die Tabelle Laden um Inital den ReportingName bestimmen zu können
EXEC etl.prc_FillMartDimAccount


UPDATE a
SET [AccountReportingName] = a.AccountName --20240222 DBR Manuelles Attribut deswegen muss der Hashkeyvalue nicht angepasst werden
FROM mart.DimAccount a
WHERE Account in 
(
1005	--UE Dienstleistung
,1050	--UE Software
,1090	--Umsatzerlöse
,1150	--WE Software
,1200	--So. betr. Erlöse
,1300	--Personalkosten
,1310	--Raumkosten
,1320	--Versich./Beiträge
,1350	--Kfz-Kosten o.St.
,1360	--Werbekosten
,1370	--Reisekosten
,1380	--Kosten Warenabg
,1390	--Verwaltungskosten
,1400	--Fortbildung
,1420	--Leasing und Miete
,1430	--Geldverkehr
,1450	--Sonstige Kosten
,1480	--EBITDA
,1500	--Betriebsergebnis
,1590	--Neutraler Ertrag
,1600	--Ergebnis vor Steuern
)