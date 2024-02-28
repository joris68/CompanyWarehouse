/*
*		Author:dbretz
*		Name: Pre_20240115_DeleteNewAbsences.sql
*		Function: HashkeyBK und HashkeyValue wurden in der Dimension vertauscht, wir müssen das korrigieren; Faktentabelle ist ein Fullload
*		Wir legen eine Bakcup Tabelle an für den Fall das uns zu einem späteren Zeitpunkt auffällt das wir eine Tabelle vergessen haben zu aktualisieren.
*		Stand jetzt haben wir nur die FactBlueAntInvoice identifziert
*
*		Nach dem Ausführen des Scriptes muss das publish script erneut durch geführt werden damit die Spaltenreihenfolge korrigiert wird
*
*		DEV: done
*		PROD: 20240202 DBR 
*
*/


EXEC sp_rename 'dwh.DimBlueAntInvoiceState.HashkeyBK', 'HashkeyBK_NEW', 'COLUMN';
EXEC sp_rename 'dwh.DimBlueAntInvoiceState.HashkeyValue', 'HashkeyBK', 'COLUMN';
EXEC sp_rename 'dwh.DimBlueAntInvoiceState.HashkeyBK_NEW', 'HashkeyValue', 'COLUMN';




SELECT 
	*
	,ROW_NUMBER() OVER (PARTITION BY HashkeyBK ORDER BY UpdateDate) RN
INTO #dim
FROM dwh.DimBlueAntInvoiceState

SELECT 
	good.BlueAntInvoiceStateID AS GoodBlueAntInvoiceStateID
	,bad.BlueAntInvoiceStateID AS BadBlueAntInvoiceStateID
INTO dwh.DimBlueAntInvoiceStateBlueAntInvoiceStateID_BAK_DBR_20240202
FROM #dim good
	INNER JOIN #dim bad on good.HashkeyBK= bad.HashkeyBK
WHERE good.RN = 1 and bad.RN != 1





BEGIN TRANSACTION; 

BEGIN TRY 

BEGIN 


	DELETE a
	FROM dwh.DimBlueAntInvoiceState a
		INNER JOIN dwh.DimBlueAntInvoiceStateBlueAntInvoiceStateID_BAK_DBR_20240202 b on a.BlueAntInvoiceStateID = b.BadBlueAntInvoiceStateID

	SELECT *
	INTO dwh.FactBlueAntInvoice_BAK_DBR_20240202
	FROM dwh.FactBlueAntInvoice

	--lösche Duplikate, nach einem Ladeprozess sollten diese wieder da als einfache Zeile da sein
	DELETE 
	FROM dwh.FactBlueAntInvoice
	WHERE HashkeyBK in 
	(
		SELECT HashkeyBK
		FROM dwh.FactBlueAntInvoice
		GROUP BY HashkeyBK
		HAVING COUNT(1) > 1
	) 

	--Update schlechte Ids zu guten Ids
	UPDATE b
	SET BlueAntInvoiceStateID = a.GoodBlueAntInvoiceStateID
	FROM dwh.DimBlueAntInvoiceStateBlueAntInvoiceStateID_BAK_DBR_20240202 a
		INNER JOIN  dwh.FactBlueAntInvoice b on a.BadBlueAntInvoiceStateID = b.BlueAntInvoiceStateID
 

	END 

 

IF @@TRANCOUNT > 0 

COMMIT TRANSACTION; 

END TRY 

BEGIN CATCH 

IF @@TRANCOUNT > 0 

ROLLBACK TRANSACTION; 

THROW; 

END CATCH; 