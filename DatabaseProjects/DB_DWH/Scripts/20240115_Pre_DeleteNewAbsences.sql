-- Author:Moez
-- Name: Pre_20240115_DeleteNewAbsences.sql
-- Function: Lösche alle Absences ab 2024 damit keine Duplikate entstehen.
-- Wir legen noch eine Backup Tabelle an um sicherzu gehen das wir einen alten STand haben, Wenn nicht gewünscht beim Release dann auskommentieren oder überschpringen
/*
*
*		DEV: done
*		PROD: DBR 20240130
*
*/


GO
THROW 51000, 'Make sure to active smart defaults and SET IsDeleted = 0; [DateToID] = 19000101; [State] = N/A', 1;  

--to speed up release process since we are adding some columns
TRUNCATE TABLE tmp.dwh_factblueantabsence;

--delete actual blueant values
delete from dwh.factblueantabsence where DateFromID >= 20240101;
