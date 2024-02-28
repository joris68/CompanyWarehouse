/*
*		Author:dbretz
*		Name: 20240205_Pre_DropunusedView.sql
*		Function: Wir haben eine nicht genutzte View aus dem SSDT gelöscht. 
*		Da wir im publish skript die Einstellung "nicht vorhandenden Elemente" löschen nicht aktiv iert haben müssen wir die view so löschen
*
*		DEV: 20240205 DBR
*		PROD: 20240205 DBR
*
*/

DROP VIEW IF EXISTS [pbi].[vw_FactAbsence];