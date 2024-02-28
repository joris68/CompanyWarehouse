CREATE VIEW [pbi].[vw_DimAbsence] AS


/* Hier werden alle Arbeitskalender aus BA ausgelesen inkl. User und Start/Enddate*/ 

with cte_worktime as (
		SELECT  
			  [BlueAntUserID]
			  ,[WorkTimeCalendarID]
			  ,[StartDateID]
			  ,[EndDateID]
		FROM [dwh].[FactBlueAntWorktimeCalendar]),

/* Ausgelesene Kalender werden auf die richtigen User IDs gematcht und nach Arbeitstagen (Mo-Fr) aufgeschlüsselt, so das wir am Ende eine Übersicht haben, welcher User an welchem Tag arbeiten muss
Wir nehmen wir extra auch das Team Extendet mit rein um deren Urlaube abbilden zu können - das ist unsere Capa Basis */

cte_TeamCapacity AS (
		SELECT u.UserID
			  ,work.StartDateID
			  ,work.EndDateID
			  ,WorkTimeCalendar.Weekyday
		FROM cte_worktime work
		LEFT JOIN [pbi].[vw_FactBlueAntWorkTimeCalendarDetail] WorkTimeCalendar ON work.WorkTimeCalendarID = WorkTimeCalendar.BlueAntWorkTimeCalendarDetailID
		LEFT JOIN [dwh].[DimBlueAntUser] bu ON work.BlueAntUserID = bu.BlueAntUserID
		LEFT JOIN [dwh].DimUser u ON bu.BlueAntUserBK = u.BlueAntUserBK),

/* ausfgelesene Kalender werden auf die DimDate gejoint, damit wir am Ende eine Tabelle haben in dem es für jeden MA einen Eintrag pro Tag gibt */

cte_capa_basis as (
	SELECT 
		ddate.DateID
		,cte_TeamCapacity.UserID
		,ddate.YearID
	FROM [pbi].[vw_DimDate] ddate
	INNER JOIN cte_TeamCapacity  ON ddate.DateID BETWEEN cte_TeamCapacity.StartDateID AND cte_TeamCapacity.EndDateID AND ddate.WeekdayID = cte_TeamCapacity.Weekyday),

/* ausfgelesene Abwesenheiten werden auf die DimDate gejoint, damit wir am Ende eine Tabelle haben in dem es für jeden MA einen Eintrag pro Tag und Abwesenheit gibt */

cte_abscense as (
		SELECT  
			ddate.DateID
			,ansence.UserID
			,ansence.BlueAntAbsenceTypeID
		FROM dwh.DimDate ddate
		INNER JOIN [dwh].[FactBlueAntAbsence] ansence ON ddate.DateID BETWEEN ansence.DateFromID AND ansence.DateToID
		WHERE ansence.State = 'released' /* nur bestätigte Abnwesenheiten, keine nur geplanten */
		AND ansence.IsDeleted = 0 ), /* keine gelöschten */

/* Capa Basis wird per Left Outer Join gegen die Feiertage gejoint um alle Feiertage aus der Liste zu filtern */

cte as (
		SELECT 
			  cte_capa_basis.UserID
			  ,cte_capa_basis.DateID
			  ,cte_capa_basis.YearID
		  FROM cte_capa_basis cte_capa_basis
		  LEFT JOIN [pbi].[vw_FactNoWorking] NoWorking ON cte_capa_basis.UserID = NoWorking.UserID AND cte_capa_basis.DateID = NoWorking.HolidayDateID
		  WHERE  NoWorking.HolidayDateID IS NULL AND NoWorking.UserID IS NULL)

/* Join der bereinigten Capa Basis mit der Abwesenheitstabelle, weil man ja nur Urlaub nehmen kann, an einem Tag wo man arbeitet */

SELECT 
       cte.UserID
	  ,cte.YearID
	  ,cte.DateID
      ,AbsenceType.AbsenceTypeName
FROM cte 
INNER JOIN cte_abscense  ON cte.UserID = cte_abscense.UserID AND cte.DateID = cte_abscense.DateID
LEFT JOIN dwh.DimBlueAntAbsenceType AbsenceType ON cte_abscense.BlueAntAbsenceTypeID = AbsenceType.BlueAntAbsenceTypeID
