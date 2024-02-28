CREATE view pbi.vw_DimDate AS 
    SELECT  
         d.DateID						AS DateID
        ,d.DateBK						AS DateBK
        ,d.DayID						AS DayID
        ,d.DayText						AS DayText
        ,d.YearID						AS YearID
        ,d.YearID * 10 + d.QuarterID	AS YearQuarterID         --DBR 20240222: Aktuell nicht bekannt wie die DimDate befüllt wird, kann die darunterliegende Tabelle nicht anpassen
        ,d.QuarterID					AS QuarterID
        ,d.QuarterText					AS QuarterText
        ,d.YearMonthID					AS YearMonthID
        ,d.MonthID						AS MonthID
        ,d.MonthNameDE					AS MonthNameDE
        ,LEFT(d.MonthNameDE, 1)			AS MonthInitialDE       --DBR 20240222: Aktuell nicht bekannt wie die DimDate befüllt wird, kann die darunterliegende Tabelle nicht anpassen
        ,d.MonthText					AS MonthText
        ,d.MonthShortNameDE				AS MonthShortNameDE
        ,d.WeekdayID					AS WeekdayID
        ,d.WeekdayNameDE				AS WeekdayNameDE
        ,d.WeekdayShortNameDE   		AS WeekdayShortNameDE
        ,s.JiraSprintID					AS JiraSprintID
  FROM dwh.DimDate d
    LEFT JOIN dwh.DimJiraSprint s ON d.DateID BETWEEN s.StartDateID AND s.EndDateID
    /*
     Anmerkung DBR 20240222
     Durch den JOin entstehen Duplikate, 
     Nach Rücksprache mit JKN ist das auch nicht BP; Idealerwäre gewesen Sprint als eigene View bereitzustellen und diese dann in PowerBI-Modell zu joinen

     Wir können die Spaltee hier nicht rauswerfen da sie zu verdrahtet innerhalb der Ceteris ist
    */
