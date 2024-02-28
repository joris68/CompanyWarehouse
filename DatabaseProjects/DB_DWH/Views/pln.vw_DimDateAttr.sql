CREATE VIEW pln.vw_DimDateAttr
AS
SELECT        *
FROM            (SELECT        [YearID] AS pElement, [YearID] AS pIndexWert, [YearID] + '0101' AS pFirstDay, [YearID] + '1231' AS pLastDay, 1 AS ORDERBY, YearNext AS NextID, YearPrev AS PrevID
                          FROM            [pln].[vw_DimDate]
                          GROUP BY [YearID], [YearID] + '0101', [YearID] + '1231', YearNext, YearPrev ) TY
UNION
SELECT        *
FROM            (SELECT        [QuarterID] AS pElement, [QuarterNr] AS pIndexWert, [YearID] + CAST((1 + ([QuarterNr] - 1) * 3) AS VARCHAR(255)) + '01' AS pFirstDay, [YearID] + CAST((([QuarterNr] - 1) * 3) AS VARCHAR(255)) + '31' AS pLastDay, 
                                                    2 AS ORDERBY, [QuarterNext] AS NextID, [QuarterPrev] AS NextPrev
                          FROM            [pln].[vw_DimDate]
                          GROUP BY [QuarterID], [QuarterNr], [YearID] + CAST((1 + ([QuarterNr] - 1) * 3) AS VARCHAR(255)) + '01', [YearID] + CAST((([QuarterNr] - 1) * 3) AS VARCHAR(255)) + '31', [QuarterNext], [QuarterPrev]) TQ
UNION
SELECT        *
FROM            (SELECT        [MonthID] AS pElement, [MonthNr] AS pIndexWert, [YearID] + RIGHT('0' + CAST([MonthNr] AS VARCHAR(255)), 2) + '01' AS pFirstDay, [YearID] + RIGHT('0' + CAST([MonthNr] AS VARCHAR(255)), 2) + '31' AS pLastDay, 
                                                    3 AS ORDERBY, MonthNext AS NextID, MonthPrev AS PrevID
                          FROM            [pln].[vw_DimDate]
                          GROUP BY [MonthID], [MonthNr], [YearID] + RIGHT('0' + CAST([MonthNr] AS VARCHAR(255)), 2) + '01', [YearID] + RIGHT('0' + CAST([MonthNr] AS VARCHAR(255)), 2) + '31', MonthNext, MonthPrev) TM
UNION
SELECT        *
FROM            (SELECT        [WeekID] AS pElement, [CalendarWeekText] AS pIndexWert, [FirstDayOfWeek] AS pFirstDay, [LastdayOfWeek] AS pLastDay, 4 AS ORDERBY, WeekNext AS NextID, WeekPrev AS PrevID
                          FROM            [pln].[vw_DimDate]
                          GROUP BY [WeekID], [CalendarWeekText], [FirstDayOfWeek], [LastdayOfWeek], WeekNext, WeekPrev) TW

GO
EXECUTE sp_addextendedproperty @name = N'MS_DiagramPane1', @value = N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[22] 4[16] 2[44] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1 [50] 4 [25] 3))"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1 [50] 2 [25] 3))"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4 [30] 2 [40] 3))"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1 [56] 3))"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 0
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
      End
   End
   Begin SQLPane = 
   End
   Begin DataPane = 
      Begin ParameterDefaults = ""
      End
      Begin ColumnWidths = 9
         Width = 284
         Width = 1500
         Width = 1500
         Width = 1500
         Width = 1500
         Width = 1500
         Width = 1500
         Width = 1500
         Width = 1500
      End
   End
   Begin CriteriaPane = 
      Begin ColumnWidths = 11
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1320
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
', @level0type = N'SCHEMA', @level0name = N'pln', @level1type = N'VIEW', @level1name = N'vw_DimDateAttr';


GO
EXECUTE sp_addextendedproperty @name = N'MS_DiagramPaneCount', @value = 1, @level0type = N'SCHEMA', @level0name = N'pln', @level1type = N'VIEW', @level1name = N'vw_DimDateAttr';

