CREATE VIEW pbi.vw_DimDeal AS 
SELECT [DealID]
      ,[DealName]
      ,[CustomerName]
      ,[DealStateName]
      ,[DealStatePipeline]
      ,[DealPlannedDays]
      ,[LastChangedDateID]
  FROM [dwh].[DimDeal] d
  LEFT JOIN DWH.DimDealState s ON d.[DealStateID] = s.[DealStateID]