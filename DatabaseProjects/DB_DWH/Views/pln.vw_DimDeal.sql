CREATE VIEW [pln].[vw_DimDeal] AS 
SELECT [DealID]
        ,DealBK
      ,[DealName]
      ,[CustomerName]
      ,[DealStateName]
      ,[DealStatePipeline]
      ,[DealPlannedDays]
      ,[LastChangedDateID]
  FROM [dwh].[DimDeal] d
  LEFT JOIN DWH.DimDealState s ON d.[DealStateID] = s.[DealStateID]
