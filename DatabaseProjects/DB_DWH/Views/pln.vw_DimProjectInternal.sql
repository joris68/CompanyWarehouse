
CREATE VIEW [pln].[vw_DimProjectInternal] AS 
SELECT [DealID]
      ,[DealName]
      ,[ProjectNumber]
      ,[ProjectManagerID]
	  ,ISNULL([UserInitials] , '<N/A>') AS UserInitials
  FROM [pln].[pln_DimProjectInternal]
  LEFT OUTER JOIN  [pln].[vw_DimUser]
  ON [ProjectManagerID] = [UserID]