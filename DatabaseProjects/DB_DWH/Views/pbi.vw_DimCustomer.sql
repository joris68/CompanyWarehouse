CREATE VIEW [pbi].[vw_DimCustomer] AS 

SELECT  [BlueAntCustomerID]
 
      ,[CustomerName]
      ,SUBSTRING(CustomerName, 1, CASE CHARINDEX(',', CustomerName)
          WHEN 0
            THEN LEN(CustomerName)
        ELSE CHARINDEX(',', CustomerName) - 1
        END) AS CustomerNameShort
  FROM [dwh].[DimBlueAntCustomer]

UNION

SELECT  
    -2 AS [BlueAntCustomerID]
    ,'Deals' AS [CustomerName]
    ,'Deals' AS CustomerNameShort

