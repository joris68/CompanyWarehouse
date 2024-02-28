




CREATE VIEW [pln].[vw_DimProjectAll]
AS
SELECT        'Alle Projekte' AS pParent, [ProjectNumber] AS pChild, '1' + ProjectName AS ORDERBY
FROM            [pln].[vw_DimProject]
UNION
SELECT        'Alle Projekte' AS pParent, 'de-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5) AS pChild, '2' + DealName  AS ORDERBY
FROM            [pln].[vw_DimDeal]
UNION
SELECT        'Alle Projekte' AS pParent, 'in-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5) AS pChild, '3' + DealName  AS ORDERBY
FROM            [pln].[pln_DimProjectInternal]
UNION
SELECT        'BlueAnt' AS pParent, [ProjectNumber] AS pChild, '4' + ProjectName AS ORDERBY
FROM            [pln].[vw_DimProject]
UNION
SELECT        'Deals' AS pParent, 'de-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5) AS pChild, '5' + DealName  AS ORDERBY
FROM            [pln].[vw_DimDeal]
UNION
SELECT        'Intern' AS pParent, 'in-' + RIGHT('00000' + CAST([DealID] AS VARCHAR(255)), 5) AS pChild, '6' + DealName  AS ORDERBY
FROM            [pln].[pln_DimProjectInternal]
UNION
SELECT        'Jahre' AS pParent, 'J' + LEFT([EndDateID], 4) AS pChild, '7'  AS ORDERBY
FROM            [pln].[vw_DimProject]
UNION
SELECT        'Kunden' AS pParent, 'K' + CAST([CustomerID] AS VARCHAR(255)) AS pChild, '8' + ProjectName AS ORDERBY
FROM            [pln].[vw_DimProject]
UNION
SELECT        'J' + LEFT([EndDateID], 4) AS pParent, [ProjectNumber] AS pChild, '9'  + ProjectName AS ORDERBY
FROM            [pln].[vw_DimProject]
UNION
SELECT        'K' + CAST([CustomerID] AS VARCHAR(255)) AS pParent, [ProjectNumber] AS pChild, 'A' + ProjectName AS ORDERBY
FROM            [pln].[vw_DimProject]
