CREATE VIEW [pbi].[vw_DimProjectRole] AS

SELECT 
    fr.ProjectRoleID
    ,fr.UserID
    ,fr.BlueAntProjectID
    ,dr.ProjectRoleName
    ,dr.ExternalCost AS ExternalCostPerHour
    ,dr.TravelCost AS TravelCostPerHour
  FROM [dwh].[FactBlueAntProjectResource] fr
  LEFT JOIN [dwh].[DimBlueAntProjectRoles] dr ON fr.ProjectRoleID = dr.ProjectRoleID