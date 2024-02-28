CREATE VIEW [pbi].[vw_DimSprint] AS 

SELECT [JiraSprintID]
   --   ,[SprintBK]
      ,[SprintName]
      ,SUBSTRING(SprintName, CASE CHARINDEX('t', SprintName)
            WHEN 0
                THEN LEN(SprintName) + 2
            ELSE CHARINDEX('t', SprintName) + 2
            END, 1000) AS SprintNameShort
   
  FROM [dwh].[DimJiraSprint]
