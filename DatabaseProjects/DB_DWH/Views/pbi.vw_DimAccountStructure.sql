CREATE VIEW [pbi].[vw_DimAccountStructure] AS 
/*
This View shoul be a persisted table to isolate logic und calculation

*/
	--Get First Valie
	WITH FirstValues AS
	(
		SELECT
			AccountId
		FROM mart.DimAccount
		WHERE AccountId != -1
	 
	)
	--Get all parents for each first value
	,rekursion AS 
	(
		SELECT 
			sn.ChildAccountId
			,sn.ParentAccountId
			,1 as HierarchyLevel
			,b.AccountId as FirstAccountId
		
		FROM mart.RelAccountToAccount sn
			INNER JOIN FirstValues b on sn.ChildAccountId= b.AccountId
		WHERE 1=1 
			AND sn.IsDeleted = 0
		--and sn.OBJID = b.StructureNodesObjid --SELECT objid FROM #tabasd

		UNION ALL 
	
		SELECT 
			sn.ChildAccountId
			,sn.ParentAccountId
			,sn2.HierarchyLevel+1
			,sn2.FirstAccountId as FirstAccountId
		From mart.RelAccountToAccount sn
			inner join rekursion sn2 on sn2.ParentAccountId = sn.ChildAccountId
		WHERE sn.IsDeleted = 0
		
	), CTE_BuildTree AS
	(
		
		SELECT 
			t1.ChildAccountId
			,t1.ParentAccountId
			,t1.FirstAccountId
			,HierarchyLevel
		FROM rekursion t1
	
		UNION 
	
		--Add first Value to Tree
		SELECT 
			AccountId
			,AccountId
			,AccountId
			,0
		FROM FirstValues
	)


	/*
	Deepth set to max 9 level. If we have more then 9 level last one will be cutted and will not be displayed

	pivot 
	row 1
	Row 2 
	Row 3

	To 
	Row 1  2  3

	*/
	,CTE_BASE AS 
	(
		SELECT 
			FirstAccountId
			,[0]	AS Level0AccountId
			,[1]	AS Level1AccountId
			,[2]	AS Level2AccountId
			,[3]	AS Level3AccountId
			,[4]	AS Level4AccountId
			,[5]	AS Level5AccountId
			,[6]	AS Level6AccountId
			,[7]	AS Level7AccountId
			,[8]	AS Level8AccountId
			,[9]	AS Level9AccountId
			,COALESCE
			(
				IIF( [9] is not null,9,NULL)		--NumberLeafAccount
				,IIF([8] is not null,8,NULL)		--NumberLevelEightAccount
				,IIF([7] is not null,7,NULL)		--NumberLevelSevenAccount
				,IIF([6] is not null,6,NULL)		--NumberLevelSixAccount
				,IIF([5] is not null,5,NULL)		--NumberLevelFiveAccount
				,IIF([4] is not null,4,NULL)		--NumberLevelFourAccount
				,IIF([3] is not null,3,NULL)		--NumberLevelThreeAccount
				,IIF([2] is not null,2,NULL)		--NumberLevelTwoAccount
				,IIF([1] is not null,1,NULL)		--NumberLevelOneAccount
				,IIF([0] is not null,0,NULL)			--NumberLevelRootAccount
			) DistanceToRoot
		FROM 
		(
			SELECT 
				FirstAccountId
				,(ROW_NUMBER() OVER (PARTITION BY FirstAccountId ORDER BY HierarchyLevel DESC)) -1 DistanceToRoot
				,ParentAccountId
			FROM CTE_BuildTree
		--	WHERE FirstAccountId in(29,4)
		) p
		PIVOT
		(
		MAX(ParentAccountId) FOR DistanceToRoot IN ([0],[1],[2],[3],[4],[5],[6],[7],[8],[9])
		) as pvt
	)

	SELECT 
		r.*
		,DistanceToRoot + 1						AS LevelDepth
		,a.AccountName							AS Level0AccountName	
		,a.Account								AS Level0Account	
		,b.AccountName							AS Level1AccountName	
		,b.Account								AS Level1Account	
		,c.AccountName							AS Level2AccountName	
		,c.Account								AS Level2Account	
		,d.AccountName							AS Level3AccountName	
		,d.Account								AS Level3Account	
		,e.AccountName							AS Level4AccountName	
		,e.Account								AS Level4Account	
		,f.AccountName							AS Level5AccountName	
		,f.Account								AS Level5Account	
		,g.AccountName							AS Level6AccountName	
		,g.Account								AS Level6Account	
		,h.AccountName							AS Level7AccountName	
		,h.Account								AS Level7Account	
		,i.AccountName							AS Level8AccountName
		,i.Account								AS Level8Account	
		
	FROM CTE_BASE r
		LEFT JOIN mart.DimAccount  a on r.Level0AccountId	= a.AccountId
		LEFT JOIN mart.DimAccount  b on r.Level1AccountId	= b.AccountId
		LEFT JOIN mart.DimAccount  c on r.Level2AccountId	= c.AccountId
		LEFT JOIN mart.DimAccount  d on r.Level3AccountId	= d.AccountId
		LEFT JOIN mart.DimAccount  e on r.Level4AccountId	= e.AccountId
		LEFT JOIN mart.DimAccount  f on r.Level5AccountId	= f.AccountId
		LEFT JOIN mart.DimAccount  g on r.Level6AccountId	= g.AccountId
		LEFT JOIN mart.DimAccount  h on r.Level7AccountId	= h.AccountId
		LEFT JOIN mart.DimAccount  i on r.Level8AccountId	= i.AccountId
		LEFT JOIN mart.DimAccount  j on r.Level9AccountId	= j.AccountId


		