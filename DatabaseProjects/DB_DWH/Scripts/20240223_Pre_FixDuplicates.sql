/*
*		Author:MOEZ
*		Name: 20240223_Pre_FixDuplicates.sql
*		Function: Es gibt in einigen Tabellen Duplikate beim HashkeyBK. 
*                 In diesem Pre Script löse ich das Problem bei folgenden Tabellen:
*                   -DimJiraSprint
*                   -DimBlueAntUser
*                   -DimUser
*
*		DEV: 
*		PROD: 
*
*/

Update dwh.DimJiraSprint
SET HashkeyBK =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                           CONVERT(VARCHAR, SprintName)

                          ), 2))
,HashkeyValue =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                    CONCAT(
                           CONVERT(VARCHAR, StartDateID),'|',
                           CONVERT(VARCHAR, EndDateID)
                          )), 2))
,UpdateDate = GETDATE()
WHERE SprintBK = 0



;
Update dwh.DimBlueAntUser
SET HashkeyBK =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                           CONVERT(VARCHAR, BlueAntUserBK)
                          ), 2))
,HashkeyValue =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                    CONCAT(
                           CONVERT(VARCHAR, UserInitials),'|',
                           CONVERT(VARCHAR, FirstName),'|',
						   CONVERT(VARCHAR, LastName),'|',
						   CONVERT(VARCHAR, UserEmail)
                          )), 2))
,UpdateDate = GETDATE()
WHERE BlueAntUserBK = -2



;
Delete FROM dwh.DimUser where UserID in(49,50) --Doppelt eingefügt wegen dem Merge Statement. Merge Statement habe ich auch angepasst
;
Update dwh.DimUser
SET HashkeyBK =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                           CONVERT(VARCHAR, UserEmail)
                          ), 2))
,HashkeyValue =LOWER(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', 
                    CONCAT(
                           CONVERT(VARCHAR, BlueAntUserBK),'|',
                           CONVERT(VARCHAR, JiraUserBK),'|',
						   CONVERT(VARCHAR, UserInitials),'|',
						   CONVERT(VARCHAR, FirstName),'|',
						   CONVERT(VARCHAR, LastName),'|',
						   CONVERT(VARCHAR, UserName)
                          )), 2))
,UpdateDate = GETDATE()
WHERE BlueAntUserBK = -2
