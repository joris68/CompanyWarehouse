
CREATE view [pln].[vw_DimUser]
AS

    SELECT [dwh].DimUser.UserID
    , [dwh].DimUser.BlueAntUserBK
    , [dwh].DimUser.JiraUserBK
    , [dwh].DimUser.UserInitials
    , [dwh].DimUser.FirstName
    , [dwh].DimUser.LastName
    , [dwh].DimUser.UserName
    , [dwh].DimUser.UserEmail
    , [dwh].DimUser.InsertDate
    , [dwh].DimUser.UpdateDate
    , [dwh].DimUser.HashkeyValue
    , [dwh].DimUser.HashkeyBK
    , ue.TeamName
    , ue.State
    FROM [dwh].DimUser 
    LEFT JOIN [tmp].[dwh_DimUserExtended] ue ON [dwh].DimUser.BlueAntUserBK = ue.BlueAntUserBK
    WHERE ue.TeamName IS NOT NUll OR [dwh].DimUser.UserID = -2
 
 

 
