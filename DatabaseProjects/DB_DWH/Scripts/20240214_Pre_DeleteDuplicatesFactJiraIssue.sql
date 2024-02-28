-- Author:Moez
-- Name: 20240214_Pre_DeleteDuplicatesFactJiraIssue.sql
-- Function: Delete Duplicates from dwh.FactJiraIssue
/*
*
*		DEV: done
*		PROD: 28.02.2024
*
*/

DELETE a
FROM
(
	SELECT *, ROW_NUMBER() OVER (PARTITION BY JiraIssueID ORDER BY InsertDate) rn
	FROM dwh.factjiraissue
	WHERE JiraIssueID in 
	(
		SELECT JiraIssueID
		FROM dwh.factjiraissue
		GROUP BY JiraIssueID
		HAVING COUNT(1) != 1
	)
) a
WHERE rn != 1