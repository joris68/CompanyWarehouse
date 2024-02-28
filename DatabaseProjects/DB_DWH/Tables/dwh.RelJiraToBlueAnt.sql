CREATE TABLE [dwh].[RelJiraToBlueAnt] (
    [JiraIssueID]      INT NOT NULL,
    [BlueAntProjectID] INT NOT NULL,
    CONSTRAINT [JiraIssueID] PRIMARY KEY CLUSTERED ([JiraIssueID] ASC)
);

