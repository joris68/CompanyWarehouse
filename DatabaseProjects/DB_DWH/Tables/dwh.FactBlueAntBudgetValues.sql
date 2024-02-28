CREATE TABLE [dwh].[FactBlueAntBudgetValues_rename_DBR_20240228] (
/*
    Wir sind uns nicht sicher ob jemand noch diese Tabelle verwendet. 
    Deswegen haben wir sie umbenannt von: FactBlueAntBudgetValues zu FactBlueAntBudgetValues_rename_DBR_20240228.

    Sollte keiner in einem halben Jahr schreihen, war sie wohl nicht relevant
*/
    [BlueAntProjectID]    INT            NULL,
    [ProjectPropertyType] NVARCHAR (100) NULL,
    [Value]               DECIMAL (9, 2) NULL,
    [HashkeyBK]           VARCHAR (255)  NULL,
    [HashkeyValue]        VARCHAR (255)  NULL,
    [InsertDate]          DATETIME       NULL,
    [UpdateDate]          DATETIME       NULL
);

