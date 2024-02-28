CREATE TABLE [dwh].[FactPlanningSCD2] (
    [BlueAntProjectID] INT            NOT NULL,
    [DealID]           INT            NOT NULL,
    [UserID]           INT            NOT NULL,
    [StartDateID]      INT            NULL,
    [EndDateID]        INT            NULL,
    [Billable]         BIT            NOT NULL,
    [PlanningValue]    DECIMAL (9, 4) NULL,
    [PlanningType]     BIT            NOT NULL,
    [RowStartDate]     INT            NULL,
    [RowEndDate]       INT            NULL,
    [RowIsCurrent]     BIT            NULL,
    [InsertDate]       DATETIME       NULL,
    [UpdateDate]       DATETIME       NULL,
    [HashKeyBK]        VARCHAR (255)  NULL,
    [HashKeyValue]     VARCHAR (255)  NULL
);


GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'der SK, die das Projekt identifiziert.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'BlueAntProjectID'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'der SK, die Deal identifiziert.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'DealID'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'der SK des Mitarbeiters.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'UserID'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Datums-SK, die den Startzeitpunkt eines Datensatzes darstellt.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'StartDateID'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Datums-SK, die den Endzeitpunkt eines Datensatzes darstellt.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'EndDateID'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Ein Indikator, ob die Arbeitszeit fakturierbar ist.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'Billable'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Typ der Planung (geplant oder reserviert).',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'PlanningValue'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Der geplante Wert oder Aufwand.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'PlanningType'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Zeitfenster, für das der Datensatz gültig ist (Startdatum).',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'RowStartDate'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Zeitfenster, für das der Datensatz gültig ist (Enddatum).',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'RowEndDate'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Ein Indikator, ob der Datensatz der aktuelle ist.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'RowIsCurrent'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Zeitstempel für das Einfügen des Datensatzes.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'InsertDate'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Zeitstempel für das Aktualisieren des Datensatzes.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'UpdateDate'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Hash-Wert zur Identifikation von Datensätzen.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'HashKeyBK'
GO
EXEC sp_addextendedproperty @name = N'MS_Description',
    @value = N'Hash-Wert zum Vergleich von Datensätzen.',
    @level0type = N'SCHEMA',
    @level0name = N'dwh',
    @level1type = N'TABLE',
    @level1name = N'FactPlanningSCD2',
    @level2type = N'COLUMN',
    @level2name = N'HashKeyValue'