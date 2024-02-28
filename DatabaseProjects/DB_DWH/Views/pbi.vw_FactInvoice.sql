

CREATE view [pbi].[vw_FactInvoice]
AS
    SELECT
        InvoiceBK
        , InvoiceNumber
        , InvoiceDateID
        , i.BlueAntInvoiceStateID
        , InvoiceAmount
        , StartDateID
        , EndDateID
        , BlueAntProjectID
        , st.InvoiceStateName
        , WorktimeAccountableInHours * 60           AS WorktimeAccountableMinutes
        , WorktimeNotAccountableInHours * 60        AS WorktimeNotAccountableMinutes
        , WorktimeTravelAccountableInHours * 60     AS WorktimeTravelMinutes
        , WorktimeTravelNotAccountableInHours * 60  AS WorktimeTravelNotAccountableMinutes
    FROM dwh.FactBlueAntInvoice i 
    LEFT JOIN  [dwh].[DimBlueAntInvoiceState] st ON i.BlueAntInvoiceStateID = st.BlueAntInvoiceStateID 
