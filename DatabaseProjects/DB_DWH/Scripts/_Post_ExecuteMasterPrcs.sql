-- Author:DBR
-- Name: _Post_Master_ExecutePrcs.sql
-- Function: MasterToExecuteAll Prcs'
/*
*
*		DEV: 
*		PROD: 
*
*/

--SEQ 1
EXEC etl.prc_FillMartDimAccount


--SEQ 2
EXEC etl.[prc_FillMartFactBusinessManagementEvaluationPlan]
EXEC etl.prc_FillMartFactBusinessManagementEvaluation
EXEC etl.prc_FillMartRelAccountToAccount