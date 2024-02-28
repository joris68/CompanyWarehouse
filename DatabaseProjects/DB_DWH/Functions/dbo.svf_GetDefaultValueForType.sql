CREATE FUNCTION [dbo].[svf_GetDefaultValueForType] 
(
@Type NVARCHAR(255)
)
RETURNS NVARCHAR(255)
AS
BEGIN
 
DECLARE @Value NVARCHAR(255)
 
SET @Value =
CASE @Type
WHEN 'tinyint' THEN '0'
WHEN 'int' THEN '-1'
WHEN 'smallint' THEN '-1'
WHEN 'nvarchar' THEN '''<N/A>'''
WHEN 'varchar' THEN '''<N/A>'''
WHEN 'bit' THEN '0'
WHEN 'bigint' THEN '-1'
WHEN 'binary' THEN '0x0'
WHEN 'decimal' THEN '-1'
WHEN 'numeric' THEN '-1'
WHEN 'nchar' THEN '''<N/A>'''
WHEN 'char' THEN '''<N/A>'''
WHEN 'date' THEN '''1900-01-01'''
WHEN 'datetime' THEN '''1900-01-01'''
WHEN 'datetime2' THEN '''1900-01-01'''
WHEN 'uniqueidentifier' THEN '''00000000-0000-0000-0000-000000000000'''
ELSE 'ERROR'
END
 
RETURN @Value
 
END;

