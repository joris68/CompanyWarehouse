CREATE PROCEDURE [dbo].[prc_AddUnknownDimensionMembersDynamic]
 (@SchemaName nvarchar(255), 
 @TableName nvarchar(255))
AS
BEGIN
 DECLARE @column_name nvarchar(255), @data_type nvarchar(255)
 DECLARE @query nvarchar(max), @insert_into nvarchar(max),@select nvarchar(max)
 DECLARE @identity nvarchar(255), @table_inc_schema nvarchar(255)
 DECLARE @default_value nvarchar(255)
 SET @table_inc_schema = @SchemaName+'.'+@TableName
 SET @query =  'SET IDENTITY_INSERT '+ @table_inc_schema+' ON '
 SET @insert_into = 'INSERT INTO '+ @table_inc_schema+' ('
 SET @select = 'SELECT '
 SET @identity = (SELECT 
   COLUMN_NAME
 FROM   
   INFORMATION_SCHEMA.COLUMNS 
 WHERE   
   TABLE_NAME = @TableName AND 
   TABLE_SCHEMA = @SchemaName AND 
   COLUMNPROPERTY(object_id(@table_inc_schema), COLUMN_NAME, 'IsIdentity') = 1)
 DECLARE column_cursor CURSOR FOR 
 SELECT 
   COLUMN_NAME
   ,DATA_TYPE
 FROM   
   INFORMATION_SCHEMA.COLUMNS 
 WHERE   
   TABLE_NAME = @TableName AND
   TABLE_SCHEMA = @SchemaName
   
   
 OPEN column_cursor
 FETCH NEXT FROM column_cursor 
 INTO @column_name, @data_type
 WHILE @@FETCH_STATUS = 0
 BEGIN
  SET @insert_into = @insert_into + '['+@column_name+']'+','
   SET @default_value = [dbo].[svf_GetDefaultValueForType] (@data_type)
   IF @default_value = 'ERROR'
    BEGIN
    PRINT 'ERROR in table "'+@table_inc_schema+'" : The Type "'+@data_type+'" of column "'+'['+@column_name+']'+'" is not mapped.'
    CLOSE column_cursor;
    DEALLOCATE column_cursor;
    RETURN
    END
   SET @select = @select + @default_value +','
  FETCH NEXT FROM column_cursor
  INTO @column_name, @data_type
 END
 SET @insert_into = LEFT(@insert_into, LEN(@insert_into) - 1) + ') '
 SET @select = LEFT(@select, LEN(@select) - 1)
 SET @query = @query+@insert_into+@select+' WHERE NOT EXISTS(SELECT * FROM '+@table_inc_schema+' WHERE '+@identity+' = -1)'
 SET @query = @query+'SET IDENTITY_INSERT '+@table_inc_schema+' OFF'
 
 CLOSE column_cursor;
 DEALLOCATE column_cursor;
 --PRINT @query
 EXEC(@query)
 PRINT 'Added unknow members to '+@table_inc_schema
END
