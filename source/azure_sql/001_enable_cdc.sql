/* =============================================================================
   Enable CDC on the WideWorldImporters operational database (Azure SQL DB).

   Prereqs:
     - Azure SQL DB on vCore (serverless is fine) or DTU S3+ - CDC isn't
       available on Basic / S0-S2.
     - WideWorldImporters restored (bacpac: WideWorldImporters-Standard.bacpac).
     - Run as a db_owner.

   Copy job only supports the DEFAULT capture instance and net changes, so we
   keep defaults and require a primary key (supports_net_changes = 1).
   StockItemHoldings is intentionally NOT CDC-enabled - it demonstrates the
   watermark read method.
============================================================================= */

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1)
    EXEC sys.sp_cdc_enable_db;
GO

DECLARE @tables TABLE (schema_name SYSNAME, table_name SYSNAME);
INSERT INTO @tables VALUES
    (N'Sales',     N'Customers'),
    (N'Sales',     N'Orders'),
    (N'Sales',     N'OrderLines'),
    (N'Warehouse', N'StockItems');

DECLARE @s SYSNAME, @t SYSNAME;
DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT schema_name, table_name FROM @tables;
OPEN c;
FETCH NEXT FROM c INTO @s, @t;
WHILE @@FETCH_STATUS = 0
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables tb JOIN sys.schemas sc ON sc.schema_id = tb.schema_id
        WHERE sc.name = @s AND tb.name = @t AND tb.is_tracked_by_cdc = 1)
    BEGIN
        EXEC sys.sp_cdc_enable_table
             @source_schema = @s,
             @source_name = @t,
             @role_name = NULL,
             @supports_net_changes = 1;
        PRINT CONCAT('CDC enabled on ', @s, '.', @t);
    END
    FETCH NEXT FROM c INTO @s, @t;
END
CLOSE c; DEALLOCATE c;
GO

-- Verify
SELECT sc.name AS schema_name, tb.name AS table_name, tb.is_tracked_by_cdc
FROM sys.tables tb JOIN sys.schemas sc ON sc.schema_id = tb.schema_id
WHERE sc.name IN ('Sales', 'Warehouse')
ORDER BY tb.is_tracked_by_cdc DESC, sc.name, tb.name;
