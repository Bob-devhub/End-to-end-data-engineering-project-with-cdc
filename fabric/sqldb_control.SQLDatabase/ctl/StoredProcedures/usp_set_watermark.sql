
CREATE   PROCEDURE ctl.usp_set_watermark
    @entity_id VARCHAR(150), @watermark_value NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    MERGE ctl.watermark AS t
    USING (SELECT @entity_id AS entity_id, @watermark_value AS watermark_value) AS s
       ON t.entity_id = s.entity_id
    WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value, updated_at = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT (entity_id, watermark_value) VALUES (s.entity_id, s.watermark_value);
END;

GO

