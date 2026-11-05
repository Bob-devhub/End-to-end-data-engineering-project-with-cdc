
-- ---------------------------------------------------------------- logging procedures
CREATE   PROCEDURE ctl.usp_log_run_start
    @pipeline_run_id VARCHAR(100), @entity_id VARCHAR(150), @stage VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.run_log (pipeline_run_id, entity_id, stage, status)
    VALUES (@pipeline_run_id, @entity_id, @stage, 'running');
    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS run_log_id;
END;

GO

