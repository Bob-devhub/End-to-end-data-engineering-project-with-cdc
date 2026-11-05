
-- Notebook returns DQ results as JSON array; pipeline passes it here.
CREATE   PROCEDURE ctl.usp_log_dq_results
    @pipeline_run_id VARCHAR(100), @entity_id VARCHAR(150), @results_json NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.dq_result (pipeline_run_id, entity_id, rule_name, action, failed_rows, total_rows, breached)
    SELECT @pipeline_run_id, @entity_id, j.[rule], j.action, j.failed_rows, j.total_rows, j.breached
    FROM OPENJSON(@results_json)
         WITH ([rule] VARCHAR(200), action VARCHAR(20), failed_rows BIGINT, total_rows BIGINT, breached BIT) j;
END;

GO

