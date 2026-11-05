
CREATE   PROCEDURE ctl.usp_log_run_end
    @run_log_id BIGINT, @status VARCHAR(20),
    @rows_read BIGINT = NULL, @rows_written BIGINT = NULL, @rows_quarantined BIGINT = NULL,
    @error_message NVARCHAR(MAX) = NULL, @details_json NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE ctl.run_log
       SET status = @status, rows_read = @rows_read, rows_written = @rows_written,
           rows_quarantined = @rows_quarantined, error_message = @error_message,
           details_json = @details_json, ended_at = SYSUTCDATETIME()
     WHERE run_log_id = @run_log_id;
END;

GO

