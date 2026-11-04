/* =============================================================================
   Control plane for the metadata-driven framework.
   Target: SQL database in Fabric (sqldb_control). Idempotent - safe to re-run.

   Config tables  (reloaded from Git by scripts/sync_metadata.py):
       ctl.source_system, ctl.source_entity, ctl.column_mapping, ctl.dq_rule,
       ctl.stream_entity, ctl.alert_rule
   Operational tables (written by pipelines/notebooks, never reloaded):
       ctl.watermark, ctl.run_log, ctl.dq_result
   Operational tables intentionally have NO foreign keys to config tables so
   config can be reloaded without touching history.
============================================================================= */

IF SCHEMA_ID('ctl') IS NULL EXEC('CREATE SCHEMA ctl');
GO

-- ---------------------------------------------------------------- config tables
IF OBJECT_ID('ctl.source_system') IS NULL
CREATE TABLE ctl.source_system (
    system_id        VARCHAR(100)  NOT NULL PRIMARY KEY,
    source_type      VARCHAR(20)   NOT NULL,          -- azure_sql | adls
    description      NVARCHAR(500) NULL,
    connection_name  VARCHAR(200)  NOT NULL,
    database_name    VARCHAR(200)  NULL,
    container        VARCHAR(200)  NULL,
    base_path        VARCHAR(500)  NULL,
    updated_at       DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('ctl.source_entity') IS NULL
CREATE TABLE ctl.source_entity (
    entity_id         VARCHAR(150)  NOT NULL PRIMARY KEY,
    system_id         VARCHAR(100)  NOT NULL REFERENCES ctl.source_system(system_id),
    description       NVARCHAR(500) NULL,
    is_active         BIT           NOT NULL DEFAULT 1,
    priority          INT           NOT NULL DEFAULT 100,
    ingestion_engine  VARCHAR(20)   NOT NULL,          -- copy_job | pipeline_copy | notebook
    read_method       VARCHAR(20)   NOT NULL,          -- cdc | watermark | full | new_files
    write_method      VARCHAR(20)   NOT NULL,          -- append | overwrite | scd1_merge | scd2
    copy_job_group    VARCHAR(100)  NULL,
    source_schema     VARCHAR(128)  NULL,
    source_table      VARCHAR(128)  NULL,
    file_pattern      VARCHAR(500)  NULL,
    file_format       VARCHAR(20)   NULL,
    target_lakehouse  VARCHAR(128)  NOT NULL,
    bronze_table      VARCHAR(128)  NOT NULL,
    silver_table      VARCHAR(128)  NOT NULL,
    primary_keys      VARCHAR(500)  NULL,              -- comma separated
    watermark_column  VARCHAR(128)  NULL,
    config_json       NVARCHAR(MAX) NOT NULL,          -- full validated entity, passed to notebooks
    updated_at        DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('ctl.column_mapping') IS NULL
CREATE TABLE ctl.column_mapping (
    entity_id      VARCHAR(150) NOT NULL REFERENCES ctl.source_entity(entity_id),
    ordinal        INT          NOT NULL,
    source_column  VARCHAR(128) NOT NULL,
    target_column  VARCHAR(128) NOT NULL,
    data_type      VARCHAR(50)  NULL,
    is_nullable    BIT          NOT NULL DEFAULT 1,
    CONSTRAINT pk_column_mapping PRIMARY KEY (entity_id, ordinal)
);
GO

IF OBJECT_ID('ctl.dq_rule') IS NULL
CREATE TABLE ctl.dq_rule (
    dq_rule_id   INT IDENTITY(1,1) PRIMARY KEY,
    entity_id    VARCHAR(150)  NOT NULL REFERENCES ctl.source_entity(entity_id),
    rule_type    VARCHAR(30)   NOT NULL,              -- not_null | unique | range | regex | accepted_values
    column_name  VARCHAR(128)  NOT NULL,
    action       VARCHAR(20)   NOT NULL,              -- warn | quarantine | fail
    threshold    DECIMAL(9,6)  NOT NULL DEFAULT 0,
    params_json  NVARCHAR(MAX) NULL
);
GO

IF OBJECT_ID('ctl.stream_entity') IS NULL
CREATE TABLE ctl.stream_entity (
    stream_id             VARCHAR(100)  NOT NULL PRIMARY KEY,
    event_type            VARCHAR(100)  NOT NULL,
    description           NVARCHAR(500) NULL,
    is_active             BIT           NOT NULL DEFAULT 1,
    kql_table             VARCHAR(128)  NOT NULL,
    time_column           VARCHAR(128)  NOT NULL,
    retention_days        INT           NOT NULL,
    onelake_availability  BIT           NOT NULL DEFAULT 0,
    config_json           NVARCHAR(MAX) NOT NULL,
    updated_at            DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('ctl.alert_rule') IS NULL
CREATE TABLE ctl.alert_rule (
    stream_id       VARCHAR(100)  NOT NULL REFERENCES ctl.stream_entity(stream_id),
    alert_name      VARCHAR(100)  NOT NULL,
    description     NVARCHAR(500) NULL,
    severity        VARCHAR(10)   NOT NULL,
    action          VARCHAR(20)   NOT NULL,
    lookback        VARCHAR(20)   NOT NULL,
    condition_expr  NVARCHAR(1000) NOT NULL,
    CONSTRAINT pk_alert_rule PRIMARY KEY (stream_id, alert_name)
);
GO

-- ---------------------------------------------------------------- operational tables
IF OBJECT_ID('ctl.watermark') IS NULL
CREATE TABLE ctl.watermark (
    entity_id        VARCHAR(150)  NOT NULL PRIMARY KEY,
    watermark_value  NVARCHAR(100) NULL,
    updated_at       DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('ctl.run_log') IS NULL
CREATE TABLE ctl.run_log (
    run_log_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id  VARCHAR(100)   NOT NULL,
    entity_id        VARCHAR(150)   NULL,
    stage            VARCHAR(30)    NOT NULL,         -- copy_job | bronze | silver | gold
    status           VARCHAR(20)    NOT NULL,         -- running | succeeded | failed
    rows_read        BIGINT         NULL,
    rows_written     BIGINT         NULL,
    rows_quarantined BIGINT         NULL,
    started_at       DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    ended_at         DATETIME2(3)   NULL,
    error_message    NVARCHAR(MAX)  NULL,
    details_json     NVARCHAR(MAX)  NULL
);
GO

IF OBJECT_ID('ctl.dq_result') IS NULL
CREATE TABLE ctl.dq_result (
    dq_result_id     BIGINT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id  VARCHAR(100)  NOT NULL,
    entity_id        VARCHAR(150)  NOT NULL,
    rule_name        VARCHAR(200)  NOT NULL,
    action           VARCHAR(20)   NOT NULL,
    failed_rows      BIGINT        NOT NULL,
    total_rows       BIGINT        NOT NULL,
    breached         BIT           NOT NULL,
    checked_at       DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- ---------------------------------------------------------------- views for pipeline Lookups
CREATE OR ALTER VIEW ctl.vw_active_entities AS
SELECT e.entity_id, e.system_id, s.source_type, s.connection_name, e.priority,
       e.ingestion_engine, e.read_method, e.write_method, e.copy_job_group,
       e.bronze_table, e.silver_table, e.config_json
FROM ctl.source_entity e
JOIN ctl.source_system s ON s.system_id = e.system_id
WHERE e.is_active = 1;
GO

CREATE OR ALTER VIEW ctl.vw_active_copy_job_groups AS
SELECT copy_job_group,
       'cj_' + copy_job_group AS copy_job_name,
       MIN(priority)          AS priority,
       COUNT(*)               AS table_count
FROM ctl.source_entity
WHERE is_active = 1 AND ingestion_engine = 'copy_job'
GROUP BY copy_job_group;
GO

-- ---------------------------------------------------------------- logging procedures
CREATE OR ALTER PROCEDURE ctl.usp_log_run_start
    @pipeline_run_id VARCHAR(100), @entity_id VARCHAR(150), @stage VARCHAR(30)
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO ctl.run_log (pipeline_run_id, entity_id, stage, status)
    VALUES (@pipeline_run_id, @entity_id, @stage, 'running');
    SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS run_log_id;
END;
GO

CREATE OR ALTER PROCEDURE ctl.usp_log_run_end
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

CREATE OR ALTER PROCEDURE ctl.usp_set_watermark
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

-- Notebook returns DQ results as JSON array; pipeline passes it here.
CREATE OR ALTER PROCEDURE ctl.usp_log_dq_results
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
