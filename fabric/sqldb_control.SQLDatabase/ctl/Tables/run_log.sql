CREATE TABLE [ctl].[run_log] (
    [run_log_id]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [pipeline_run_id]  VARCHAR (100)  NOT NULL,
    [entity_id]        VARCHAR (150)  NULL,
    [stage]            VARCHAR (30)   NOT NULL,
    [status]           VARCHAR (20)   NOT NULL,
    [rows_read]        BIGINT         NULL,
    [rows_written]     BIGINT         NULL,
    [rows_quarantined] BIGINT         NULL,
    [started_at]       DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    [ended_at]         DATETIME2 (3)  NULL,
    [error_message]    NVARCHAR (MAX) NULL,
    [details_json]     NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([run_log_id] ASC)
);


GO

