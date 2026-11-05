CREATE TABLE [ctl].[dq_result] (
    [dq_result_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [pipeline_run_id] VARCHAR (100) NOT NULL,
    [entity_id]       VARCHAR (150) NOT NULL,
    [rule_name]       VARCHAR (200) NOT NULL,
    [action]          VARCHAR (20)  NOT NULL,
    [failed_rows]     BIGINT        NOT NULL,
    [total_rows]      BIGINT        NOT NULL,
    [breached]        BIT           NOT NULL,
    [checked_at]      DATETIME2 (3) DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([dq_result_id] ASC)
);


GO

