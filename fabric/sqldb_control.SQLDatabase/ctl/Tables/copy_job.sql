CREATE TABLE [ctl].[copy_job] (
    [copy_job_name]  VARCHAR (128)    NOT NULL,
    [copy_job_group] VARCHAR (100)    NOT NULL,
    [item_id]        UNIQUEIDENTIFIER NULL,
    [updated_at]     DATETIME2 (3)    DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([copy_job_name] ASC)
);


GO

