CREATE TABLE [ctl].[source_system] (
    [system_id]       VARCHAR (100)  NOT NULL,
    [source_type]     VARCHAR (20)   NOT NULL,
    [description]     NVARCHAR (500) NULL,
    [connection_name] VARCHAR (200)  NOT NULL,
    [database_name]   VARCHAR (200)  NULL,
    [container]       VARCHAR (200)  NULL,
    [base_path]       VARCHAR (500)  NULL,
    [updated_at]      DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([system_id] ASC)
);


GO

