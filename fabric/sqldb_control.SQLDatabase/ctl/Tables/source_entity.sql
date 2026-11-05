CREATE TABLE [ctl].[source_entity] (
    [entity_id]        VARCHAR (150)  NOT NULL,
    [system_id]        VARCHAR (100)  NOT NULL,
    [description]      NVARCHAR (500) NULL,
    [is_active]        BIT            DEFAULT ((1)) NOT NULL,
    [priority]         INT            DEFAULT ((100)) NOT NULL,
    [ingestion_engine] VARCHAR (20)   NOT NULL,
    [read_method]      VARCHAR (20)   NOT NULL,
    [write_method]     VARCHAR (20)   NOT NULL,
    [copy_job_group]   VARCHAR (100)  NULL,
    [source_schema]    VARCHAR (128)  NULL,
    [source_table]     VARCHAR (128)  NULL,
    [file_pattern]     VARCHAR (500)  NULL,
    [file_format]      VARCHAR (20)   NULL,
    [target_lakehouse] VARCHAR (128)  NOT NULL,
    [bronze_table]     VARCHAR (128)  NOT NULL,
    [silver_table]     VARCHAR (128)  NOT NULL,
    [primary_keys]     VARCHAR (500)  NULL,
    [watermark_column] VARCHAR (128)  NULL,
    [config_json]      NVARCHAR (MAX) NOT NULL,
    [updated_at]       DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([entity_id] ASC),
    FOREIGN KEY ([system_id]) REFERENCES [ctl].[source_system] ([system_id])
);


GO

