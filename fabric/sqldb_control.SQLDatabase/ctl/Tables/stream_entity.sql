CREATE TABLE [ctl].[stream_entity] (
    [stream_id]            VARCHAR (100)  NOT NULL,
    [event_type]           VARCHAR (100)  NOT NULL,
    [description]          NVARCHAR (500) NULL,
    [is_active]            BIT            DEFAULT ((1)) NOT NULL,
    [kql_table]            VARCHAR (128)  NOT NULL,
    [time_column]          VARCHAR (128)  NOT NULL,
    [retention_days]       INT            NOT NULL,
    [onelake_availability] BIT            DEFAULT ((0)) NOT NULL,
    [config_json]          NVARCHAR (MAX) NOT NULL,
    [updated_at]           DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([stream_id] ASC)
);


GO

