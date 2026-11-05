CREATE TABLE [ctl].[watermark] (
    [entity_id]       VARCHAR (150)  NOT NULL,
    [watermark_value] NVARCHAR (100) NULL,
    [updated_at]      DATETIME2 (3)  DEFAULT (sysutcdatetime()) NOT NULL,
    PRIMARY KEY CLUSTERED ([entity_id] ASC)
);


GO

