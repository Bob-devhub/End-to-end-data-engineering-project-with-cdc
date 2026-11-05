CREATE TABLE [ctl].[alert_rule] (
    [stream_id]      VARCHAR (100)   NOT NULL,
    [alert_name]     VARCHAR (100)   NOT NULL,
    [description]    NVARCHAR (500)  NULL,
    [severity]       VARCHAR (10)    NOT NULL,
    [action]         VARCHAR (20)    NOT NULL,
    [lookback]       VARCHAR (20)    NOT NULL,
    [condition_expr] NVARCHAR (1000) NOT NULL,
    CONSTRAINT [pk_alert_rule] PRIMARY KEY CLUSTERED ([stream_id] ASC, [alert_name] ASC),
    FOREIGN KEY ([stream_id]) REFERENCES [ctl].[stream_entity] ([stream_id])
);


GO

