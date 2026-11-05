CREATE TABLE [ctl].[dq_rule] (
    [dq_rule_id]  INT            IDENTITY (1, 1) NOT NULL,
    [entity_id]   VARCHAR (150)  NOT NULL,
    [rule_type]   VARCHAR (30)   NOT NULL,
    [column_name] VARCHAR (128)  NOT NULL,
    [action]      VARCHAR (20)   NOT NULL,
    [threshold]   DECIMAL (9, 6) DEFAULT ((0)) NOT NULL,
    [params_json] NVARCHAR (MAX) NULL,
    PRIMARY KEY CLUSTERED ([dq_rule_id] ASC),
    FOREIGN KEY ([entity_id]) REFERENCES [ctl].[source_entity] ([entity_id])
);


GO

