CREATE TABLE [ctl].[column_mapping] (
    [entity_id]     VARCHAR (150) NOT NULL,
    [ordinal]       INT           NOT NULL,
    [source_column] VARCHAR (128) NOT NULL,
    [target_column] VARCHAR (128) NOT NULL,
    [data_type]     VARCHAR (50)  NULL,
    [is_nullable]   BIT           DEFAULT ((1)) NOT NULL,
    CONSTRAINT [pk_column_mapping] PRIMARY KEY CLUSTERED ([entity_id] ASC, [ordinal] ASC),
    FOREIGN KEY ([entity_id]) REFERENCES [ctl].[source_entity] ([entity_id])
);


GO

