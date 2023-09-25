--DROP TABLE IF EXISTS ddl_changes.ddl_changes_columns;

CREATE TABLE if NOT EXISTS ddl_changes.ddl_changes_columns (
    column_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    table_hash text NOT NULL,
    column_name text NOT NULL,
    column_ordinal_position int4 NOT NULL,
    data_type text NOT NULL,
    character_maximum_length int4 NULL,
    numeric_precision int4 NULL,
    numeric_scale int4 NULL,
    CONSTRAINT ddl_changes_columns_pkey PRIMARY KEY (column_id)
);

COMMENT ON TABLE ddl_changes.ddl_changes_columns IS 'The table that stores the diferent versions of ddls.';

COMMENT ON COLUMN ddl_changes.ddl_changes_columns.column_id IS 'Identity column';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.table_hash IS 'Hash of the table';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.column_name IS 'Column name';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.column_ordinal_position IS 'Column position (Not always a progressively increasing series of numbers)';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.data_type IS 'Column data type';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.character_maximum_length IS 'Column maximum length';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.numeric_precision IS 'Numeric column precision';
COMMENT ON COLUMN ddl_changes.ddl_changes_columns.numeric_scale IS 'Numeric column scale';
