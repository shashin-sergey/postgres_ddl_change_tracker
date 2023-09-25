--DROP TABLE IF EXISTS ddl_changes.ddl_changes_meta;

CREATE TABLE if NOT EXISTS ddl_changes.ddl_changes_meta (
    meta_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    ddl_version int4 NOT NULL,
    db_name text NOT NULL,
    schema_name text NOT NULL,
    table_name text NOT NULL,
    table_name_hash text NOT NULL,
    table_hash text NOT NULL,
    active bool NOT NULL DEFAULT true,
    create_time timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'::text),
    last_mod_time timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'::text),
    CONSTRAINT ddl_changes_meta_pkey PRIMARY KEY (meta_id)
);

COMMENT ON TABLE ddl_changes.ddl_changes_meta IS 'The table that stores the meta data of the diferent versions of ddls.';

COMMENT ON COLUMN ddl_changes.ddl_changes_meta.meta_id IS 'Identity column';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.ddl_version IS 'Schema version';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.table_name IS 'Table name';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.table_hash IS 'Hash of the table';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.active IS 'True if the schema is the current';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.create_time IS 'Schema version creation time';
COMMENT ON COLUMN ddl_changes.ddl_changes_meta.last_mod_time IS 'Last ddl chamge time';
