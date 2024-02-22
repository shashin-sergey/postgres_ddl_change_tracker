--DROP TABLE IF EXISTS public.ddl_changes_version_info;

CREATE TABLE if NOT EXISTS  public.ddl_changes_version_info (
    change_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
    table_name_hash text NOT NULL,
    ddl_version_from int4 NOT NULL,
    ddl_version_to int4 NOT NULL,
    change_time timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'::text),
    CONSTRAINT ddl_changes_version_info_pkey PRIMARY KEY (change_id)
);

COMMENT ON TABLE public.ddl_changes_version_info IS 'The table that stores the ddl changes time';

COMMENT ON COLUMN public.ddl_changes_version_info.change_id IS 'Identity column';
COMMENT ON COLUMN public.ddl_changes_version_info.table_name_hash IS 'Table name';
COMMENT ON COLUMN public.ddl_changes_version_info.ddl_version_from IS 'Old ddl version';
COMMENT ON COLUMN public.ddl_changes_version_info.ddl_version_to IS 'New ddl version';
COMMENT ON COLUMN public.ddl_changes_version_info.change_time IS 'Change time';
