DROP EVENT TRIGGER IF EXISTS et_log_ddl_drop_info;

CREATE EVENT TRIGGER et_log_ddl_drop_info ON sql_drop
    WHEN TAG IN ('DROP TABLE')
        EXECUTE PROCEDURE ddl_changes.f_log_ddl ();
