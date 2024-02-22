DROP EVENT TRIGGER IF EXISTS et_log_ddl_info;

CREATE EVENT TRIGGER et_log_ddl_info ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE')
        EXECUTE PROCEDURE public.f_log_ddl();
