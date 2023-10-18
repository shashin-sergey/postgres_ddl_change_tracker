CREATE OR REPLACE function 
    ddl_changes.f_log_ddl()
RETURNS 
    EVENT_TRIGGER
AS
    $code$
DECLARE
    v_add_ddl_col text;
    v_add_ddl_meta text;
    v_change_time timestamp;
    v_ddl_version text;
    v_ddl_version_select text;
    v_event_tuple record;
    v_last_ddl_version text;
    v_last_ddl_version_select text;
    v_new_ddl_version_select text;
    v_status_false text;
    v_status_true text;
    v_table_hash text;
    v_table_hash_select text;
    v_table_name_hash text;
    v_table_name_hash_select text;
    v_tmp_create text;
    v_tmp_ddl_change_colums text;
    v_tmp_drop text;
    v_trigger_type text;
BEGIN 
    
    ------------------------
    ---Check trigger type---
    ------------------------

    IF 
        tg_tag = 'DROP TABLE'
    THEN
        v_trigger_type := 
            'SELECT distinct 
                replace(object_identity,''"'','''') as  object_identity
            FROM 
                pg_event_trigger_dropped_objects() ddl 
            WHERE 
                ddl.schema_name not in 
                (
                    ''pg_catalog''
                    ,''information_schema''
                    ,''ddl_changes''
                )
            AND object_identity not like ''%pg_toast%'' 
            AND object_identity not like  ''%[]%''  
            AND object_identity not in 
                (
                SELECT 
                    CASE 
                        WHEN  
                            cast(inhrelid::regclass as text) like ''%.%''
                        THEN
                            cast(inhrelid::regclass as text)
                        ELSE
                            concat_ws(
                                ''.''
                                ,current_schema
                                ,cast(inhrelid::regclass as text))
                    END  AS partition_name 
                FROM   
                    pg_catalog.pg_inherits
                )         
            '
        ;
    ELSE
        v_trigger_type := 
            'SELECT distinct 
                replace(object_identity,''"'','''') as  object_identity
            FROM 
                pg_event_trigger_ddl_commands() ddl 
            WHERE 
                ddl.schema_name not in 
                (
                    ''pg_catalog''
                    ,''information_schema''
                    ,''ddl_changes''
                )
            AND object_identity in 
               (
                SELECT 
                    concat_ws(
                        ''.''
                        ,table_schema
                        ,table_name
                    ) 
                FROM 
                    information_schema.tables
                WHERE 
                    table_type = ''BASE TABLE'')
            AND object_identity not in 
                (
                SELECT 
                    CASE 
                        WHEN  
                            cast(inhrelid::regclass as text) like ''%.%''
                        THEN
                            cast(inhrelid::regclass as text)
                        ELSE
                            concat_ws(
                                ''.''
                                ,current_schema
                                ,cast(inhrelid::regclass as text))
                    END  AS partition_name 
                FROM   
                    pg_catalog.pg_inherits
                )
            '
            ;  
    END IF;   

    FOR v_event_tuple 
    IN 
        EXECUTE 
            v_trigger_type
    LOOP    
        
        ---------------------------
        ---Fillter null triggers---
        ---------------------------
        CONTINUE WHEN v_trigger_type is null;
      
        -------------------------------
        ---Add independent variables---
        -------------------------------
        --v_change_time
        --v_table_name_hash
        --v_last_ddl_version_select

        v_change_time := 
            (STATEMENT_TIMESTAMP() AT TIME ZONE 'UTC'::TEXT);

        v_table_name_hash :=
             md5(lower(current_database() || '.' || v_event_tuple.object_identity));

        v_last_ddl_version_select := 
            'SELECT
                ddl_version_to as last_ddl_version
            FROM
                ddl_changes.ddl_changes_version_info
            WHERE 
                table_name_hash = 
                    ''' || v_table_name_hash || '''
            ORDER BY change_id DESC
            LIMIT 1 
            ;';
  
  
        EXECUTE 
            v_last_ddl_version_select 
        INTO 
            v_last_ddl_version;

        IF 
            v_last_ddl_version is null 
        THEN
            v_last_ddl_version := '0';
        END IF;
        
        --------------------
        ---Check ddl type---
        --------------------

        IF 
            tg_tag != 'DROP TABLE'

        -------------------------------
        ---ddl type isn't drop table---
        -------------------------------

        THEN  
               
            -------------------------------------------------------
            ---Make a snapshot from information_schema."columns"---
            -------------------------------------------------------

            v_tmp_ddl_change_colums := 
               'ddl_change_tmp_colums' || md5(random()::text);
        
            v_tmp_drop := 
                'DROP TABLE IF EXISTS ' || v_tmp_ddl_change_colums;
     
            EXECUTE 
                v_tmp_drop;
    
            v_tmp_create := 
                'CREATE TEMP TABLE ' 
                    || v_tmp_ddl_change_colums || ' 
                AS 
                SELECT 
                    ''' || v_table_name_hash || ''' as table_name_hash
                    ,cast(
                        is_c.table_catalog as text
                    ) as db_name
                    ,cast(
                        is_c.table_schema as text
                    ) as schema_name
                    ,cast(
                        is_c.table_name as text
                    ) as table_name
                    ,cast(
                        is_c.column_name as text
                    ) as column_name
                    ,ROW_NUMBER () 
                        OVER (
                            PARTITION BY 
                                is_c.table_catalog
                                ,is_c.table_schema
                                ,is_c.table_name
                            ORDER BY 
                                cast(
                                    is_c.ordinal_position as integer
                            )
                    ) as column_ordinal_position
                    ,cast(
                        is_c.data_type as text
                    ) as data_type
                    ,cast(
                        is_c.character_maximum_length as integer
                    ) as character_maximum_length
                    ,cast(
                        is_c.numeric_precision as integer
                    ) as numeric_precision
                    ,cast(
                        is_c.numeric_scale as integer
                    ) as numeric_scale
                FROM
                    information_schema."columns" is_c
                WHERE
                    is_c.table_schema 
                    not in (
                        ''pg_catalog''
                        ,''information_schema''
                        ,''ddl_changes'')
                AND 
                    concat_ws(
                        ''.''
                        ,is_c.table_schema
                        ,is_c.table_name
                    ) 
                    = '''|| v_event_tuple.object_identity || ''' 
                ORDER BY
                    is_c.table_catalog
                    ,is_c.table_schema
                    ,is_c.table_name
                    ,is_c.ordinal_position;';
                
            EXECUTE 
                v_tmp_create;

            -------------------
            ---Add variables---
            -------------------
            --v_table_hash
            --v_ddl_version
  
            v_table_hash_select := 
                'SELECT 
                    cast(
                        md5(
                            concat(
                                table_name_hash
                                ,string_agg(
                                    column_hash::CHARACTER varying
                                    ,'',''
                                    ORDER BY 
                                        column_ordinal_position
                                )
                            )
                        )
                        as text
                    ) as table_hash
                FROM 
                    (SELECT
                        table_name_hash
                        ,md5(
                            concat_ws(
                                ''.''
                                ,cast(
                                    column_name as text
                                )
                                ,cast(
                                    column_ordinal_position as text
                                )
                                ,replace(
                                    cast(
                                        data_type as text
                                    )
                                    ,'' ''
                                    ,''_''
                                )
                                ,cast(
                                    character_maximum_length as text
                                )
                                ,cast(
                                    numeric_precision as text
                                )
                                ,cast(
                                numeric_scale as text
                                )
                            )
                        ) as column_hash
                        ,column_ordinal_position
                    FROM
                        ' || v_tmp_ddl_change_colums || ' ) th
                    where 
                        table_name_hash = 
                            ''' || v_table_name_hash || '''
                    group by 
                        table_name_hash;';
              
            EXECUTE 
                v_table_hash_select 
            INTO 
                v_table_hash;

              
            ----------------------------
            ---Table hash exits check---
            ----------------------------

            IF 
                v_table_hash is not null
            THEN
              
                v_ddl_version_select :=  
                    'SELECT
                        ddl_version
                    FROM
                        ddl_changes.ddl_changes_meta dcm
                    WHERE
                        ''' || v_table_hash || ''' = 
                            dcm.table_hash;';
                    
                EXECUTE 
                    v_ddl_version_select 
                INTO 
                    v_ddl_version;
    
                IF 
                    v_ddl_version is null 
                THEN
                    v_new_ddl_version_select :=
                        'SELECT
                            max(ddl_version) + 1 as max_ddl_version
                        FROM
                            ddl_changes.ddl_changes_meta
                        WHERE
                            table_name_hash = 
                                ''' || v_table_name_hash || ''';'; 
                
                    EXECUTE 
                       v_new_ddl_version_select 
                    INTO  
                        v_ddl_version;
                  
                    IF 
                        v_ddl_version is null
                    THEN
                        v_ddl_version = '1';
                    END IF;
                
                END IF;

                -------------------------------------
                ---Filtering out irrelevant alters---
                -------------------------------------

                CONTINUE WHEN v_ddl_version = v_last_ddl_version;              
    
                --------------------------------
                ---De-Activate the old schema---
                --------------------------------
        
                v_status_false := 
                    'UPDATE
                        ddl_changes.ddl_changes_meta dcm
                    SET
                        active = 
                            false
                        ,last_mod_time = 
                            ''' || v_change_time || ''' 
                    WHERE 
                        dcm.table_name_hash = 
                            ''' || v_table_name_hash || '''
                    AND 
                        active = true
                    AND 
                        ''' || v_table_hash || ''' != dcm.table_hash;';
                       
                EXECUTE v_status_false;   
                
                IF 
                    v_table_hash not in 
                        (SELECT 
                            table_hash 
                        FROM 
                            ddl_changes.ddl_changes_meta)
                THEN
    
                     -------------------------------------------------------
                    --Add the new scheama to ddl_changes.ddl_changes_meta---
                    --------------------------------------------------------
                  
                    v_add_ddl_meta := 
                        'INSERT INTO ddl_changes.ddl_changes_meta(
                            ddl_version
                            ,table_name_hash
                            ,db_name
                            ,schema_name
                            ,table_name
                            ,table_hash
                            ,active
                            ,create_time
                            ,last_mod_time
                        )
                        SELECT distinct
                            CASE 
                                WHEN ddl_ver.max_ddl_version is not null 
                            THEN
                                ddl_ver.max_ddl_version + 1
                            ELSE
                                1
                            END as ddl_version
                            ,tmp_c.table_name_hash
                            ,tmp_c.db_name
                            ,tmp_c.schema_name
                            ,tmp_c.table_name
                            ,''' || v_table_hash || '''
                            ,TRUE AS active
                            ,cast(''' || v_change_time || ''' as timestamp)  as create_time
                            ,cast(''' || v_change_time || ''' as timestamp)  as last_mod_time
                        FROM
                            ' || v_tmp_ddl_change_colums || ' tmp_c
                        FULL JOIN (
                            SELECT
                                max(ddl_version) as max_ddl_version
                                ,table_name_hash
                            FROM
                                ddl_changes.ddl_changes_meta
                            GROUP BY
                                table_name_hash
                            ) ddl_ver 
                        ON 
                            ddl_ver.table_name_hash = tmp_c.table_name_hash
                        WHERE
                            tmp_c.table_name_hash = 
                                ''' || v_table_name_hash || '''
                            AND 
                            ''' || v_table_hash || ''' 
                                not in 
                                (SELECT distinct 
                                    table_hash 
                                FROM 
                                    ddl_changes.ddl_changes_meta dcm);';
                           
                    EXECUTE v_add_ddl_meta;
           
                    --------------------------------------------------------------------
                    --Add the new scheamas colums to  ddl_changes.ddl_changes_columns---
                    --------------------------------------------------------------------
        
                    v_add_ddl_col := 
                        'INSERT INTO ddl_changes.ddl_changes_columns (
                            table_hash
                            ,column_name
                            ,column_ordinal_position
                            ,data_type
                            ,character_maximum_length
                            ,numeric_precision
                            ,numeric_scale
                        )
                        SELECT
                            ''' || v_table_hash || ''' AS table_hash
                            ,tmp_c.column_name as column_name
                            ,tmp_c.column_ordinal_position as column_ordinal_position
                            ,tmp_c.data_type as data_type
                            ,tmp_c.character_maximum_length as character_maximum_length
                            ,tmp_c.numeric_precision as numeric_precision
                            ,tmp_c.numeric_scale as numeric_scale
                        FROM
                            ' || v_tmp_ddl_change_colums || ' tmp_c';
                    
                    EXECUTE v_add_ddl_col;
    
                ELSE
                    -----------------------------------
                    ---Re-Activate the actual schema---
                    -----------------------------------
        
                    v_status_true := 
                        'UPDATE
                            ddl_changes.ddl_changes_meta dcm
                        SET
                            active = 
                                true
                            ,last_mod_time = 
                                ''' || v_change_time || ''' 
                        WHERE 
                            ''' || v_table_hash || ''' = dcm.table_hash;';
                        
                    EXECUTE v_status_true;
                    
                END IF;
    
                ---------------------------------------------------------------
                ---Log schema change to ddl_changes.ddl_changes_version_info---
                ---------------------------------------------------------------
       
                INSERT INTO 
                    ddl_changes.ddl_changes_version_info
                    (table_name_hash
                    ,ddl_version_from
                    ,ddl_version_to
                    ,change_time)
                VALUES
                    (v_table_name_hash
                    ,cast(v_last_ddl_version as integer)
                    ,cast(v_ddl_version as integer)
                    ,v_change_time);
          
                EXECUTE v_tmp_drop;

            -- END - Table hash exits check
            END IF;
          
        ----------------------------
        ---ddl type is drop table---
        ----------------------------
     
        ELSE     

            -------------------------------------
            ---Filtering out irrelevant alters---
            -------------------------------------

            CONTINUE WHEN v_last_ddl_version = 0;  

            ----------------------------
            ---De-Activate the schema---
            ----------------------------
  
            v_status_false := 
                'UPDATE
                    ddl_changes.ddl_changes_meta dcm
                SET
                    active = false
                    ,last_mod_time = ''' || v_change_time || ''' 
                WHERE 
                    dcm.table_name_hash = ''' || v_table_name_hash || '''
                    AND 
                    active = true;';
               
            EXECUTE v_status_false;

            ---------------------------------------------------------------
            ---Log schema change to ddl_changes.ddl_changes_version_info---
            ---------------------------------------------------------------

            INSERT INTO 
                ddl_changes.ddl_changes_version_info
                (table_name_hash
                ,ddl_version_from
                ,ddl_version_to
                ,change_time)
            VALUES
                (v_table_name_hash
                ,cast(v_last_ddl_version as integer)
                ,0
                ,v_change_time);
                     
        END IF;

    END LOOP;

END

$code$
LANGUAGE plpgsql;