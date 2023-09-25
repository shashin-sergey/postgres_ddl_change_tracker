DO $$ 
DECLARE
    v_change_time timestamp;
    
    v_tmp_create_dcc text;
    v_tmp_ddl_change_colums text;
    v_tmp_drop_dcc text;
    
    v_tmp_create_th text;
    v_tmp_ddl_change_th text;
    v_tmp_drop_th text;
   
   v_ddl_changes_meta_load text;
   v_ddl_changes_columns_load text;
   v_ddl_changes_version_info_load text;
    
BEGIN 
        
    -------------------
    ---Add variables---
    -------------------
    --v_change_time

    v_change_time := 
        (NOW() AT TIME ZONE 'UTC'::TEXT);      
       
     -------------------------------------------------------
     ---Make a snapshot from information_schema."columns"---
     -------------------------------------------------------

    v_tmp_ddl_change_colums := 
       'ddl_change_tmp_colums' || md5(random()::text);
    
    v_tmp_drop_dcc := 
       'DROP TABLE IF EXISTS ' || v_tmp_ddl_change_colums;
    
    EXECUTE 
       v_tmp_drop_dcc;
    
    v_tmp_create_dcc := 
        'CREATE TEMP TABLE ' 
           || v_tmp_ddl_change_colums || ' 
        AS 
        SELECT 
            md5(
                lower(
                    concat_ws(
                        ''.''
                        ,is_c.table_catalog
                        ,is_c.table_schema
                        ,is_c.table_name
                    )
                )
            ) as table_name_hash
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
            ,cast(
                is_c.ordinal_position as integer
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
            is_c.table_schema not like ''pg_temp%''
        ORDER BY
            is_c.table_catalog
            ,is_c.table_schema
            ,is_c.table_name
            ,is_c.ordinal_position;';
            
    --RAISE NOTICE 'v_tmp_create_dcc %', v_tmp_create_dcc;
        
    EXECUTE 
        v_tmp_create_dcc;

    -----------------------
    ---Create table_hash---
    -----------------------
    
    v_tmp_ddl_change_th := 
       'ddl_change_th' || md5(random()::text);
    
    v_tmp_drop_th := 
        'DROP TABLE IF EXISTS ' || v_tmp_ddl_change_th;
    
    EXECUTE 
        v_tmp_drop_th;            
  
  
    v_tmp_create_th := 
        'CREATE TEMP TABLE ' 
            || v_tmp_ddl_change_th || ' 
        AS 
        SELECT 
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
            ,table_name_hash
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
            GROUP BY 
                table_name_hash;';
    
    --RAISE NOTICE 'v_tmp_create_th %', v_tmp_create_th;
            
    EXECUTE 
         v_tmp_create_th;

    v_ddl_changes_meta_load := 
        'INSERT INTO ddl_changes.ddl_changes_meta
        (
            ddl_version
            ,db_name
            ,schema_name
            ,table_name
            ,table_name_hash
            ,table_hash
            ,active
            ,create_time
            ,last_mod_time
        )
        SELECT DISTINCT
            1
            ,tmp_c.db_name
            ,tmp_c.schema_name
            ,tmp_c.table_name
            ,tmp_c.table_name_hash
            ,th.table_hash
            ,cast(true as boolean)
            ,cast(''' || v_change_time || ''' as timestamp)
            ,cast(''' || v_change_time || ''' as timestamp)
        FROM
            ' || v_tmp_ddl_change_colums || ' tmp_c
        INNER JOIN 
            ' || v_tmp_ddl_change_th || ' th
        ON 
            th.table_name_hash = tmp_c.table_name_hash
        WHERE 
            th.table_hash not in 
                (SELECT distinct 
                    table_hash 
                FROM 
                ddl_changes.ddl_changes_meta);';
                
    --RAISE NOTICE 'v_ddl_changes_meta_load %', v_ddl_changes_meta_load;
    
    EXECUTE 
        v_ddl_changes_meta_load;    
        
    v_ddl_changes_columns_load :=    
        'INSERT INTO ddl_changes.ddl_changes_columns
        (
            table_hash
            ,column_name
            ,column_ordinal_position
            ,data_type
            ,character_maximum_length
            ,numeric_precision
            ,numeric_scale            
        )
        SELECT DISTINCT
            th.table_hash
            ,tmp_c.column_name
            ,tmp_c.column_ordinal_position
            ,tmp_c.data_type
            ,tmp_c.character_maximum_length
            ,tmp_c.numeric_precision
            ,tmp_c.numeric_scale    
        FROM
            ' || v_tmp_ddl_change_colums || ' tmp_c
        INNER JOIN 
            ' || v_tmp_ddl_change_th || ' th
        ON 
            th.table_name_hash = tmp_c.table_name_hash
        WHERE th.table_hash not in 
                (SELECT distinct 
                    table_hash 
                FROM ddl_changes.ddl_changes_columns);';
                
    --RAISE NOTICE 'v_ddl_changes_columns_load %', v_ddl_changes_columns_load;
    
    EXECUTE 
        v_ddl_changes_columns_load;
        
    v_ddl_changes_version_info_load := '
    INSERT INTO ddl_changes.ddl_changes_version_info
    (
        table_name_hash
        ,ddl_version_from
        ,ddl_version_to
        ,change_time
    )
    SELECT DISTINCT
        tmp_c.table_name_hash
        ,0
        ,1
        ,cast(''' || v_change_time || ''' as timestamp)
    FROM
        ' || v_tmp_ddl_change_colums || ' tmp_c
    WHERE 
        tmp_c.table_name_hash not in 
            (SELECT distinct 
                table_name_hash 
            FROM ddl_changes.ddl_changes_version_info);';
            
    --RAISE NOTICE 'v_ddl_changes_version_info_load %', v_ddl_changes_version_info_load;
    
    EXECUTE 
        v_ddl_changes_version_info_load;
    
    EXECUTE 
        v_tmp_drop_dcc;
            
    EXECUTE 
        v_tmp_drop_th;
        
    --RAISE NOTICE 'v_ddl_changes_version_info_load %', v_ddl_changes_version_info_load;
    
    RAISE INFO 'JOB DONE';
    
END $$;
