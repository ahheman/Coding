CREATE OR REPLACE PROCEDURE STAGING.dynamic_mapping_value_result (
    p_field_to_be_derived   IN  VARCHAR2,
    p_interface_file        IN  VARCHAR2,
    p_effective_date        IN  DATE,
    o_result_rows           OUT STAGING.mapping_table_type
)
AS
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_sql           VARCHAR2(32767);
    v_cursor_id     INTEGER;
    v_column_value  VARCHAR2(4000);
    v_fvm           VARCHAR2(4000);
    col_val         VARCHAR2(4000);
    v_status        INTEGER;
    v_col_cnt       INTEGER;
BEGIN
    o_result_rows := STAGING.mapping_table_type();  -- initialize output

    -- Step 1: Identify relevant field_map columns with non-null values
    FOR col_rec IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'MAPPING_VALUE'
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE '
                SELECT ' || col_rec.column_name || '
                  FROM STAGING.MAPPING_VALUE
                 WHERE FIELD_TO_BE_DERIVED = :1
                   AND INTERFACE_FILE = :2
                   AND TRUNC(EFFECTIVE_DATE) = :3
                   AND ACTIVE = ''A''
                   AND ' || col_rec.column_name || ' IS NOT NULL
                   AND ROWNUM = 1'
            INTO col_val
            USING p_field_to_be_derived, p_interface_file, p_effective_date;

            IF col_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    IF v_count = 0 THEN
        RETURN;
    END IF;

    -- Step 2: Construct dynamic SQL
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.' ||
        CASE UPPER(p_field_to_be_derived)
            WHEN 'PRODUCT_FIELD_MAPPING' THEN 'PRODUCT_FIELD_MAPPING'
            WHEN 'MCC_FIELD_MAPPING' THEN 'MCC_FIELD_MAPPING'
        END ||
        ' WHERE INTERFACE_FILE = :iface AND TRUNC(EFFECTIVE_DATE) = :effdate AND ACTIVE = ''A''';

    -- Step 3: Use DBMS_SQL for dynamic column handling
    v_cursor_id := DBMS_SQL.OPEN_CURSOR;

    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);

    -- Bind variables
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':iface', p_interface_file);
    DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':effdate', p_effective_date);

    -- Define Columns
    DBMS_SQL.DEFINE_COLUMN(v_cursor_id, 1, v_fvm, 4000);
    FOR i IN 1 .. v_count LOOP
        DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i + 1, v_column_value, 4000);
    END LOOP;

    v_status := DBMS_SQL.EXECUTE(v_cursor_id);

    WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
        DECLARE
            temp_list SYS.ODCIVARCHAR2LIST := SYS.ODCIVARCHAR2LIST();
        BEGIN
            DBMS_SQL.COLUMN_VALUE(v_cursor_id, 1, v_fvm);
            FOR j IN 1 .. v_count LOOP
                DBMS_SQL.COLUMN_VALUE(v_cursor_id, j + 1, v_column_value);
                temp_list.EXTEND;
                temp_list(j) := v_column_value;
            END LOOP;

            o_result_rows.EXTEND;
            o_result_rows(o_result_rows.COUNT) := STAGING.mapping_row_type(v_fvm, temp_list);
        END;
    END LOOP;

    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
END;
/
