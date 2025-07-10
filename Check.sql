SET SERVEROUTPUT ON;

DECLARE
    -- Inputs
    p_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    p_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    p_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Working variables
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql           VARCHAR2(32000);
    v_cursor_id     INTEGER;
    col_cnt         INTEGER;
    col_desc        DBMS_SQL.DESC_TAB;
    col_value       VARCHAR2(4000);

    -- Procedure for generic printing
    PROCEDURE execute_dynamic_sql(p_sql IN VARCHAR2, p_title IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE(CHR(10) || '📌 ' || p_title || ' SQL:');
        DBMS_OUTPUT.PUT_LINE(p_sql);

        v_cursor_id := DBMS_SQL.OPEN_CURSOR;
        DBMS_SQL.PARSE(v_cursor_id, p_sql, DBMS_SQL.NATIVE);

        -- Bind interface file and date
        DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':1', p_INTERFACE_FILE);
        DBMS_SQL.BIND_VARIABLE(v_cursor_id, ':2', p_EFFECTIVE_DATE);

        -- Describe columns
        DBMS_SQL.DESCRIBE_COLUMNS(v_cursor_id, col_cnt, col_desc);

        FOR i IN 1 .. col_cnt LOOP
            DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i, col_value, 4000);
        END LOOP;

        -- Execute and fetch
        IF DBMS_SQL.EXECUTE(v_cursor_id) > 0 THEN
            WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
                FOR i IN 1 .. col_cnt LOOP
                    DBMS_SQL.COLUMN_VALUE(v_cursor_id, i, col_value);
                    DBMS_OUTPUT.PUT(col_desc(i).col_name || ': ' || col_value || ' | ');
                END LOOP;
                DBMS_OUTPUT.PUT_LINE('');
            END LOOP;
        ELSE
            DBMS_OUTPUT.PUT_LINE('❗ No rows returned.');
        END IF;

        DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
    END;
BEGIN
    -- Step 1: Identify non-null fields
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
            INTO v_val
            USING p_FIELD_TO_BE_DERIVED, p_INTERFACE_FILE, p_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
                DBMS_OUTPUT.PUT_LINE('✔ ' || col_rec.column_name || ' = ' || v_val);
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('⚠ Error on ' || col_rec.column_name || ': ' || SQLERRM);
        END;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No FIELD_MAP_X fields found.');
        RETURN;
    END IF;

    -- Step 2: Build dynamic SQL
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.PRODUCT_FIELD_MAPPING
                       WHERE INTERFACE_FILE = :1
                         AND TRUNC(EFFECTIVE_DATE) = :2
                         AND ACTIVE = ''A''';

    -- Execute and display
    execute_dynamic_sql(v_sql, 'PRODUCT');

    -- Repeat for MCC
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.MCC_FIELD_MAPPING
                       WHERE INTERFACE_FILE = :1
                         AND TRUNC(EFFECTIVE_DATE) = :2
                         AND ACTIVE = ''A''';

    execute_dynamic_sql(v_sql, 'MCC');
END;
/
