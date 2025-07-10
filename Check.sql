SET SERVEROUTPUT ON;

DECLARE
    -- Input parameters
    v_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'MCC_FIELD_MAPPING'; -- Or 'PRODUCT_FIELD_MAPPING'
    v_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    v_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Field tracking
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql           VARCHAR2(32767);

    -- DBMS_SQL handles
    v_cursor_id     INTEGER;
    col_val         VARCHAR2(4000);
    col_cnt         INTEGER;
    desc_tab        DBMS_SQL.DESC_TAB;
BEGIN
    -- Step 1: Find which FIELD_MAP_X columns have non-null values
    FOR col_rec IN (
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = 'STAGING'
          AND table_name = 'MAPPING_VALUE'
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    )
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'SELECT ' || col_rec.column_name || '
                               FROM STAGING.MAPPING_VALUE
                              WHERE FIELD_TO_BE_DERIVED = :1
                                AND INTERFACE_FILE = :2
                                AND TRUNC(EFFECTIVE_DATE) = :3
                                AND ACTIVE = ''A''
                                AND ' || col_rec.column_name || ' IS NOT NULL
                                AND ROWNUM = 1'
                INTO v_val
                USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    -- Step 2: Exit if no columns found
    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No field_map_x values found.');
        RETURN;
    END IF;

    -- Step 3: Build dynamic SQL
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    IF UPPER(v_FIELD_TO_BE_DERIVED) = 'PRODUCT_FIELD_MAPPING' THEN
        v_sql := v_sql || ' FROM STAGING.PRODUCT_FIELD_MAPPING WHERE ';
    ELSIF UPPER(v_FIELD_TO_BE_DERIVED) = 'MCC_FIELD_MAPPING' THEN
        v_sql := v_sql || ' FROM STAGING.MCC_FIELD_MAPPING WHERE ';
    ELSE
        DBMS_OUTPUT.PUT_LINE('❌ Unsupported FIELD_TO_BE_DERIVED: ' || v_FIELD_TO_BE_DERIVED);
        RETURN;
    END IF;

    v_sql := v_sql || ' INTERFACE_FILE = ''' || v_INTERFACE_FILE || '''
                       AND TRUNC(EFFECTIVE_DATE) = TO_DATE(''' || TO_CHAR(v_EFFECTIVE_DATE, 'DD-MON-YYYY') || ''', ''DD-MON-YYYY'')
                       AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE('✅ Final SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql);

    -- Step 4: Use DBMS_SQL to execute dynamically and display result
    v_cursor_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor_id, v_sql, DBMS_SQL.NATIVE);

    col_cnt := 1 + v_count; -- FIELD_VALUE_MAPPING + FIELD_MAP_X
    FOR i IN 1 .. col_cnt LOOP
        DBMS_SQL.DEFINE_COLUMN(v_cursor_id, i, col_val, 4000);
    END LOOP;

    -- Execute
    IF DBMS_SQL.EXECUTE(v_cursor_id) > 0 THEN
        NULL;
    END IF;

    -- Fetch results
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor_id) > 0 LOOP
        DBMS_OUTPUT.PUT_LINE('🔽 Result Row:');
        FOR i IN 1 .. col_cnt LOOP
            DBMS_SQL.COLUMN_VALUE(v_cursor_id, i, col_val);
            DBMS_OUTPUT.PUT_LINE('Col ' || i || ': ' || col_val);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('------------------------');
    END LOOP;

    DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(v_cursor_id) THEN
            DBMS_SQL.CLOSE_CURSOR(v_cursor_id);
        END IF;
        DBMS_OUTPUT.PUT_LINE('💥 Error: ' || SQLERRM);
END;
/
