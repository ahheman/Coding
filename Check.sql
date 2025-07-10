SET SERVEROUTPUT ON;
DECLARE
    v_field_names    DBMS_SQL.VARCHAR2_TABLE;
    v_field_values   DBMS_SQL.VARCHAR2_TABLE;
    v_count          INTEGER := 0;
    v_val            VARCHAR2(4000);
    v_sql_product    VARCHAR2(32000);
    v_sql_mcc        VARCHAR2(32000);
    v_field_to_be_derived VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    v_interface_file      VARCHAR2(100) := 'MAJESCO';
    v_effective_date      DATE := TO_DATE('01-JUL-2024','DD-MON-YYYY');

    -- DBMS_SQL variables
    v_cursor       INTEGER;
    col_val        VARCHAR2(4000);
    col_num        NUMBER;

BEGIN
    -- Step 1: Get FIELD_MAP_X columns with values
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
            USING v_field_to_be_derived, v_interface_file, v_effective_date;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
                DBMS_OUTPUT.PUT_LINE('✔ Found: ' || col_rec.column_name || ' = ' || v_val);
            END IF;
        EXCEPTION
            WHEN OTHERS THEN NULL;
        END;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No FIELD_MAP_X values found.');
        RETURN;
    END IF;

    -- Step 2: Build dynamic SQL for PRODUCT_FIELD_MAPPING
    v_sql_product := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_product := v_sql_product || ', ' || v_field_names(i);
    END LOOP;

    v_sql_product := v_sql_product || '
        FROM STAGING.PRODUCT_FIELD_MAPPING
        WHERE INTERFACE_FILE = ''' || v_interface_file || '''
          AND TRUNC(EFFECTIVE_DATE) = TO_DATE(''' || TO_CHAR(v_effective_date, 'DD-MON-YYYY') || ''')
          AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '▶ PRODUCT SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_product);

    -- Execute PRODUCT query
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor, v_sql_product, DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, col_val, 4000);
    FOR i IN 1 .. v_count LOOP
        DBMS_SQL.DEFINE_COLUMN(v_cursor, i + 1, col_val, 4000);
    END LOOP;
    col_num := DBMS_SQL.EXECUTE(v_cursor);

    DBMS_OUTPUT.PUT_LINE('📦 PRODUCT_FIELD_MAPPING Result:');
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        FOR j IN 1 .. v_count + 1 LOOP
            DBMS_SQL.COLUMN_VALUE(v_cursor, j, col_val);
            DBMS_OUTPUT.PUT(CHR(9) || col_val);
        END LOOP;
        DBMS_OUTPUT.NEW_LINE;
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cursor);

    -- Step 3: Build dynamic SQL for MCC_FIELD_MAPPING
    v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
    END LOOP;

    v_sql_mcc := v_sql_mcc || '
        FROM STAGING.MCC_FIELD_MAPPING
        WHERE INTERFACE_FILE = ''' || v_interface_file || '''
          AND TRUNC(EFFECTIVE_DATE) = TO_DATE(''' || TO_CHAR(v_effective_date, 'DD-MON-YYYY') || ''')
          AND ACTIVE = ''A''';

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '▶ MCC SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_mcc);

    -- Execute MCC query
    v_cursor := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(v_cursor, v_sql_mcc, DBMS_SQL.NATIVE);
    DBMS_SQL.DEFINE_COLUMN(v_cursor, 1, col_val, 4000);
    FOR i IN 1 .. v_count LOOP
        DBMS_SQL.DEFINE_COLUMN(v_cursor, i + 1, col_val, 4000);
    END LOOP;
    col_num := DBMS_SQL.EXECUTE(v_cursor);

    DBMS_OUTPUT.PUT_LINE('📦 MCC_FIELD_MAPPING Result:');
    WHILE DBMS_SQL.FETCH_ROWS(v_cursor) > 0 LOOP
        FOR j IN 1 .. v_count + 1 LOOP
            DBMS_SQL.COLUMN_VALUE(v_cursor, j, col_val);
            DBMS_OUTPUT.PUT(CHR(9) || col_val);
        END LOOP;
        DBMS_OUTPUT.NEW_LINE;
    END LOOP;
    DBMS_SQL.CLOSE_CURSOR(v_cursor);

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '✅ Finished processing both PRODUCT and MCC mappings.');
END;
/
