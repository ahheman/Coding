SET SERVEROUTPUT ON;

DECLARE
    -- Input Parameters
    p_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    p_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    p_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Field holders
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql_product   CLOB;
    v_sql_mcc       CLOB;

    -- Cursors
    v_cursor_product SYS_REFCURSOR;
    v_cursor_mcc     SYS_REFCURSOR;

    -- Output fields
    v_field_value_mapping VARCHAR2(4000);
    v_dynamic_val1        VARCHAR2(4000);
    v_dynamic_val2        VARCHAR2(4000);
    v_dynamic_val3        VARCHAR2(4000);

BEGIN
    -- Step 1: Dynamically fetch which FIELD_MAP_X columns have values
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
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('Error on ' || col_rec.column_name || ': ' || SQLERRM);
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
        WHERE INTERFACE_FILE = :1
        AND TRUNC(EFFECTIVE_DATE) = :2
        AND ACTIVE = ''A''';

    -- Step 3: Build dynamic SQL for MCC_FIELD_MAPPING
    v_sql_mcc := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql_mcc := v_sql_mcc || ', ' || v_field_names(i);
    END LOOP;

    v_sql_mcc := v_sql_mcc || '
        FROM STAGING.MCC_FIELD_MAPPING
        WHERE INTERFACE_FILE = :1
        AND TRUNC(EFFECTIVE_DATE) = :2
        AND ACTIVE = ''A''';

    -- Print SQL
    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔍 PRODUCT SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_product);

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔍 MCC SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql_mcc);

    -- Execute PRODUCT cursor
    OPEN v_cursor_product FOR v_sql_product USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    LOOP
        FETCH v_cursor_product INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor_product%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 PRODUCT => ' || v_field_value_mapping || ' | ' ||
                             v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor_product;

    -- Execute MCC cursor
    OPEN v_cursor_mcc FOR v_sql_mcc USING p_INTERFACE_FILE, p_EFFECTIVE_DATE;
    LOOP
        FETCH v_cursor_mcc INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor_mcc%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 MCC => ' || v_field_value_mapping || ' | ' ||
                             v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor_mcc;

END;
/
