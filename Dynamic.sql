-- Set up server output
SET SERVEROUTPUT ON;

DECLARE
    -- Input parameters
    v_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    v_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    v_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Arrays to hold dynamic columns and their values
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql           CLOB;
    v_cursor        SYS_REFCURSOR;

    -- Output placeholders (supporting up to 3 dynamic fields for now)
    v_field_value_mapping VARCHAR2(4000);
    v_dynamic_val1 VARCHAR2(4000);
    v_dynamic_val2 VARCHAR2(4000);
    v_dynamic_val3 VARCHAR2(4000);
BEGIN
    -- Step 1: Fetch FIELD_MAP_X columns from STAGING.MAPPING_VALUE dynamically
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
            v_sql := 'SELECT ' || col_rec.column_name ||
                     ' FROM STAGING.MAPPING_VALUE
                      WHERE FIELD_TO_BE_DERIVED = :1
                        AND INTERFACE_FILE = :2
                        AND TRUNC(EFFECTIVE_DATE) = :3
                        AND ACTIVE = ''A''
                        AND ' || col_rec.column_name || ' IS NOT NULL
                        AND ROWNUM = 1';

            EXECUTE IMMEDIATE v_sql INTO v_val
            USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
                DBMS_OUTPUT.PUT_LINE('✔ FIELD FOUND: ' || col_rec.column_name || ' = ' || v_val);
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN NULL;
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('🔥 Error for ' || col_rec.column_name || ': ' || SQLERRM);
        END;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No FIELD_MAP_X values found for input filters.');
        RETURN;
    END IF;

    -- Step 2: Build dynamic SQL for STAGING.PRODUCT_FIELD_MAPPING
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.PRODUCT_FIELD_MAPPING
                        WHERE INTERFACE_FILE = :iface
                          AND TRUNC(EFFECTIVE_DATE) = :effdate
                          AND ACTIVE = ''A''';

    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ' AND ' || v_field_names(i) || ' = :val' || i;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔎 PRODUCT_FIELD_MAPPING SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql);

    OPEN v_cursor FOR v_sql USING v_INTERFACE_FILE, v_EFFECTIVE_DATE,
        v_field_values(1), v_field_values(2), v_field_values(3);
    LOOP
        FETCH v_cursor INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 PRODUCT_MAPPING => ' || v_field_value_mapping || ' | ' || v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor;

    -- Step 3: Build dynamic SQL for STAGING.MCC_FIELD_MAPPING
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM STAGING.MCC_FIELD_MAPPING
                        WHERE INTERFACE_FILE = :iface
                          AND TRUNC(EFFECTIVE_DATE) = :effdate
                          AND ACTIVE = ''A''';

    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ' AND ' || v_field_names(i) || ' = :val' || i;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔎 MCC_FIELD_MAPPING SQL:');
    DBMS_OUTPUT.PUT_LINE(v_sql);

    OPEN v_cursor FOR v_sql USING v_INTERFACE_FILE, v_EFFECTIVE_DATE,
        v_field_values(1), v_field_values(2), v_field_values(3);
    LOOP
        FETCH v_cursor INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 MCC_MAPPING => ' || v_field_value_mapping || ' | ' || v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor;
END;
/
