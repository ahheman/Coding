DECLARE
    -- Input test values
    v_FIELD_TO_BE_DERIVED   VARCHAR2(100) := 'PRODUCT_FIELD_MAPPING';
    v_INTERFACE_FILE        VARCHAR2(100) := 'MAJESCO';
    v_EFFECTIVE_DATE        DATE := TO_DATE('01-JUL-2024', 'DD-MON-YYYY');

    -- Variables
    v_field_names   DBMS_SQL.VARCHAR2_TABLE;
    v_field_values  DBMS_SQL.VARCHAR2_TABLE;
    v_count         INTEGER := 0;
    v_val           VARCHAR2(4000);
    v_sql           CLOB;
    v_cursor        SYS_REFCURSOR;

    -- Output variables
    v_field_value_mapping VARCHAR2(4000);
    v_dynamic_val1 VARCHAR2(4000);
    v_dynamic_val2 VARCHAR2(4000);
    v_dynamic_val3 VARCHAR2(4000);
BEGIN
    DBMS_OUTPUT.PUT_LINE('🔍 Checking FIELD_MAP_X values in MAPPING_VALUE table...');

    -- Step 1: Get non-null FIELD_MAP_X columns from MAPPING_VALUE
    FOR col_rec IN (
        SELECT column_name
        FROM user_tab_columns
        WHERE table_name = 'MAPPING_VALUE'
          AND column_name LIKE 'FIELD_MAP_%'
        ORDER BY column_name
    )
    LOOP
        BEGIN
            -- Build dynamic SQL for each FIELD_MAP_X
            v_sql := '
                SELECT ' || col_rec.column_name || '
                FROM MAPPING_VALUE
                WHERE FIELD_TO_BE_DERIVED = :1
                  AND INTERFACE_FILE = :2
                  AND TRUNC(EFFECTIVE_DATE) = :3
                  AND ACTIVE = ''A''
                  AND ' || col_rec.column_name || ' IS NOT NULL
                  AND ROWNUM = 1';

            DBMS_OUTPUT.PUT_LINE('----------------------------------');
            DBMS_OUTPUT.PUT_LINE('Trying column: ' || col_rec.column_name);
            DBMS_OUTPUT.PUT_LINE('SQL: ' || v_sql);

            EXECUTE IMMEDIATE v_sql INTO v_val
            USING v_FIELD_TO_BE_DERIVED, v_INTERFACE_FILE, v_EFFECTIVE_DATE;

            IF v_val IS NOT NULL THEN
                v_count := v_count + 1;
                v_field_names(v_count) := col_rec.column_name;
                v_field_values(v_count) := v_val;
                DBMS_OUTPUT.PUT_LINE('✅ Found: ' || col_rec.column_name || ' = ' || v_val);
            ELSE
                DBMS_OUTPUT.PUT_LINE('⛔ No value in: ' || col_rec.column_name);
            END IF;

        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE('⛔ No data found for: ' || col_rec.column_name);
        END;
    END LOOP;

    IF v_count = 0 THEN
        DBMS_OUTPUT.PUT_LINE('⚠ No FIELD_MAP_X values found for input filters.');
        RETURN;
    END IF;

    -- Step 2: Query PRODUCT_FIELD_MAPPING
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM PRODUCT_FIELD_MAPPING 
        WHERE INTERFACE_FILE = :iface 
        AND TRUNC(EFFECTIVE_DATE) = :effdate 
        AND ACTIVE = ''A''';

    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ' AND ' || v_field_names(i) || ' = ''' || REPLACE(v_field_values(i), '''', '''''') || '''';
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔎 Executing SQL for PRODUCT_FIELD_MAPPING:');
    DBMS_OUTPUT.PUT_LINE(v_sql);

    OPEN v_cursor FOR v_sql USING v_INTERFACE_FILE, v_EFFECTIVE_DATE;
    LOOP
        FETCH v_cursor INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 PRODUCT_MAPPING => ' || v_field_value_mapping || ' | ' ||
                             v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor;

    -- Step 3: Query MCC_FIELD_MAPPING
    v_sql := 'SELECT FIELD_VALUE_MAPPING';
    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ', ' || v_field_names(i);
    END LOOP;

    v_sql := v_sql || ' FROM MCC_FIELD_MAPPING 
        WHERE INTERFACE_FILE = :iface 
        AND TRUNC(EFFECTIVE_DATE) = :effdate 
        AND ACTIVE = ''A''';

    FOR i IN 1 .. v_count LOOP
        v_sql := v_sql || ' AND ' || v_field_names(i) || ' = ''' || REPLACE(v_field_values(i), '''', '''''') || '''';
    END LOOP;

    DBMS_OUTPUT.PUT_LINE(CHR(10) || '🔎 Executing SQL for MCC_FIELD_MAPPING:');
    DBMS_OUTPUT.PUT_LINE(v_sql);

    OPEN v_cursor FOR v_sql USING v_INTERFACE_FILE, v_EFFECTIVE_DATE;
    LOOP
        FETCH v_cursor INTO v_field_value_mapping, v_dynamic_val1, v_dynamic_val2, v_dynamic_val3;
        EXIT WHEN v_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('📦 MCC_MAPPING => ' || v_field_value_mapping || ' | ' ||
                             v_dynamic_val1 || ' | ' || v_dynamic_val2 || ' | ' || v_dynamic_val3);
    END LOOP;
    CLOSE v_cursor;

END;
/
